"""Genera un archivio locale degli export di exportChatGPTFullPagination."""

import argparse
import asyncio
from base64 import b85decode
import codecs
from collections import Counter, defaultdict
from contextlib import aclosing
from dataclasses import dataclass
from difflib import SequenceMatcher
from gzip import decompress
from html import escape
import json
from mimetypes import MimeTypes
from pathlib import Path
import re
from unicodedata import normalize
from urllib.parse import quote, unquote

CHUNK_SIZE = 256 * 1024
SEARCH_SHARD_SIZE = 256 * 1024
PREVIEW_LIMIT = 64 * 1024
TEXT_PREVIEW = {".txt", ".md", ".csv", ".tsv", ".json", ".xml", ".log", ".yaml", ".yml", ".html", ".svg"}
IMAGE_SUFFIXES = {".png", ".jpg", ".jpeg", ".gif", ".webp", ".avif", ".bmp"}
FILE_SUFFIX = re.compile(r"\.[a-zA-Z0-9]{1,12}")
MIME_TYPES = MimeTypes()
ZIP_SUFFIXES = {".docx", ".docm", ".xlsx", ".xlsm", ".pptx", ".pptm", ".odt", ".ods", ".odp", ".epub", ".jar"}
MIME_SUFFIXES = {
    "image/jpeg": ".jpg", "audio/mpeg": ".mp3", "audio/wav": ".wav",
    "application/pdf": ".pdf", "application/json": ".json", "text/plain": ".txt",
}
SITE_FOLDER = "archivio-chat"
BATCH_NAME = re.compile(r"chatgpt_business_export_COMPLETO_\d{4}-\d{2}-\d{2}\.json$")
NUMBERED_NAME = re.compile(r"^(\d{4,})_(.+)$")
UUID = re.compile(r"[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}$")
FILE_ID = re.compile(r"(?<![a-z0-9])file[_-]([a-z0-9][a-z0-9_-]*)", re.ASCII | re.IGNORECASE)
BARE_FILE_ID = re.compile(r"[a-zA-Z0-9][a-zA-Z0-9_-]{19,}")
BROWSER_COPY = re.compile(r" \(\d+\)(?=(?:\.[^./ ]+)?$)")
GENERATED_FOLDER = re.compile(r"archivio-chat(?:-\d+)?$")
SANDBOX_LINK = re.compile(r"""sandbox:(/mnt/data/[^\s)"\]'<>]+)""")
CONTROL = re.compile(r"[\x00-\x1f\x7f-\x9f]")


class Terminal:
    def __init__(self, color=True):
        self.color = color

    def line(self, label, message, code):
        label = f"\x1b[{code}m{label}\x1b[0m" if self.color else label
        print(f"{label}  {CONTROL.sub(' ', str(message))}", flush=True)

    def info(self, message):
        self.line(" INFO ", message, "36")

    def warning(self, message):
        self.line(" AVVISO ", message, "33")

    def error(self, message):
        self.line(" ERRORE ", message, "31")

    def done(self, message):
        self.line(" OK ", message, "32")


class LocalFolder:
    def __init__(self, path):
        self.path = Path(path)
        self.name = str(self.path)

    async def entries(self):
        return {
            entry.name: "directory" if entry.is_dir() else "file"
            for entry in self.path.iterdir()
            if not entry.is_symlink() and (entry.is_file() or entry.is_dir())
        }

    async def read(self, name):
        with (self.path / name).open("rb") as stream:
            while chunk := stream.read(CHUNK_SIZE):
                yield chunk
                await asyncio.sleep(0)

    async def write(self, name, chunks):
        path = self.path / name
        stream = path.open("xb")
        try:
            with stream:
                async for chunk in chunks:
                    stream.write(chunk)
        except BaseException:
            try:
                path.unlink()
            except OSError:
                pass
            raise

    async def mkdir(self, name):
        path = self.path / name
        path.mkdir()
        return LocalFolder(path)

    async def subfolder(self, name):
        return LocalFolder(self.path / name)


class BrowserFolder:
    """Accede agli handle del browser con al massimo un blocco per lettura."""

    def __init__(self, handle):
        self.handle = handle
        self.name = str(handle.name)

    async def entries(self):
        entries = {}
        iterator = self.handle.entries()
        while True:
            step = await iterator.next()
            if step.done:
                return entries
            name, handle = step.value
            entries[str(name)] = str(handle.kind)

    async def read(self, name):
        handle = await self.handle.getFileHandle(name)
        file = await handle.getFile()
        for start in range(0, file.size, CHUNK_SIZE):
            chunk = file.slice(start, min(start + CHUNK_SIZE, file.size))
            yield bytes((await chunk.arrayBuffer()).to_py())

    async def write(self, name, chunks):
        from js import Object
        from pyodide.ffi import to_js

        create = Object.new()
        create.create = True
        handle = await self.handle.getFileHandle(name, create)
        writer = await handle.createWritable()
        try:
            async for chunk in chunks:
                await writer.write(to_js(memoryview(chunk)))
            await writer.close()
        except BaseException:
            try:
                await writer.abort()
                await self.handle.removeEntry(name)
            except Exception:
                pass
            raise

    async def mkdir(self, name):
        from js import Object

        create = Object.new()
        create.create = True
        return BrowserFolder(await self.handle.getDirectoryHandle(name, create))

    async def subfolder(self, name):
        return BrowserFolder(await self.handle.getDirectoryHandle(name))


async def write_text(folder, name, content):
    async def encoded():
        for start in range(0, len(content), CHUNK_SIZE):
            yield content[start:start + CHUNK_SIZE].encode("utf-8", "backslashreplace")
            await asyncio.sleep(0)

    await folder.write(name, encoded())


def script_data(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False)


async def write_data(folder, name, callback, value):
    await write_text(folder, name, f"window.{callback}({script_data(value)});\n")


async def read_individual(folder, name, canonical):
    body = bytearray()
    async with aclosing(folder.read(name)) as chunks:
        async for chunk in chunks:
            if not body and not canonical and b'"mapping"' not in chunk:
                return {}
            body.extend(chunk)
    return json.loads(body)


async def batch_records(folder, name):
    """Legge l'array cumulativo un record alla volta."""
    decoder = json.JSONDecoder()
    utf8 = codecs.getincrementaldecoder("utf-8-sig")()
    chunks = folder.read(name).__aiter__()
    buffer = ""
    eof = False

    async def refill():
        nonlocal buffer, eof
        chunk = await anext(chunks, None)
        eof = chunk is None
        buffer += utf8.decode(chunk if chunk is not None else b"", final=eof)

    async def next_token():
        nonlocal buffer
        while True:
            buffer = buffer.lstrip(" \t\r\n")
            if buffer or eof:
                return buffer[:1]
            await refill()

    try:
        if await next_token() != "[":
            raise ValueError("atteso un array JSON")
        buffer = buffer[1:]
        if await next_token() != "]":
            while True:
                await next_token()
                while True:
                    try:
                        record, end = decoder.raw_decode(buffer)
                    except json.JSONDecodeError:
                        if eof:
                            raise
                        target = max(len(buffer) * 2, CHUNK_SIZE)
                        while len(buffer) < target and not eof:
                            await refill()
                    else:
                        break
                buffer = buffer[end:]
                yield record
                if await next_token() != ",":
                    break
                buffer = buffer[1:]
        if await next_token() != "]":
            raise ValueError("array JSON incompleto")
        buffer = buffer[1:]
        if await next_token():
            raise ValueError("dati inattesi dopo l'array JSON")
    finally:
        await chunks.aclose()


def obj(value):
    return value if isinstance(value, dict) else {}


def array(value):
    return value if isinstance(value, list) else []


def string(value):
    return value if isinstance(value, str) else ""


def timestamp(value):
    return value if type(value) in (int, float) and abs(value) < 8.64e12 else None


def last_update(summary, raw):
    for field in ("update_time", "create_time"):
        for record in (summary, raw):
            date = timestamp(record.get(field))
            if date is not None:
                return date
    return None


def sanitize(value):
    value = value or "untitled"
    # La regex dell'exporter opera sulle unità UTF-16 di JavaScript.
    value = re.sub(r"[\U00010000-\U0010ffff]", "__", value)
    return re.sub(r"[^a-zA-Z0-9_.-]", "_", value)[:60]


def sandbox_path(url):
    if isinstance(url, str) and url.startswith("sandbox:/mnt/data/"):
        return unquote(url.removeprefix("sandbox:"))
    return ""


def file_id(value, allow_bare=False):
    value = unquote(string(value)).strip().split("?", 1)[0].split("#", 1)[0]
    _, _, target = value.rpartition("://")
    for part in reversed(target.strip("/").split("/")):
        if match := FILE_ID.fullmatch(part):
            return match[0]
        if allow_bare and not part.startswith("user-") and BARE_FILE_ID.fullmatch(part):
            return f"file_{part}"
    return ""


def file_key(identifier):
    return "file_" + identifier[5:]


def add_file_ref(refs, data, identifier, image=False):
    title = next(
        (data[field] for field in ("name", "filename", "file_name", "fileName") if string(data.get(field))),
        "",
    )
    source_id = string(data.get("id") or data.get("file_id"))
    if not identifier and not title and not source_id:
        return
    if identifier:
        key = file_key(identifier)
    elif source_id:
        key = f"riferimento:{source_id}"
    else:
        key = f"nome:{title}"
    previous = refs.get(key, {})
    title = title or (previous["name"] if previous and not previous.get("generic") else "")
    mime = string(data.get("mime_type") or data.get("mimeType") or previous.get("mime"))
    if not mime and data.get("content_type") == "audio_asset_pointer" and data.get("format") == "wav":
        mime = "audio/wav"
    image = image or previous.get("image", False) or mime.startswith("image/")
    label = "Immagine" if image else "Audio" if data.get("content_type") == "audio_asset_pointer" else "Allegato"
    refs[key] = {
        "key": key, "id": identifier, "source_id": source_id,
        "name": title or label,
        "image": image, "generic": not title, "mime": mime,
    }
    return key


def path_nodes(raw):
    mapping = obj(raw.get("mapping"))
    current = raw.get("current_node")
    if not isinstance(current, str) or current not in mapping:
        candidates = []
        for position, (key, node) in enumerate(mapping.items()):
            message = obj(obj(node).get("message"))
            if visible(message):
                date = timestamp(message.get("create_time"))
                candidates.append((date if date is not None else float("-inf"), position, key))
        if not candidates:
            return []
        current = max(candidates)[2]
    path = []
    visited = set()
    while isinstance(current, str) and current in mapping and current not in visited:
        visited.add(current)
        node = obj(mapping[current])
        message = obj(node.get("message"))
        if visible(message):
            path.append(message)
        current = node.get("parent")
    return list(reversed(path))


def visible(message):
    role = obj(message.get("author")).get("role")
    return (
        role in ("user", "assistant")
        and not obj(message.get("metadata")).get("is_visually_hidden_from_conversation")
        and (role == "user" or message.get("recipient") in (None, "", "all"))
        and obj(message.get("content")).get("content_type") not in (
            "thoughts", "reasoning_recap", "user_editable_context", "model_editable_context",
        )
    )


def source_links(entries):
    links = {}
    for entry in entries:
        entry = obj(entry)
        url = next((url for field in ("url", "content_url", "original_content_url")
                    if (url := string(entry.get(field))).startswith(("http://", "https://"))), "")
        if url:
            links[url] = string(entry.get("title") or entry.get("name")) or links.get(url, "")
    return [{"url": url, "title": title} for url, title in links.items()]


def message_sources(metadata, refs):
    sources = {}
    for value in array(metadata.get("content_references")):
        reference = obj(value)
        marker = string(reference.get("matched_text"))
        if not marker.startswith("\ue200") or not marker.endswith("\ue201"):
            continue
        kind = reference.get("type")
        if kind == "hidden":
            continue
        items = [reference, reference.get("item"), *array(reference.get("items"))]
        items.extend(obj(image).get("image_result") for image in array(reference.get("images")))
        items.extend({"url": url} for url in array(reference.get("safe_urls")))
        previous = sources.get(marker, {})
        links = source_links([*previous.get("links", ()), *items])
        alt = string(reference.get("alt"))
        key = ""
        if kind == "file":
            asset = next((item for name in ("id", "input_pointer", "library_file_id")
                          if (item := file_id(reference.get(name), allow_bare=True))), "")
            if asset:
                key = add_file_ref(refs, reference, asset)
            alt = ""
        data = obj(reference.get("data"))
        if data.get("widget_type") == "map_widget":
            places = [
                " — ".join(filter(None, (string(point.get("name")), string(point.get("address")))))
                for item in array(data.get("points")) if (point := obj(item))
            ]
            alt = " · ".join(filter(None, [string(data.get("answer")), *places]))
        if links or alt or key:
            sources[marker] = {
                "marker": marker, "links": links, "alt": alt or previous.get("alt", ""),
                "ref": key or previous.get("ref", ""),
                "show_alt": bool(previous.get("show_alt") or kind in {"forecast", "stock"}
                                 or data.get("widget_type") == "map_widget"),
            }
    return list(sources.values())


def embedded_results(generated):
    components = []
    for item in obj(generated.get("componentResults")).values():
        entry = obj(item)
        name = string(entry.get("componentName")).lower()
        if name not in {"asyncimage", "asyncimagegroup", "cite", "link", "entity", "filecite"}:
            continue
        state = obj(entry.get("state"))
        images = array(state.get("images"))
        links = source_links([*images, *array(state.get("items")), state])
        if links:
            components.append({
                "type": name, "links": links,
                "queries": list(dict.fromkeys(string(obj(image).get("image_search_query")) for image in images)),
            })
    return components


def prepare_messages(raw, fallback):
    messages = []
    combined_refs = {}
    active = path_nodes(raw)
    active_ids = {id(message) for message in active}
    nodes = [(message, True) for message in active]
    for node in obj(raw.get("mapping")).values():
        message = obj(obj(node).get("message"))
        if message and id(message) not in active_ids:
            nodes.append((message, False))

    for message, in_path in nodes:
        content = obj(message.get("content"))
        metadata = obj(message.get("metadata"))
        text_parts = []
        parts = []
        refs = {}
        blocks = []

        for part in array(content.get("parts")):
            if isinstance(part, str):
                text_parts.append(part)
                parts.append(part)
                continue
            if not isinstance(part, dict):
                continue
            text = string(part.get("text"))
            if text:
                text_parts.append(text)
                parts.append(text)
            if part.get("content_type") == "real_time_user_audio_video_asset_pointer":
                part = obj(part.get("audio_asset_pointer"))
            pointer = part.get("asset_pointer") or part.get("file_id")
            if pointer:
                asset = file_id(pointer, allow_bare=not part.get("asset_pointer"))
                if key := add_file_ref(refs, part, asset, part.get("content_type") == "image_asset_pointer"):
                    parts.append({"ref": key})

        body = "\n".join(text_parts) or string(content.get("text"))
        if not text_parts and body:
            parts.insert(0, body)
        if in_path and body and content.get("content_type") in ("code", "execution_output"):
            fence = "`" * max(3, 1 + max((len(run) for run in re.findall(r"`+", body)), default=0))
            if any(isinstance(part, dict) for part in parts):
                parts = [f"{fence}\n{part}\n{fence}" if isinstance(part, str) else part for part in parts]
            else:
                parts = [f"{fence}\n{body}\n{fence}"]

        for attachment in array(metadata.get("attachments")):
            attachment = obj(attachment)
            asset = next(
                (
                    identifier for field in ("id", "file_id", "asset_pointer")
                    if (identifier := file_id(attachment.get(field), allow_bare=field != "asset_pointer"))
                ),
                "",
            )
            add_file_ref(refs, attachment, asset)

        for match in SANDBOX_LINK.finditer(body):
            path = unquote(match[1])
            refs[path] = {"key": path, "name": path.rsplit("/", 1)[-1]}
        app_data = obj(obj(metadata.get("model_dil_v2")).get("appData"))
        generated = obj(app_data.get("opGenui"))
        for component in obj(generated.get("componentResults")).values():
            component = obj(component)
            state = obj(component.get("state"))
            path = sandbox_path(state.get("url"))
            if path and path not in refs:
                refs[path] = {"key": path, "name": path.rsplit("/", 1)[-1]}

        for canvas_id, block in obj(metadata.get("writing_blocks")).items():
            block = obj(block)
            value = block.get("content")
            if not value:
                continue
            key = f"Canvas_{canvas_id}.md"
            title = string(block.get("title") or block.get("subject"))
            text = string(value) or string(obj(value).get("text"))
            refs[key] = {"key": key, "name": "Canvas.md", "match_name": key, "source_id": canvas_id}
            blocks.append({"id": canvas_id, "title": title, "text": text, "ref": key})

        for reference in array(metadata.get("content_references")):
            data = obj(obj(reference).get("data"))
            if data.get("widget_type") == "writing_block" and (text := string(data.get("content"))):
                block_id = string(data.get("id")) or "writing-block"
                if not any(block["id"] == block_id for block in blocks):
                    asset = f"Canvas_{block_id}.md" if re.fullmatch(r"[a-zA-Z0-9_-]+", block_id) else ""
                    if asset:
                        refs[asset] = {"key": asset, "name": "Canvas.md", "match_name": asset, "source_id": block_id}
                    blocks.append({"id": block_id, "title": "", "text": text, "ref": asset})
        sources = message_sources(metadata, refs)
        components = embedded_results(generated) if in_path else []

        for key, ref in refs.items():
            previous = combined_refs.get(key)
            if previous is None or (previous.get("generic") and not ref.get("generic")):
                combined_refs[key] = ref
        if in_path and (body.strip() or refs or blocks or components):
            prepared = {
                "role": obj(message["author"])["role"],
                "parts": parts, "refs": list(refs), "blocks": blocks,
            }
            if sources:
                prepared["sources"] = sources
            if components:
                prepared["components"] = components
            messages.append(prepared)
    if not messages and not obj(raw.get("mapping")) and fallback.strip():
        messages.append({"role": "transcript", "parts": [fallback], "refs": [], "blocks": []})
    return messages, combined_refs


def scan_names(entries):
    batches = []
    individual_candidates = defaultdict(list)
    for name, kind in sorted(entries.items()):
        if kind != "file":
            continue
        original = without_copy(name)
        if BATCH_NAME.fullmatch(original):
            batches.append(name)
            continue
        match = NUMBERED_NAME.fullmatch(original)
        if not match:
            continue
        rest = match[2]
        if rest.endswith(".json") and "_" in rest:
            _, _, identifier = rest[:-5].rpartition("_")
            if identifier and not identifier.isdecimal():
                individual_candidates[identifier].append(name)
    return sorted(batches, key=without_copy, reverse=True), individual_candidates


def individual_file(identifier, title, candidates):
    names = candidates.get(identifier, ())
    suffix = f"_{identifier}.json"
    for name in names:
        number = name.split("_", 1)[0]
        if without_copy(name) == f"{number}_{sanitize(title)}{suffix}":
            return name
    if len({without_copy(name) for name in names}) == 1:
        return names[0]
    return None


def group_unknown(numbered, rows, known_stems):
    title_counts = Counter(row["safeTitle"] for row in rows)
    title_names = set(title_counts)
    claimed_numbers = {stem.split("_", 1)[0] for stem in known_stems}
    possibilities = defaultdict(set)
    for number, files in numbered.items():
        if number in claimed_numbers:
            continue
        group_titles = set()
        for name in files:
            rest = name.split("_", 1)[1]
            boundary = rest.rfind("_")
            while boundary >= 0:
                title = rest[:boundary]
                if title in title_names:
                    group_titles.add(title)
                    break
                boundary = rest.rfind("_", 0, boundary)
        if len(group_titles) == 1:
            title = group_titles.pop()
            if title_counts[title] == 1:
                possibilities[title].add(f"{number}_{title}")
    return {title: next(iter(stems)) for title, stems in possibilities.items() if len(stems) == 1}


@dataclass(frozen=True)
class Attachment:
    folder: object
    parents: tuple
    name: str


async def collect_attachments(source, entries, excluded, terminal):
    folders = [(source, (), entries)]
    while folders:
        folder, parents, contents = folders.pop()
        for name, kind in sorted(contents.items()):
            if kind == "file":
                if (parents or name not in excluded) and not BATCH_NAME.fullmatch(without_copy(name)):
                    yield Attachment(folder, parents, name)
            elif kind == "directory" and not GENERATED_FOLDER.fullmatch(name):
                try:
                    child = await folder.subfolder(name)
                    folders.append((child, (*parents, name), await child.entries()))
                except Exception as exc:
                    terminal.warning(f"Cartella allegati saltata: {'/'.join((*parents, name))} ({exc})")
                await asyncio.sleep(0)


def without_copy(name):
    return BROWSER_COPY.sub("", name)


def name_forms(name):
    base = without_copy(name.rsplit("/", 1)[-1].rsplit("\\", 1)[-1])
    result = set()
    for variant in (base, sanitize(base), sanitize(name)):
        stem, ext = Path(variant).stem, Path(variant).suffix
        flat = "".join(char for char in normalize("NFKD", stem.casefold()) if char.isalnum())
        result.add((flat, ext.casefold()))
    return result


def name_score(ref, name, fuzzy=False):
    if ref.get("generic"):
        return 0
    expected = ref.get("match_name", ref["name"])
    if name == expected:
        return 300
    if without_copy(name) == without_copy(expected):
        return 290
    if name_forms(name) & name_forms(expected):
        return 260
    if not fuzzy:
        return 0

    best = 0
    for expected_stem, expected_ext in name_forms(expected):
        for actual_stem, actual_ext in name_forms(name):
            if (expected_ext != actual_ext or min(len(expected_stem), len(actual_stem)) < 6
                    or max(len(expected_stem), len(actual_stem)) > 256):
                continue
            if re.findall(r"\d+", expected_stem) != re.findall(r"\d+", actual_stem):
                continue
            similarity = SequenceMatcher(None, expected_stem, actual_stem, autojunk=False).ratio()
            if similarity >= 0.84:
                best = max(best, round(similarity * 100))
    return best


FILE_MARKER = re.compile(r"file[_-]", re.ASCII | re.IGNORECASE)
ID_PAYLOAD = re.compile(r"[a-zA-Z0-9][a-zA-Z0-9_-]*")
ID_FINGERPRINT_SIZE = 6
UNLINKED_FILE_ID = re.compile(
    r"file[_-](?:" + UUID.pattern.removesuffix("$") + r"|[a-zA-Z0-9]{20,})",
    re.ASCII | re.IGNORECASE,
)


def ids_in_name(name, id_suffixes, partial_ids):
    name = unquote(without_copy(name))
    matches = []
    for end in range(ID_FINGERPRINT_SIZE, len(name) + 1):
        anchor = name[end - ID_FINGERPRINT_SIZE:end].lower()
        for key, variant, prefixed in id_suffixes.get(anchor, ()):
            start = end - len(variant)
            if start < 0:
                continue
            spelling = name[start:end]
            comparable = spelling[:5].lower() + spelling[5:] if prefixed else spelling
            if comparable != variant:
                continue
            bounded = ((start == 0 or not name[start - 1].isalnum())
                       and (end == len(name) or not name[end].isalnum()))
            strength = (4 if bounded else 3) if prefixed else 2
            matches.append((start, end, key, strength, spelling))

    # Un ID breve contenuto in un altro ID conosciuto non identifica un secondo file.
    retained = []
    for start, end, key, strength, spelling in sorted(
        matches, key=lambda match: (match[0], -match[1], -match[3])
    ):
        if any(left <= start and end <= right for left, right in retained):
            continue
        retained.append((start, end))
        yield key, strength, spelling
    if retained:
        return

    for marker in FILE_MARKER.finditer(name):
        token = ID_PAYLOAD.match(name, marker.end())
        if not token or token.end() != len(name) or len(token[0]) < 20:
            continue
        payload = token[0]
        keys = [key for key in partial_ids.get(payload[:20], ()) if key[5:].startswith(payload)]
        if len(keys) == 1:
            yield keys[0], 1, name[marker.start():]


class AttachmentIndex:
    def __init__(self, rows, inferred):
        known_ids = {ref["key"] for row in rows for ref in row["refs"].values() if ref.get("id")}
        self.id_suffixes = defaultdict(list)
        self.partial_ids = defaultdict(list)
        for key in sorted(known_ids):
            payload = key[5:]
            variants = [(key, True), (f"file-{payload}", True)]
            if len(payload) >= 20:
                self.partial_ids[payload[:20]].append(key)
                variants.append((payload, False))
            for variant, prefixed in variants:
                anchor = variant[-ID_FINGERPRINT_SIZE:].lower()
                self.id_suffixes[anchor].append((key, variant, prefixed))
        self.numbered = defaultdict(list)
        self.by_parent = defaultdict(list)
        self.by_id = defaultdict(dict)
        self.loose = defaultdict(list)
        self.marked = set()
        self.spellings = defaultdict(set)
        self.prefixes = defaultdict(set)
        for row in rows:
            stem = row["stem"] or inferred.get(row["safeTitle"])
            if stem:
                self.prefixes[stem.split("_", 1)[0]].add(stem.casefold() + "_")
        self.chat_folders = {
            value.casefold() for row in rows
            for value in (row["id"], row["stem"] or inferred.get(row["safeTitle"])) if value
        }

    def add(self, asset):
        numbered = NUMBERED_NAME.fullmatch(asset.name)
        filename = asset.name
        if numbered:
            self.numbered[numbered[1]].append(asset)
            prefix_length = max(
                (len(prefix) for prefix in self.prefixes[numbered[1]]
                 if filename.casefold().startswith(prefix)),
                default=0,
            )
            filename = filename[prefix_length:]
        parents = {parent.casefold() for parent in asset.parents}
        for parent in parents:
            self.by_parent[parent].append(asset)
        if not numbered and not parents.intersection(self.chat_folders) and not any(
            NUMBERED_NAME.fullmatch(parent) for parent in parents
        ):
            for form in name_forms(asset.name):
                self.loose[form].append(asset)
        if UNLINKED_FILE_ID.search(unquote(filename)):
            self.marked.add(asset)
        for key, strength, spelling in ids_in_name(filename, self.id_suffixes, self.partial_ids):
            self.marked.add(asset)
            self.spellings[asset].add(spelling)
            self.by_id[key].setdefault(asset, set()).add((strength, spelling))

    def nearby(self, row, stem):
        files = {}
        if stem:
            prefix = stem.casefold() + "_"
            number = stem.split("_", 1)[0]
            for asset in self.numbered[number]:
                if asset.name.casefold().startswith(prefix) and len(asset.name) > len(prefix):
                    files[asset] = asset.name[len(prefix):]
            for asset in self.by_parent[stem.casefold()]:
                files.setdefault(asset, asset.name)
        for asset in self.by_parent[row["id"].casefold()]:
            files.setdefault(asset, asset.name)
        return files

    def match(self, ref, nearby):
        if ref.get("id"):
            candidates = self.by_id.get(ref["key"], {})
            if candidates:
                def rank(asset):
                    strength = max((level, spelling == ref["id"]) for level, spelling in candidates[asset])
                    display = nearby.get(asset, asset.name)
                    names = [display]
                    for _, spelling in candidates[asset]:
                        if spelling:
                            suffix = unquote(display).partition(spelling)[2].lstrip("_- ")
                            if suffix and not suffix.startswith("."):
                                names.append(suffix)
                    similarity = max(name_score(ref, name, fuzzy=True) for name in names)
                    return (strength, asset in nearby, similarity,
                            without_copy(asset.name) == asset.name, -len(asset.parents))

                return max(candidates, key=rank)
        if ref.get("generic"):
            return None
        ranked = sorted(
            ((name_score(ref, name, fuzzy=True), asset) for asset, name in nearby.items()
             if asset not in self.marked),
            key=lambda item: item[0], reverse=True,
        )
        if ranked and ranked[0][0]:
            best, asset = ranked[0]
            other = next((score for score, candidate in ranked[1:]
                          if candidate.folder is not asset.folder
                          or without_copy(candidate.name) != without_copy(asset.name)), 0)
            if best > other and (best >= 260 or best - other >= 8):
                return asset
            return None
        matches = {
            candidate
            for form in name_forms(ref.get("match_name", ref["name"]))
            for candidate in self.loose.get(form, ())
            if candidate not in self.marked
        }
        if len(matches) == 1:
            return matches.pop()
        return None


def filename_parts(name):
    basename = without_copy(unquote(name).replace("\\", "/").rsplit("/", 1)[-1])
    path = Path(basename)
    suffix = path.suffix.casefold()
    if not FILE_SUFFIX.fullmatch(suffix):
        return basename, ""
    if suffix in {".bin", ".dat", ".file", ".download", ".tmp"}:
        return path.stem, ""
    mime, encoding = MIME_TYPES.guess_type(f"file{suffix}", strict=False)
    if mime or encoding or suffix in TEXT_PREVIEW:
        return path.stem, suffix
    return path.stem, ""


def signature_suffix(sample):
    signatures = (
        (b"%PDF-", ".pdf"), (b"\x89PNG\r\n\x1a\n", ".png"), (b"\xff\xd8\xff", ".jpg"),
        (b"GIF87a", ".gif"), (b"GIF89a", ".gif"), (b"fLaC", ".flac"), (b"ID3", ".mp3"),
        (b"OggS", ".ogg"), (b"PK\x03\x04", ".zip"), (b"PK\x05\x06", ".zip"),
        (b"\x1f\x8b", ".gz"), (b"7z\xbc\xaf\x27\x1c", ".7z"),
    )
    for marker, suffix in signatures:
        if sample.startswith(marker):
            return suffix
    if sample.startswith(b"RIFF"):
        return {b"WEBP": ".webp", b"WAVE": ".wav", b"AVI ": ".avi"}.get(sample[8:12], "")
    if sample.startswith(b"\x1aE\xdf\xa3"):
        return ".webm" if b"webm" in sample[:256] else ".mkv"
    if sample[4:8] == b"ftyp":
        brand = sample[8:12]
        if brand in (b"avif", b"avis"):
            return ".avif"
        if brand in (b"heic", b"heix"):
            return ".heic"
        if brand == b"mif1":
            return ".heif"
        if brand in (b"M4A ", b"M4B "):
            return ".m4a"
        if brand == b"qt  ":
            return ".mov"
        return ".mp4"
    return ""


def attachment_suffix(sample, mime, preferred, original):
    mime = mime.split(";", 1)[0].strip().lower()
    hinted = MIME_SUFFIXES.get(mime) or MIME_TYPES.guess_extension(mime, strict=False)
    hints = (preferred, original, hinted)
    signature = signature_suffix(sample)
    if signature == ".zip":
        return next((suffix for suffix in hints if suffix in ZIP_SUFFIXES), signature)
    if signature:
        return signature
    for suffix in hints:
        if suffix and suffix not in {".bin", ".dat"}:
            return suffix

    # Un campione che finisce in mezzo a un carattere UTF-8 resta leggibile.
    try:
        text = codecs.getincrementaldecoder("utf-8-sig")().decode(sample[:4096])
    except UnicodeDecodeError:
        return ".bin"
    if text.strip() and all(char.isprintable() or char in "\n\r\t" for char in text):
        return ".txt"
    return ".bin"


def simple_filename(display, ref, spellings, sample, used_names):
    original_stem, original_suffix = filename_parts(display)
    title = ref.get("name", "") if not ref.get("generic") else ""
    expected, expected_suffix = filename_parts(title)
    suffix = attachment_suffix(sample, ref.get("mime", ""), expected_suffix, original_suffix)
    identifiers = set(spellings)
    if identifier := ref.get("id"):
        payload = identifier[5:]
        identifiers.update((identifier, f"file_{payload}", f"file-{payload}"))
        if len(payload) >= 20:
            identifiers.add(payload)
    for candidate in (expected, original_stem):
        for identifier in sorted(identifiers, key=len, reverse=True):
            candidate = candidate.replace(identifier, "")
        candidate = UNLINKED_FILE_ID.sub("", candidate)
        candidate = re.sub(r"[^\w .-]+", "_", normalize("NFC", candidate))
        stem = re.sub(r"[\s_]+", " ", candidate).strip(" ._-")[:52].rstrip(" ._-")
        if stem:
            break
    else:
        stem = "Immagine" if ref.get("image") or suffix in IMAGE_SUFFIXES else "Allegato"

    if re.fullmatch(r"CON|PRN|AUX|NUL|COM[1-9]|LPT[1-9]", stem.split(".")[0], re.IGNORECASE):
        stem = "_" + stem
    name = stem + suffix
    number = 2
    while name.casefold() in used_names:
        name = f"{stem} ({number}){suffix}"
        number += 1
    used_names.add(name.casefold())
    return name, suffix


async def copy_attachment(folder, asset, display, ref, spellings, used_names):
    async with aclosing(asset.folder.read(asset.name)) as source:
        first = await anext(source, b"")
        sample = bytearray(first[:PREVIEW_LIMIT])
        name, suffix = simple_filename(display, ref, spellings, bytes(sample), used_names)
        size = len(first)

        async def chunks():
            nonlocal size
            if first:
                yield first
            async for chunk in source:
                size += len(chunk)
                if len(sample) < PREVIEW_LIMIT:
                    sample.extend(chunk[:PREVIEW_LIMIT - len(sample)])
                yield chunk

        await folder.write(name, chunks())
    return name, suffix, sample, size

async def text_preview(folder, name, sample, size):
    text = sample.decode("utf-8-sig", errors="replace")
    notice = f"<p>Anteprima dei primi {PREVIEW_LIMIT // 1024} KiB.</p>" if size > len(sample) else ""
    await write_text(folder, name, (
        '<!doctype html><html lang="it"><meta charset="utf-8">'
        '<meta http-equiv="Content-Security-Policy" content="default-src \'none\'; style-src \'unsafe-inline\'">'
        '<title>Anteprima allegato</title><style>:root{color-scheme:light dark}'
        'pre{white-space:pre-wrap;overflow-wrap:anywhere}</style>'
        f'{notice}<pre>{escape(text)}</pre></html>'
    ))


async def attach_files(target, previews, row, index, stem, terminal):
    nearby = index.nearby(row, stem)
    matches = {}
    for ref in row["refs"].values():
        if asset := index.match(ref, nearby):
            matches[ref["key"]] = asset

    # Più ID con lo stesso nome e una sola copia senza ID sono un caso ambiguo.
    claims = defaultdict(set)
    for key, asset in matches.items():
        if row["refs"][key].get("id"):
            claims[asset].add(key)
    matches = {key: asset for key, asset in matches.items() if len(claims[asset]) < 2}

    unmatched_images = [
        ref for ref in row["refs"].values() if ref.get("image") and ref.get("generic")
        and ref["key"] not in matches
    ]
    if len(unmatched_images) == 1:
        assigned = set(matches.values())
        images = [
            asset for asset, display in nearby.items()
            if asset not in assigned and asset not in index.marked
            and Path(without_copy(display)).suffix.casefold() in IMAGE_SUFFIXES
        ]
        if len(images) == 1:
            matches[unmatched_images[0]["key"]] = images[0]

    references = {}
    for key, asset in matches.items():
        ref = row["refs"][key]
        if asset not in references or references[asset].get("generic"):
            references[asset] = ref
    files = {asset: nearby.get(asset, asset.name) for asset in matches.values()}
    files.update(nearby)
    if not files:
        return {"links": {}, "all": []}

    folder = await target.mkdir(row["key"])
    available = {}
    used_names = set()
    for position, (asset, display) in enumerate(files.items(), 1):
        try:
            name, extension, sample, size = await copy_attachment(
                folder, asset, display, references.get(asset, {}), index.spellings.get(asset, ()), used_names,
            )
        except Exception as exc:
            terminal.warning(f"File saltato: {'/'.join((*asset.parents, asset.name))} ({exc})")
        else:
            file = {"name": name, "url": f"file/{row['key']}/{quote(name)}"}
            if extension in TEXT_PREVIEW and b"\0" not in sample:
                preview = f"{row['key']}-anteprima-{position}.html"
                try:
                    await text_preview(previews, preview, sample, size)
                except Exception as exc:
                    terminal.warning(f"Anteprima saltata: {display} ({exc})")
                else:
                    file["preview"] = f"chat/{preview}"
            elif extension == ".pdf" and sample.startswith(b"%PDF-"):
                file["pdf"] = True
            available[asset] = file

    linked = {key: available[asset] for key, asset in matches.items() if asset in available}
    return {"links": linked, "all": list(available.values())}

async def create_site_folder(destination):
    contents = await destination.entries()
    number = 1
    while True:
        name = SITE_FOLDER if number == 1 else f"{SITE_FOLDER}-{number}"
        if name not in contents:
            try:
                return name, await destination.mkdir(name)
            except FileExistsError:
                pass
        number += 1


async def generate(source, destination, terminal):
    entries = await source.entries()
    batches, candidates = scan_names(entries)
    if not batches and not candidates:
        raise ValueError("Nessun export riconosciuto nella cartella di origine.")

    site_name, site = await create_site_folder(destination)
    terminal.info(f"Destinazione: {site.name}")
    chat_dir = await site.mkdir("chat")
    search_dir = await site.mkdir("ricerca")
    files_dir = await site.mkdir("file")
    rows = []
    seen = {}
    excluded = set()
    shards = 0
    pending_search = []
    pending_size = 0

    async def flush_search():
        nonlocal shards, pending_size
        if pending_search:
            shards += 1
            await write_data(search_dir, f"{shards}.js", "registraRicerca", pending_search)
            pending_search.clear()
            pending_size = 0

    async def add(raw, summary, from_file=None, file_identifier=None):
        nonlocal pending_size
        identifier = (
            string(summary.get("id")) or string(raw.get("conversation_id"))
            or string(raw.get("id")) or file_identifier
        )
        fallback = string(summary.get("markdown_text"))
        if not identifier or not (isinstance(raw.get("mapping"), dict) or fallback):
            return False

        if from_file and identifier != file_identifier:
            return False
        title = string(summary.get("title")) or string(raw.get("title"))
        filename = from_file or individual_file(identifier, title, candidates)
        stem = None
        if filename:
            stem = without_copy(filename).removesuffix(f"_{identifier}.json")
            excluded.add(filename)
        if identifier in seen:
            if stem and not seen[identifier]["stem"]:
                seen[identifier]["stem"] = stem
            return True
        messages, refs = prepare_messages(raw, fallback)
        key = str(len(rows) + 1)
        row = {
            "key": key,
            "id": identifier,
            "title": title or "Senza titolo",
            "safeTitle": sanitize(title),
            "stem": stem,
            "updated": last_update(summary, raw),
            "archived": raw.get("is_archived") is True or summary.get("is_archived") is True,
            "refs": refs,
        }
        await write_data(chat_dir, f"{key}.js", "registraChat", {
            "key": key,
            "messages": messages,
            "refs": [{
                "key": ref["key"], "name": ref["name"], "image": ref.get("image", False),
                "id": ref.get("id") or ref.get("source_id") or (ref["key"] if ref["key"].startswith("/") else ""),
            } for ref in refs.values()],
        })
        search = "\n".join(
            [row["title"]]
            + [part for message in messages for part in message["parts"] if isinstance(part, str)]
            + [block["text"] for message in messages for block in message["blocks"]]
            + [ref["name"] for ref in refs.values() if not ref.get("generic")]
        )
        size = len(search.encode("utf-8", "backslashreplace"))
        if pending_search and pending_size + size > SEARCH_SHARD_SIZE:
            await flush_search()
        pending_search.append([key, search])
        pending_size += size
        if pending_size >= SEARCH_SHARD_SIZE:
            await flush_search()
        rows.append(row)
        seen[identifier] = row
        if len(rows) % 25 == 0:
            terminal.info(f"{len(rows)} conversazioni elaborate")
        await asyncio.sleep(0)
        return True

    for name in batches:
        terminal.info(f"Lettura di {name}")
        try:
            async for record in batch_records(source, name):
                summary = obj(record)
                await add(obj(summary.get("raw_data")), summary)
        except (ValueError, OSError) as exc:
            terminal.warning(f"{name}: {exc}. Verifico anche i JSON individuali.")

    for identifier, names in candidates.items():
        for name in names:
            if name in excluded:
                continue
            canonical = UUID.fullmatch(identifier) is not None
            try:
                raw = obj(await read_individual(source, name, canonical))
            except Exception as exc:
                if canonical:
                    terminal.warning(f"JSON individuale saltato: {name} ({exc})")
                continue
            # Un allegato JSON può avere una proprietà mapping. Per gli ID non
            # standard serve anche il nome atteso o un ID esplicito coincidente.
            explicit_id = string(raw.get("conversation_id")) or string(raw.get("id"))
            expected_name = f"{name.split('_', 1)[0]}_{sanitize(string(raw.get('title')))}_{identifier}.json"
            if not isinstance(raw.get("mapping"), dict):
                continue
            if not (canonical or explicit_id == identifier or without_copy(name) == expected_name):
                continue
            await add(raw, {}, from_file=name, file_identifier=identifier)

    if not rows:
        raise ValueError("L'export non contiene conversazioni leggibili.")

    await flush_search()
    terminal.info(f"{len(rows)} conversazioni. Copia dei file associabili in corso.")
    known_stems = {row["stem"] for row in rows if row["stem"]}
    asset_names = defaultdict(list)
    assets = []
    async for asset in collect_attachments(source, entries, excluded, terminal):
        assets.append(asset)
        if match := NUMBERED_NAME.fullmatch(asset.name):
            asset_names[match[1]].append(asset.name)
    inferred = group_unknown(asset_names, rows, known_stems)
    index = AttachmentIndex(rows, inferred)
    for asset in assets:
        index.add(asset)

    file_count = 0
    linked_count = 0
    for row in rows:
        stem = row["stem"] or inferred.get(row["safeTitle"])
        files = await attach_files(files_dir, chat_dir, row, index, stem, terminal)
        if files["all"]:
            row["hasFiles"] = True
            file_count += len(files["all"])
            linked_count += len(files["links"])
            await write_data(chat_dir, f"{row['key']}-file.js", "registraFile", {"key": row["key"], **files})
        await asyncio.sleep(0)

    rows.sort(key=lambda row: (row["updated"] is not None, row["updated"] or 0), reverse=True)
    manifest = {
        "chats": [
            {
                "key": row["key"], "title": row["title"], "updated": row["updated"],
                "archived": row["archived"], "hasFiles": row.get("hasFiles", False),
            }
            for row in rows
        ],
        "shards": shards,
    }
    await write_text(site, "stile.css", STYLE)
    await write_text(site, "katex.min.js", decompress(b85decode(KATEX_0191)).decode("utf-8"))
    await write_text(site, "archivio.js", APP)
    await write_text(site, "indice.js", f"window.archivioIndice = {script_data(manifest)};\n")
    await write_text(site, "index.html", HTML)
    reference_count = sum(len(row["refs"]) for row in rows)
    terminal.done(
        f"Creato {site_name}/index.html · {len(rows)} chat · {file_count} file · "
        f"{linked_count}/{reference_count} riferimenti a file collegati"
    )
    return site_name


async def genera_da_browser(orig, dest, colore=True):
    """Da chiamare con await nel worker Pyodide, passando due DirectoryHandle."""
    terminal = Terminal(colore)
    try:
        return await generate(BrowserFolder(orig), BrowserFolder(dest), terminal)
    except Exception as exc:
        terminal.error(exc)
        raise


async def genera_da_cartelle(orig, dest, colore=True):
    """Da chiamare con await per percorsi POSIX, inclusi mount Pyodide."""
    source = Path(orig).expanduser().resolve(strict=True)
    destination = Path(dest).expanduser().resolve(strict=True)
    return await generate(LocalFolder(source), LocalFolder(destination), Terminal(colore))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("origine", help="cartella dei file esportati")
    parser.add_argument("destinazione", help="cartella in cui creare il sito")
    parser.add_argument("--no-color", action="store_true", help="disattiva i colori del terminale")
    arguments = parser.parse_args()
    color = not arguments.no_color
    try:
        asyncio.run(genera_da_cartelle(arguments.origine, arguments.destinazione, color))
    except KeyboardInterrupt:
        Terminal(color).warning("Generazione interrotta.")
        return 130
    except Exception as exc:
        Terminal(color).error(exc)
        return 1
    return 0


HTML = r"""<!doctype html>
<html lang="it">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <meta http-equiv="Content-Security-Policy" content="default-src 'none'; script-src 'self' file:; style-src 'self' file:; img-src 'self' file: data:; media-src 'self' file:; frame-src 'self' file:; base-uri 'none'; form-action 'none'">
  <title>Archivio chat</title>
  <link rel="stylesheet" href="stile.css">
  <script defer src="indice.js"></script>
  <script defer src="katex.min.js"></script>
  <script defer src="archivio.js"></script>
</head>
<body>
  <aside class="sidebar" id="sidebar" aria-label="Cronologia">
    <div class="sidebar-top">
      <div class="brand">Archivio chat</div>
      <button class="icon close-sidebar" id="close-sidebar" type="button" aria-label="Chiudi cronologia">✕</button>
    </div>
    <label class="search-label" for="search">Cerca nelle chat</label>
    <input id="search" type="search" autocomplete="off" placeholder="Titolo o contenuto">
    <p id="results" class="results" role="status"></p>
    <nav id="history" aria-label="Conversazioni"></nav>
    <button id="more" class="more" type="button" hidden>Mostra altre</button>
  </aside>
  <main id="main">
    <header class="topbar">
      <button class="icon open-sidebar" id="open-sidebar" type="button" aria-label="Apri cronologia" aria-controls="sidebar" aria-expanded="false">☰</button>
      <div class="topbar-title" id="title">Conversazioni</div>
      <div class="date" id="date"></div>
      <button class="print-chat" id="print-chat" type="button" aria-label="Stampa conversazione" disabled>Stampa</button>
    </header>
    <article class="conversation" id="conversation" aria-label="Messaggi" tabindex="-1">
      <div class="placeholder"><h1>Apri una conversazione</h1><p>Sceglila dalla cronologia o usa la ricerca.</p></div>
    </article>
  </main>
  <dialog id="attachment" aria-labelledby="attachment-title">
    <header class="attachment-bar">
      <h2 id="attachment-title"></h2>
      <button class="icon" id="close-attachment" type="button" aria-label="Chiudi allegato" autofocus>✕</button>
    </header>
    <div id="attachment-content"></div>
    <footer class="attachment-bar" id="attachment-actions"></footer>
  </dialog>
</body>
</html>
"""


STYLE = r""":root {
  color-scheme: light;
  font-family: system-ui, -apple-system, sans-serif;
  --page: #fbfaf7;
  --surface: #fff;
  --ink: #242236;
  --muted: #615e72;
  --edge: #e6e2ea;
  --sidebar: #242235;
  --side-ink: #f7f5fa;
  --side-muted: #c1bbd0;
  --accent: #564080;
  --bubble: #eeebf6;
}
*, *::before, *::after { box-sizing: border-box; }
body { display: grid; grid-template-columns: 296px minmax(0, 1fr); margin: 0; height: 100vh; height: 100dvh; background: var(--page); color: var(--ink); }
button, input { font: inherit; }
button, a { touch-action: manipulation; }
button { cursor: pointer; }
a { color: var(--accent); }
:focus-visible { outline: 2px solid #9a77e0; outline-offset: 3px; }
[hidden] { display: none !important; }
.sidebar { display: flex; min-height: 0; flex-direction: column; padding: 24px 14px 16px; overflow: auto; background: var(--sidebar); color: var(--side-ink); }
.sidebar-top { display: flex; justify-content: space-between; align-items: center; margin: 0 8px 26px; }
.brand { font-size: 1.28rem; font-weight: 700; letter-spacing: -.035em; }
.search-label { margin: 0 8px 8px; color: var(--side-muted); font-size: .83rem; }
#search { width: calc(100% - 16px); margin: 0 8px; padding: 10px 12px; border: 1px solid #5b5570; border-radius: 7px; color: var(--side-ink); background: #302d44; }
#search::placeholder { color: var(--side-muted); }
.results { margin: 15px 8px 8px; color: var(--side-muted); font-size: .79rem; min-height: 1.25em; }
#history { display: grid; gap: 3px; }
.chat-link { padding: 10px 9px; display: block; border: 1px solid transparent; border-radius: 7px; overflow: hidden; color: inherit; text-overflow: ellipsis; white-space: nowrap; text-decoration: none; font-size: .9rem; }
.chat-link:hover { background: #343047; }
.chat-link[aria-current="page"] { background: #483c65; border-color: #695884; }
.more { align-self: flex-start; margin: 8px; padding: 7px 0; background: transparent; border: 0; color: var(--side-ink); text-decoration: underline; }
main { min-height: 0; overflow: auto; }
.topbar { display: flex; align-items: center; gap: 12px; min-height: 60px; position: sticky; z-index: 1; top: 0; padding: 12px clamp(16px, 5vw, 40px); background: var(--page); border-bottom: 1px solid var(--edge); }
.topbar-title { min-width: 0; flex: 1; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; font-weight: 650; }
.date { color: var(--muted); font-size: .83rem; white-space: nowrap; }
.icon { display: inline-grid; place-items: center; min-width: 34px; min-height: 34px; border: 1px solid var(--edge); border-radius: 6px; color: inherit; background: transparent; }
.print-chat { min-height: 34px; padding: 0 9px; border: 1px solid var(--edge); border-radius: 6px; color: inherit; background: transparent; font-size: .84rem; }
.print-chat:disabled { cursor: default; opacity: .55; }
.close-sidebar, .open-sidebar { display: none; }
.conversation { width: min(100%, 854px); margin: 0 auto; padding: 45px clamp(17px, 5vw, 48px) 72px; }
.conversation:focus { outline: none; }
.placeholder { text-align: center; margin: 13vh auto 0; color: var(--muted); }
.placeholder h1 { margin-bottom: 4px; color: var(--ink); font-size: clamp(1.5rem, 4vw, 2.2rem); letter-spacing: -.035em; }
.message { margin: 0 0 34px; min-width: 0; }
.message.user { width: fit-content; max-width: 88%; margin-left: auto; padding: 12px 16px; background: var(--bubble); border-radius: 15px 15px 4px 15px; }
.role { margin: 0 0 8px; font-size: .81rem; font-weight: 700; color: var(--accent); }
.prose { font-size: 1rem; line-height: 1.62; overflow-wrap: anywhere; }
.prose > :first-child { margin-top: 0; }
.prose > :last-child { margin-bottom: 0; }
.prose p, .prose ul, .prose ol, .prose pre, .prose blockquote { margin: .8em 0; }
.prose ul, .prose ol { padding-left: 1.65em; }
.prose h2, .prose h3, .prose h4, .prose h5, .prose h6 { margin: 1.15em 0 .38em; line-height: 1.25; }
.prose pre { padding: 14px; overflow: auto; background: #222132; color: #f5f1fb; border-radius: 8px; font-size: .88em; }
.prose code { padding: .1em .26em; border-radius: 3px; background: #ebe6f3; font-family: ui-monospace, monospace; }
.prose pre code { background: none; padding: 0; }
.prose blockquote { margin-left: 0; padding-left: 12px; border-left: 3px solid #a698bc; color: var(--muted); }
.prose hr { border: 0; border-top: 1px solid var(--edge); }
.formula { white-space: normal; }
.formula-display { display: block; overflow: auto; margin: .8em 0; padding: 2px 0; text-align: center; }
.citation { margin-inline: 2px; font-size: .84em; overflow-wrap: anywhere; }
.source-summary { white-space: pre-line; }
.each-list > li + li { margin-top: .55em; }
.each-list > li > :first-child { margin-top: 0; }
.each-list > li > :last-child { margin-bottom: 0; }
.output-title { font-weight: 700; }
.output-title-lg { font-size: 1.23em; }
.output-title-xl { font-size: 1.45em; }
.output-title-tabular { font-variant-numeric: tabular-nums; }
.output-svg { display: block; max-width: 100%; max-height: 75vh; margin: .75em auto; }
.archive-quiz { margin: 1em 0; }
.quiz-question { min-width: 0; margin: .8em 0; padding: 8px 12px; border: 1px solid var(--edge); border-radius: 7px; }
.quiz-question legend { font-weight: 650; padding: 0 4px; }
.quiz-option { display: flex; align-items: baseline; gap: 7px; padding: 3px 0; cursor: pointer; }
.quiz-option input { accent-color: var(--accent); }
.quiz-feedback { color: var(--muted); }
.quiz-check { padding: 5px 10px; border: 1px solid var(--edge); border-radius: 6px; background: var(--surface); color: var(--ink); }
.quiz-score { font-weight: 650; }
.prose table { border-collapse: collapse; width: 100%; background: var(--surface); }
.prose th, .prose td { border: 1px solid var(--edge); text-align: left; padding: 7px 10px; }
.table-scroll { margin: 1em 0; overflow: auto; }
.file { display: inline-block; margin: 8px 9px 0 0; padding: 5px 9px; border-radius: 6px; border: 1px solid var(--edge); background: var(--surface); font-size: .86rem; overflow-wrap: anywhere; }
.in-line-missing { color: var(--muted); }
.message-image { display: block; max-width: 100%; max-height: 560px; width: auto; height: auto; margin-top: 9px; border-radius: 7px; object-fit: contain; }
.mapped-image { margin: 9px 0 0; }
.message-box, .message-writing { margin: 10px 0; padding: 12px 14px; border: 1px solid var(--edge); border-radius: 7px; }
.message-writing { background: var(--surface); }
.prose .writing-title { margin: 0 0 8px; font-weight: 650; }
.message-caption { margin: 6px 0; color: var(--muted); font-size: .92rem; }
.message-row { display: flex; flex-wrap: wrap; align-items: flex-start; gap: 8px 16px; }
.message-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(min(100%, 180px), 1fr)); gap: 8px 16px; }
.embedded-links { overflow-wrap: anywhere; }
.attachments { margin-top: 24px; border-top: 1px solid var(--edge); padding-top: 14px; }
.attachments > summary { font-weight: 650; }
.attachments summary { cursor: pointer; }
.attachment-list { display: flex; flex-wrap: wrap; gap: 8px; list-style: none; margin: 8px 0; padding: 0; }
.attachment-list li { min-width: 0; max-width: 100%; }
.attachment-list .file { margin: 0; max-width: 100%; }
.missing-files { margin-top: 10px; color: var(--muted); }
.missing-files li { overflow-wrap: anywhere; }
#attachment { width: min(900px, 94vw); max-height: 90dvh; padding: 0; color: var(--ink); background: var(--surface); border: 1px solid var(--edge); border-radius: 14px; }
#attachment::backdrop { background: #1c1824a6; }
.attachment-bar { display: flex; align-items: center; justify-content: space-between; gap: 12px; padding: 12px 16px; }
.attachment-bar h2 { min-width: 0; overflow-wrap: anywhere; font-size: 1rem; margin: 0; }
#attachment-content { display: grid; place-items: center; background: var(--page); padding: 12px; overflow: auto; }
#attachment-content iframe { display: block; border: 0; width: 100%; height: min(64dvh, 630px); }
#attachment-content img, #attachment-content video { max-width: 100%; max-height: 64dvh; margin: 0; }
#attachment-content audio { max-width: 100%; }
#attachment-content p { margin: 16px; color: var(--muted); }
#attachment-actions { justify-content: flex-end; }
#attachment-actions .file { margin: 0; }
@media (prefers-color-scheme: dark) {
  :root { color-scheme: dark; --page: #22212c; --surface: #292737; --ink: #f5f1fb; --muted: #c9c1d9; --edge: #434053; --accent: #d0b9ff; --bubble: #353046; }
  .prose code { background: #383248; }
  .prose pre { background: #191823; }
}
@media (max-width: 730px) {
  body { display: block; }
  main { height: 100%; }
  .sidebar { display: none; position: fixed; inset: 0 auto 0 0; z-index: 2; width: min(86vw, 320px); box-shadow: 0 0 0 100vmax #1c182459; }
  .sidebar.open { display: flex; }
  .open-sidebar, .close-sidebar { display: inline-grid; }
  .conversation { padding-top: 28px; }
}
@media print {
  @page { margin: 15mm; }
  :root { color-scheme: light; --page: #fff; --surface: #fff; --ink: #17171d; --muted: #454550; --edge: #ccc; --accent: #30254d; --bubble: #fff; }
  html, body, main { display: block; height: auto; overflow: visible; }
  .sidebar, .topbar button, #attachment, .missing-files { display: none !important; }
  .topbar { position: static; min-height: 0; padding: 0 0 10px; margin: 0 0 16px; }
  .topbar-title { white-space: normal; overflow: visible; }
  .conversation { width: auto; margin: 0; padding: 0; }
  .message { margin-bottom: 16px; }
  .message.user { width: auto; max-width: none; margin-left: 0; padding: 0; }
  .role, .prose h2, .prose h3, .prose h4, .prose h5, .prose h6 { break-after: avoid; }
  .prose p { orphans: 2; widows: 2; }
  .prose pre, .table-scroll, .formula-display { overflow: visible; }
  .prose pre { white-space: pre-wrap; overflow-wrap: anywhere; color: inherit; background: #f3f3f3; }
  .prose code { background: #f3f3f3; }
  .message-image { max-height: 190mm; object-fit: contain; }
  .mapped-image, .formula-display, .attachment-list li { break-inside: avoid; }
  .output-svg, .quiz-question { break-inside: avoid; }
  .output-svg { max-height: 180mm; }
  .quiz-check { display: none; }
  .attachments:not(:has(.attachment-list)) { display: none; }
}
"""


APP = r""""use strict";

const byId = (id) => document.getElementById(id);
const make = (tag, text, className) => {
  const node = document.createElement(tag);
  if (text !== undefined) node.textContent = text;
  if (className) node.className = className;
  return node;
};
const fold = (value) => value.normalize("NFD").replace(/\p{M}/gu, "").toLocaleLowerCase("it");
const validKey = (key) => /^\d+$/.test(key);
const isImage = (file) => /\.(?:png|jpe?g|gif|webp|avif|bmp)$/i.test(file.url);
const image = (file, alt) => {
  const node = make("img", undefined, "message-image");
  node.src = file.url;
  node.alt = alt;
  node.loading = "lazy";
  return node;
};
const fileLink = (file, label, inline = false) => {
  const link = make("a", label || file.name, inline ? undefined : "file");
  link.href = file.url;
  link.download = file.name;
  link.setAttribute("aria-haspopup", "dialog");
  link.addEventListener("click", (event) => {
    event.preventDefault();
    showAttachment(file, label);
  });
  return link;
};
const linkedImage = (file, label) => {
  const link = fileLink(file, label, true);
  link.replaceChildren(image(file, label));
  return link;
};

function showAttachment(file, label) {
  const title = label || file.name;
  byId("attachment-title").textContent = title;
  const content = byId("attachment-content");
  let preview;
  if (isImage(file)) {
    preview = image(file, title);
  } else if (file.preview || file.pdf) {
    preview = make("iframe");
    preview.title = title;
    if (file.preview) preview.setAttribute("sandbox", "");
    preview.src = file.preview || file.url;
  } else if (/\.(?:mp3|wav|ogg|m4a|mp4|webm)$/i.test(file.url)) {
    preview = make(/\.(?:mp4|webm)$/i.test(file.url) ? "video" : "audio");
    preview.controls = true;
    preview.src = file.url;
  } else {
    preview = make("p", "Anteprima non disponibile per questo formato.");
  }
  content.replaceChildren(preview);
  const download = make("a", "Scarica allegato", "file");
  download.href = file.url;
  download.download = file.name;
  download.target = "_blank";
  download.rel = "noopener noreferrer";
  byId("attachment-actions").replaceChildren(download);
  byId("attachment").showModal();
}
const externalLink = (href, label) => {
  try {
    const url = new URL(href);
    if (url.protocol !== "https:" && url.protocol !== "http:") return null;
    const link = make("a", label);
    link.href = url.href;
    link.target = "_blank";
    link.rel = "noopener noreferrer";
    return link;
  } catch {
    return null;
  }
};

const dataPath = String.raw`[a-z_]\w*(?:\.[a-z_]\w*|\[\s*\d+\s*\])*`;
const dataPathPattern = new RegExp("^" + dataPath + "$", "i");
const inlineToken = /(?<tick>\x60+)(?<code>.+?)\k<tick>|(?<formula>\uFDD0(?<formulaId>\d+)\uFDD1)|(?<title>\uFDD2(?<titleId>\d+)\uFDD3)|(?<reference>\uE200[a-z][\w-]*(?:\uE202[^\uE200\uE201]*)?\uE201)|(?<binding>\{\s*(?<path>[a-z_]\w*(?:\.[a-z_]\w*|\[\s*\d+\s*\])*)\s*(?<increment>\+\s*1)?\s*\})|(?<image>!?)\[(?<label>[^\]\n]+)\]\((?<href>(?:[^()\s]|\([^()\s]*\))+)\)|(?<strong>\*\*|__|~~)(?<strongText>[^\n]+?)\k<strong>|(?<emphasis>\*|_)(?<emphasisText>[^\n]+?)\k<emphasis>|\\(?<escaped>[\\\x60*_$\{\}\[\]\(\)#+.!>|~-])/gi;

function dataValue(path, bindings) {
  if (!dataPathPattern.test(path)) return undefined;
  const [name, ...keys] = path.match(/[a-z_]\w*|\d+/gi);
  let value = bindings.get(name);
  for (const key of keys) {
    if (!value || typeof value !== "object" || !Object.hasOwn(value, key)
        || /^(?:__proto__|constructor|prototype)$/.test(key)) return undefined;
    value = value[key];
  }
  return value;
}

function bindingText(part, bindings) {
  const value = dataValue(part.path, bindings);
  if (part.increment) return Number.isFinite(value) ? String(value + 1) : part.binding;
  return value === null ? "" : value !== undefined && typeof value !== "object" ? String(value) : part.binding;
}

function mathElement(formula) {
  const element = make("span", undefined, formula.display ? "formula formula-display" : "formula");
  try {
    window.katex.render(formula.tex, element, {
      displayMode: formula.display, output: "mathml", throwOnError: true,
      trust: false, strict: "ignore", maxSize: 20, maxExpand: 500,
    });
  } catch {
    element.textContent = formula.raw;
  }
  return element;
}

function inline(parent, text, view, depth = 0) {
  if (depth > 12) {
    parent.append(text);
    return;
  }
  let offset = 0;
  for (const hit of text.matchAll(inlineToken)) {
    const part = hit.groups;
    parent.append(text.slice(offset, hit.index));
    if (part.code !== undefined) {
      parent.append(make("code", part.code));
    } else if (part.formula) {
      const formula = view.formulas[Number(part.formulaId)];
      parent.append(formula ? mathElement(formula) : part.formula);
    } else if (part.title) {
      const title = view.titles[Number(part.titleId)];
      if (title) {
        const node = make("span", undefined, "output-title");
        if (title.size === "lg" || title.size === "xl") node.classList.add("output-title-" + title.size);
        if (title.tabular) node.classList.add("output-title-tabular");
        inline(node, title.text, view, depth + 1);
        parent.append(node);
      } else {
        parent.append(part.title);
      }
    } else if (part.binding) {
      parent.append(bindingText(part, view.bindings));
    } else if (part.reference) {
      const source = view.sources.get(part.reference);
      if (source) {
        const file = source.ref && Object.hasOwn(view.files, source.ref) ? view.files[source.ref] : null;
        if (file) {
          parent.append(fileLink(file, undefined, true));
          view.shown.add(source.ref);
        } else {
          if (source.alt && (source.show_alt || !source.links.length)) {
            parent.append(make("span", source.alt, "source-summary"));
          }
          appendSourceLinks(parent, source.links, view);
        }
      }
    } else if (part.href) {
      const {image: marker, label, href} = part;
      let link;
      if (href.startsWith("sandbox:/mnt/data/")) {
        let path = href.slice("sandbox:".length);
        try { path = decodeURIComponent(path); } catch { /* mantiene il percorso originale */ }
        view.shown.add(path);
        const file = Object.hasOwn(view.files, path) ? view.files[path] : null;
        if (file) link = marker && isImage(file) ? linkedImage(file, label) : fileLink(file, label, true);
      } else {
        link = externalLink(href, label);
      }
      if (link) {
        if (!link.querySelector("img")) {
          link.replaceChildren();
          inline(link, label, view, depth + 1);
        }
        parent.append(link);
      } else if (!marker) {
        inline(parent, label, view, depth + 1);
      }
    } else if (part.strong || part.emphasis) {
      const node = make(part.strong === "~~" ? "del" : part.strong ? "strong" : "em");
      inline(node, part.strongText ?? part.emphasisText, view, depth + 1);
      parent.append(node);
    } else {
      parent.append(part.escaped);
    }
    offset = hit.index + hit[0].length;
  }
  parent.append(text.slice(offset));
}

const fence = /^ {0,3}(\x60{3,}|~{3,})(.*)$/;
const heading = /^ {0,3}(#{1,6})\s+(.+)$/;
const listItem = /^\s*(?:([-+*])|(\d+)[.)])\s+(.+)$/;
const quote = /^ {0,3}>\s?(.*)$/;
const rule = /^ {0,3}(?:(?:-\s*){3,}|(?:\*\s*){3,}|(?:_\s*){3,})$/;
const cells = (line) => line.trim().replace(/^\||\|$/g, "").split("|").map((cell) => cell.trim());
const tableSeparator = (line) => line.includes("|") && cells(line).every((cell) => /^:?-{3,}:?$/.test(cell));

function renderMarkdown(container, text, view, depth = 0) {
  const lines = text.replace(/\r\n?/g, "\n").split("\n");
  let i = 0;
  const startsBlock = (line) => fence.test(line) || heading.test(line) || listItem.test(line) || quote.test(line) || rule.test(line);
  while (i < lines.length) {
    const line = lines[i];
    if (!line.trim()) { i += 1; continue; }

    const code = line.match(fence);
    if (code) {
      i += 1;
      const body = [];
      const end = new RegExp("^ {0,3}" + code[1][0] + "{" + code[1].length + ",}\\s*$");
      while (i < lines.length && !end.test(lines[i])) body.push(lines[i++]);
      if (i < lines.length) i += 1;
      const pre = make("pre");
      pre.append(make("code", body.join("\n")));
      container.append(pre);
      continue;
    }

    const header = line.match(heading);
    if (header) {
      const node = make("h" + Math.min(1 + header[1].length, 6));
      inline(node, header[2].replace(/\s+#+\s*$/, ""), view);
      container.append(node);
      i += 1;
      continue;
    }

    if (rule.test(line)) {
      container.append(make("hr"));
      i += 1;
      continue;
    }

    if (line.includes("|") && i + 1 < lines.length && tableSeparator(lines[i + 1])) {
      const wrapper = make("div", undefined, "table-scroll");
      const table = make("table");
      const headerCells = cells(line);
      const head = table.createTHead().insertRow();
      for (const text of headerCells) {
        const cell = make("th");
        cell.scope = "col";
        inline(cell, text, view);
        head.append(cell);
      }
      const body = table.createTBody();
      i += 2;
      while (i < lines.length && lines[i].includes("|") && lines[i].trim() && !startsBlock(lines[i])) {
        const row = body.insertRow();
        const parts = cells(lines[i++]);
        for (let col = 0; col < headerCells.length; col += 1) {
          inline(row.insertCell(), parts[col] || "", view);
        }
      }
      wrapper.append(table);
      container.append(wrapper);
      continue;
    }

    const item = line.match(listItem);
    if (item) {
      const ordered = Boolean(item[2]);
      const list = make(ordered ? "ol" : "ul");
      if (ordered) list.start = Number(item[2]);
      while (i < lines.length) {
        const next = lines[i].match(listItem);
        if (!next || Boolean(next[2]) !== ordered || rule.test(lines[i])) break;
        const li = make("li");
        inline(li, next[3], view);
        list.append(li);
        i += 1;
      }
      container.append(list);
      continue;
    }

    if (depth < 5 && quote.test(line)) {
      const block = make("blockquote");
      const body = [];
      while (i < lines.length) {
        const next = lines[i].match(quote);
        if (!next) break;
        body.push(next[1]);
        i += 1;
      }
      renderMarkdown(block, body.join("\n"), view, depth + 1);
      container.append(block);
      continue;
    }

    const paragraph = make("p");
    inline(paragraph, line, view);
    i += 1;
    while (i < lines.length && lines[i].trim() && !startsBlock(lines[i])) {
      if (lines[i].includes("|") && i + 1 < lines.length && tableSeparator(lines[i + 1])) break;
      if (/ {2}$/.test(lines[i - 1])) paragraph.append(make("br"));
      else paragraph.append(" ");
      inline(paragraph, lines[i], view);
      i += 1;
    }
    if (paragraph.textContent.trim() || paragraph.querySelector("a, code")) container.append(paragraph);
  }
}

const styledTags = new Set(["writingblock", "box", "caption", "row", "grid", "grid-item", "text", "list", "list-item", "divider", "button", "radio", "radio-group", "pressable"]);
const embeddedTags = new Set(["asyncimage", "asyncimagegroup", "cite", "link", "entity", "filecite"]);
const foreignTags = new Set(["svg", "path", "g", "math", "mrow", "mi", "mo", "mn", "ms", "mtext", "mfrac"]);
const styledToken = /^ {0,3}(?<fence>\x60{3,}|~{3,})[^\r\n]*|(?<code>\x60+)|(?<svg>\\?(?:<|&lt;)svg(?=[\s/>]|&gt;))|(?<!\\)\{(?:(?<each>#each)|(?<body>@body)|(?<condition>#if))(?=\s)|(?<math>(?<!\\)(?:\\\(|\\\[|\\begin\{(?:align(?:at)?|aligned|gather|equation|multline|split)\*?\})|(?<![\\$])(?:\$\$|\$(?![\s$])))|\\?(?:<|&lt;)(?<closing>\/?)(?<tag>[a-z][\w-]{0,63})(?=[\s/>]|&gt;)(?<attributes>[^<>\r\n]{0,1024}?)(?:>|&gt;)/gim;

function literalEnd(text, start, left, right) {
  let quote = "";
  let depth = 0;
  for (let i = start; i < text.length; i += 1) {
    const char = text[i];
    if (quote) {
      if (char === "\\") i += 1;
      else if (char === quote) quote = "";
    } else if (char === '"' || char === "'") {
      quote = char;
    } else if (char === left) {
      depth += 1;
    } else if (char === right && --depth === 0) {
      return i + 1;
    }
  }
  return -1;
}

function skipCode(text, token, start) {
  if (token.fence) {
    const end = new RegExp("^ {0,3}" + token.fence[0] + "{" + token.fence.length + ",}[ \\t]*\\r?$", "gm");
    end.lastIndex = start;
    return end.exec(text) ? end.lastIndex : text.length;
  }
  const end = text.indexOf(token.code, start);
  const lineEnd = text.indexOf("\n", start);
  return end >= 0 && (lineEnd < 0 || end < lineEnd) ? end + token.code.length : start;
}

function templateBranches(source, start, kind) {
  const scan = /^ {0,3}(?<fence>\x60{3,}|~{3,})[^\r\n]*|(?<code>\x60+)|(?<!\\)\{(?:#(?<open>if|each)(?=\s)|\/(?<close>if|each)\s*\}|(?<else>:else)\s*\})/gm;
  scan.lastIndex = start;
  const stack = [kind];
  let alternative = null;
  let invalid = false;
  for (let match; (match = scan.exec(source));) {
    const token = match.groups;
    if (token.fence || token.code) {
      scan.lastIndex = skipCode(source, token, scan.lastIndex);
    } else if (token.open) {
      stack.push(token.open);
    } else if (token.else && stack.length === 1) {
      if (alternative || kind !== "if") invalid = true;
      else alternative = {left: match.index, right: scan.lastIndex};
    } else if (token.close) {
      if (token.close !== stack.at(-1)) return null;
      stack.pop();
      if (!stack.length) {
        return {
          body: source.slice(start, alternative?.left ?? match.index),
          otherwise: alternative ? source.slice(alternative.right, match.index) : "",
          close: scan.lastIndex,
          invalid,
        };
      }
    }
  }
  return null;
}

const eachHeader = /^\s+as\s+([a-z_]\w*)(?:\s*,\s*([a-z_]\w*))?\s*\}/i;
const namedEachHeader = new RegExp("^\\s+(" + dataPath + ")" + eachHeader.source.slice(1), "i");
const rangeEachHeader = new RegExp(
  "^\\s+Array\\.from\\(\\s*\\{\\s*length\\s*:\\s*(\\d+|" + dataPath
  + ")\\s*\\}\\s*,\\s*\\(\\s*_,\\s*([a-z_]\\w*)\\s*\\)\\s*=>\\s*\\2\\s*\\)"
  + eachHeader.source.slice(1), "i"
);
const sliceEachHeader = new RegExp("^\\.slice\\(\\s*0\\s*,\\s*(\\d+|" + dataPath + ")\\s*\\)", "i");

function readDataLiteral(source) {
  // Gli export osservati usano anche chiavi senza virgolette e virgole finali.
  const text = source.replace(
    /"(?:[^"\\]|\\.)*"|([,{]\s*)([a-z_]\w*)(?=\s*:)|,(?=\s*[}\]])/gis,
    (value, prefix, key) => key ? prefix + '"' + key + '"' : value === "," ? "" : value,
  );
  try { return JSON.parse(text); }
  catch { return undefined; }
}

function readEach(text, opener, start) {
  const source = text.slice(start, start + 65536);
  const arrayStart = source.search(/\S/);
  const arrayEnd = source[arrayStart] === "[" ? literalEnd(source, arrayStart, "[", "]") : -1;
  const tail = arrayEnd > 0 ? source.slice(arrayEnd) : "";
  const slice = sliceEachHeader.exec(tail);
  const sliceLength = slice?.[0].length || 0;
  const header = arrayEnd > 0 ? eachHeader.exec(tail.slice(sliceLength)) : null;
  const named = header ? null : namedEachHeader.exec(source);
  const range = header || named ? null : rangeEachHeader.exec(source);
  const bodyStart = header ? arrayEnd + sliceLength + header[0].length
    : named?.[0].length ?? range?.[0].length ?? 0;
  const branches = templateBranches(source, bodyStart, "each");
  if (!branches) return null;
  const next = start + branches.close;
  const block = {raw: text.slice(opener, next), next};
  if ((!header && !named && !range) || branches.invalid) return block;

  const [item, index] = header ? header.slice(1) : named ? named.slice(2) : range.slice(3);
  const body = branches.body.trim();
  if (!body || item === index || /(?<!\\)\{@html\b/.test(body)) return block;
  const rows = header ? readDataLiteral(source.slice(arrayStart, arrayEnd)) : undefined;
  if (header && !Array.isArray(rows)) return block;
  const leadingIndex = /^\{\s*([a-z_]\w*)\s*\+\s*1\s*\}[ \t]*(?:\r?\n|$)/i.exec(body);
  const numbered = Boolean(leadingIndex && leadingIndex[1] === index);
  return {
    ...block, item, index, rows, collection: named?.[1], range: range?.[1], slice: slice?.[1],
    quizHint: ["questions", "qs"].includes(named?.[1]), numbered,
    body: numbered ? body.slice(leadingIndex[0].length).trimStart() : body,
  };
}

function readCondition(text, opener) {
  const source = text.slice(opener, opener + 65536);
  const header = /^\{#if\s+([^}\r\n]+)\}/.exec(source);
  if (!header) return null;
  const branches = templateBranches(source, header[0].length, "if");
  if (!branches) return null;
  const expression = header[1].trim();
  return {
    ...branches, expression, raw: source.slice(0, branches.close), next: opener + branches.close,
    quizHint: /^(?:revealed|show|(?:answers|ans)\[\s*i\s*\]\s*===\s*(?:item|q)\.c)$/.test(expression),
  };
}

function readTagBody(text, index, tag) {
  const close = new RegExp("\\\\?(?:<|&lt;)\\/" + tag + "\\s*(?:>|&gt;)", "i");
  const match = close.exec(text.slice(index, index + 65536));
  if (!match) return null;
  const end = index + match.index;
  return {text: text.slice(index, end), next: end + match[0].length};
}

const svgNamespace = "http://www.w3.org/2000/svg";
const svgAliases = new Map(
  ("alignmentBaseline baselineShift className clipPath clipRule colorInterpolation colorInterpolationFilters "
    + "dominantBaseline fillOpacity fillRule floodColor floodOpacity fontFamily fontSize fontStyle fontWeight "
    + "markerEnd markerMid markerStart paintOrder pointerEvents shapeRendering stopColor stopOpacity "
    + "strokeDasharray strokeDashoffset strokeLinecap strokeLinejoin strokeMiterlimit strokeOpacity strokeWidth "
    + "textAnchor textDecoration vectorEffect").split(" ").map((name) => [
      name.toLowerCase(),
      name === "className" ? "class" : name.replace(/[A-Z]/g, (char) => "-" + char.toLowerCase()),
    ]),
);
const excludedSvgTags = new Set([
  "script", "foreignobject", "animate", "animatecolor", "animatemotion", "animatetransform", "set", "discard",
]);

function readSvg(text, opener) {
  const source = text.slice(opener, opener + 262144);
  const encoded = /^\\?&lt;/.test(source);
  const tags = encoded
    ? /&lt;!--[\s\S]*?--&gt;|\\?&lt;(\/?)svg(?=[\s/]|&gt;)([\s\S]*?)&gt;/gi
    : /<!--[\s\S]*?-->|<!\[CDATA\[[\s\S]*?\]\]>|\\?<(\/?)svg(?=[\s/>])((?:"[^"]*"|'[^']*'|[^"'<>])*)>/gi;
  let depth = 0;
  for (const match of source.matchAll(tags)) {
    if (match[1] === undefined) continue;
    if (match[1]) depth -= 1;
    else if (!match[2].trimEnd().endsWith("/")) depth += 1;
    if (depth === 0) {
      const end = match.index + match[0].length;
      return {svg: source.slice(0, end), next: opener + end};
    }
  }
  return null;
}

function limitValue(source, bindings) {
  return /^\d+$/.test(source) ? Number(source) : dataValue(source, bindings);
}

function eachRows(block, bindings) {
  if (block.range) {
    const length = limitValue(block.range, bindings);
    if (!Number.isInteger(length) || length < 0 || length > 500) return undefined;
    return Array.from({length}, (_, index) => index);
  }
  const rows = block.rows ?? (block.collection ? dataValue(block.collection, bindings) : undefined);
  if (block.slice) {
    const end = limitValue(block.slice, bindings);
    return Array.isArray(rows) && Number.isInteger(end) ? rows.slice(0, end) : undefined;
  }
  return rows;
}

function rowBindings(block, bindings, row, index) {
  const child = new Map(bindings);
  child.set(block.item, row);
  if (block.index) child.set(block.index, index);
  return child;
}

const graphicOperators = new Map(
  [["||"], ["&&"], ["===", "!=="], ["<", ">", "<=", ">="], ["+", "-"], ["*", "/", "%"]]
    .flatMap((group, level) => group.map((operator) => [operator, level + 1])),
);

function graphicBinary(operator, left, right) {
  if (left === undefined) return undefined;
  if (operator === "||") return left || right;
  if (operator === "&&") return left && right;
  if (right === undefined) return undefined;
  if (operator === "===") return left === right;
  if (operator === "!==") return left !== right;
  if (operator === "+" && (typeof left === "string" || typeof right === "string")) return String(left) + String(right);
  if (!Number.isFinite(left) || !Number.isFinite(right)) return undefined;
  switch (operator) {
    case "+": return left + right;
    case "-": return left - right;
    case "*": return left * right;
    case "/": return left / right;
    case "%": return left % right;
    case "<": return left < right;
    case ">": return left > right;
    case "<=": return left <= right;
    case ">=": return left >= right;
  }
}

function graphicValue(expression, bindings) {
  const input = expression.trim();
  if (!input || input.length > 240) return undefined;
  const pattern = new RegExp(
    "\\s*(Math\\.floor|" + dataPath
    + '|"(?:[^"\\\\]|\\\\.)*"|\\d+(?:\\.\\d+)?|===|!==|<=|>=|\\|\\||&&|[?:!<>()+*/%\\-])', "gy",
  );
  const tokens = [];
  let offset = 0;
  while (offset < input.length) {
    pattern.lastIndex = offset;
    const token = pattern.exec(input);
    if (!token) return undefined;
    tokens.push(token[1]);
    offset = pattern.lastIndex;
  }
  let position = 0;
  let valid = true;
  const expect = (token) => {
    if (tokens[position++] !== token) valid = false;
  };
  const factor = () => {
    const token = tokens[position++];
    if (token === "+" || token === "-" || token === "!") {
      const value = factor();
      if (value === undefined) return undefined;
      if (token === "!") return !value;
      return Number.isFinite(value) ? (token === "-" ? -value : value) : undefined;
    }
    if (token === "(" || token === "Math.floor") {
      if (token === "Math.floor") expect("(");
      const value = parse(0);
      expect(")");
      return token === "Math.floor" ? Number.isFinite(value) ? Math.floor(value) : undefined : value;
    }
    if (token === undefined) {
      valid = false;
      return undefined;
    }
    if (/^["\d]/.test(token) || /^(?:true|false|null)$/.test(token)) return readDataLiteral(token);
    const value = dataValue(token, bindings);
    return value !== null && typeof value === "object" ? undefined : value;
  };
  const parse = (minimum) => {
    let left = factor();
    for (let level; (level = graphicOperators.get(tokens[position])) && level >= minimum;) {
      const operator = tokens[position++];
      left = graphicBinary(operator, left, parse(level + 1));
    }
    if (minimum === 0 && tokens[position] === "?") {
      position += 1;
      const yes = parse(0);
      expect(":");
      const no = parse(0);
      return left === undefined ? undefined : left ? yes : no;
    }
    return left;
  };
  const value = parse(0);
  if (!valid || position !== tokens.length || (typeof value === "number" && !Number.isFinite(value))) return undefined;
  return value;
}

function svgMarkup(source, bindings, budget, depth = 0) {
  if (depth > 8 || source.length > 262144) return null;
  const scan = /\{#(each|if)(?=\s)/g;
  const pieces = [];
  let offset = 0;
  let valid = true;
  const xml = (value) => String(value ?? "").replace(/[&<>"'{}]/g, (char) =>
    ({"&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&apos;", "{": "&#123;", "}": "&#125;"})[char]);
  const valueFor = (raw, expression) => {
    const value = graphicValue(expression, bindings);
    if (value !== undefined) return xml(value);
    valid = false;
    return raw;
  };
  const interpolate = (fragment) => fragment
    .replace(/(=\s*)\{\x60([^\x60]*)\x60\}/g, (_, attribute, template) =>
      attribute + '"' + template.split(/(\$\{[^{}\r\n]+\})/g).map((item, index) =>
        index % 2 ? valueFor(item, item.slice(2, -1)) : xml(item)).join("") + '"')
    .replace(/(=\s*)?\{([^{}\r\n]+)\}/g, (raw, attribute, expression) => {
      const value = valueFor(raw, expression);
      return attribute ? attribute + '"' + value + '"' : value;
    });
  for (let match; (match = scan.exec(source));) {
    pieces.push(interpolate(source.slice(offset, match.index)));
    if (match[1] === "if") {
      const block = readCondition(source, match.index);
      const chosen = block && !block.invalid ? graphicValue(block.expression, bindings) : undefined;
      if (chosen === undefined) return null;
      const child = svgMarkup(chosen ? block.body : block.otherwise, bindings, budget, depth + 1);
      if (child === null) return null;
      pieces.push(child);
      scan.lastIndex = block.next;
    } else {
      const block = readEach(source, match.index, scan.lastIndex);
      const rows = block && eachRows(block, bindings);
      if (!Array.isArray(rows) || rows.length > budget.remaining || !block.body) return null;
      budget.remaining -= rows.length;
      for (const [index, row] of rows.entries()) {
        const child = svgMarkup(block.body, rowBindings(block, bindings, row, index), budget, depth + 1);
        if (child === null) return null;
        pieces.push(child);
      }
      scan.lastIndex = block.next;
    }
    offset = scan.lastIndex;
  }
  pieces.push(interpolate(source.slice(offset)));
  const result = pieces.join("");
  return valid && result.length <= 262144 ? result : null;
}

function renderDiagram(raw, view) {
  const rendered = /\{#(?:each|if)|=\s*\{/.test(raw)
    ? svgMarkup(raw, view.bindings, {remaining: 1500})
    : raw;
  if (rendered === null) return null;
  let source = rendered.replace(/\\(?=(?:<|&lt;)\/?[a-z])/gi, "");
  if (source.startsWith("&lt;")) {
    source = source.replace(/&lt;|&gt;|&quot;/g, (entity) => ({"&lt;": "<", "&gt;": ">", "&quot;": '"'})[entity]);
  }
  if (/<!\s*(?:DOCTYPE|ENTITY)\b/i.test(source)) return null;
  if (!/^<svg[\s>]/.test(source)) return null;
  // Le risposte generate spesso omettono il namespace, necessario per un'immagine autonoma.
  if (!/^<svg(?:[^"'<>]|"[^"]*"|'[^']*')*\sxmlns\s*=/.test(source)) {
    source = source.replace(/^<svg\b/, '<svg xmlns="' + svgNamespace + '"');
  }
  const documentSvg = new DOMParser().parseFromString(source, "image/svg+xml");
  const root = documentSvg.documentElement;
  if (root.localName !== "svg" || root.namespaceURI !== svgNamespace) return null;

  const elements = [root, ...root.querySelectorAll("*")];
  if (elements.length > 5000) return null;
  for (const element of elements) {
    if (element.namespaceURI !== svgNamespace || excludedSvgTags.has(element.localName.toLowerCase())) {
      element.remove();
      continue;
    }
    for (const attr of [...element.attributes]) {
      const name = attr.name.toLowerCase();
      const value = attr.value.trim();
      const localReference = /^#[^\s"'<>]+$/.test(value);
      const rasterData = element.localName === "image"
        && /^data:image\/(?:png|jpeg|webp|avif);base64,[a-z0-9+/=]+$/i.test(value);
      if (name.startsWith("on") || name === "xml:base" || name === "src"
          || (attr.localName === "href" && !localReference && !rasterData)
          || [...value.matchAll(/url\(\s*(['"]?)(.*?)\1\s*\)/gi)].some((match) => !match[2].startsWith("#"))) {
        element.removeAttributeNode(attr);
      } else if (svgAliases.has(name)) {
        const canonical = svgAliases.get(name);
        element.removeAttributeNode(attr);
        if (!element.hasAttribute(canonical)) element.setAttribute(canonical, value);
      }
    }
  }
  if (!root.hasAttribute("color")) {
    const style = documentSvg.createElementNS(svgNamespace, "style");
    style.textContent = ":root{color:#242236}@media(prefers-color-scheme:dark){:root{color:#f5f1fa}}@media print{:root{color:#17171d}}";
    root.prepend(style);
  }
  // Il browser elabora l'SVG in un documento immagine isolato, con i suoi ID locali.
  const image = make("img", undefined, "output-svg");
  image.alt = root.querySelector(":scope > title")?.textContent.trim() || "Diagramma";
  image.src = "data:image/svg+xml;charset=utf-8," + encodeURIComponent(new XMLSerializer().serializeToString(root));
  return image;
}

function readBodyDirective(text, opener) {
  const source = text.slice(opener, opener + 65536);
  const end = literalEnd(source, 0, "{", "}");
  if (end < 0) return null;
  const raw = source.slice(0, end);
  const statement = raw.slice("{@body".length, -1).trim();
  const block = {raw, next: opener + end};

  const state = /^const\s*\[\s*([a-z_]\w*)\s*,\s*[a-z_]\w*\s*\]\s*=\s*DIL\.useState\(([\s\S]*)\)\s*;?$/i.exec(statement);
  if (state) {
    const initial = readDataLiteral(state[2]);
    if (initial !== undefined) {
      const quizHint = ["answers", "ans", "revealed", "show"].includes(state[1]);
      return {...block, binding: [state[1], initial], quizHint};
    }
  }
  const definition = /^const\s+(questions|qs)\s*=\s*(\[[\s\S]*\])\s*;?$/.exec(statement);
  if (!definition) return block;
  const items = readDataLiteral(definition[2]);
  if (!Array.isArray(items) || !items.length || items.length > 100) return block;
  const quiz = items.map((item) => item && ({q: item.q, a: item.a ?? item.opts, c: item.c, why: item.why}));
  const valid = (item) => item && typeof item.q === "string" && item.q.trim()
    && Array.isArray(item.a) && item.a.length >= 2 && item.a.length <= 12
    && item.a.every((answer) => typeof answer === "string")
    && Number.isInteger(item.c) && item.c >= 0 && item.c < item.a.length
    && (item.why === undefined || typeof item.why === "string");
  return quiz.every(valid) ? {...block, quiz} : block;
}

function isQuizScore(text) {
  return /^\{(questions|qs)\.filter\(\((\w+),(\w+)\)=>(?:answers|ans)\[\3\]===\2\.c\)\.length\}\/\{\1\.length\}$/.test(text.replace(/\s+/g, ""));
}

function readComponent(text, match, start) {
  const token = match.groups;
  const opener = match.index;
  if (token.svg) return readSvg(text, opener);
  if (token.each) {
    const block = readEach(text, opener, start);
    if (!block) return null;
    return {each: block, next: block.next, quizHint: block.quizHint};
  }
  if (token.body) {
    const block = readBodyDirective(text, opener);
    if (!block) return null;
    return block.quiz || block.binding ? block : {...block, widgetCode: true};
  }
  if (token.condition) {
    const condition = readCondition(text, opener);
    return condition && {condition, next: condition.next, quizHint: condition.quizHint};
  }
  const tag = token.tag?.toLowerCase();
  if (token.closing) return null;
  if (embeddedTags.has(tag) && token.attributes.trimEnd().endsWith("/")) {
    const attributes = blockAttributes(token.attributes);
    return {
      embedded: {type: tag, query: attributes.get("query"), url: attributes.get("url"), title: attributes.get("title")},
      next: start,
    };
  }
  if (tag !== "title") return null;
  const content = readTagBody(text, start, tag);
  if (!content) return null;
  const attributes = blockAttributes(token.attributes);
  return {
    title: {text: content.text, size: attributes.get("size"), tabular: /\btabularNums\b/i.test(token.attributes)},
    next: content.next,
  };
}

function readFormula(text, index, opener) {
  const environment = opener.startsWith("\\begin{");
  const closer = environment ? opener.replace("begin", "end")
    : opener === "\\(" ? "\\)" : opener === "\\[" ? "\\]" : opener;
  const display = opener !== "$" && opener !== "\\(";
  let groups = 0;
  const limit = Math.min(text.length, index + 65536);
  for (let end = index; end < limit; end += 1) {
    if (groups === 0 && text.startsWith(closer, end)) {
      const next = end + closer.length;
      if (opener === "$" && (/\s/.test(text[end - 1]) || /[\d$]/.test(text[next] || ""))) return null;
      const raw = text.slice(index - opener.length, next);
      const tex = environment ? raw : text.slice(index, end).trim();
      return tex ? {tex, raw, display, next} : null;
    }
    if (!display && text[end] === "\n" && /^\s*\n/.test(text.slice(end + 1, end + 60))) return null;
    if (groups === 0 && opener !== closer && text.startsWith(opener, end)) return null;
    if (text[end] === "\\") end += 1;
    else if (text[end] === "{") groups += 1;
    else if (text[end] === "}" && groups) groups -= 1;
  }
  return null;
}

function* markupTokens(text, formulas) {
  const scan = new RegExp(styledToken);
  let offset = 0;
  for (let match; (match = scan.exec(text));) {
    const token = match.groups;
    if (token.fence || token.code) {
      scan.lastIndex = skipCode(text, token, scan.lastIndex);
    } else if (token.math) {
      const formula = readFormula(text, scan.lastIndex, token.math);
      if (!formula) continue;
      if (offset < match.index) yield text.slice(offset, match.index);
      yield "\uFDD0" + (formulas.push(formula) - 1) + "\uFDD1";
      scan.lastIndex = formula.next;
      offset = formula.next;
    } else {
      const component = readComponent(text, match, scan.lastIndex);
      if (component) {
        if (offset < match.index) yield text.slice(offset, match.index);
        yield component;
        scan.lastIndex = component.next;
        offset = component.next;
        continue;
      }
      if (!token.tag) continue;
      const tag = token.tag.toLowerCase();
      const custom = tag.includes("-") || tag.includes("_") || document.createElement(tag) instanceof HTMLUnknownElement;
      if (!styledTags.has(tag) && (!custom || foreignTags.has(tag))) continue;
      if (offset < match.index) yield text.slice(offset, match.index);
      yield {tag, closing: Boolean(token.closing), attributes: token.attributes, raw: match[0]};
      offset = scan.lastIndex;
    }
  }
  if (offset < text.length) yield text.slice(offset);
}

function styledParts(parts, view) {
  const tokens = [];
  let run = [];
  const flush = () => {
    for (const token of markupTokens(run.join("\n"), view.formulas)) tokens.push(token);
    run = [];
  };
  for (const part of parts) {
    if (typeof part === "string") run.push(part);
    else {
      flush();
      tokens.push(part);
    }
  }
  flush();
  const hasQuiz = tokens.some((token) => token.quiz);
  for (const token of tokens) {
    if (token.binding && !(token.quizHint && hasQuiz)) view.bindings.set(...token.binding);
  }
  const root = {children: []};
  const stack = [root];
  const append = (token) => {
    const parent = stack.at(-1);
    if (token.binding || (token.quizHint && hasQuiz)) return;
    if (token.quizHint && !token.each && !token.condition) {
      parent.children.push({...token, widgetCode: true});
    } else if (token.title) {
      if (!hasQuiz || !isQuizScore(token.title.text)) {
        parent.children.push("\uFDD2" + (view.titles.push(token.title) - 1) + "\uFDD3");
      }
    } else if (typeof token === "string" || !token.tag) {
      parent.children.push(token);
    } else if (token.closing) {
      const open = stack.findLastIndex((node) => node.tag === token.tag);
      if (open > 0) {
        stack[open].closed = true;
        stack.length = open;
      } else if (!styledTags.has(token.tag)) {
        parent.children.push(token.raw);
      }
    } else if (stack.length < 32) {
      const ended = token.attributes.trimEnd().endsWith("/");
      const node = {...token, closed: ended, children: []};
      parent.children.push(node);
      if (!ended) stack.push(node);
    } else {
      parent.children.push(token.raw);
    }
  };

  for (const token of tokens) append(token);
  const normalize = (children) => {
    const result = [];
    const push = (part) => {
      const last = result.length - 1;
      if (typeof part === "string" && typeof result[last] === "string") result[last] += part;
      else result.push(part);
    };
    for (const part of children) {
      if (typeof part !== "string" && part.tag) {
        part.children = normalize(part.children);
        if (!styledTags.has(part.tag)) {
          if (!part.closed) push(part.raw);
          for (const child of part.children) push(child);
          continue;
        }
      }
      push(part);
    }
    return result;
  };
  return normalize(root.children);
}

function mappedAttachment(container, key, view) {
  const ref = view.refs.get(key);
  if (!ref || !Object.hasOwn(view.files, key)) return;
  view.shown.add(key);
  const file = view.files[key];
  if (ref.image && isImage(file)) {
    const figure = make("figure", undefined, "mapped-image");
    figure.append(linkedImage(file, file.name));
    container.append(figure);
  } else {
    container.append(fileLink(file));
  }
}

function blockAttributes(source) {
  const decoded = source.replace(/&quot;|&apos;|&#39;/g, (entity) => entity === "&quot;" ? '"' : "'");
  const attributes = new Map();
  const pattern = /([\w-]+)\s*=\s*(?:"([^"]*)"|'([^']*)'|([^\s/]+))/g;
  for (const match of decoded.matchAll(pattern)) {
    attributes.set(match[1].toLowerCase(), match[2] ?? match[3] ?? match[4]);
  }
  return attributes;
}

function writingBlock(container, node, view) {
  const attributes = blockAttributes(node.attributes);
  const children = node.children.filter((child) => typeof child !== "string" || child.trim());
  const innerId = children.length === 1 && typeof children[0] === "string" ? children[0].trim() : "";
  const explicitId = attributes.get("id") || attributes.get("block-id") || attributes.get("block_id")
    || attributes.get("data-block-id");
  let block = view.blocks.get(explicitId || innerId);
  if (!block && !explicitId) {
    const remaining = [...view.blocks.values()].filter((entry) => !view.usedBlocks.has(entry.id));
    if (remaining.length === 1) block = remaining[0];
  }

  const hasInline = children.length > 0 && innerId !== block?.id;
  const useSaved = block?.text && (!hasInline || !node.closed);
  if (block && !useSaved && !hasInline && !Object.hasOwn(view.files, block.ref)) return;
  const element = make("div", undefined, "message-writing");
  const title = attributes.get("title") || attributes.get("subject") || block?.title || "Blocco di scrittura";
  element.append(make("p", title, "writing-title"));
  if (useSaved) {
    renderSavedText(element, block.text, view);
  } else if (hasInline) {
    renderStyled(element, node.children, view);
  } else if (!block) {
    element.append(make("p", "Contenuto del blocco non disponibile.", "in-line-missing"));
  }
  if (block) {
    view.usedBlocks.add(block.id);
    view.shown.add(block.ref);
    if (Object.hasOwn(view.files, block.ref)) mappedAttachment(element, block.ref, view);
  }
  container.append(element);
  if (useSaved && !node.closed && hasInline) renderStyled(container, node.children, view);
}

function renderStyled(container, parts, view) {
  for (const part of parts) {
    if (typeof part === "string") {
      renderMarkdown(container, part, view);
    } else if (part.ref) {
      mappedAttachment(container, part.ref, view);
    } else if (part.each) {
      renderEach(container, part.each, view);
    } else if (part.condition) {
      renderCondition(container, part.condition, view);
    } else if (part.embedded) {
      renderEmbedded(container, part.embedded, view);
    } else if (part.svg) {
      container.append(renderDiagram(part.svg, view) || literalBlock(part.svg));
    } else if (part.quiz) {
      container.append(renderQuiz(part.quiz));
    } else if (part.widgetCode) {
      container.append(literalBlock(part.raw));
    } else if (part.tag === "writingblock") {
      writingBlock(container, part, view);
    } else {
      const element = part.tag === "divider" ? "hr" : part.tag === "list" ? "ul" : part.tag === "list-item" ? "li" : "div";
      const nested = make(element, undefined, "message-" + part.tag);
      renderStyled(nested, part.children, view);
      if (element === "hr" || nested.hasChildNodes()) container.append(nested);
    }
  }
}

function literalBlock(source) {
  const pre = make("pre");
  pre.append(make("code", source));
  return pre;
}

function renderQuiz(questions) {
  const form = make("form", undefined, "archive-quiz");
  form.append(make("h3", "Quiz"));
  const fields = questions.map((question, index) => {
    const field = make("fieldset", undefined, "quiz-question");
    field.append(make("legend", (index + 1) + ". " + question.q));
    const choices = question.a.map((answer) => {
      const radio = make("input");
      radio.type = "radio";
      radio.name = "risposta-" + index;
      const label = make("label", undefined, "quiz-option");
      label.append(radio, make("span", answer));
      field.append(label);
      return radio;
    });
    const feedback = make("div", undefined, "quiz-feedback");
    feedback.hidden = true;
    field.append(feedback);
    form.append(field);
    return {choices, feedback};
  });
  const button = make("button", "Verifica risposte", "quiz-check");
  button.type = "submit";
  const score = make("p", undefined, "quiz-score");
  score.setAttribute("role", "status");
  score.hidden = true;
  form.append(button, score);
  form.addEventListener("submit", (event) => {
    event.preventDefault();
    let correct = 0;
    for (const [index, question] of questions.entries()) {
      const {choices, feedback} = fields[index];
      const matched = choices[question.c].checked;
      if (matched) correct += 1;
      feedback.replaceChildren(
        make("p", matched ? "Corretto" : "Risposta corretta: " + question.a[question.c]),
      );
      if (question.why) feedback.append(make("p", question.why));
      feedback.hidden = false;
    }
    score.hidden = false;
    score.textContent = "Risposte corrette: " + correct + " / " + questions.length;
  });
  form.addEventListener("change", () => {
    score.hidden = true;
    for (const {feedback} of fields) feedback.hidden = true;
  });
  return form;
}

function appendSourceLinks(container, links, view, deduplicate = false) {
  for (const source of links) {
    if (deduplicate && view.shownUrls.has(source.url)) continue;
    const link = externalLink(source.url, source.title || "Fonte");
    if (link) {
      link.className = "citation";
      view.shownUrls.add(source.url);
      container.append(link);
    }
  }
}

function renderEmbedded(container, part, view) {
  const candidates = view.components.filter((entry) => entry.type === part.type && !view.shownComponents.has(entry));
  const component = (part.query && candidates.find((entry) => entry.queries.includes(part.query))) || candidates[0];
  const line = make("p", undefined, "embedded-links");
  if (component) {
    view.shownComponents.add(component);
    appendSourceLinks(line, component.links, view, true);
  } else if (part.url) {
    appendSourceLinks(line, [{url: part.url, title: part.title}], view, true);
  }
  if (line.hasChildNodes()) container.append(line);
}

const equalityTest = new RegExp("^(" + dataPath + ")\\s*(===|!==)\\s*(.+)$", "i");

function conditionValue(expression, bindings) {
  const comparison = equalityTest.exec(expression);
  if (comparison) {
    const left = dataValue(comparison[1], bindings);
    let right;
    try { right = JSON.parse(comparison[3]); }
    catch { right = dataValue(comparison[3], bindings); }
    if (left === undefined || right === undefined || (left && typeof left === "object")
        || (right && typeof right === "object")) return undefined;
    return comparison[2] === "===" ? left === right : left !== right;
  }
  const inverse = expression.startsWith("!");
  const path = inverse ? expression.slice(1).trim() : expression;
  const value = path === "true" || path === "false" ? path === "true" : dataValue(path, bindings);
  return value === undefined ? undefined : inverse ? !value : Boolean(value);
}

function renderCondition(container, block, view) {
  const matched = block.invalid ? undefined : conditionValue(block.expression, view.bindings);
  if (matched === undefined || view.depth >= 12) {
    container.append(literalBlock(block.raw));
    return;
  }
  const chosen = matched ? block.body : block.otherwise;
  renderStyled(container, styledParts([chosen], view), {...view, depth: view.depth + 1});
}

function renderEach(container, block, view) {
  const rows = eachRows(block, view.bindings);
  if (!Array.isArray(rows) || rows.length > 500 || rows.length > view.repeatBudget.remaining || view.depth >= 12
      || rows.length * block.body.length + JSON.stringify(rows).length * (block.body.split("{").length - 1) > 1000000) {
    container.append(literalBlock(block.raw));
    return;
  }
  if (!rows.length) return;
  view.repeatBudget.remaining -= rows.length;
  const list = make(block.numbered ? "ol" : "ul", undefined, "each-list");
  const parts = styledParts([block.body], view);
  for (const [position, row] of rows.entries()) {
    const entry = make("li");
    const bindings = rowBindings(block, view.bindings, row, position);
    renderStyled(entry, parts, {...view, bindings, depth: view.depth + 1});
    list.append(entry);
  }
  container.append(list);
}

function renderSavedText(container, text, view) {
  renderStyled(container, styledParts([text], view), {...view, blocks: new Map()});
}

function renderMessage(container, message, files, refs) {
  const view = {
    files, refs, shown: new Set(), usedBlocks: new Set(),
    blocks: new Map(message.blocks.map((block) => [block.id, block])),
    formulas: [], titles: [],
    bindings: new Map(), depth: 0, repeatBudget: {remaining: 10000},
    sources: new Map((message.sources || []).map((source) => [source.marker, source])),
    components: message.components || [], shownComponents: new Set(), shownUrls: new Set(),
  };
  renderStyled(container, styledParts(message.parts, view), view);
  const rawText = message.parts.filter((part) => typeof part === "string").join("\n");
  for (const block of message.blocks) {
    if (block.text && !view.usedBlocks.has(block.id) && !rawText.includes(block.text.trim())) {
      // Il blocco salvato nei metadati può non avere un marcatore testuale.
      const element = make("div", undefined, "message-writing");
      element.append(make("p", block.title || "Blocco di scrittura", "writing-title"));
      renderSavedText(element, block.text, view);
      view.shown.add(block.ref);
      if (Object.hasOwn(files, block.ref)) mappedAttachment(element, block.ref, view);
      container.append(element);
    }
  }
  for (const key of message.refs) {
    if (!view.shown.has(key)) mappedAttachment(container, key, view);
  }
  for (const component of view.components) {
    if (!view.shownComponents.has(component)) renderEmbedded(container, component, view);
  }
}

const index = window.archivioIndice;
const rows = index.chats;
const rowByKey = new Map(rows.map((row) => [row.key, row]));
const chatRequests = new Map();
const fileRequests = new Map();
const searchRequests = new Map();
const inFlight = new Map();

function loadData(url, pending, key) {
  if (inFlight.has(url)) return inFlight.get(url);
  const request = new Promise((resolve, reject) => {
    const script = make("script");
    script.src = url;
    let payload;
    pending.set(key, (data) => { payload = data; });
    const dispose = () => {
      pending.delete(key);
      script.remove();
    };
    script.onload = () => {
      dispose();
      if (payload === undefined) reject(new Error("Dati del file mancanti"));
      else resolve(payload);
    };
    script.onerror = () => {
      dispose();
      reject(new Error("File dell'archivio non leggibile"));
    };
    document.head.append(script);
  }).finally(() => inFlight.delete(url));
  inFlight.set(url, request);
  return request;
}

window.registraChat = (data) => chatRequests.get(data.key)?.(data);
window.registraFile = (data) => fileRequests.get(data.key)?.(data);
window.registraRicerca = (data) => {
  const source = document.currentScript?.getAttribute("src");
  searchRequests.get(source)?.(data);
};

let activeKey = "";
let shown = rows;
let limit = 120;
let rendered = 0;
let searchRun = 0;
let openRun = 0;
let searchTimer;
let cache = null;
const resultLabel = new Intl.NumberFormat("it");
const dateFormat = new Intl.DateTimeFormat("it", {dateStyle: "medium"});
const publishedDate = (value) => value == null ? "" : dateFormat.format(new Date(value * 1000));

function selectHistory() {
  for (const link of byId("history").children) {
    if (link.dataset.key === activeKey) link.setAttribute("aria-current", "page");
    else link.removeAttribute("aria-current");
  }
}

function showHistory(reset = false) {
  const history = byId("history");
  if (reset) {
    history.replaceChildren();
    rendered = 0;
  }
  const group = document.createDocumentFragment();
  for (const row of shown.slice(rendered, limit)) {
    const link = make("a", row.title, "chat-link");
    link.href = "#" + row.key;
    link.dataset.key = row.key;
    link.title = row.archived ? row.title + " · Archiviata" : row.title;
    if (row.key === activeKey) link.setAttribute("aria-current", "page");
    group.append(link);
  }
  rendered = Math.min(shown.length, limit);
  history.append(group);
  byId("more").hidden = shown.length <= limit;
}

function updateResults(label) {
  byId("results").textContent = label ?? (resultLabel.format(shown.length) + (shown.length === 1 ? " conversazione" : " conversazioni"));
}

function setSidebar(open) {
  byId("sidebar").classList.toggle("open", open);
  byId("open-sidebar").setAttribute("aria-expanded", String(open));
}

function renderChat(chat, fileData) {
  const article = byId("conversation");
  const files = fileData.links;
  const refs = new Map(chat.refs.map((ref) => [ref.key, ref]));
  const content = document.createDocumentFragment();
  for (const message of chat.messages) {
    const prose = make("div", undefined, "prose");
    renderMessage(prose, message, files, refs);
    if (!prose.hasChildNodes()) continue;
    const section = make("section", undefined, "message " + message.role);
    const label = {user: "Utente", assistant: "ChatGPT", transcript: "Trascrizione"};
    section.append(make("p", label[message.role], "role"), prose);
    content.append(section);
  }
  if (!content.childNodes.length) content.append(make("p", "Nessun messaggio testuale."));

  const missing = chat.refs.filter((ref) => !Object.hasOwn(files, ref.key));
  if (fileData.all.length || missing.length) {
    const attachments = make("details", undefined, "attachments");
    attachments.append(make("summary", "Allegati"));
    if (fileData.all.length) {
      const list = make("ul", undefined, "attachment-list");
      for (const file of fileData.all) {
        const item = make("li");
        item.append(fileLink(file));
        list.append(item);
      }
      attachments.append(list);
    }
    const unavailable = make("details", undefined, "missing-files");
    unavailable.append(make("summary", "Allegati non disponibili"));
    if (missing.length) {
      const list = make("ul");
      for (const ref of missing) {
        const item = make("li");
        if (ref.id) {
          item.append(make("code", ref.id));
          if (ref.name && ref.name !== ref.id) item.append(" · " + ref.name);
        } else {
          item.textContent = ref.name + " (senza ID)";
        }
        list.append(item);
      }
      unavailable.append(list);
    } else {
      unavailable.append(make("p", "Nessun allegato mancante."));
    }
    attachments.append(unavailable);
    content.append(attachments);
  }
  article.replaceChildren(content);
}

async function openChat() {
  const key = location.hash.slice(1);
  const request = ++openRun;
  byId("print-chat").disabled = true;
  if (!validKey(key) || !rowByKey.has(key)) {
    activeKey = "";
    document.title = "Archivio chat";
    selectHistory();
    byId("title").textContent = "Conversazioni";
    byId("date").textContent = "";
    const empty = make("div", undefined, "placeholder");
    empty.append(make("h1", "Apri una conversazione"), make("p", "Sceglila dalla cronologia o usa la ricerca."));
    byId("conversation").replaceChildren(empty);
    return;
  }
  activeKey = key;
  const row = rowByKey.get(key);
  document.title = row.title;
  byId("title").textContent = row.title;
  byId("date").textContent = publishedDate(row.updated);
  byId("date").title = row.archived ? "Conversazione archiviata" : "";
  selectHistory();
  setSidebar(false);
  byId("conversation").replaceChildren(make("p", "Caricamento…", "placeholder"));
  try {
    if (cache?.key !== key) {
      const [chat, files] = await Promise.all([
        loadData("chat/" + key + ".js", chatRequests, key),
        row.hasFiles
          ? loadData("chat/" + key + "-file.js", fileRequests, key).catch(() => ({links: {}, all: []}))
          : {links: {}, all: []},
      ]);
      if (request !== openRun) return;
      cache = {key, chat, files};
    }
    renderChat(cache.chat, cache.files);
    byId("print-chat").disabled = false;
    byId("main").scrollTop = 0;
    byId("conversation").focus({preventScroll: true});
  } catch {
    if (request === openRun) {
      byId("conversation").replaceChildren(make("p", "Impossibile aprire la conversazione.", "placeholder"));
    }
  }
}

let expandedForPrint = null;
window.addEventListener("beforeprint", () => {
  const attachments = byId("conversation").querySelector(".attachments");
  if (attachments && !attachments.open) {
    expandedForPrint = attachments;
    attachments.open = true;
  }
});
window.addEventListener("afterprint", () => {
  if (expandedForPrint) expandedForPrint.open = false;
  expandedForPrint = null;
});

byId("print-chat").addEventListener("click", async (event) => {
  const button = event.currentTarget;
  const request = openRun;
  button.disabled = true;
  try {
    const images = [...byId("conversation").querySelectorAll("img")];
    await Promise.allSettled(images.map((image) => {
      image.loading = "eager";
      return image.decode();
    }));
    if (request === openRun) window.print();
  } finally {
    if (request === openRun) button.disabled = false;
  }
});

let queue = Promise.resolve();
async function search(terms, run) {
  const matches = new Set(rows.filter((row) => {
    const title = fold(row.title);
    return terms.every((term) => title.includes(term));
  }).map((row) => row.key));
  const present = (label) => {
    shown = rows.filter((row) => matches.has(row.key));
    showHistory(true);
    updateResults(label);
  };
  present(index.shards ? "Ricerca nei messaggi…" : undefined);
  let failed = false;
  for (let part = 1; part <= index.shards && run === searchRun; part += 1) {
    const url = "ricerca/" + part + ".js";
    try {
      const records = await loadData(url, searchRequests, url);
      if (run !== searchRun) return;
      for (const [key, text] of records) {
        if (matches.has(key)) continue;
        const searchable = fold(text);
        if (terms.every((term) => searchable.includes(term))) matches.add(key);
      }
    } catch {
      failed = true;
    }
  }
  if (run === searchRun) present(failed ? "Ricerca parziale · " + resultLabel.format(matches.size) + " risultati" : undefined);
}

byId("search").addEventListener("input", (event) => {
  const terms = fold(event.target.value).trim().split(/\s+/).filter(Boolean);
  const run = ++searchRun;
  clearTimeout(searchTimer);
  limit = 120;
  if (!terms.length) {
    shown = rows;
    showHistory(true);
    updateResults();
    return;
  }
  updateResults("Ricerca…");
  searchTimer = setTimeout(() => {
    queue = queue.catch(() => {}).then(() => {
      if (run === searchRun) return search(terms, run);
    });
  }, 180);
});
byId("more").addEventListener("click", () => { limit += 120; showHistory(); });
byId("close-attachment").addEventListener("click", () => byId("attachment").close());
byId("attachment").addEventListener("close", () => byId("attachment-content").replaceChildren());
byId("open-sidebar").addEventListener("click", () => { setSidebar(true); byId("search").focus(); });
byId("close-sidebar").addEventListener("click", () => { setSidebar(false); byId("open-sidebar").focus(); });
byId("history").addEventListener("click", (event) => {
  const link = event.target.closest("a");
  if (link?.dataset.key === activeKey) setSidebar(false);
});
document.addEventListener("keydown", (event) => {
  if (event.key === "Escape" && byId("sidebar").classList.contains("open")) {
    setSidebar(false);
    byId("open-sidebar").focus();
  }
});
window.addEventListener("hashchange", openChat);
if (!location.hash && rows.length && !matchMedia("(max-width: 730px)").matches) {
  history.replaceState(null, "", "#" + rows[0].key);
}
showHistory();
updateResults();
openChat();
"""


# Distribuzione KaTeX 0.19.1 e licenza MIT, compresse per il generatore autonomo.
# https://www.npmjs.com/package/katex/v/0.19.1
KATEX_0191 = (
    b'ABzY8000000t4i|`*#~Rk}&#v{)!G~k0VV;8gDe@C^;U-8T%VM&f1>Geztd)rrH)a6seGu?Tn-U{Z<u#egZ8gGduU5bMp|pQ'
    b'Fs;#^#V|R`}dz0e_A}R{#LL#cXR&P^Yf~B{P214Xtk_1`>HsCm*dYqySKgkdAE9dekhKX$Aw^A&IA*z_|y4fQ`}uH&Z>)_i^'
    b'b-w*dETSU9sG54!hOstHXA;|Ln6Ls@=tEzh7-P#cE#wyn6j}@piY^9ICTv@n%<5#r93HJYVeIR@36Jg^GSIF00)h3T$5=7OT'
    b'x_^R`$(t(QN4200JsP|be(=J0;8tEkn*e!pF=7EoVtwq0IbRGY)%fbG3mt*d=;1f41-&q9gGF*R~lE!Ll{HU+{WsCd6RoNuo'
    b'V#je`J0G3$Cv{-GH>#H;DLj<o^7pu?$7AMT?KZ8oI_AmnM=d`%ko~_>CZ$-npyn4M}?a!yh*=p}`1BrV~Bt%bf9JjZ-VqdM-'
    b'pFs^P=r@hE=^f?4u3sWT4*^AcOnHC4z37f)wg2qR)oue#R#fI}3n-%&|EF3WFa@)}*{;{y_c%0|va=PAXn*n<QNiN%_FY9|@'
    b'e{w<9-ynf3pfFn&8!B{{(P}s7q6>;FlZOhyJ!t(ht2N~F!8I!y13l#sJ;F$=g|Ix`^B>--#!29-KY19htG;1o<8}@!*B0@TT'
    b'JdggZIg_`0K;x51#z_c>y_|-u?dhe-ux?EAD>(AH|;@e*f*Xxc|2wp5A}<ta$SDvxkp=c=Yf-q&@up-lIQ%`|$fe7Jq<p-#-'
    b'Bwe+a}4l|Fw`V1uEmhxeahWsmPaz4riK@BZQ8qleG`WBS>551)UJb$<8cX>qss;qKGt5AXf?(cP!T4}X68!;@$Cq4{s2+V3B'
    b'J|J_q)<^JRQ-#?#2tB_XQ{|men&mP=;^az{!?CzhT_fN5Z#l0s#{KwOWe|+$~c<|)WxA!6O5BH&GcmMF{zHbT!b??#LhmWVl'
    b'w|5`k{o{Qq^#p2p`Wa^Py(|9u;65f{dw1c#d(R&}`5uRH@5%SipTg@DM)~x4UG%RHpWUApcb`6dh6wrY>66FP&k&tZ;t5p(<'
    b'-Wh~Ye7^Oof&~F`2Oc-_v;FaZ|~oI1a&>b!Z^g3dH&h&Z-4gro2v~irlV?lIQ}@<5)n>LPY*v|Rxth52PB65ty=>KtfH%R^^'
    b'O0ThkU1pqvMlFth32{R=rtms#~}G?|gA__Kkl$dhrU{J;}BEP57Pv2rT3S)m}Bv*GH49jj!iy@_8%@>-5bx_~!&y!?YgY@yA'
    b'_tfQ47IQYOF_3eQj`cit^_MRoe|?4-_cnC^~0zS-`M5VXNXvpcTl+oRodbA0R8=lFP_$0yYNx$nylyDcoi!_NqteyldY^>&N'
    b'b>+0llHho(iPIfOguda`;r`wY*@<aDRU6;G<VT(hWpD*@L-fv>9^Cc`?DsX*0J)C~LzEjQ5psA10_~L4PI2jK3?e_Zm_|9?-'
    b'%(#FdemGQ{vwd-Yx7+SM0(XJ&UoAl(_+G3l;3(7WbbmVW^5=4~10+?L`=pqBS>2%`#p?8M>(*hu-Y)N~-W;uN-CE6of$R=no'
    b'v!9k|FN$b5W1SLs}I#~zS>+~9qw#SL%!+OXPj<MPfshzvw3?s|7QQ?>7-adF_c(u3MvDwp2YOa?VflcbmVCBB|rXh0?^Q-15'
    b'~p|u6+dkKc4TZ%k=_YZol}?SGV6zC;xWq<rR}sOpfp17<be4o!#l?Yp%bUyu1RLX<%%z{ETau*OU0p;AOf#-G0gSS9Mn-a68'
    b'3x=t-!hF10<LLbrEc9==>(@2_C;j`qh>q{;baaZ#O4en7<1EKGcI9Ng)<?dq&xKGC<<N!l&mKd$!s#oH<-2b$SehaV!@Jb80'
    b'O^uKE;>A2U-X1cjruRlNC+`8o-wzqB_wHw{eHphb|tLtl84yU_-_wDKJqZfB)|M%4~pm~2<oqk=_$lp#Uvw!20<N0CxX#2j}'
    b'-COLdqvPr7^yB2#<YaPdadCNPGM#*l&u<U-_!T~`@$rkv7x4Jg)t1t~!1UjJ5cbaG8s_5mi(6lRHTeQ(c7aVkgGIb~dsH>^z'
    b'Mewz>P7YHc)G+KlkM*8?QVN@IU#~Wg#7Z$WVu}f{_B))I6c1Jp8Xu>d3ogDU$9p|5y0I*g=!UuV1P}}PdC;3;#qY#dNH|JfJ'
    b'U-AgPsGoI{2r>VS6!w{#;;x&T(NMEwRgfa;|{)toG3PpC50}s*{hHdNO&v-L9*}W->ji_RHPs5@+sY@)Q;bsB_o|vZYWCnKC'
    b'FN@6T7ubI@xJdlKLMO#8#nYtSF}+v4TR)#gBo!^@ZZtBV5FleJg&fa?LLfazI7&nOFMLkG}nnks{ibqPd)&nswd`+fr|7;yN'
    b'>bh%!gOlF`)&Ss(E8TJk)5A-x#)qZ@$A5UIP&JP#syPLDeQ1QhYP=)XCbK&1#U3Z87ZFN8;XH&)Q?0&qugsyMiUQutn-r^go'
    b'`|=(31;zz+7GGiGUn3<|KW(<3gzFl|0i9GbB6D56In4g&YJXV0S)p3Dc)LHIL-lVk*RQYs&H8G8USOqI$>Dsreg9-btRhjDX'
    b'Zs*r{4ftV%*}R2rDogBtnw@dpx-9iB`9d(LE)zLP`q8et2PA~12bQGM#;x8?ERryoOuNAynebU8b$^rv#h9oAVR32+AOzMNH'
    b'&yl;~T>v(ZfBgG!AG_3%D82<af&@gMZ@eEvMbi6kpNL*TaF`4TJDChOva<h3Wrd_xAM{sP=xb+r54J8lI0}E`N7CFP@_Yz4b'
    b'#l>r$Do6_pc@vK=81W*hK?@Yx|0^b`l2cnK^MMu^PkcOYG^rx%OmZo7{|4oXiL-~|qlG8A6{5)uRZmeh-431kLFh0hvD9miF'
    b'C@$%)H?e+w8ro{%;e;5_&Hy41;6|Cc8hAaFn8DV=`;5Eee>v$g8^fLgeS!WoeADn)D1iA@O=Mjn%&|&~TPeB6&ye)qOOx1&f'
    b'W&OI^^=SRsWwpc=vInYxf~$)wG>$?^wC9TIV(*#Ho85xc7~&_8_TRvwe%c-W8w{k&I_=|Pw_E&t{olnqP>oig8MShs<Kut($'
    b'Fs%0^@>X4u6YG(D!zFesw!RszeZjM*o6#lUT;76tng5sg?tDgl0Yb6?y^1K$j9aviw~eW0G|8=&tSx}9pGwK54reizxsRib&'
    b'p<1<N9vBT^z2b)WXC4bC4mO<&x$lj~71zZePCir24u#Ux1~!Uj0}Vk5HHUVqb_^Bmu~Vl8LfT+bcU}-EOLOW?v`raP$al-Zo'
    b'7BHBv5tXRG}TEA?-%&b@9OI{hLtdoU)U=abdj%@(Zw>BMs=VDind8YALWcZknXqXn|?H5a#icZ)ZR)#3cj)ta!{7K6m6_BHb'
    b'<R(`>dW{F+_SP9hv)MQ%Zz#32ng4qlP@g<rbetA<H^?0A*c!Sl@UNE0?;MF@=opHRMG91j`^k@n!6Kn-ymte9#c>efN)4%-{'
    b'jAw!Kd$n8Fy?Ktkp=Kw@U_LwpiW)9L-$Q<zb(??q1_ryC{k__4r_>@~WH;M~WqMX1dBSq4=5Ob?I$yp-`u(UGt1hm?4<LHwy'
    b'$j35<)!y&gl^z!o)=Hh^Yng&u9RwBdG3P?XnzT6+8#=6u<&B{dUe<>cB}QzUI+Pv6}9&)^TQ0gS$wrgE~JN>1L5U9vc<D3W-'
    b'8gm%p>$9(Zu+yxH3G;t@-OdI%^ty`Ml)Dy4+RoaODKt0qb5J0;et3+wCP_<{?_E<nnpw;XhA<K2x`YBAW1f*!_$dfy4np9)z'
    b'?2lM;#ZMs!sXU7`38VJN9$S4hYG1}LURBe4AI>D%@8^<w=;RHO&I2%CXLaD69IdvOV6K7xJtiAL=tOveOlYH!+nj*50jJr4G'
    b'4op7gb(fh`tE>XJ#s-`4zAkgIR?BA~rOB7-smtd0ALpbpnKj(KSIaCx<Clj9<XeXo=%%=uv9#X+BkEBH$s6fkrbflxnpEoc8'
    b'kQdk2^1QlO6xh!@NDBvO4)_=HgtH1Pc5<WQbc02?!ZfOKJ^|zZ=q}2>`D#zUN8TJc{>J~p5KltNbv+qpD8Z-by!aR`w@;dv9'
    b'SHhwb}tUEPJ+3)|Fm+O>7=H~$?-Rvc|)PTqB|8-dRkBP_M81TdoY_%wkMZIo8xQmv3T3{Q5=8t`H?!oT&+$&UI!bsWjuAvWp'
    b'4)bjB3!`J1L~7cT(C~cL&huuAOABLv@pSx={z%m+T#&moDA@AdBy{NL;p`<{4clwY6wtIvDc5`~p;p?ycZIK^7M#_Q&ey{n6'
    b'ENB$VCh70lF~-K|?^-m7q!R?{62;c8C;tU8NJ4<yqB)wo-?syRH9myD<ch8t@;oWityUBBG^&lfL0FgAPnL9kc9KYH<>FJIp'
    b'N-8Zj(|IP8y$uGBl$9}I%`1!zoFBiYuLac3$Z?ERnhYD@y$N~=EOip0IpB`R_SKpvF;{>W*%>IskiSWy2?#q|6`K#am+wJ+G'
    b'+8>S%FZiqDZ}4x|J#j+iC;zpp){6sJ50mTTBO1*yy1Ebu?Ht#fq8Z@n;W`c~;RJ1F@tfumhH%on#nJr1v!f$)^7&Qprthgk*'
    b'CZ8Ob*FmYr-<batoW*%3CdKh?1yg79lEO;_u=aoKfeg2UcCa)>o({^3xq2B_g5Ef@cSP4+iKGWziWZ1^)~Rw77&%nHtg>$7|'
    b'<Z@gEZiJU|*k#>o6m)PhTJhegpE5O+V+yQ~$>Cjm0-H<t>=Rw;HCVq6O3VR>RCxwqO?D*zv3B&!;ccR8R4LGqw2ttLcZ+7kc'
    b'Xdwf^5%)Ay$@1eEFIdw2RmLRu%^kEbs<^r@BpYWnxn7Yx#{eE9zrIkKOfe*E^~vmYMa{g0E^APlC@@Bi&NJc(D+XZM~y`~ja'
    b';e6CZ>tLxy?dwlxhBYG)MCb$GZw7*_&mp|@LUc3<6Ots^$rZ221r=@i;1JjUq`7@|*AgNl}ITLEiMJL;02TI^-(XN$CQBJuM'
    b'vX$-iZgGCmsZ2<wrb=kt%JsI|?%sAQQ`ko(HE(zAe|~H?CqxPD39TdyggXaAQ49w&ePOgH+0<xhO{+XwJXh;>BZ^5e)mm`u&'
    b'8vX7yQeQ+HQLW}<ci2((cyeJWp@r=-3@NVJBKe%#WAoGAkw?QTY+DGg%4i>OA4HY-H9yb`tclUxxYY#1-+}j6YaFS-X@>V=h'
    b'gA?ap=rF^2L5RL9hMv52rAypH4rXZLeOhD|9V^?Yw;tHlr}2;M%_2tT~%;wv=inO<}~`&a5p;Gvh)QaxQ0Op=TT(@W&KvrXk'
    b'Hhu5u<A{IPTS60{-6J{NPyqUO5b@F1WBpQ(9SKnmo7R0a>qLArxNP=L>E!3`wL1h<9YW+qCgi>sN24296MvMeOE1{N&m(47*'
    b'xQfRCm8nH78l?(U_)$ci^Lw0<AxmgmtFmoIOG);vHQx+U+=AxX7LRrYqOXyYMpvQ&AE-MMe95hro19`YmSRG;tvq1(ZqNxh}'
    b'126L2EV-EBP|BIYKb!$NpudR88DIoDi+EIKrp2rTjL$rF*E7QWxpdCT^4(0a2ZEPNSQ7<{|5BcEITMI+z_-!xs{nc2&WtNyw'
    b'xBvum>G5(vg$%&PM9bQ9h^xTErYxYr@5rIN`Y<t{pIEY8hWQf2N)$@u!1|)O9Xp~$cHwbBmMYZ7t)C}bOFW;G&GY6+Mgj26&'
    b'5JO0Ggl^h_x9`0`v%efQSl2Ax>*q2nK(!n+$OZh$M6)&ETBiAUI4ipcdXNrs8}d70sNVN>1a&sR1eiN-~9jIZy@C&`ju}1e%'
    b'&jY7VJ}&v0@%^aQKNSt?*|V5%h&>3+rmP|oOO8D`NSCb4NMG&5Ks_aqE-G($R?5gpBtj%G+lGo+&#($Ng*Xhw82BPyC<-=Tn'
    b'}3h)orC(mbQ2?+%b2Nn(fK}sq-CEEfp2OUOaDg(4yl(<R}!D~YPgL*1U0rWrXk`ZJnmx2}&E0BnQ&;b{)LflNy@-hYX81m^E'
    b'k`PSO3`wYvw7>xXc@>glQBqTXf6Q=Id>Z|<8IMB$E(}ZY2b{Atpmu3WXj)@GIHz6+hD0DWo@zl<C|t|1jPQ?!)rH81Y#gQ=|'
    b'CY#qB=8?XD^gA1Ke$+6YGFoU2ngZCqY#Hk4YX!(vVbih>J|1wBF}Pwkdhb&duEVWL3H>9FO)^}0v;4CGe2FxoB%P{OQc=T87'
    b'y)bd_+1&UZX6LE0Cmq#+@mY!k!3*Oiwws#Ca$%2}V%3J%^stOkjy64F~>5o(y2K7s$c@g!~PLwydFuI%v%x8urj1BqP3r6#+'
    b'=+#3%@B#H7TG5P)b!_F|DX4RUefB7Op(zBMceiBnoSn0W4|FU+!I?O6u#y#&Ge7)Vr=YpB?~n@PO}hR2s0{xXI0N>opSmmo%'
    b'@2#d8O3P6^o0QJ_*G_|0ueFr4*0A%kH7Z&jNSqZ~}egb0`SS!P243)wFfPDj9NT|uG88Jv8X;V1jQIZ(-jOGQ#R`3U=-Er~}'
    b'L+@dYK$p!qx4_%yro^Sh0XHNEu~?ztUyBI^(FIlw4dKX8*5J^9A(Zg!Ji9SOd?-Y;rV6F>gPn7D(V9dNPN#vKm;r?<%rlZIP'
    b'GL!fh_M3UV3QWGhS*V>dF%*>_~uJuHjo=7G|ZskBqXGSNjLnSVE}`cB_dnGDgnB8$UT9>s7FB6{4WtDv~Sw1*b)}y15mK;83'
    b'oV4AWB*nZmxi!U>bn`>$wBk6SyLn|0Cdkm=~lUAcXQB^xi;bZ3|rDP+Q@r*ywhnaULSCFrsOi6c=in<J@4|rJ$x~iKbzifrq'
    b'`5<q}%vx}>HBccF23>NbDA*j;YPsgBKBT$8x`X^H`BiYVSmlGaWxX}OVvgLNc;wL_IC2w=WIBe9+#L8HWx0I<}ISRF_uAYSG'
    b'N0c5L@o6yonUW<d1#M_WL5x0YhE#Z&nbR;97rTvT;BPwHn5?C8Bl_ha;B5<o{=8@F^`yw?;5fm8`@;lT&a1Lk4BE59{`voi}'
    b'P&bl`J0RzBeYO7;2<o5*@cs`XsEZ!t75@-|`sg89_v;Y^*Z_LML{GC(#bRE?Y9)41dA5Czv}Tc?Lib>hplnEFD#n2NSP<9Nn'
    b'1l(1Ljl3}lz@DV@=`ySj!_N`+(H|aTSXWciUdg$fN)<u_dgDW6z~`-h++d%&Owy;ytu}x9|F}{gJ__14ikyVGzSuOTC8nW5|'
    b'or2_(eQ|zAHGdg@<F-<H`X_fq_2=%Nr9P5ekOL3ABd#imnlalD37Mc^ff>*UoV?gp?dg3YulC43SZzg7mBg4FL&W%dU~&QP4'
    b'#I4&M*n<_7-2{`~@m4vcR|JVSXJNoTkUx7#0Eyb%$}ncUA%{vii6&?is|p5btVVr|r(D*~a?AaEJZO2~81f{-dST0<zAQ9)5'
    b'?7!^w#GzuV1Du_U~MjHBzWHjPm04^Bx%BvixE?G2<NbQ%TN#eqVf+X!>{gN_^$aAO*A#MfoOPn5*+9X>QYBPev>{(L(z0#o_'
    b'Djh^LhzGBTpz0YgLfgO9Tij$ixp#!9A+-EO!BjtB#b2C4S}XnnNN$J@OCQ&w|3@&oN$6mC?CCkkR4CzC@qjp*E>Oxsb-hx!j'
    b'~ogq6^n7*DmLquyI5TA58G}(73kD|u?qTzeGFY}_3-)jyWRGJcrI7=yoNs@G7(&X3XKTnI01#jKO7Z&W6y<HlEDhuaGFTxnx'
    b'XQCvW2FGG;XjUQIVaIZiad(n3pq)_9>9D^_HV!+Ou`Nh7>fiNnZxuD9A7^e12M4Wb>jCA+<LqKp|r@T(;QS4Eu0C6XKrlu|O'
    b'RgCLKm01*+200HcN4tcnxvJVKEB0-34d%Ghq+RzF=W)?qSW2F{zQ21FI!Ht~r*39x&yI*Tf+U<?UHO63)N97Tx;a)c95CQBS'
    b'2n6mVasxv1e4^RlYhX#Wle$<i}gqq>F5I?|66j~o-29sJXNcGdmJpftG%?z$FFq}Er>)Mb<1ay1>Q~{HF&QU`tm)uf!NS`Yt'
    b'CkF${LePsrcJczHpQ8rQ%=M;Z5PzH;6DV+HuujNc05qX`iR%r`9~73_5pN`Gf)RVPNOVwE5qDKy=_?GQZdrlL0`MOZ8C5qAM'
    b'A`&wdIg<yHbf@01cZ-PE2+NlE~rWLyZ|3?h@3UzZHYWpk?ckGg6KuDM{X%HjRX;|M%y1o>>R4k79ClH${C^(KtP@%AdysMoM'
    b'{O<BOw&WPQD~gb~d6|A+KQ^xP3Vz)<oVX^74>70Qo=SaPQp!WX+J`Jtyuy(=$|7U<PK0(ix(Zhz0-R<Z@gn3X3C7XNXf+3qS'
    b'|VPdK1+yIO4yxX;4@mzoq^;D}}^EIz^l5iHD58J{h(aZth-dP^*J(t;#L^)$fY4eU-i=Tri4M9)G2KVE|E$3c+?yU)#hO%k}'
    b'W_RpxuNMP{XHv*Xf`yh!KWEQZ1(RB#W_zay=sCyglpmE`eplBwOIgW*qI<Wr%OwXX131R?PArvlR)c|edMW9xo$%O~R#h0>F'
    b'B(W42U!11jgl8d<pF84d*fTWcOR_Nuif>?<Af<pf(<H?u`f2<#!2#j~6vgs0tS`VM>3EtU3|;^+oJ26_0+0_7E@lMu&wF@*)'
    b'n*L+QWC5U<l2A~OaP;q0A}w?LDnLcH-7LfsxT$a7*T~{C14Y}MczzL72&x}6_~fCiU>!l04u+y3jf@p3d<rTfNns<9H~MJvR'
    b'vC#0e{Ls2?({}SpiNRx{rmT73IQHpo*6pBnxDaWuSxzCKs~w<MS0FRm7@ms%Vh~X9D#cEtiH#S}m3lp%`q!KNI!~!aX`A;8|'
    b'GjCW>%$0~&@NZ%f7tl@8AYy$2iv`iJD9U=Y|~Q8$gqv4A%}&m1X3l6fi+M^XURXO{3v&(DM^=cowbIQ_GfK-Qi<P;(_Rlvuj'
    b'3R*>xu&wF17=(UvY>w{;&Nyl5;RxnG4rUouK681oOmTWC-1GRP%MmR>49mrqNQqYJi<aGj_5Hs|_&b&)%7TmPxib7Ne&o1JI'
    b'ym(-qV_Av5U2>kIZ!UOt$>b8`nZtC>y-${Q8*qBD8qh}2r)G(lqTq8ga+FvUg}|cr7W)Y71f?G6Ea(d}pshQSz`)Ib&q|QY_'
    b'yptj+9-}2m0(5K3&mi4BKNoGZjQAtXJjKP(gXxKeW6EQdm>RYCl^|YV?g8u*Q59T`=QR_n9(Z@rXwd$EBuhtP@v_4{MmRd8|'
    b'5U9A1x@r7VVLML=;S?EdrY6sKNw=M=Ns5o5GyKQbmUlvc?&Duuxcm+74p~b~C9INS$8wL)X;|U02?DO^&M>Ij&xA_PFhU)1}'
    b'GbjUH|ERxM{#yO@!)0wtsOt<J2ODPk5-Bxo|^&Esp}LZ}tc3Wv2O!O9><yac2ezz+&iNmPL1m&m;YUd78bV&k3Eoq<P{gA{H'
    b'+F;s6L&l(VbQ;xPDQ!{cq7UXsW#*M8C+Gj$K1o;HeVT)`Im<xLVQmpPV*cwZ8JA%nsax`T)-nTS`R}s;iMz`b4p{OCd*D6{h'
    b'Fe=pkYKKHY<bzfdKj$_MA$r-+!4`Z`OKFKTNH63HJ#32O-V&O-<Y7Y<j`=@=JZV^Q@Ubls@mvEF)<|X!Sg*$42dvX%<LeZl9'
    b'Ss|v)eX-=!?RYy!9B?eayokt+swPzX2Hca3l6rX-HvmuF?gL^()z}!o{=lkuM`)YYc_0_AZei-wy4)(BJ@ZS7<6bM(YGyWcg'
    b'4_-0dBGw+QPxj0zn&b(YwPun<IB2G7FOWs5}>_i1;l&P)cbhq$F=Ts>$Tuvwl+qG-X7BaexyC|G{b)S)c>Y*(Z&@J$CX7{)3'
    b'}$j~&VW9~*sl?2WIhHTuGvYfgQNZ=;Q<;1|P9Bn553GVV#1My`0Ng8X%_NIS=A@f$&)7tMenI8s@RH)cxOZ03Gv8x>>PgrhA'
    b'H@*Vl@O5Dc7Z6IRmw0TX!injM~5rmD%z327JZ%63|&7ex!3?<)(A+rkR7BGd?c^ra{UeY!x_L<NdT1LKsTj0P6(~hAbrUF?7'
    b'Y+0s9u|;yFjjw+YTZrxkU;iMsP;9mF^((Oze6THSAzjV_1-87fNyn_mt9P$?zmxaNTBDl1R4})xZQ8UX??p-84dnBJFzbC8@'
    b'TBM@0In{FHW|4^5Jg&{EViWR;D#2mXVkPc`9(-dppY`SgJW=;iqQ^n7_dMFjejLhsYN{k*P%m+si}wPlBbOk?gfa?fr>}%UW'
    b'559(f&liE6^{F?!e_tkSiW&*5{ptS|1S|{=In2IzBk<-ZgmZJK=B+43#4X+&Mbn(7$p%BUc={Exqd(-7vK<@A%<@2aQ3Q#v8'
    b'|iBhJrUzi{o23uVbCH*1`7Ec)X-ye?jQ*n$eh4d_i4XAB)Z$a%?eg)hdQ!#pi#-mQka(xg$4|D5^#TU<Nj<Uv;`icHcyXuH?'
    b'A0v#%hgDO+DmH?<Lkg)Wdpb`a@7PM18^VvP`rjsOCtM;cn(5r?{Yclr%TJUq|ngH9JBni131g;R0e8$2Q5=T~c=z^9BPIGv_'
    b'J+BsL?frX?8}c`jdOzxM{8j!&GQJv*BXoQX$f6w~(E5@3H63|i&<-wL16Gv^=QBv@M4dy#T1mT~8QL2HJ<OnYjyC%&H~z3qs'
    b'n(FR#KnY2S7`5{&4an|q=0u~qJD>6lQuj<8$iQ&SObhyMaMmYDj0eOEfqF+Ni#lD+MZ}N@QHp>OB7V%PBOXmZ8Jq3bFU#{bY'
    b'+^7EEaOkJ0cuBK}2LM24s8#5#zq;fQ)g0X~ZwA8QQ;-bjC%~K_>=n!E7PMSfS#c3+)!TLeWM|Tpz3;PmnPMOqVNMbEbvk^c8'
    b'MNyAFs^1?mU3M0ve|<8etFo`s*R8MT0f0yLo&o$v~W1wyxg04xz&XYsX`9!*qh@yQ#PpIDNaNK<9Wb}9nCak5tHGjiStG}A!'
    b'nlG^NbvJy<%xzi{CY<$F-q0Of`hq}<DRnk5JZjV+So!?M1Ji7x60n|C*`!lyj`vM8Z(9WP&=ovJzgtg<BtRfl$dj;Dhy#Ppr'
    b'4#!#iQ6xg3kwUMr3b%x@VFwC4ae_)c%%R4EI7kh1v@QSx29HOf5?PjOL_E+AC?Y7u4Ln&iu*}>6#=z8;3=K%YkC}U?0|-+Ab'
    b'6+aV2FSzF!%B8MJVw-^dq*mTr~@Nqi3}M%jWDGKq#@dNxP-`$fX*m7T2;;&ncPO8ET(gabV9^C9gKK{u3hHw$?32Hs^1#SDT'
    b'kU7TLZDGnY+i)Wl5fQ^wE^a;3R0lY{`|kiEv#CZ^R0*m!Ot7a?{MveniJ7nfirYqj@yv-ZPGvRdjF%<u$rPO24bA@OYz}8z@'
    b'i5lnk;kA^h1x11zD`=nCfOv#>A$nwSdu1o4aonqvMLk>u!1gd^`0ZdpnZ9wS~QdQ*Wuw7aJ?ZUR8RfgAwK=$e=DaY>}cQ2|n'
    b'#OkG+UK$3Gbi;<XN#?Tn`d*^ZudJfR%T-}ovom#Z*XQU$WP%`>mN*PXra89~Bkccwj$sc30s4+pEmfSMHOcCE?dxF`rCjSt#'
    b'8juNd4eT3mMcxyjQqUj`zT&}8<pf}m16c|>yR#(6w8J9-Tp3{gjz$4E23*6cEL*Qo9iR;z!HVH#4^(Z@Rixr`#H5OtEIlSmk'
    b'4b2qPTj=E7AB?AH71$V9+gTCP$>|R_MR+JnM0QeFgdOXkN&|U5^#)EN=!s4`fVXmSdT<uQ%JN<EzwO;GCd>i4~n<*=P?z?vl'
    b'+RkV9{8GxPv*;Xf9aXxgcLZ@_h?@$ePw$jW)y~5t1|>bN(4H_pVyv!nSxkaY;vvQ7Fz!<hIPa2Guj^XlKwND^uZsLg^1A_`d'
    b'=VF?fM0!eIx0XaaqkF1!g}6m1zv`_1?UIx13~Px6()Y6{07aO(&0XK)vdjueUg%vVM(Jx6kba#C5`>cck>5_E_IX9_1c3<^b'
    b'voO8@T_7U!|gK4Djl&eG*il!Je<xgJFX=*HjE>AkoLbaf(RG>T4;{ogojz=#%#zMW%B#CYGBqRBwH=l<C9KL6G`U?k)J5(<>'
    b'4+O^}$(G|zmOp=jJCbDZQLV_-S!hxDv@j-gDV|HLWJyj3n!g!np-SUl?O=-Eug<FT#euf71XOgc*BYe+*GqrmLi>qgXgs28q'
    b'JZAuW(`#enAYUWEgWbqk2I_)Lka}fm;h=ABEfIFx%EtY&4!L^09l|Pv*`kD5dd?9K8Q6(0m*?&8owGe5esObU@zekgjvTVTA'
    b'=U?n$9CA>syBj5tvf1WxwkO`k(Ly0;DTn4*oVBkb@4uD)@~ZD1S=_)UX5YH+Dd(U)_P_dA0m8(7_bvk0@a}BT|5$DLiZZ!+{'
    b'szJN_r=FOtO6&+QZ~B+NLYqVNNB05i|U)X}emB;jde$}pFrd`KNrUxS&7DP%9FHFZ$eB7ID8c0GQeB$5c!!x^@Te+flXKg{X'
    b'OP*)(G@Pto*mNn`;P(2<EfG0b}t!j@h>=NYmeIkTSeN6_^C~4Y7n5=tMHoYRc`Hj8u%bj{9_;2l1q@*d*LES6&5A@15y|TZu'
    b'SFaa4ijtQ&ZEkR(`t!?cS%f3!MeEFYL0$n4V$~mc28AsgJ1^+mIs8Sh8l4=TVQyaF77pzxp>Uw{=LMdos*j)};|izHi+BvZs'
    b'869U<1zH2K8Ie=LG)!jiC)B`=tX@N9rs`?Rz>c)Sv*bdkE0jiJbK{|q%VgD(wBn+=^*4n9eY$`NP{k7YfCnIBAxY*q(kqrht'
    b'fxmq!-~#dO==pM6#g&JW4#~(cML-(;-0@{(L&(+)vkfhI1Ovr@!2gj{~N>?~P8nQ^$O)L`6hXKwH0J8#QjCdT;MPyp0;%w=h'
    b'}%oLy8<qR=k-=j@`=9xAQm|BvpX_BK%+n(fZdpsLT%h1m}Y4KLZBZ+D08!Env&)SpI0mIMoN3JYpFvs11>UI9|3&^*X55H7U'
    b'KImZi{n2i5}yb0%0ry7`-HXwX}eL0XtqQukEQ*<><u~s~n3f16!h^3%9ftg~$5^u9=|34j4MP6&L4^VjtU0Ncqg?E&Sm@JXS'
    b'`%6JnI;=XS)2dT{Tor$?0QVJR?(p9la3VFIvNa^5yANt`rM5nsb4uFw6D%<8B%P}VG?I%|hvU7tDE(naeDmH{R?;ENA_wU1b'
    b'2PINmf{pW%g`|ec8_yl&O%rGep>4R899y+|5(I^#2=a{dO_ARB}(>61TUAg$2O%+HiV#dA$>0AP+|!(9I*gZV5TG|0Hg2##x'
    b'^P{m~Oq!0)h^84L?c57N(e7P7R5Nsh@@^P7{y`C(r3ri7D>AO>ysSYEZ`E8p3rm#l5$whE%YtLD89py*JO9YP1syBg<1(H^|'
    b'lD=xY`*nMNenva})ne=*^A$Q~B*ZzKF1+4~dfS;BX)%md*g*#`=n)g)FwA^O{7-#XQfD}uGA<`Y3-4hH1!DPKz@{Yd#?Io7K'
    b'&KK%cN@;g|>(Kob@pyaenu0vqdhy5@kJ|x2VYJIk=x{*PsK-G=rabDpRb&}4i%U<Bli^YfUw*GWlFWSe?>t4`?*lyKr62s-T'
    b'n(MTbXrxpB$Ls6!#r_`FgyU-mO9<7^Lwsn2Umv#LZ_lcuCXj0k;5}dNs>PxDHoShfThLbD(ea(`Rrk(7nrrW09#?Y=6ty|Kh'
    b'p?kgwigGC#o-&SV|!H4m&X+1@yN%gtxhMCPj2kX(?kogx^@%KJ257@6<#bZDP{?VIPp0t%1IMfC>Fmni1u?o#`?jKOy6!VaB'
    b'vi_yzphF#oO%xujtQP1V|xbkFSsC|Fd0fjwTrI$4|r4(~p;j6F$ATII%+5+d?ZgT{bV%)drsD!cJdbp71$i((38u@&uUf*%|'
    b'zm<!!L8?Q{uAs?^56ZO%?isc#!tbQ=Ij%WiXEdV8qiw$WOe>E(x5+x7Ix>Bs5=2Ar%e@bASH{JoyuKW&F$3K5eAk(elCPc^^'
    b'VtPaOh40={P{pA<>ee%mMlj;LSow}YrIPJzprii^5eZ?20a>u*Vr!V~btJ{Y;#$|n6VceYk(eeE9@I$q`e*x(j2;{L3bhxT^'
    b'cdGRsnuBS)xbjh4sVlqFeg(_@{{98j@%W00P!G1)gDqsG1vRe&k9>2OpD*6QK%ZgyQH1-TCC4X+X&hK=Y};$>!vk8+z9sBjb'
    b'usz8&fky}kix^wyTuw91wu}WFOY8V@yp2<6QJfn%|Bf&&aSr@7zZ%F`#r$B_!uDQ_&U(Ek01urSIvFEtKaXYhZu9{_<H(%V!'
    b'pq`h49_#Lv?nfj=#iFTu;AE<bwtCX0<+4JBnu!sk{I(pFZkj_X|9%_7IDxAEv0P!xRC#e6f3Vrw%B!!Fa}Bp6-u!7$|!3<>t'
    b'%Dok=~w!}avL4w+#{oz<J84#TZ}`6cq)#UXqiU=OVHx3l3LhFwCM9+1(kTODRh5j`ds2Lw)HOpgkv?``EHMgg~}V#H=&6~&Y'
    b'~tuQqB4;?(lutYr`e&5IA!#GL@-|qJqn)=%g4vu4ZX?R<`-9++kjp?{GJym~tdbm42ra0AiW5Q9@$1a9!uDLrP?@pEG?iFzN'
    b'!-y!U5~2j={4b}sU%cE;zWDm~X$U*T-@Lj#{bP#mzQTCtok{U+d%F6fpw!dJ7hf)pdN@1&@{7qAang3DkB-{d+QssoBB=Z>l'
    b'=wB6L~8wuk7WgmAju}`>;*~`;uoG(95A9fzJ1}_PfP-K=o<5VJ^AGM=u6uwC2g_JuWnOUzMf1EQ3d;-VT>z`RhL)9S}XFOqM'
    b'yBAo&9EwZFK;AFJsboJ^~)aN~>rXJ-zOLeC4{o5W-+P({OtDZE9P{_(Q8Fll|plGdXUnQz6$M6S+d<u|NO6Ju9Cy2W^BOhUn'
    b'fkY`lZ>$@#8&GnoS3{?${lxOpBH%_RIsHvHRux9nnnefU=!E7)WRA&0%`n=F$HjDG(Q7%sbis@)Q0-eh(0cG4IC)b%)qY2^n'
    b'MMhCBraP-6J$;4_X6Q>EY<%_SN(E?j)crLslr!BC-7AIaGi2v5#w&+QafCN;hdxFH5gXV$BesLT4YZYyWzny-Zy!=pApbEXb'
    b'Vq9|kAO;hYDW<;Zrj|bSteeVx>av^4Y9souDPDme6Pd22>p-xm75{o_?6OyniXW@@zma{X7R8XRsi#$^lfbq+UEJ00p@W+`f'
    b'x5S?Uw!BH^?U5Xs>PY82Wzh%4pu~<ghfYB=^DY0+lFr)y7MuH?(AmV#Sn7C{J<*%5R3T$b(<H=2dLY;sKe70-bTHS+BAm6yg'
    b'sV&Hpj1q-GhaA_i)5O|5~J!+r^iY|9S)b?)75%<K%b>dKpkB(jrnG(%GvRsWVo49Q9qAo4>uNpI;sSQ<(%B=9Hsp`Hn{)j`L'
    b'2tdOz`BGwfNG*LaaSjVk}eV)u5nc}gVp{ZXr$<Kt;U-3*lj@!eQ|R*A2*_(qlVyyVxmaicK+-T3CHtC-Wb0!|VP@pC>!HJhr'
    b'=DB6$-mP}jQ#<!uuO?gm*CSL*CjQ#1^0Y~(hpOHfqEW$4G$vm7wwhptM?mE3l=<IRjEZXD}1<N-_UWLZeo2gfQalDvve6ooW'
    b'{=bHwV*2$eR_!%RTCE4<ET1YLzkdeG(cy4;a{KoC_wVQL<$Sw)ds{HZZtvf{1v!9!kH^9X56qRSt9u;=hTq}UztV^J#aA$d;'
    b'={#y14K14)GvPBAiZ66$k|l~Z&%%^x^~q+$BXHS>)qQR^@*_lEkjtBi^DlFik>Q0?KVIKozyyO)z5YKr;!u3(WqnHX1?iB&g'
    b'5*;>j-_TtDohk7oFa}>ZxSuroQ+J^A~4bT`aT$Gk5#zFHX972F>_f5&!=;xv`L0!L8)#pSoJ{PR{?vavgEee?@KnuPmMxy$1'
    b'~!V;=tr^xM0SpUs|DZ?D#i-Q?t>6ellmPYnN^3;k+pG$vq7ywdqpE8rghm_ES3KNbKsSE9@TT&60^1^^EK0Dupwf`3W?h#Wx'
    b'n0TLeppiW-QIkwk<!qBbG)s-8%9Sq$mrLd~p(5*|h9vUih<1hJWF_CGl93Vl%KL!9QGoms#{iVqP`Z&Pu=7wLo48XBWhd8sD'
    b'F>u3rw96y$T0NaJp{agM_j3-#8~~jVDoz%(@oFj*%QPX}03BK}fh56wnUY#zQ#$x771Eub4A)Dxx%`2&d-)w=YR*bL-8a|t)'
    b'1Gn8m`AaaOdA+1_NqONLiwI+<w_GUgs3Q$k(nWts?4qA5^Bj&l4c@HOKi}V^6LXYc?#r%Qby9!EwzKTW_a@7l~9*~j@PRffG'
    b'Q6(j7oATxdc{VOlfOkHQ3E8%-}q5sTEWtgmf)XnFS$rBtUZ>mNP8tn=-iG{86f4$yDC%KM+V6Y6~n6a*T{#gqdj81tCo6WU^'
    b'!}x~uDuJejII4dZ-h`sr{jBlQh%L@-B|+UbYlo=}W*<8UQzfXf@;#5PJ+dX`d;OPNf%&7`NtQTnA0aF;Hp*?2k4vjnI`9yCJ'
    b'jT8+ST9%cw5(#t3MX+Sm)a%s9nnDr~&D5NSI>DiClsw~Vh-QlxDw3cPLmt~`TwzS;#D0{Ai<47J>j4&fBTD0GqySiRs#@m&$'
    b'zx*1I8GxcFmNpL}ZY4FhZHj2Y(})lejzSWoELQ>wDd193s)SxCo-7|(kfp^qEKkVNgAqkxV71MHOqy65L>Zl=uT}<kLk%dka'
    b'f*Q(;uOoUI$IfVnk5q!Xwf=Z%0Aps1sq5ZDkvov{S4(e9h9_?K@5qGFmK@U2KYFSwWYZMZpYzKaZAvtLy8cfLzPHo_|t>8ns'
    b'(guhhCEC6<PyQL5<UW+?J>rj>5|p#c3-{AMVECTqNt)1jBh3Oj}UCEdBJ#>Kou@lx|97JEQP&6t^XJ<8UDcBBuqEW56=S&s%'
    b'Ue4wr1aXOiCl7vq>k#yJKjJ~o0`;$vfQ!W=Mt8Xx}j;PM8zZSmpZf-O}!4%g-Af&?{q9B%jxa8cV(J%G#sR1TmsfM~V1n0nB'
    b'AODl5{th56~wG6_gOjzi3g*Fe$nyICAS&-+*`3dqIc?7P;7YK3eF?iXx0cxwEbS;gsrLshArzDm<)F^9=I57%$j1R3!-z~@F'
    b's33Dp-vBo^z->;@xTxNxKrXGL)fsr-qMBR76uBxm&x6Kkz;Ss4T=lDtT8-m656Y@Zj(e`*IF!V{z2@8)RIpTMd2~fU27XfpM'
    b'29T3akwV&r`LkK0d7V~-U)jHyd34(j%3mZoD5YVVS#sa(*~|@fSViO_6B%4DzhBv<|FVMWSYA{ra7mQ%f5l@F*sCj(*8B(eS'
    b'4}*xfme1OeQ-o9a;*zPd2G!UAFtyu<IZi0B#z13-=LjWLn+<KxP1X$TETxlPvNaNR6=sXH33Uldsj>P^<TR;y)?A;mFKP%xr'
    b'LDbH>;}9tZaWz=r&&hFMbMvJG=qs`k7z+I0{$kim{FddyJXkby`eYan0-b5jO8rp#f6@}>;5bPb8Mp^*U@NMw&@KuoF<KiN2'
    b'U^wZBE#xs!A9z&F4;)V=fsm_^7m_gl?L5~kJ@X)ag_NENw4H=XhpKhnwO&Mqupri8?BgG76EQ7X^n#j6YFvF-H1v7{-MF}&='
    b'v8@5jV8$j3GuWFlAgUF2nxpBCKm81&5*IQUc2fp^LlfqvQJ9-XVal5_xEnH9Hn#jR10Tt7J)PWJtXFS$i_7!X@{@LDfxluJ@'
    b'=cA119P&qHH%P?2226YIPRH1qBbROZPNNIK-#L#qJ`12wO#A8fMO3SGlMsrA<Ly}ZQS}Spjconp2=(B_exnOi*u6KSu8Jc4~'
    b'lTYoJDP@?vRsBO`sXr*4D1i!9ax8SxOkbXl?TPELN7nZ)ka_8nnX!U+@-pQp5JH?$p8wwG2%r)cAq)T06iI{{gur)ZW?#?m#'
    b'(cY|ORxf_)DZkX4P>QZVKGuJ8c$5oykl?bXF=NxEdH#W-)?4e-hugoQl~(d}vMhN8W}9yWC|PS)KurVK(WZUefFYHc02XFm2'
    b'&0L8U7kz0cxnJ>IYFh4|B2J5$rOQU4nNgz?_cbNOc)kPMhs@8^c7_2AdP?La3N^CI0di`#<`0;SH8(XWj=bo$RBX`<DlTr-;'
    b'Kn*arvg`wZag=~?>4qf)gA(2u5BeS%6izil!axs5-Oob<4G7L%ZNuXPDsRC{jy$0hHA~h;05+8GJ?I-uc8lrJVkTG2COfGaM'
    b'}DQk_O{hhbpb8|2wHgpKr9#?3`wXMh00OrU_v86j}|kzVs^CH03`w_6sUw`Ef<EV6x4*xVOWs3$59&7nIAKtR%wK4iJTTua%'
    b'&o4)1;TBbggqh4Zz^{!yCcUDj~Tp8*$PqVMG)naYUW^fMlD)K&_sY8pV_KdnKS%bfJRDMpm@24vede2Z9ETX)PpgghRWJrD}'
    b'<UHo&l!G^ha_bTEe!)+P`E3%cR|Er2l2d9EKRq64*9pJ({MDtNN27*<*`+wl1g!0?8@cg9Lp4UwQU5<_VcQ;TRTQ%E56ltX6'
    b'6T|66<8mkmedVZZE2}b<Tr8!<;HBOE~2cuTbA)|D5$WdSrcsOT3N-$_FF<?#-bR<K@a-Zoc3`c<w-Y=sV!`bCbBoQKo5maT4'
    b'H%Y2iB@1hyH!3`sQCN65)tn8c6sV>wTPLwvU5TSBhqXCBh5<$_C?Z&f5BasA<5-*zF@j!kgtRaM%hQ?8v~Zrrh(doYf>mO6k'
    b'fHG4b8VMV!uQ4W=MD4)q&Uup)WIz^6a4e7{y`c7TpwW3+yjVU$&Lqs%|%**0R@%lY!PV@CLKfWJ3J;=3T`r*2!Lz`1g0hzLn'
    b'5}QzamT~2wZwlniK(%c<c-_=nSDd5o$0wQdc3vkTm@?A*>uup<<!e32g$*2-Nf*FIJn`AGYhWQ5_H6SH4c&Qt;|jU{5H|WXF'
    b'G6Lmeo^(0}rLM0+D1TS0}KJRUtjEK~NILSsy|RK})f*l<)CbyJf9Q%FQ*F_G>kyVIetw@E6qk=}-AnpQ0@GN)d5+7D|?H626'
    b'UyU`GbC2X%}z7JvKO+7x@C6j|zI7sH{WRXsm>135o*6C!EPPXafa)2$56x}-WHiR1QT_TFnWsT4<K|vK{WaLQK_`(BNj20e{'
    b'yhaZb091w)Yo~>&8#2C^W&q$NRb>YNXhOGq6Ahq*5w|=NHNfI#XUh-K0E9H{qvSQfF~v>G-_QVB9)>zmNwUY|W^@D4e$-YE>'
    b'j%5C<wK|e22Z`T+y@Z=x>A6YsV=!8T4mWjJ>CqtG|lPpxJ5E3CWiHiwmYEdZ#afK7<$>V{#!GoZ2RCi0_Kc+A6O)X(x#vsn0'
    b';Bc?fpIpm9A4xl*A(<)D1C#PL-%9wpdD7H%_Ejjn^9!xd#;Qvm=)$cb6noGtAyVZGS~;?NA|h16CgJ9JR?-iMTn!GDT`_vQ<'
    b'V{Rt9adE;C1`zw~m8JbOW#Z1a_7uBiW<scfS<BXdZZObEWZQ9Bfv#;LYOIjnMkl^_ypjZzId$cI?5jXpVGrgbry28TXIeX@a'
    b'|Z;!Y1$reNhNLUS0InfcfwLY2XMq3s%vr>1%CBcDnaxuh8C@6S-%IiELM}tD9mJAmvy=J)3>HKsM4w7YmauU}&1|q05C7Btf'
    b'Os+_72i>>E8yU6v-m1%YH0(QOlpOAl?^woF>|4Z$D~Ehs5g!*`!_C!6`Z(l;g6R4~7j*|LhN9OI(Fi%1+%WAzjgCV2Q)b>xs'
    b'#U#-<ke25Gs=gQ!y$4LK!bG|+S2|6_)ul>Od#H*>Cc@Fi`E1#+N67IXxB(ypRO|X=ybX$GwC@E+Dew8nRw}EP}k5-@UnpGj4'
    b'n{0@o9;C&)%FE2(^G<CkB$bWTXVq!J3{R^z`pUO0{s3XB03eWv+FpvSm!ROis}Q1C)2zSx1HfRHUFlh01xK7dM&NYniq~kYE'
    b'|h4M02(Qc39+_yNH6I6RnGx@Gw&!cVD!WV9k!Q;JS@1T3f_BQ(ZpQq9bO+9PI_8H_*3D^%2yOVlbDD#1;zR?P<Z3A$Fitu?S'
    b'Wn%UA$rZHq&7rI8v>(TP&#_~xM6Vl66z3qZ-JcD8ErVRXs4DzPl8(c->ITX7w2frzYzOf(LCTAhifk9*Th8o;>4d%vin7(Eo'
    b'%i65o3ZOPw<IVw{9-A9M+v)(f`Jyi4v?*)BHDKWYN}Je$DVHRx#^B!7my_xMmp8zZN?5p!lu9QG3mej?sERJQ^k5@hU?yF#8'
    b'wE@6^~CLm9-4x%8R`-M>MPw14UArla%r_R+*?WvUCBOku4f@^g(g8d4>EbsU;)iJ;T-du(s#(HQSS|8DAUGP9H~Ed!DusFvw'
    b'AM;@|O001)WqWYgoL+1Q4oOi{Ye{<29G;Cb$=*{!oUi&!07LJr3tdw^u_MrN3@Fk|A8j%lN(kZpY!_s%svuZ-AHMBMetvv+z'
    b'7|mFk9~;g)G0&a+i(kq3=)qEf3H;Q9u*xdCp+;l||DmU5^kHK3f^9Y8Jza*U1fK{}q90>_2x{N42&7k1$oTZ@Cm>l()8AeLn'
    b'zJO>Htm_H+fUeLY}rgP$BIs5_Xl-9%QW71<1#~+0rEpJB4lWZQ*x?JgXdf|*r%_SbMy#a1+fZK6+u#9?}F|L$p=H9^F7@RZj'
    b'V@T}tAjsxG<<L+izg@VIkz*Q|VU(fa$;hN@kaf1+9#Yh?8#mza`=I_Pn)D9u&PE&_?vg>TBb#No8waXi=?}y?Z-Di7-tfz-v'
    b'?^<O*=Yb6Vd-7oWOjG<;Bc~)_6`q%wm0IyvMZShrWkF8wN@XPx3UYYx8#6c!wEj^Xm$FXIp*0N3+F&X>Cmgr_8d-dbi;V4?i'
    b'kj>&-Yw$zqJ?k=!V5?YA~@=?X5as3*OCCL){p(vvD_s@=<8}7F#BZ9IZf&LiNqno6!pFD75SDHO956XSn}2$dcIRYY03LgXy'
    b'myD_UJJ-Sy)HYm;|sU(W-&TRo0k+;sHR^!E0El470y&_`#34~pV_c7$4Aac8#&dMkN2!bZDftOFHcX9J8Einzo(HKTjgGa@Z'
    b')Z_{tiZS0k!twPxW;z?@18vW*+>JCWzBWlEAI-7v4!8(OfCbbi2t?lgu*1ZAgW=a3(YzGcH>(Ut1Djp;oPK^Z{ExA89EHAqI'
    b'ZFtp-pk77kB8m&6{2?)~7pV07P`68+9#hFW2ZlWqHzr&3*4;deP!Q%iX!XhIi-o&1<^1{72z4wuftJy>WP2NN&coiL)FgY_n'
    b'}&#Jzpx^(yReL9uC?q28;Gv-TsXpQhch@O+T7N4Ws(gNOG~dAb$5w$IC>qR-5o{b9j4uIcUuwVU8k%V$Wu@$y!EQmTZ^ztIy'
    b'Ln8O?P0;{-T6*ATL?k7Q5?hGWt9wqCI4>U3SMg@3dbgM-N&W?G-VX#Cy;My8Dg|I8Z<DIkasLwZlEgM4lTl*x8G04UloVZpX'
    b'XQJ=_|at1@gMws$3A1jg%2(jU4@Q(aJ|db^VY)OL3#Yp^xM@VdK`z#IIXtGPB|^O9!=ZioqYHz<SYL5I&m1>Nn%fuu|AGM0V'
    b'O*jBU>8;5w2wM_2}1|+yLCBMjOK`bQp|5^;#3tb`PP-}H~mxR3ky1yA17io~Mi48xTN?o>w*4_7OW;!gQ?&e=>6kO3k!|o1X'
    b'ul>X(ARY+z#6Wb`-3J`-#_o0?-s!Ns1CG0bfk^!9G+>of*lX}`k1#+)k2Q4F?(SycK-Ev|7zSeL?idcp(#lc&th;~Mh}57)q'
    b'&5*%*eYacEujqig^8WSuwU4f#F_;S!ms1*c4A*O>251F`dgrl?%pEcILMCJsm=x?h%O^Vr?eR_ZIeo=FFe?8>`C0ghGT%^0V'
    b'N^$V8d}h0li(v1YC3_FW^(O*lTb9v2V*pWBHQ}Wf2W!M)$#bhB6rHe)QhzOB%|+cfD3m6F)GNWsn^a-c?GR@w>UfP1rY-yPf'
    b'MA$|h_CD@HpoJww@afo((Cg<ER8F_eLZ==Q9?D_oZjz3Ldstl66lz0#fAwW5qnk}jy#*PRB%Y-%vEQ$0i37p|dh4B9u8dr&?'
    b'KO&Q9)EON90H44=?S8qluu%pm!2vqGae+GPa?Brw)!w13N8uAXL&@+>TH$OuAeJeQ}or*=fS9WPPoxC{R+q$4VYhR2>B;D>s'
    b'U9c*>pt4+d&cTK|mMWN19V6M76!F@(rA@W&y<8ZzSEG#fmL>0Az4UG#yq`d(W#=X?kK3eSJl#vUwGT*o{%citJfT-w=>EmqW'
    b'~|1GNYh842^PIgcBk~(dhfbzzq2llz_4w>l2a2<*fo>W6?E?`%hLD}iLROKtLMQs)FuSkP0mo5`R?u7{;;|iYkBGS5NfDRLY'
    b'?cLvF*-ZEZe23a8o}*ySHfT88qBOlV-ACeH}9y*rhKRfjzNgRD0bwX0q_>I{K#~+;;c^s3^P*Eqi<Z6q0dH_qtq{p%^VAXkZ'
    b'{mJ&<<|<k+U>swtYXkAnaR1Mb?$p%Oem-Bi+ZX?F)zo5=JjQs^Fco!Z+La>$~Tv7J0!Jq225jUcTAi)dZDB*KFVP}|4-`7ps'
    b'k?4mACNE}(mKF;O23475z_ONDf!W)siE7QwBPeGfg&12AdOIq{(FqVuaZie1uxXxCl+cquhxCb-Fdlp0obMUGHt&*tdTF3oi'
    b'd-hw~$FU0G_1`$O#y;k@B<G4Q+cIv%J~qApQ?{+jqz=}?Y`LU;OgM05Qf}d%`_MM_#$S)?rfg$a5q=4VX%LbnS7>0bUHdr1z'
    b'HnVFtucVRJ#{6yNNf8y-NL}W?J~96zQwagM1xLuz`?%tDr=wr^zGZIXV>7NeaqAKZQQ}(UHdjnHn+O1LZwc``pK^CTazTKzJ'
    b'1$OtQzySr>uDS-<Y?}wmuJLC>s;+jd|Os6wZ4`MOT1o;o1waF>jO7x3O=#gb`-Gv2TOiuYF*_fK}btEIx`?Q`(G|wrO>e>+X'
    b'44nUs}{f!l@GJL*FNw`-&g4ctDwZ{S8kP~qq;A2UJo-z$iw@25oKsHrnVs~ld`)Y(g#A*5OIl4kmfW-Ku{D-yy24X<d1ZVj$'
    b'zhM-S_BWf|dcSW-a;en)}Tb4(tKW*5?)^$J9IUbvlDVsKy7@VF>Ue8Qk&P-m-OkT`PUdv2g%1mC#OkT)LUdK#c#_XAy?coni'
    b'%r?|t#Z1TCOqiY>uox*ShljZRM|D(&+!BVU?pdMXkP`*QF?Imxea@!=cs-<JczUi{GHrS`CyVB0*)uo8O+~saXho{8cEGmjY'
    b'*Ej+jDZ;FBR|72_dey*GAi{Tf)fh?$$Qph(;=6szGN9x3~k5c2YecUzhznuM&AE`Pk)xm<ZSl>FYze9^dRa|0C9hWs%ITG_4'
    b'NW))t}mNLj**b-s2TvnGA1UChlFvyeYOlum<acvpq|&E-<|J<8j(C|Jq-xN!+)L-ymW<TSjyZw$LldLcxT|5~n47+p=y_w=}'
    b'CZdb(y_1FoAKoNEj*+Y+}e>naC0i6K5Y8{jr{kn;rA6Lws;kW7O^4?6~22yfBd+H6Do40O!4w%p{yk9ySI==v1v8HwF+NpM('
    b'uU3;z5|CFiLue@Wbb!cgL*D`g}GS1oXkpIxotJ6Q_mZdcg<7IGTGiMBaFFS@|-GCj8D#m~vT8jS8rXAB^cqx*dh;d^RMrP=q'
    b'8*nn`FLH@+>mt))!UFAp6Sp(_=4P8g`<t2h)`o^?t!px!(L}nIX*dz<C1KYp?L?>SY{|Bkk7pODdzpPRwMmWEX}iCv8<}ZaG'
    b'<_ShZ+^B%oUlB-rXUhDn7W49w=x@DAZ1>LkFp7tk^4!@lE|)WS$2Xii==4n8k3!9&r;@^mnvJvWXt3fy@AcxlBffFF~E5G?q'
    b'&QsK;rIY4H79jOWnKFBX=(2cja<-E^Bz88(^vN<TnMWNZiE?-^+_PF?qO&8Dosz@GZUJJ<R%BdcFGvF+^wl>{$0QW>}YXC~a'
    b'w!N#Dq<6~N(*%=rC=cq23Q4X^B7%+^iJbU8gz&Ww~xSdZD8mm_yBM{Zk=+_R)_?9rti3OO5ZDp0Z=B+n^o8i2|yNalwcFKQ-'
    b'X;fiJ&#z#KKCSq5-zd0t4bR%<5Gg3#qr<pG6#><+KVGQnO#!@z`<qqy<dKXKG;i@Hb>HC?z@45~AHLcs3eeND_XZGyw`g3Dm'
    b'OUOZm7?bMZX66_?p|qv$Wwvf*`h#M?X++ru`a7A3h`{#RKsN&PUuFV)^w1(@MuTW>V|Kp#_^a+=cE2At80zpAW{vUTEzAa_^'
    b'B|K4C2nCxZ);@EpYYMS^hKR|oR;78Njd3nFrog}eCYSnz9v`RRCyV@k8DG>D=*m%Wy3Aa{Fkl#EzLZfCp==jxt*B+_*<Eo@7'
    b'9DnnK`Jxk(q_+QD`|n5`Woh<QrdEA`7=M^F=kg0nW2k4{u{OpmFZwZ)1+Z^$l=y1Kf_ohqp27Pf`tUV>Y0ihdp&0v-9yWj0W'
    b'3s*!yoNg=?Bg<`^$&4uH+~uSA|HJ6bx7X*`jw>w9#iQ(oerDOyLz_FwGrA8Ex4JiM!^I^{1};u*#W5v5381dUqn@FHjpu`CP'
    b'WIY`h3mB51pQu|G7zhvVT&;-nJ|A`O(A)fkMyR@O_zwO~6x(&%(7;Qe^qw}&bcU!b6Y(@&_u8NLa5{;Hs?viK&H#flTIJ|dB'
    b'G`O|0mqZ)58-u4ViMBvC3lbtduLzRqiA9hb>e7b!8YN4>qz_{!`4Cp>P)_(j1$4lyU#CpKVx4LTD|LXGY+p*A^tRXcWAony3'
    b'io5jz8o@kKlX5b!~IxQCL`V{mrT|yoy?O_f$C7HS(qGw<%6bXi{vAx=8Ak;=uvd)$#9{}sQwj2W}FR2ndz2H?}}m!Oyy*W1l'
    b'_%&$h6Yo3>3qV>%A+AI-LA7ehCdjlA_p?{o9H79Si3VoCwj6P1@`*#3L5nTX&2Ze{LL<#=!-}dZ@_tlA%gtR8`wM$S-2}Eeg'
    b'YK%M^I-Fi`X0hGIluDZ6JcyC?#~iEO=G2vZ-pzKbWOghxIU?gui3sR<q&Fi$KW+)k{g26`n2*AwH2O6CJ&b*PDs!};@~w(~u'
    b'cPR$tHmEz`huP4^i=ltFK?)Ai0)e6~C_j+PIeHP3}n+i2*q`*(=B(}Z;-Lp)E8kI_<Lya=}=uIlrXb5tdIPl1MIJ>4z$xJ5R'
    b'sI*Ll56ZZ7z)>INta~9a#LQP^`YIqZ{w%YyG}7KVN5)HkqRE7?`Q6i@tn{09s^mHqdlV*93YCmKYEgctV~_g1?rii?2GSvY>'
    b'92($!R6rEUlS~kW;+pvRQuJR3PZ}ZDAQp`frp7<7}B~cA)sb23~BTkgON%zlL$kq9e$T18HTj3+>txAcgqjgw+!AMzcJO{#O'
    b'hy=s#mopR{p6`7F$fp_@if^7-|ugbGTbqD#Cd0f>fWC_ijiv;sfkKJG>!P569DVA_QZ7I;?v+iiac377yYNFGtnl(2~A54B2'
    b'iC^PkKx!<#`e9JVu>_=8i48-MZO9Ly`uy;I%g2BI|Vh=vb`4%w0@(bjRAC9Wn3?vIu+Kb3lfun;%8yF*_u9)lv#=|K!Kg$Hm'
    b'EGKl*d_>$8Dr+=IwZPEc2nI*ogyLW6&>AWbnrt}{<O`w|z-WjdS@EK=Va^V^<W6+v0X)Vu?CgA*8B)=V5Ur!EKs+5nY?u^Vd'
    b'py{%_>E2D2@h)Q!Jw~TXhajRRgSim+CKFuN`hfDzq%ap+uwls?m+XM$mPpMG9T31?=paejytIDBm&s72pknzGC2j=kAX|sj='
    b'f9SK7WH7JrIVVz9_e^`G8Kg_jHbiQW<%%bhH{BS%~FXxeTbuHE_5L>3&|bk%Vf`;=u2lGd{Qxd`%MqNr>GgX&6f~$sfXWFv|'
    b'P(33#WCL{+^=WkpK(diNW_2$y-v_B9Z#~*S4sS@k@3vATu~jSC{uu{Ci(f1Y4!~xM3M=f!=rj=m2`$l$1`F{aaiNct%-&MNy'
    b'-$8hk}DU?eo89DPfx_Z7u}lXzONcaRxfV)f_$@Ic4lZdWHuiEf<CrxYVf{MW_GQJj(?lRB<f*W&z%zTW34t!+_&b|}?*A5`p'
    b'2ui-Lr=d0ZnVBfMFT>J`Sj+pZU>d-5FO4jrL)a|brAt8L?xqJI7On&`!N*@Q+WvVAU^gpE7UJ1}YQWu8Dtps^&hW3aOAX^4U'
    b'@gf~5DXbY9B{e1buW9xV<<+G@`;!^-gl^u%dQ|DO%bd}Z8uwo6_;SYC^kF?~c>A!Pc3RXSp6Q!U-8)$bMhfxfrQ-C(zrL-4s'
    b'&tT?!&_O5gRrYHFVGp7KEG#6|NW`~0|&!H$l>|D8ht6VrOp#*7WybN7qSB8d`A3aC?8zR>M}{d6F0*Xe$8~2HgU`^wh)d8_s'
    b'{ypHW1Z`1ApD=jU(=!`V+?S7PHqbarCcghHIHQ{}<W{qv=^X@#D?Xm2CoMdfePeu{3Yl^^Y}0Vo#oJN;A&>DX027x~Zd1cmc'
    b'{Zy|Ab(4sOyU<x%fCO;0RZSDN9SnlNG=0<4B>IVvd`<N2%p#hSXj;o<C*9nk}<P%RTFOcyAR4RfZiVkS)`JO$sZ8PArw_ENl'
    b'pTHkoLHf1l_vcaATAwE=}cZL;bjLcb1__^68qx2uMxtU_=v(bGsszW#`e2+)V<Pw%zy0%5t5)ht$CM+tPT-CRzdaKT0fU=fZ'
    b'RbLx54U++t#FZJ_N*TsmDv-yA8{>+#k=4c&jl&zdhL12!Zpj$$m|F1?AzlV+SehVn(8x?%+#7V=%dVhkG=r9wzEf#*p6i7pY'
    b'+b{v*JXp>e(qaeJ<NdmX<W_(qXy%dHvb`z+79at%Lw`&ZQl;7vFEstoSHDknmqLR>DnagoofU=hon~5C`->$W195|fuYXOGS'
    b'7N6DFq3fzKzyvMV5-u=LerHHv4DQZuREZe1p&ogSrIAEM4u%eTLAVZAsp6OJ8xr+adH#&BSL2C256;&k*`!#lz1K`qS^j&k%'
    b'<6-W|94D}>yO9^QnO^+&(_B{#aOhF?AIoobH!T9?gLA|Ib6Tyh(h7#ve<-EwQAxVh%mP1P)!%9E)gnJSa1Dw(R2sV13fld0t'
    b'ZMIICW2p|i(Ss&`}v@wV0hF~!mp*z!(_<SHxV)FBW)@vk*6SM(fa)4yu?y~g->IUFdmf<lGqv%?RWtmeamS-BbBGZN)u2a88'
    b'tjeIo4oB{9+6@o=>aOkFTGZj3d~W2{WdJ41Aki~eKmCbfB3Df2&hO+VzRV53>>df_F<wkOsF;G{4vqh0N11ER_4F6r0=gj`f'
    b'P9vzF9~{Y%<!9mAw9WT{pB@^fKvL1F+m*Y*6p=sscDhEzvfMyXf!wZRXn1>GDT`rxz$FDDAK#X7GpyCi-}#^So_Y$3DKDgY^'
    b'E2^vpF0YuePN>7bwcm=8#|cm6W=`7ONZ}r3}}Pq5<3*dNdh_NEzH<OO%r1VAZO$!IiWjh{!-NVQ>=L*tfcL!quCd+_p^HL}O'
    b'*LG2Jp|dmtOHnhmkiH(wK#Mys{ednB0L_x<$Efj66sHplLHnUt)lD`0*d)EB6lZU!>x-i2y`J>{kDLUle-r<G_Y2bL$B+fbP'
    b'+PMPn`g&SCjJ5E8?$fTZ-y64pP4h+PG8{XIn%AgzG$_awp4ez@}mMC&F<wQ;l)wX^DB6B?$D!6qSlPzO&@=kP?dzS?_*PXgR'
    b'jt6R3=F(G`zT^kzA}Dd`$?NFJi%$gV7zLji@R4cjeIL@R7QKLvO!{zlnXe*fR@!_F47A_0Ne99}XQdB}n2Lr$cr4={T>q=(='
    b'!N#(O2oyWaQP||90PBnOOuR{fw616WKV|4Xi9csWsq~KIo#Kalo7v~>XaFV%SaTnsbRFN36H%tR-;8RghZ<%0pVGQ$U<ZmqO'
    b'uU3g_tbFW+C0vB|<YX2(=+}Hbac4fdaKfxT&BS&}`{L3#TGypxZJWweotx&<u%yt~Oc<C)?0o#Bt~2=7sOanKIcV_DA-tlBR'
    b'6<=&S9<(~&Yp5%jhLPq*uPWxGlp;6b%Qe^{BUIbOL{$+7{79h;@qZm>3nhD)%sY_d}F?Avm*vo4Lo5IBRSCnwkfJd#tcxJVZ'
    b'VF`#-T4TVh7iO?K9EXF_}IytEDk+*KBCBt>+f>H|D+s5lR8j5xa$_gK2DLTSgdg_LOJMiR@KgK7*k@C*R4od5f??hn#D?F|f'
    b'vV%U45U^lsG1U9U0R~+58k|>H22v!%_Vokl1t6E6qbH+yP#8biD%}6Zfr(!*NXGL3+0Jw}o=1#SUrLjV=K(UKKCBxW4`g8-@'
    b'h@jNfkphndfPCHdAN_=EletaV>9|_1D46r*DdtEA|Co@V=N7p1jp|8qG%fv&gW&mZxBy`#nOqBj=&zedAf&)B5I{de&8SmTk'
    b'#t~wFzapqK&B#Zmz#Ym}|OBHmzc5mjoNXPtp6r0nVChmrj1+pw~k0^kW%<@G0rHCmRz>7)?8FAtv>>nH9(gSfZ|DbtDvcizk'
    b'cnVKw?64T#7Kbd`?#AM*sE)QgPRFp5FYe<ejP<b+{iuhUwSj62cd&z}~2qQx0n)>*(7h(dKk7z&TE+C;O1Iq3yIZ1=Bgs<ni'
    b'>C-a28*5AJE%d=j5ih&odYqa5q&bUbhsYnaquKCu;j()_7I8qiq+|&zO(QLcPRP2geX`2+%gD4hiAY=w%{R1o!(7$hs^y3{4'
    b';k|f=s+rwEu*1gu>KJ#wDx_h`u=^K=G)&lU|N4-IpJ1l3Uu&VUlySyVb{R{VWGrQkv6LakQZ^V%nO`hrd9jqy#ZvYbOPN}1#'
    b'L8kh1M4>iB>Ytt70Ve@EN4ftoC(Ep))UJaPAq3Lv7EWYa+VUy8A<F{+ea*I8nK*J#Bv4^8?l90&J1EX3y9^6AC|LwSkC0BW1'
    b'=N(o*J4mdRW%p3E>G-*3Ni@!l8}Ba^?-onKp@NgzYokL#u}644RuF5~j?Uv518DdxHgYQ$)g)@xsRK7M3+xSk_u$Swn?oZ4{'
    b'O@PgveEVdGW_%Nry$rjiB;%NiprYlpDB2~szfHbI(2lrlls4c5n?@{aYvMhp-3i8cqzn;S7S;3t|JY~0ddStElb4GfmFE&dn'
    b'6*|k<e){cnMr(;KaVnn-v82MDgf&B)Xfo07FmbDaE)<|Gk`+#Ll1D3T4*qA|(3~Seg{cFP7q4K}ntu7B^nt#xmES|u3;WMbX'
    b'X;G_9Xr8Lz_jX_aHhPn#u<KE-@<wkWH<Gl$s#2;^oUlk)e~givMsu?MBVQ_fL>JHA1=R^5K(66Bt!sTb$kMko!~G}YcyoCj%'
    b'QzlB-~$$(_1cq1V+YK^CgHk??QGHX5&ko@7+OTtT2!Y_Fvn%BMp^P;mf_=zC6mpab*#?EegNe0=70>H_+`NvCh4|b$@&nCwk'
    b'Z%tWsM0IKUx5WKSgJ$F@Z;;!pC!KMXJ+fSn9UcqzH9!H$%~NgR-FnU3xFDb<UYunX(kTP{?q4k~N28+7Qq^=n6_beqk6hZS~'
    b'4EJ@AJ=te;g^BS0u7{SKj^s2+XJkCeUod&S2Hn@mKh86LSU<KaJVR+5#N0ofr)2NsYj6}PqXrDV30kH6b57KN+M!v=ydN+BC'
    b'A(KL#4<nsxL&6bG^ZvcVmz!*KwRWnrVSuem9#d_o()>WJ-nqv||k#;(eSR2I#OhPR8afb*wQGJfoWd$1p@&S;0uS6ZHu8VXY'
    b'!^_Pu(2{dF?Cy6Ae2lpvxQ-d<X^I&xXmw0Uo6s%$4YHIzOFo0UAp@PiA8n*`W{8*=jjs%0Gc&+)4CwMee@eSg{-HxYd{ho-*'
    b'%**#fsVAo0gXl#Fa#q?myPj=a!{*9f_!*9+Ge#J2di;#etr;|orAy)#f{w1KxsiAGVk~<h#lr&HUTSJmTeyvXF-gVllgL<C-'
    b'#AH@`x$sfO)Q1ujHB=HYs+HCvv~m`X_#)3HQh?VtBMV$R-;H^AtFSJ@ZVbHz;27jnd$*5}!-c#D{CKmeOa*XK*)UAigo$i02'
    b'!F{__|vk2uE&xEu$&F|g+!1KJGVknjv73$<(%%Jb03B?hEcA7bTp4D9*DU{rVpRy2%a46bf~k4;Hr78!V87xG4S;h?-kO4S|'
    b'j9kGiX%no7D0Z-~KRWM+j_(is$%op?wW6&KPxkVCkBddt3v(GT<beqZFV;;R#-XCC1;e)}IQGU8HsAZ#2o`*&XPogJ~cLEk@'
    b'N8Lm_oFo+%uEV|h?`k0N<bgH;tQo0G7LE7z#As1DQj{oY2rYyE5~(N;%~Dd?1pM>C*a9UD(rqSxprn4fr=;OfBPk8L8XfXPO'
    b'Cw;*#=(39?8q^RZv4Oyb!%6_k#Dl2$;Wg%R;U~Ux^#^sigSfX5D$RS$3z~<oZjeyVv;Yc#=&~DZZlrEHj!L6(kK@x@z_9#dE'
    b'klSNTD?-I0j`unde%PVyq<!f|+iKHd-;@lt70{&*TbhD8IfuE6T#Pl}dTzOL^m~*8pbF;~7jDzugc$0X~%{Q$;dW_EUjTKL7'
    b'c$dcRv8s$Gs->h{)q2nk*%t8}tXC$|D6cf#=18}AudnXb+ylSwPc)I?fA4i+LvV`QdwvX4k!(ydv%a--Wo9~GQV;c(G`=&@q'
    b'qzGb(>2nJLd11gPyxEJ3~=ILaSPEOCbPS3bb&v@LI?<d=I@-P4sCA$X6eVD1kR+~=nZx8g__D*BGYsIw@ZJ-z{nI61J58k8)'
    b'Z_<M|>A{=y6r1!E+w>IM^c36l6x;MDZF-b8JxZG%rA?30rblVhqb$>-^t<++AO^7XP|NgC%k)so^ia$6P|NgC%k)so^iW-Ts'
    b'BVN|IWYp0Be3+KY4@Hv-E2Gh<n*9ZVU1e%GyAz+GIza`-^-r(vS3ITnf&C^B2RFlkjfO&nRL@sHU!#Cp)!-68F}Ihf<8Sna@'
    b'w!YjgXHGfB5ylkghZ7@%s%@mz{K*$zNvDGb0z7k$b0iW)Q>A1BUbwI``Uc7O2O7X`&|`H<jKouzXU6FNWQH7r&uUD>E!T9e!'
    b's&gO<31uT338J*3A^(&>)gQSagp#72g<iwwJ#8S#@7OF6leL6Xb#r{2l$r;7~t6f%>Zt0%+0B+sO0>d)xK_0IhuJtIOyX28w'
    b'n`v9OsW@tX9k&ikfxN8&q_4S?QcC$Ye|L64Me!bWn!l5t!|EuZ#a<Qxeg~QunR~?q;&HMRowfV7r{qUe#y*<Y&l%xnk{?DuF'
    b'PgjdGu#)Iw9{88e(NrG}yT!9k{hO-`GJ&l{j~yCh*at|Z{Y&Qq#o!-HsmKBRxfhF{DZmZivud;Tb(D0DS5VLv9RO5~^0%^4H'
    b'-}#AuP!mW!XuuV$xscXQeONLg&#&pC3cJ2hrs%Uqtw@gU)<BedlppbYyWKs8#4OB-|n`TSZnEHKw&T@Up@eETa4jQIQDnF!h'
    b'r}FH`<l(KgMmMWb!4E>y&E*V8rspht<ADe|rYcsyB<P_2KE&x_W-TTK>4H_IvuUC!U*d^yk&|_3G`DOPq_<=4~LBY5k`Qrqg'
    b'2>P%8fEf~EhMmLNQ|0px!m>ZCtiARX8a_~UpUWB=1G(`Ees)%5c4L$$kq0f+!w8UKTUZm(XiD=hS^x@`AivD+<v#`M3g&JO3'
    b'jir&25e%MofJmU-h`J^3W@8-Oo{`=|2$;%H*Jvo`&olN2JjKSl#{;`6`Ej?D4VL^{?FvFRDtTS9O#aXNaUpTz{A-;(Ai-ec&'
    b';)`mR!8Y#47XvSUiZ2#kWPB;%<@@;J+AoWCnb&Q|5?&t1mos>I5MOWzPvXm)RvCfg`60gGc<#j)9MALkg5&u)zNl7h0>|^W_'
    b'=4m4b9}+^oW~a&&tKz<Yria7coVPNwJqC_vlezl)y{_Fc^Y0Mi&Ka%IG)$>MYJI}o?U!Vt-(ngPZeKqJQwlBwjns4O?+|fmq'
    b'q*Kb^B%6uI;Q1X_J_I(}E}*&t-hU@hszuXq8dz`BADhY=z_b5MNBYYTJIn@w|^OuKluT;Z4164M8p2kh2zcRMpP*hH!m-=gr'
    b'k-d01^XMO7VD)5COk{BiZ>=<`3lIJ`POobR^pi%s>uxWC(NcSn=&wwpt7Q5|-x<-XW#55=49)#eQT?%;nAcE#k&!<Uo!<oM3'
    b'IIux7JYQ8*Q?Cx#Ps=J3Hc0AplV!bamukK(Q+grD)7wpw)Q~dj_TSuGIe}93muX1JJ@t0pF2at1<&reTxzx?w2Xmbn&Lk4Rx'
    b'$1zCMtKH_~S#=4#X$$pD&pjL8z5ph<qJRhRj8f&t>ODLGhk8$pZ2JPZ(zO@ghwAiW6y-a3zF0tpkJnRd9e%Ixyj$!5F{it#L'
    b'EttZaJrdpr~BiNyXr;t>c#fe=|{rNVfuEry}CTvO?TDh8pZVHdb+=LYXd{@SvRjvW0~X7k9~DI!L}#U6+F?{r)x@E0g)`=X>'
    b'qZiOqckuTvnUI?6Cc@0?;|6yv95i@UUEO_do?#@VMG+Dp+Edq2BEd*y$O(Y%gQUxAcZjZ{X<?M>m<ihR0pCo=ks+hrJgvlj('
    b'=hC#>ZT41Zri=O8bVm=J~|t-Y*%x>~)POh3n|>}Rn=s4p*f)iRfZmdwlj)iReuE`5&GMJB^S`$nqW<zxt6EZ)?8sD4UUv3Sk'
    b'uUgYu!C1XFbS%lAWu{z6SFdiT4bq?;a@%C-54<sC4F7`iPT;}=^u(8-J)@zv2M3!YqSzd47A68Ym8X1PNzgVp^S%mZRzyAp|'
    b'vVC>PV#SF%-{n|7?I+=Eo6RDHuj&o3txcvD=25s=W#IfQki+)!u+34FZ|2?EVt<~?;_-ty-i`MJy1H-A!W#h3JsH4IFZ&>xA'
    b'U}u8arlSV)&o}r{E!CjYvAvw(%<#VbdsCgw<!5mKX)g2-)@*o-|NM?y6j^u_U-?n?p+w$wvk2QzasPQaioY+M7<wj+%)MUyG'
    b'`0{(sp+pZx1ce78{9FNGfp}>;Cp{9(eL0C*7X=-MiaZ5-=DHfWcq@3}$*f#IT+Or8ywc^mx9mFN3rW3-^5pDtw;R!AzJ9g3C'
    b'=%4Kf}xCtPiUyoMA{R8&XncpQpaU`+~Yff&nj1Jh(5)z-z6^8Kz33wKKfYiBJ8v+XJhwn@A?Ru_xZAHzCCJhRtfkmkqGFxaz'
    b'g9U383cfl&GLvs`GK)qC%QB`KC*yWgVeyA*_GRs8u`a;SC*A<iJ({gc11c+-`XsasVURl-Q_@r!I#p=?M;aedIFBvY~Ci^T8'
    b'mh0kBUQ0M8c~b``@J&KyuatC{YFv|$xqVXwvzFpjEP-|TGai~`8^!yq24>3j?01`5keOJX-Y$b$N+x2h%$+I(Ly4fSDI?J(u'
    b'<u?*>9Vet83XNMvCXSt%pouEjKnhJQF@<NV+aOJO0W$!*TwQIksrsailu&X#WH!5#MhhfDu=Co^1gbx@>Wd4cgMjdS1hN#yB'
    b'p)=YOE?xQAIymJIk&g?|_?J%~bVw;uK?yWTtoGomd@pH@!pETX*9QtS-9~cfkF&4R%?biZZ!cDteE>c{8yNhDR&pPF*L-o4d'
    b'Fq2o&hu{m-nD9j*IvsRjv`wj_!7HL9(*WDccz?}D9ild|_m>s}S>Uezj=-qI`8egs;`x%g=P%cVwX5Mc?uz}OuKZDSK=)gWQ'
    b'd<1YFFVBXUCv{MD-X`r`dfPOF)lOeVhag&s(f_Pg)N{<`&#pYd@=5cjR{ZNRAxUju5iJ1wIU<XS3O$|X)Zg^bWYMON!Ahit('
    b'r)sx&gr>`xhB`{tkh{7T6?D^B3`Pc<^&Gd36=?b_ZQ{bt(trrKx8u^D#e`W2n5XqHA<yp^q*Q?73fZ!9;Fe{$+D@GYVtbAI#'
    b'5$$|zGSd3-UX=!t?2#brH!r@-=vJMk_&}y9be~Z-6&m&QMx`duX>>ds-9QOT-QhD)l5Ou&MO|pFRqWytBJsKWL^aV<oLXj3|'
    b'u&%G3zow>gUx=ov<96SAm8)KCgs$T_*8$T|Z6CiB&oeF=v-RreJEOkKmG@k4>LTds>iauN<IN1z`Qmn98<@Bb3TPpUQc$J3h'
    b'@WjcKtvGR>AYx>|g*GQLXs0<nJmTrZ=7%aN9>byWh?So6sd!jxzg2UHeSQun@O*}9K6>s4j`NaUyMD%tGK0=X(zXig@aV2x_'
    b'E$pBmm*7)uYgiFDXKi-8f2sMRw*B<KH?*blO@-_@cUh=WoL&Xyv5S%dXWq^^;!z2z;t17^j;`B86Jp>V30WlOOo48*CG%rga'
    b'H=-*9z>jJNejLE6q%XE}+c3!XX}IEM+x??Xp~Si^j;bp9rq5DpyH2Wd2fQo#I*Q*_*Wd{S<C|bxLqks-%UuS`H@h_2hLr=r^'
    b';~WM=*UQpgtBhI)s0rr)3DjweUMgRoC$$RQ1t^l?v3PLJOhTT92IfOyjz1TT?U(@!@`}mOt!mCxDDgH3J!0h>GUsOR~|BPHh'
    b'xqze&Wfaxa<9{5<s2RUDtM?JYTQrI|=Y5tdjM!<td*AR!jx%(hp&5Ht$%rg?}PWdY0>O`DPoWZ>q3VfmN|N3+hX=s|yfQaW1'
    b'zx0wZ)D;PFi*NN=!OiC(`Bw%ecvNQ^rOwKt%H1SY}aBy1%2Hv7#oN$JAP!XysWMshc{Pb<MBHg70)N_SurO2DQXn^Gagma2a'
    b'BoXBmjP-Uy3W+QREM?kfE&VFC8GC~~DHb)BRxMIg#%P=JsMy|=LvBI`WD)p<C*r0Cew7H2!QK$9;mOuf8Z)?E<p?EP^Jsb?7'
    b'zTv8&do&P70$CJ;WXQ8xjd~>R!a!xc3v{qjOF-=i^_Lb<9j-v!O=*>?rstUTUlLf|-r~x-K&%_6rP2tO`*ic6wv{WfbTiPAg'
    b'Mdk>Fdo^8Occ;jh80Vvp#s%ynT4WR*fCe>$;`XQw&~&+?&l-heSL)s9K!dwRX}|W#Mi1-lTw_7@3*_!-XC(Oet8ULEF|<1(G'
    b'*IEqoQeSil+McHsKxsfFpuwEMY2#SxKCU1gjG`V&{H+T}O<<a731_E+tU-wjL~)E;ZN@xc*3XRPhGGgqbLfw_esPE!M4-GXe'
    b'~a5V6Or*kr0fM2A-inX$3;JR)Q~A!K&-;$zmctPko2XQeijcL<p)Q^*`S6`5Qqj7NmbQ~@0qGNpZ1?ZJZBt`^S~s$>Xr?2+B'
    b'LG-x?`G&#~B#|~M>OR3|%4@)|w>RlF*yC`3-uM$J2w84Z82Xu!p<EYr0E_>yzN}XgL2+0gvhY+d_YpU((6*Amq(I%-9mLqXC'
    b'VO3l^T5@v+i_AW&t5@qXIMa{Ho;<BQ!Vnr-%>jo{IVz7g`oq4IwY3IVV%PoGB&u9Jgx2#k$!qmFp#h`4OX1Gf1R5LbS(x(;a'
    b'+RoNil5jw!zKVX_{qKoAQ**$9JG+W4AS@E>N>1JHWO=Oa#Om}HlacZb)<pIiP)XIi&kM0=b(kvf{II#qcE%^q(@;c)eTs_PQ'
    b'&mhtZ`d)H?Scmk9FVDP}<&wwVk{&Ph~0umIAyq0rBp?rdnZP1$+OnxT33h!TlE@$THZ}F;m~*6|_dEXE*_~uY{4uCCN=yx60'
    b'GGu&>O{?mWp0dkXfhvq`~u2&O>mNLY?5Zq@66#RlrTI}b;h4f6NNIt*5KK?i+B0)gd0w5bCX44x!A0;MIOreT+M!;as^dP2?'
    b'v=e`T?LM>Q{+44?cVTe(;DtkiS?sxm$(ayiT5^$m=$J#DA^nF|dY{+}j@&tM*xvGO<j*7Oyb!Y>bx(YXByGJ=z64z=7SN6DM'
    b'Z}-vlQEr}TL3KBaE{FCd{83;rO9h|aIR%4(#(gKJE^SU_ouOuACk5U+3Vc~)zN65*TK`zjeSu;X{CAuKLz@GSRlMvMS@0ONI'
    b'LZ4ohFD1s3RYRAw&;m{*N=COsAy3dX81?~-vw0J0Mb$2e;0H9LPh^lH2XW6Ooo8TW)qZb4gZVs|EpNcO4iW7Qu41w=%dM4_d'
    b'Z^QX|_y^6KKQARv1K<&e{zSFBGfjRsNwGq$h4U?AwWraqBR!wlrms-ayy`ep5WwH9>fuKMgl|EeOk_SL?7Igc;?#V0*c$h2b'
    b'XuBdW<4Lga_qd~VFk=(@Hs;&lEeu??vK5@xXL+KRZnKV-E4EJ?rLhSyb;i05^KnO7W>FJ(@@nDdp)85MK>C3D8boNr}L$)^3'
    b'8%$XH){w8yRV$O4!vn=MkkU6Vj&i69ss+c35@eP7fAH0$|!(z@qWX_~)*enfs!s}{tX|YPgNnSM}1F@4$c9pqew^?0Q+_8n*'
    b'UH$=2N~$i-Msni~^{?@bkb!3>m`1E1!3Ly$p$!V8FAoE1A;i&L#e`NMV$)bLp(Thge+?_AiNeFv<o6@QOvFuKEr2}LIs(E|D'
    b'N5=gcwyyBry3BUqE<;C1+x@NRb9R?FR0CD>2(v;<zGn)?qyorr{eTwSBFFzQ?8Rb0QuVLx_119;+6+>EfJf%{n62134@b$Ev'
    b'Z7+ss|7zgIO)WO!Qb;imFak6YDsyiW5&f2t2}T;(83v$LeUhQizU>fx6l_It;q9QWsga`1dO^+YbJ#DuH!(YS0<)qv|Tuh9F'
    b'fCH&^PYr7~NHu-&#YSKI}Z_UT6beID}q*9m>GBdmW&Ax^vGL01hLv2hP<^M92sP7agGR|leS>3F7$pl~u{s&~?2FSQ6zPe!J'
    b'azOsgRZ67j|ov>4tFRqS%F686A_?d(M3W+BU{;PWAA~uFq)NE~P(svcL8PudjYH)+#JNU1VICt=0)nm@7an)V-;8SpV!OYQo'
    b'GCC4BX3nw9w{>RfJpqts#qy8D6;s{o#;MTjEAvD7R_cp_qN478`^GhLO%>CDaSK!jGq$dy>!2pqeNx?h^i&$MULFOTNwA~OS'
    b'%q@FO+Z1(ocg}M<HbdPEMKuW$1<y^cqq<@>&CucIGi{RSGjTS3qz;)y0*I}J!ySdr11l5w4C2?>T<}ZPHk?ST&7|0#sG?Hk('
    b'Y8BwIz<@D_5#1GXW~|g3Ts*U&_X}z5M?|;=;jyRp-;<M(z0hgisklf|noEs#9~JpwzFY!Wbwy_9TzzSW>kts{KP>K2Fc0(tB'
    b'Bjh$l|M=At#{{N{n`s^WHA9jkltSiQD!@_gYkx|T-_`<+t-w<^L}rhDd#KlO~X=E0xPT~`I;JQG3)wmhgpFj@{u#w3*_&&s#'
    b'K#WnULfqX2U0Ub@oqoK<bxW8C2E|UiWITTACO;!kxkhmd$>e|v$9+o@T<=#{naaLw)%oiGSRq4uXU3Zg_1Fwg11@ab`wzD<z'
    b'hpJaB^9~L0NC8}fD9OXL<^ga|n6O=Kk|eFxc?QB$L0Sc)@CZ$A8Ah8rG(x?q03iYJS{s;Imf+@rBC_>uZ4SyYsdnkExYZm~>'
    b'oY*8ZPlnb@8=5CMz+AO-eYT^|E=<aY#Bv)v{{8EM6V4bRtIR9f7^Ias_nOxnu8}cwnty6dg6%i=-RIDeU9Q9h%5Z#U^BVlz7'
    b'wB3zN`aW$?Zi?=)a&NTVhfIh=T->$+$<U0_aHqUM0&Qfw0mC5PouAg{80Ff(N@w0JVlJ_mu$sOL?1oJU6K>&1-#BS9~S1Yw~'
    b';22TpY-iU*;Y&26fCRNKYcUIn9uA*t!Ypr-~RStZr2;FG7Mu94#4sjiWOp3JWX2*guMd_?f}_$ZN$iNeA&bT8NTv6`kK0PV8'
    b'KYHk4xoiN#8mDtb~Oi^k2bdbJqRq(~~2+WoALXgxtO%V<r-UaD9f~pXPl^~2|$p=Ex?j%_5^RO~s*f^%ayK?@G*fy;SJhIes'
    b'R*TmH-kH>gt8#@A73kgXs!LDwJB4CC3+8o@mkaiL;xy=8xU8&pCM}yPa|6XBY2|5p=nceU?TFljl>z$V*~1sV6#>L0j=;cQ%'
    b'Gshke<=;PEd4sFdIO_Mzy7WAiHtCotkhp#--x&0L0FC26Q=M<SpED)>=G(7h4TFPt2)#y(!T$ynrue>eIc{J-}juVJm&5HuF'
    b'PYi{#})~V)_2RE6Xtp`IY5_DM)1=m+O}DZp3xPva*!08mpv4uzDFI&f0sR<J_yl$cxjrWsJ(l3JO!PmvdUeE#Ldl|LAl&&it'
    b'h3!l(H1-~Y{@_wM26VNr6RL+_0`oyvh1xk7lys|p>AfQAm&Sc7AzacpaGumwO3hN;5vcAw<NCaM63BY3Y4$;?J#Y@7;!$Xow'
    b'iOUu$m1Nv<(IXIB_YUx>(5S8|F!abAN9fz=Ush-?6L|r!7M%YNKt5P289lEg0oVsk3ni=%oCNhp#qvL2QvXPb&8vDVVI1~WY'
    b'A`i3Bsdy__gb*{~UFhHIdHum~G@eXn55DL+_ZC-4+C(3mq2F7C553_cY`0q<g>y^)9XsZHoHxVP!fbc$=F;`#8w=~n7x$a3j'
    b'^B0`^RowCw^%`cWLNN}P(f5t!SA8(JpS^@)33h%=G$j~{o8lXUwr?=-+z4h>ZhN7`Nu#16<jV?;nnpz`t8kT8z;NBX_oKby}'
    b'$YJyHlup)VJ&Yy|QsTuG=jUgNR;+3u`#o-7f4d`o&===IAot%A8fPOfg57c_DJVQL#)hN0<3Q=FEy^iaEN>-(^lvEK|(UWqy'
    b'=7rS=qabeVsNoc^#_rkJC3l(p;~Ya$nOK4;8dhW^VO^`-7rnrxp4Z?etNXfT|)qw%2MYlk)wx%)CKvIeHyg=y0NlQ0eY!#@e'
    b'rX!>WhZan?7XqpVhe-@_c^iRSx9sXJUG#h?$KY^eepPn}F>iEH*##WP0?fBN=E~3WM!o+>!lu(JZ;_B10Cu`Wv6RxUx*JPda'
    b'XMYkCP6pnehN=IjVH$iYCb4Edhcy!&<)epua)Mvz>Ve(U&p&S|<YJG7zw0NPAj{_DCK_#eP=gUkbV><H<k#AH9z=2H%Ve{14'
    b'(_XR**HnJc$ZTskM2HY`F!1ZMvl1w!xJ0BCE(3IY~jPgU?vx+P?)~7F<IqhDFAz0CiyB2-joRDL@xF<e9!kf(BHv*R?%sf<x'
    b'abl7#=@=X_Wr4qBJz6k`A3wN6YtRY*~2#Gc|xOgE)H$`-3Y3$5+)9Rp61yMO=a-zAYn)@(MIjB|&O1UzE}0c?n6bn5rTg<H3'
    b'Cz`sa^B7yf%i|2?7qN`19V%bkU3g1BWG?Q#R_m!bIYE&cb5{`-;s`=0*$ivIhS{$t1&_}_*59n<rrgn~5fPX6l<Caobe*`VA'
    b'_;cJNA_)(C!iTw3EyLK~@tZ%~Djo@1v?vgYoNBkQ<M~C~4#O?Tg4*io{HS!juM=bzorsTDL5pJ@uk*6O%>a@c$$d_xRX5%|l'
    b'n{&wC;~UK81}F@&@T51#50N=1oH}?w+;qMRUWNbYL@~Nl3G(Q26E>oFx6d0vylQaE8fXd8h_Xfo6z56f9wEDjF$I#g(>Qn9X'
    b'&dj<fJXN5f6`k}-5%E+UB_sJj+LK8A`HNZx1fvkg^1dMnGTw)Uqx}aa!zD7U@s?|@bpxC>AnxrKS(u}ED_yu4U%MxfgeI&4u'
    b'}zXAr8;ei}Ub854}w3insxb98`$V2YD@G9lQ%R`*4o;Y!sMbt|B=c4(`vzN-#Qs#GUy+NKjyztSb;O&mhNykVx3w2nHWp=to'
    b'_Sp|EIjI0BzSF~p~*VrpYT+{LMau^Dn7cDfrMx!@P5De7`C2KYh&8eIH_aTwr(f-nGW2jW2B{*T<l^H{RJV*9znvj0VrKZ`L'
    b'Yh2zReFn3-g4P5hrCCq&ls{$ib+;iGt+qnlr2xIpF#_}FTtGGv_NyB)LhU=c=^4!4#B$N7{`hU_|Y`WXv2J6NE+0fz)KAz)4'
    b'rQIe9p_jW<M>on|VS}5{8Jq4B1xlE8cl&JJbhbGke~4Xt07$OG{7JHb6<zXpBY;la2wY3d@&h30;C@pcSByA8W!@#xs?o!7p'
    b's*U}7cJLZFmc3afKE?o_>Bb|lmN*DXFr$nuo5652650lmwLfPt2M{lOTZf5-g00HtUAP)9Kf_#k%>#nO+oXVv&`kNSW&Re(k'
    b'<3=aDTH~E%`Xr!7PxBF3##Rl{8l|Do9-7NVBMfRA75(3FckEK<7lQ8;CDo2is`#!Ci`<zcA~r#m|@P02HaMh|dA_s^}#6{q*'
    b'$YWaIAr?b&7?(1hCgpmnU^4^Avn00AzugPq6*g+5HZM@j;Cz5eiivk*+BGaB`0<5t^F_aGg2xUKdW>KJ2Po;REJrVo7>@BE?'
    b'H?+vFzT@NZ>UAotAHuDaHwjT7yiUFTuM3Mud>GvknzCh!97~I#Z#Y3>I2VPI%`Od&IoQ|OF`Ry%zALI9yuC7PXVL0odBpT5P'
    b'kS{?3nrq8XXlaM@HC*YLj}jQP=2jDyiyaI$4eZJf&p|BHy6zA$JBbw7FmrdX+@VAgthZ{lkdi%%zR-<n5w&O1Ud!pCFlWq)`'
    b'7maUo^hb^8p^6U?<iW#i(nhh%V3yQ)_@OltBjhk2DF&0hXb@S0pb>+;9@M~f{fr_1bx90czcG6On$z(umCp~tvO_bc{6aA?z'
    b')9(^zDK=>j&&d&`VT%tPT$C4vx8lzrXw~T;|<fn&b&i-R?TbzJDM8kS4n@%|CRP!DiEZ50e&zsG->LT+g6m!X}0+YiV(Cexd'
    b'e78`u|Z9%kK57+>e>#Rj%VDGOkv2%1wtlw#U{g-u*Vao7z3lUi-E_AfT7b8`c9!Hre?K#0`*0eP8RoM#uOr}7*1I+ef>KiF0'
    b'c4ZSbKB8|b~NFSSUEzuGX(ZG$M?u5F_0A*N4mfFZtfyN>P8}uoZaM434sMJ0VZ+5@{Fnw|KapuA*jLY;kcoTk)hT{kKClrY='
    b'O+YF_8=j)j<HfWEEqU6^)z918kTeQN&gly#7)v#!8^3RFd)OC3tAQ>-?xT6oG)J$cHA)mt#Cmc$XVx-xb&(@PnX|p*MKQ+(Q'
    b'jC4B^+9A+F$@h*M#Xe^6Mo2=Aus(cRMW+GPzJee;)f>e2%h|N@JRLg;QkXv7<ydJCD7&;`NL2&xZ^Eyeb@jT1S>vB1Ndlr7m'
    b'E}IFw28@iTd>~z}AJ|Kf%;;DSlT<D{&jtA6k)9p(FB=0wdLwDWDYamlgn#{%FFmLK*WXLhy(JTK?ifyLTikfIio2xp}7}Rp>'
    b'@|aQ|WI7E;^o!G5a;jp`|RlEC-mA+EX6W|fAq6fBAEip<y9FqapZAp22H8gZstkukB4QtSmklcZ-Re?DR*&PoFvS#}a=#iA='
    b'xH?&pKgZoRBlE^eBq}BeWlI6txN7zhSZgOyc<q1cb{v#XM{Y!c4!f0dvm#_&7y?cHElT{*(%R_p5%PY=vUL~5XZdG7S7p7_y'
    b'TS+aYT3^CU3ezu>JWsY1iqe14)Ep99o#DCA9sjaNSnsdiH=f}EneGB8pC8=66}3&tEG8>IJHr-3gF|fb92n`ZbxNvPIf{iSC'
    b'RZ-|VDoYvUFE!<?UF1)AYA7xks=rh81g*FFnPe09sJVGurPlOH{q7qVZpo_8|Q=GB7B%jb~TO(2~Q~TB>Y2T0$n-oG$FC$iD'
    b'd5Lwwc773zNVH0(p7;NW$EC6M_!p;_hgk9a=II>rnew*|d4;a@fB3K(}(d`k}>JMb98WYNS35@?cRERK-b}o2@n`X%6zL<1?'
    b'+af2AD+Z|Y$GX|_5x8hpt8l$9k`43QHnXwCgjfle%=<))@XtQS}V>5D{1DGcMiucH)7QmMvv4`0{8yO0*o2AA#LV_hSw;VYm'
    b'hHQG^09vv8AMPCM4xQXJBbcO<+;#&SJeh0F4)i9w@6Ss<SgvbI7hY~j!JkM;AgZdhx!kOxv#7_-|&kRA$2jX!B!^4yZ_R~F$'
    b't1<Vq(ra>}Y>{b+mD%Fel@b^<nyl^j#U?%?`RDAiJl*FGia{6%A+LlCe1TgMhTfo}inIoP0^7_4_Lj-#(Jwhx(7$fuf)3=F9'
    b'41=>k|XcFM>mFH`h*pf+uL)LBP#hN5_)_4@c{k$3og*-kqdH-4-{~fNJLv;QbI#ISqFJXEL*5nrPmR82FI|R0#qmZCWMA3|K'
    b'z0tKl1Ws{%W}!mL9M|K}%(iwgm-;K#W!t!`=i4TI$h5-BTR9ZFz{^ZK4cEXv48e_t>4=;z%u9Zt$?D*+Pi=3n1Rcg5qqE7mI'
    b'fSInt>C-pQ~4A1aY$Oz+ZZLGitRZUChS1Y-tu6;tzzme`|J6@g4q>CUVasobkIu8&(+!CI%MyTZwoD1jypJK35gd}T$W6S%v'
    b'B`**F7TTR^ZqbH#O0CRmyV6x)0Vvu3khK1qahPhBV5Ge(ab(4iSXb$<HS?p%!v5!YajyT<=9I*|-EZhT^^0F(+Q+Ep`fIKGv'
    b'V}n^ZJccDh>WhglFTrr~!M0RfY2l{s%rUm?a}$npoVg9HAk_g_+;%!n8<em-=Ub1<tNF)w0Y1{ndFRiY`{IuO;F6{2)&Q2bZ'
    b'v}9Dd%>g_B_iXj!6>0_V)&T%GYc>ocN8+O0r}F(swaby>tu8-R4!d{WyE)I1_Ji4)<N04d}9{2FXOP>(jqpaD<8AO6R6`FWF'
    b'}~(g_3wRXB55S+c~pr`NB@zIvuUCbnV1FRM|h(9b*^G&92=hal-S~C|Pxdc5qW*$#>y$4srz@D0hbw&+SgU;e|_v_qaBCkg0'
    b'zF%xp9Pz}d`z>6<Wp$MpbF-|db^3}j={sJ!j~<Ht)}HZl|2Y|6+EZk*Ss<0&eks7+vyXwV}tK6C~j{R=muO~GGKF7fgIug&x'
    b'5!u`mBqF>Mg#8IVjHFrs*%^l8l+LZq;3g3U3+{~R`qX!z07Y^>@jSzp!++7;NP`wKuXL$7SV*upMotx)T{7?AjfTA4uzoIyO'
    b'6Tk%RB=5p>6MR6ax-mCaft1df8<e#SmG)ljj)1HHmUbAyc)z_2)A_*N#}SVI9=IaY=|eL;(&-~JeS^XY_F!}PyPGGui^m-E3'
    b'_jCvGmqetf=xpXa2!A|Fv1D&3NXaLa2dn#N62^?Fhf8r%YYFBMiNkCjJRS*1BGTwL9joGR*HFDPcC7^1jdNpxI|*3;jbk$#f'
    b'lZW3M2<ND0FgVSIUl){ZiX(-8hrJ9apu4st)d&{%-NFzKh=H_FXh8n{*Q$gQUpt(5Ji3x0^5b`1T^@YZj0(Dy6V7OQllVZs)'
    b'CKex8C#bK&#FCLVXJb4c)E@+I(NOFxoU>!8Tl<@$uiAjTviE|HKb#3`sH3UMs;!c}f<(8cLJ^fQMuRToRAr+oT%nXEpDcAW7'
    b'qkR!PNzy3r-qSPGkMcgYshj<y{sWBQ@ZxeJ*%14Lr!4cV=Ed^o6pZJ%9vxA0sX_h9dYFS*siwN*Wkq<D!t0>!nSed`<K7ID`'
    b'hwmQ$ljGaJ?7sTyAFo_qgW%W8CqF*>LH;hJ^527dBl(mMIEgWtOkz#Z4q{J16tk#onc!1Jso+ydu;f;UKSBDu&nwQhiZ`F}e'
    b'zS4$&xKUP6Z38$1#V2$dFCQDPY2Lu>O3W3i?{ODk&kg~uXduaH6rD?+wF$p4qK$ebnO05I#@eVcL#~su;%(a**-_c1J&Ne!h'
    b'!7^+Kl`1IN%J*bYa5}iMZM|Q`+#gVEqSKZ$UyI7_hh!G(W&(<3$1QQ_Vdq9zNXOYSxnq{6^$@n5$V!W(U5LCJC#ua#eblHxs'
    b'wGImnGeahjdR8M!(jhdFwQ1ZU7`XJ!eX*=nvJVSuj~GQ~6GungNexF2IQ7KmV}asybAO|n4sX#mS8u6UW_p<jfbAvPK$PjqJ'
    b'81&G`FGh`><Z>D~bpC=csGv>K@3JStp<i-BR%8O+lj`c=zz~95>2CZufWP;lNF0VA&i1Btpu#|P&vB)$NXVwwdd4|f&8@$z8'
    b'kX6n{=!HlhpAc@GBneHBJc{?>K^{(=#Af@2GYf8lIz<M|Zra5Lv=#{+=EFn@)BN!j=#6LAFW~<<4AefRQz@JcNNek`Ib#GqE'
    b'Q6<~W}U43fMlcNC%!8nNZM&?R&CL7zplw|=KiA;M%g;@@1dCsR7sb@2ky*d*MtFhQzfwr$Go?<*6*JRzkjoDj=~t_7(LlL3u'
    b')*N?(cPq=o>Q6gnl*d1N9LX+2JVJfQLT(Y{tAb{~d*uzL&Xh1q`18(tDi}I7~y%X&sDPke5Xw(VQxj1&N2*{0MPCzCPfvX0('
    b'960dWIzgWLbSuKni@0<6MKw54tLg}gC@1rj#rFG&E36RW1aoCPMx)x{)Qhdj=!45~EsP=GoFv;ba#FIb6)eVxLG6*v~Br+kz'
    b'~=ddJVKho_6r5XeQQ=P}OPUzz{+ZgCUT1Z&PYl{1RC~vIp>%)ckVA(f(ljIFj9q>|bAHRu{_p!JsMUy6?8?BaFhQjtLx4ZZk'
    b'y_di)b@0}kPG^h^ab{`U+7xya@*b|8>Pt5If{U#HM*j;KUhOgLk7GOnlduS^j^m{apmQHZ^b<ieFU`bj=E8`lECDTV$h>leO'
    b'U19?+70<DDphhYV*foMb$OLyv6(FTbjUB!;^J14I#?A-oCE+#Oh}ZOJhYA6jj2pjoY$hL6apI$2wnVZid0<7S};8d$*3NclT'
    b'EikvX8{2uTgKu!}f%2t3^_FpPrtS@(MC`(E`HVV^k~GMb!BxXyJ2v+Z0EKz;_#z3e+`14Jwo`3-t|mFx1Lzc+^A9r3#^&8HA'
    b'w?o-R;?PtTf>55L}{)k01$?r?#ZLdrik#`!|xjan^SgED!{tW5KaX@a{9EJ0ENcGzM5IM37Qa-WAVNNE&w*3oK3+B!@qeAE<'
    b'au^w4>Jz;T%$^CpgkTWb$Gc2r|VG>qdW(RsoeVH2zn!M;dP#-kj=1f+)U*^C>LSp9YD5JUMi&K9}2xJp8D_J(iP82<CrV+xh'
    b'gbQ|>HlCbrBrG&3CLOG9CMi+C1svh$hieN=wY&^_$decN5<?HoDp~F^)?;@`{&K$J$d4~xHl1~z@8%C4ynp|``+m?(((4D_Y'
    b'&Ly>2bT2b`FD<PWy>(b$PF`$5JUOGa#0|~Fj~r4GtNO8A!nE^!OhLV>=kpSh=msq?a&g&fNHf08*i(#%}Y^WWh)PM#-{RvR('
    b'N(c2oVcFN~9u|Zl1h?J!P}iwsT<Tuk%=l-BH7$mA3t3NWw1OVa2N_nB631N5!BkM_Py@8?1s}0liR4QJy>x(l>h$QEUk4I}e'
    b'?<8t^}7yxYD*c2D<w=bj6{0$dy6^PY3Be&8Xy!E+f(wS#7H^xOCF!9C)B<a{b8gzc&^lpUZt1#mumVDv&89Zq3a@nBdJPgS!'
    b'Gc4*fcsxfPIe@h}zaZm^4hCC|>B`J^q+!iR95b9Yh>MKlQtc3tnnU*=4QeKK|NmO+!u(YJ|$7(A1oYfYP!XDL1Ww@{i5<c>~'
    b'y%GEm_7V^I>-das2m@!1x!!m@=1llD5qUGt!w|Qeht9A>-bmz4CKHi2nM`G#fw{xA5e$DH$(r%+cp#v#S^<Q#ND3?%tX{w%O'
    b'$;--vSEM>2?Mm56d??47}VHm;Us#h^(mvRp%;IWjMsZ$ISUG_KaH#7zG&)E22CV9RPdZl;+gYsi~RM-Y1<iXm}Meoe(xdI-('
    b'dEE8E;htll{S?dv!BVR%HL2gV2%BQNB0N5GgWCzGmMo^bM4+uS|$7IpuP;d2qQ2G56c@?h6lD@D5k$AN(eOY+Q5mNz3Ue8N`'
    b'!;iDl#tQtsqa&4a@h8sO(>rqbktn3T7kbRv@)CjI(gMwkx|-Ej+zi)%_JqO>ZPnC0mF$uBku^k!axK;Lp-R30#xo0~J-y3Dc'
    b'2+q@|{7fU`BH^7l`XR1xV=O9f@gJSvg;gYYJ@k8B}ihE>neJn0Vi+7^x+O_JoRb6lDTTOX$sV*$#<*AjBUD>yj;{MUNc09P>'
    b'<)(qJdPu?VHCiey=aUR`FvS78MyAZHv#UB_eF1Mg<`CETmqsam^KoYOhS{m}YD8xhy*IDLO$d$dvHD82<<qH^Z8Qn3M<bz&f'
    b'XDGV2Ys$9?bJ_FdjF$LrcFzE+RSUiyDh(6e2T|ISTvEi%P$m@vjnEhTppM6vmJUhz{kH?%c_H4(8Xv#ZyDE9ih4oEN8_P3|3'
    b'!249H}whI<(F>Mc$IrqGY_!%!M7NI$&bkaAzBpFN+GAR+V9=acq`13d*<B6-HjDgHRpGopm68;Qo{HtbLJ_g&2u5N3ZVy)eE'
    b';(KpS;vPzR9;h|g|~YqUmL!HY+cm1lTW_am|YjX1_MtcwbE=5Mh!?yFcH7{!G}TeQ=qR+4pbRU<_<o%J0g0pcJi`GgCljHy)'
    b'@N?DQ2#7<y|x0PZr6V&UJM5JVeLBR<qIM!(q+>!j&$3(M@yuQ*3yKyH#)WkL^v8Lm*5W&o$r#yccL4lI5@iL)hoFbe<S&h!M'
    b'Fu|6KkDC_Ka2L<V_+FL~q7=*ampY+^y<C`Gk9-e!$!fywEfb>UPG+C>lsPjtXK^z;)`GCyf|j;DW|@;1@*t^`IiZmy435Ln4'
    b'8_15gQ=hnIF8zy*nJe)KaC;zv@XDk7`uI%!CO1dP_MgMZAnwx%JN#eHx38EI#~-AmrNh0yFb^x&TG8Ln?K|t4b-h~j$eP6>K'
    b'RSrt-eYSFx$da!hd{}|65-9a2r!oKF^L8k?&FGOUaLSFa>7&N-UT1=)857l{HSoGFfH>ah&7<t;uY(`V4PMFYxfy7L;MUOjh'
    b'LG%n5dQ>qdnh+;m{H%ty`wZx(Jr1VzitK_lbM1)A$BVrc7e9^kEgp_ZHN1|x2M%6^TkJc{Lo$pc`x+$8A9)3{AKPtN#^7gR7'
    b'l>tzcPyzE|H$m{Z-a{v6?bK(Dx+!vn%@i}z+^f?lrV|PHG6BlyebLv70e9qj7JBClM=T6-Teu45mcXn~%{uA(bM{alQb|-Fk'
    b'>UL*{yW2+-xgq?S^rvpW+nWHw?r=K2aQ~54KU3jrsE7TtaQ+j&)i~!Q_;y(ELB=oD2N`tIY#(IM+jBDoF_n&FUKSH~YZ&EZY'
    b'o(!zRZ3v}yp+tUXDOLf*AhAUhRw>_1~(<TiqDFy%wLU{Np>aVh@g;!T_q0Z&At-v^^ML7>ErhHm%RJs<CkB(eD<%e3OyI9ue'
    b'Z0VoZXQpC!zdq^d>)W1~KfEdNeeC*Z`t$L*s`Hz`Y#m?>f8C-T%zbRWjZ`Tm2r&Z#7*XmXfUAR<2L##^%7@I!zO`k8F~uM~G'
    b'h_8GS1Hd2S7WOG7r-qv(pRo||L9j$l+Txw?Pnh0b?%&&~_o?y63m+x_i|&YGKj=oWiuUfGqdaG7Cr-$4pEaf|}9KDv&Zxhqa'
    b'6enD}5Fl~ei*Cx#1pa{^}5u+9zE_zvX?mzOT-I@6QUSNKb2HRb@;tFj1X4SgF?h;Gjs&1G%6)8jJqIb;pEILN3QA1AWWK6rk'
    b'5ZoHg+w*_2@KkNV_fV>*LSIc)1L;EYA@qqk+w~(uvjKUz0Y)K_`N_kO+k}#>g1otquPF?IqZPlbXbj1XPGd9={ekf`qA}+5$'
    b'sAu0+7BW^e|#|8^J!fT^Z^xXc63r0Qi*}*p#=Sb=UuZJR!&j$)<WR;Myn1nO<jo>PfEX5ll52;F}*|OHq1uAQ~DSJAq46W(9'
    b'Azpo+X7@+_pk7%$B6piej-dW#Pf5gsy|e=Q;iiQR^gu=<qL9=wg0;ai}oADb2tWi(8M^rRA@v&L~M0T2F$ls!X|md)q9p1*O'
    b'N^sGsL}sWLk2;jP2q%$xV!lN=9fjhBA-)Bm{4vahy3gz5AB{IA`&`(OpB54`E&*rWQtwpzHO_y3Cineq!Y6%ga0soL6Vl~=B'
    b'V_Vi=5Qt<MblczzPQSg`CG1jBQ!RRr6yucIoUvn4$A#wha<AZ&Av5+gd&=pGW^JEpCDO+D@<|~(*uzsxxYZn^1HXmB<zbdv)'
    b'xmmSo@WnhLmGO%OZUNuM$?2(hAMDUi2OkMu;a8nOKOrL&4;1w~Vl-d-TVDGx{V#YKEY54BLyxWShLr5{-Cmi;i5(XjHX7uk2'
    b'?-S>0}!*A(mKe1313MX_9EiJ-obc~nfY}!cxZxJhb40(-PV7hF>4C@L`L~nP<h@*t8g8FbojU9;&%#FFjtP*F^OU$%rQ{HUV'
    b'o*>B{Im|T!o&a$lev{Hx+4!LdS`ltFUl{qblR37MkKTQQ82*10ZHIDKx;|ggK{TbW{nb6Nj8g`)y7NWwXpQPxnBHsG-pfRhc'
    b'(+8Q)iQ$O_y2V@0B@UWB#h42<4(W26dtDP489RRSZ~+?dI-2xWX{kJ?J6m8isKt&`+UVVE)0H>DQ~1Oa4hgI$&<B4J#h1B3V'
    b'oN<6z!ItOE?aUml+6Q!1BPZ#noAuoTQtixbcWXw=!m53%rPB1Lt9&f9B1WnLKt3W$6$)SQ-F$ao9o9Hr)_S*uT<(x|qkk$yM'
    b'I5&eQE;aGa+>HJR8Ino|$vn7!&uhI{(Ai0%&uRXOXHnwLqxs<9uU|iK9eir6Z9MbB@d!xs5ER4@+ITHLxUj$bSeW7G;+Y9ke'
    b'BpMd)2Tb^_g&}ad2mB-*e(i!<(w_Ht4fGhZx<zb@k!Mb1l6b1b91B)uR9#iN|lsGuUKqQDsodxlU0Qxs>lX~LW5F)?@MFCx4'
    b'9KvHtyM^{diX~uRH3G%jLc^o5aQJh8d+4okjU38YX_$j*%_=KEDtN7rPk6W2`A!5~^~9tE<#~QV_X+q?jbNm{wa@i!N-Sk`M'
    b'xXFc?;FmX;%hGJ{fqvP6X<#k9KGDrBxMUFJ<uyd*X3;zktQYLReZs0sp}Lq59BHd@KARFr=pw*s#Yrx&K;VHTxiqrCjBk#I^'
    b'8(;uS#<}*rp-N{Il`mR)}I4j-i>5Ul1y^E?vw>)P!tgFSIlq3*MyzxcV3T-2*K%#J4h-~ensv{7B%JgLK6*LoBGs2ZRuV*j}'
    b'8lh+5HJX9RLPnA-YbKe4#>4#|p)mDUgZbbm=9t_jlbGFx!`_9h3frYi%&O8vd9Qb2=*OZkKT#E>_=lXPL)qSP0jn+}#VjgSL'
    b'{W}^mB&W%wNS<c^sD4+_$8smFEtOQ7iC=y3SI_ja0xA}o<@{~vET%nS<kzuQ0xpm**ZzhiBjNY4P#){Goyf10xAkMQpze}E+'
    b'9R)d1_eWiM=0`Ry#lOox|a|4sZC-P%lmI@=qp>xrgD%FxuG!m*GZTyVB`OD&A_Io;vibD1uG6ej8YL^NbMc;WK_WUQo|Hw(o'
    b'Hxmuz^FN28W%6k2cG-Wn~#gtKPm5Bun!uRj<M-HqQJ_R()c+Zj5oIb;sJK4lJjm<fwa<U7;{kZ-$3qT9Sa{2vy=3v}3bSIwz'
    b'^jt)o1PzLK(f6)HV%`me+n7M;q&mDj&1XNPS1~a<-q1zqse`6PDrYxj81gZ(~xQECG13&^;umJ{Mz}KIS0XO{f`X*k$hB<_}'
    b'ho3`%np;ioaLr5DzceFx=lW~&JW$>(9Iw~=FGu^g*pz%(q~yn<#)n0m#G!ML8;p<HE{Ny9vfHF3y%^p`F?s8o_hcQLgLWu-j'
    b';=Vu^gJfRgp;J~{Om>2c=_`;ja85b<ks7{*N)rw$mN+lUP;g@D1r|UdnZAU9)Z7k;DrNduyzFt47YZ-^8NLaPOOF}XgVP*CQ'
    b'5aGB^{LxJNbUM35fuF=D7(wyC7d{$_%R3{<*UT4(jAVgzDDTKTpmF7Z=KUqffOHSg^Sw>C~Z@nXZw*4?3x|R<=yVNqKD>oFK'
    b'ZAYqlpznga0+I!}7G4X(osoxL@n1^XS)95$Bd1AQH)Av&@bK9yg`jf(#{?IrMlGB9ztk)#qy%K66<mOEM+_-Ab+qE)Fq2WeD'
    b'=-Z$fi%SEiMsS?4#8SqKV-Ag7;G!qYdXs@NYIH=*`O(hp^0WV~etV3rkJ95UdhE!nVy|m$Ct}z*Xr@1?lbq9yVuJq2U<%*Zw'
    b'JLLtD5ZKQ#c`Gj7YF1%B9GdJ84-1noJPC!KY(#w}FPxbhS2&AF$oxj;NIc{<riI=!V{^S>uN2a&1p8!g9pR)xxNLCn9U}}+V'
    b'ipXR$Pyh2Q-P|r@T!buRU%s4@!f$mZN6w}dzdUWEOXGgFD@x~-BD(FHC^%1IG*0Ki$wJL?=~*?dE+|C;b${Ea|#fQbuOY5g)'
    b'Q?rkfJ0?%>$TCDqeA)d<rFOzb%Y`$b-orB`DVfZH)UgCs58P*+MJf#X1z$KjKxoDjTR5fb2Qcag3{v3@PCSyH)UEEiAs0w$B'
    b'%sN(_-XupG)0!&0mve&K5z(>e;Tq|t*YzDbZyGQ^6tOvw*%)x-Q>pe0Sb=V_U*@o#VQ!fSoo3bj>ZG<&v8o0bu4YtA+XbkmX'
    b'_+;ytV)JaD|t1L7pc0gVVqd}qMpwr$|JYjbQ-#egv`7tMPj*s4Q3XWlI#{%+J!t{Dy<SLy055LW~o2SVZU%)&vm`Sk=x!pW8'
    b'AB%_kl7JZW4kopNO1-8&rgya6s_T4Vh30c7ZQNiHccYAa+I~1a-Li*{h#qvG_|1(kUR4A>bavKKk17Ik*WEhX%zLuhrB7jK`'
    b'L#BxpJLJ8`^z)wM!7BgCeM}gN15XmT@2rR%ZBV^C5|Kt5esW?nl@vUvTOgZVe_Nut#n@Sg6b*<rRT!MEjbZ&@$Wo4xaGvPYk'
    b'S?CgZlsOx`Qs{@W8|Ly*ZB__0HrPwhT7QCTe4iP6NG)_3uY8h%J0SA-!nMIrSj8^l)(yx-Ca6NEagVwC+Ll>L2G#_TGk`wEZ'
    b';i!Vl1u$g%%#q4eCVqGq7A{yb)*eR2>aP#v&*c$9m8`6TWY!CXM#)w3-|&yy~_d*tqk1I8?YxAy1^RJs{h^R<~+Xi#KH|D0-'
    b'HVh0p9xC=d)tP9RC1)?ipS_gFv<-S{lR*DwM?QL_@@zV!=s9mg|s0I0?JlEDnb!m61PPb16=zkgq8lIOs`%1n<b~#wSLfLtf'
    b'&_IMVfZki1M)t{B)3PA;cWzR0@Gbp@sw~h0%07DBq&456*D}%&ehjaHC+PJV2Zz^T;8n<X9pPQZa@SGYb$oUij$H;`mm$`zV'
    b'Con0Jt~H;f!&dKi+byTK-=eX7Kl!8$<x{V{}gTb!`#X9FImjo*S~&kU(Ki_^Vs9{>+kD<Dw5?fkN~nyvvfgyWML4YkG<pl_H'
    b'lZh%{_M=#H&r1&Wk>wR(TU2L8A(Lvc)g}8Tx!dd4ex+(1`m4x8J()w7SZCmBU?8ou{_#I!|s*?eQfc2TFO6!ft?N9nI3YF6i'
    b'nNV;;M5g_|cXu9WlifX?%jzq$E3bu9;UrPhbW3Uu~&Y&lifnp;<S->M8(yoICw(lB&Nc>ay_mP0b#a^hk;s1VJS*L(2+wl~M'
    b'P{hkO|&!Oo#2t9|J=fLtDLY{-WbI^DW56>apIUIaZ2AaG<ZcW0~vlvaGFm$_0Kv_Zw=`~70BxjeRi=gNop5E6n6>`*q_l?$Y'
    b'F628w5v~bc#2Hb>6fE>t4pT6zh&qNvf6kE5$J9@m8F!3=#ofyq+=ZmFOU8k%<ohIj16p*v`GA2<urny+m>p^?X0O69ELF@IA'
    b'7#iBPje)6nq<j}(fx@}YxNM@7pOe0_C>5ts}LCaP}l$~hzxo9ctj{DhyFAS<RVVZSmkX@KP#(h5_OX`d1LN~q%b#&JoDI`Ju'
    b'xbS^o>-7y&+y$gX((yc@H+&<#ifk6}YA={ke)=a%|0xk1B{&3wlzuo~NY#8Sfe7a?YJ3&*af3T;4j|A)O3?WQrSc)c=|CMqU'
    b'sYLPw10o_BU7D~4ULT7jf+!?SZTMXa@05O40CcCR}o{VA_~-`5>5z`<DFZyqxr>)CK)&xD%-x9cl#{`q4eyJCVAF6lVUy5!P'
    b'HMKE<wA}KL%hfAqq3Knm{ONkP$boW<iLm^q+8Pm`5jjowoXdB<>6l&$W8Y9uTB+--<6AP9YGRdyczVSBY4t)E5hx0So*pkRR'
    b'|Ilj(IkT-+9VG9V`y&=Rpc0^GSMu-zzmxZKbTVxVA^Yw{#10S(m+iC@)}+1RfGqL<7B*R8K_xD2TQQgr3acAKPN~Op{nnX-Z'
    b'<>}XN4ytH>^)A_b~=2@4Aah`lK~`}e5jU5xdc+&NE3meii+qcX8pWOirB#K6nE*tO_cFQz1<z%-^f!;q<jgxqa*Mg1HRMsJb'
    b'I4}n-z|-)a{Q*Ei5sW?lL^6Sa<wHdfzfUcO4^BhBY<mZBzZribbLr_UGu*khtV25B6#m0)HbigHKyOjFOQk8PXPzE+YP~Aho'
    b'oBvH#+mj=Uk}OQgc$Wltqz<>Ki??i>rN!ZwP@0v&LhQ$|Ef@4<}0t5zU??hLy8GF+U0sw;?n+Wbb%!knY~iRqx<X(}L^B?wh'
    b'qOoalUx|y3Omw6@qKGm7!YLZW=#>Nq2iK=}BC|<^5ZmW-5n%d@CsgLjZJ6{WkeQdZ<-^IlM|B$x^{7YUI@GrheFvYb}`BAGi'
    b'Bz5Z#DvYubrmo0okmt0eN0?EIMff#2s*SQI(q#_bAZ;P7pix%esK3|zLFVHIYr|%aYQ1pN3T@0rM=e!1@jn<$r#3a*aqKXK@'
    b'Sde*Wh!pXRs2B(hucuIb$Gpv$2u-Mk@oSScWZ87iwIHk?sAD;F2QoH^5CopfVrvtqjgzb^Fdqd<9V2CWu|s!OZf-?1bj!3c@'
    b'u$*`1C~M!Z-TNu14bR?Pu4(t|L;d_U>%8x6;jgt&91EdBbxegFnIAcFBP~VWpzyN<ckMLL=o&xzTpoGR@S6%QO57z)AObYD)'
    b'I)Sy(^0?86i>$9AbW@{zL(-QQJAFiJk$C4aI!SiX^h1|=8>dA$k6AGLIUn)INMAnHT`AKII>o}e|zjvQO_N~P2rK{5Hi$G%8'
    b'}c8Ibp<5Q0J!H4WyqUhy*_i}%Usr=yQSte6FSNM6tZ~GlyRcG|7kJi#6DTc&4pksEU8^~@&5G8J^n6Qiqqg^tr(NJl2n12f3'
    b'4gfz{vPMiATA7qaIw%hFZhL8tG5$PyU{ueJgz2+*v3Jorrll9;x$vKCi}IrTWO~)P4qzXKqNWFOs{tP#e++jw4qmPwQD;nXZ'
    b'!Xw`-9aA^HCt~1Ku1cQj3}5lDk-pb<g3*G=sdz#2<4sDLzQgqwtStccZiQMZNC#RKhNou`+Y3S+)|mJ!xZOh*Fn3X2In2z>('
    b'p<y{6~@>WLyKmLl-=v!byY(y`$VZ&}&0{QOPH2S0rc4F490WexbdaxR>fyw^@!P4}#UcOK(ZPCHQt%8zy{6i%-s0DaPp&Z*F'
    b'4|!K~D+b0gU>|5pLi(7)Z+QXhz25&iQVg%e(Cabelh5}EGnx!S_WBQtD_xX82mlGlSdEgBdBPzo`*Hd(+t7zOH#Ryc~#3v&H'
    b'k0Q)k60p&okRoSYG9MMpOyErVWJ4F-_&m~>7148f9X1{9V_oFBb2IfG2vVVzFOI@k1NU1j!LmT~cgk-Y|4)RI0CBF*j8#d;{'
    b'LCZ{_9QQA#+|&bM^=HKq?A;5ZMU|?{6!cj{XCT5YVoRKZ<aeHWSg-$u#N^Wspg<lm@byQ^M+{or+4r=|Rfpg@{G@pmnvsGrG'
    b'OPch*`0P&fM{h7)srsx@mr-R77pnxS2=tF%9Q2Ci>eB>8-r`RY0bH152UY((7=S_W7nE9%#O$9%J|+TS68h4^tz<Zx@l>xs3'
    b'b&)PPtlK1+YJ?z|a01H{m<dwmgT`S+~T3auV8%C{k;guZqDWriumpDh=4){w@ZS93%_aER>d62)(O|!v=~;S-}le&Y%Q47}c'
    b'O6VRnz2%Z6@^rwyN>+t@QyX{kZl%vh6gwX7z)1FE304zfm^#2u(6Ors?%v)AMVS42a|+{h=4&tlXZDNIcEB6xw$@Ga+6cj0R'
    b'|ZBhJT@Afu&<ayo@{#7GE=Ld~QL<=Uv(_Iot9HNJ#hyHjp8jQ{|pRGZ<c!vB$_=g4h=FO->AY->0WHjo}#_eYG=#dB7+GsrJ'
    b'_u5Ub*B_ih<J2viZRnC3hAtVZrpEq=-r$=<NVl4lXl^vhcbfVNQ=ov6ims!W&Zh4kWU|lV5vKCORI(+U)bLKFAf>q;vq|h!!'
    b'`dbszZNsp-LB!XgKKT!b9FgX{|cMwomWaf97Xqe$}JrvaT66ih2yHyU=;_-`-FkIxO0&tmnh%IuW&>VjhdIohHQ~`ulF0k*4'
    b'+lnG|8}aI60R46ouXwg7!q)-=$pVS2wf?L>1zMA~Jv4po<HMtgzKurnaCqI;U5<DCr#$PdXZ(#kZ;NoU^Y_(b}s$P#ZQ|q&}'
    b'bJ42@1FzCo!4O=`_KW}APK>zjDPN5bvxiE(M4AG@swX8JCQL99L1#mx)*XK{n>?A-=79qr&>zZPLh8mlBkA%_Q&*LYzsC@9?'
    b'7dxv8G%g)ca^92bo=jhfpJv{}cNQLv0Ru`n-K0LO3RYX+QzQ+Y}ii%~gV}%eaF1vUkKr{OCD+=QMZBuxF$y=>f-I^x}(i61k'
    b'3EiZopu9jbhm3KZGEGHjR?5(cdWkT#MZLi->g(6no8*$7#jCHl=%ETPHl&<N32rf=5(2bvMu%Tq`Nc{JWy+0AxhbUpr`!0&s'
    b'T6a6F_%*@q@jH%s9haD_;>Sce*XIP$HQMv|JT2Ny}14A`>$JndB6r!d10AWW|ct$eWs?#rjfyxCX7*mG#pSJ+;^$3LMI9X47'
    b'}{L1=BSMrj(7f7i_qjh&XKF%?_r&4`KkfiYVPcjHIRn*~binF5>4W3UicwrIID?19C)JB<5~{p-+q@IKl@dxNaJPt2p|l!8V'
    b'EdbRkAorfLsFvBomcbY3FsXH9(_3t|mza5%WwO?sYClcYybP2ULxI4sh3LZ0TET*x<yySRWbDQer;a&-q3e2YF4$`p-6BfvO'
    b'#4eH4TtQc3(4X!4(^snkli}4k|D>gsck=0i<M|%}j7dh;^WJil=(|E}BX;UEkoGFmBRP?T%@=AJse!-Ev7S2aLFt1v$7Wnv~'
    b'l495SU%~+|GBf#Z{#d{80YEv&4bC?gLV$mS!6lJ-sIBni!N1SF&g??=fvDQ*h<lPJl#0%Aobh!U!&8J9>|ebDU_8(Vo3^dH-'
    b'+O!nOC#A+^c&El!xT_%+HE)B*2q9$)GIJ-Mn~0+8uKkMTolu7G<1jV9Z~i`1_8Qn#Owqjv6o34sf`5sft4}mqLyN7H6d^0FP'
    b'Ml1N|s*K<5_dra+h7S<%N#I<B7aNTxG5lgiAG1Hm-_nn9YL&u)mM99Jk`7d1&Ykhr8#hTvp6w=Fgs>I*uL+qY@i}3@Y@0Njj'
    b'p1wW^^!)rFAswM%$dUMfLdr*StB4$Ps|)Oyl8pc^0Q6g!Hd6a1$mpcizvyrBP|9*SyNiuYHw-7b44t012{NSB4F$Az=X)a35'
    b'Y6b*Qj*7|4*!)DBJyG587`CWQ3y1nHR^v*6Wzq9)(7VlQovl2AKZ95I<;Rf38G*F}DCxBhYFm!N#Pb=a{OCC<UMc1EXa0Tjl'
    b'EF+L6rLdqaI^0dsRw}tOBB&rFQ-MJvH<N)u@#{9YAwQAm9<+$f%?|F%`cBPD#HYwBO|Mf5R2ml&TKDU%o)%$Y*Fj=VJoqL3U'
    b'OjEWH>9q7ZQAh@SG?9~`^mw5o>l}qb~8zTR52`~vwp@%fX++yEZKHCKZd}S`09ef<q<e)kFe1Ho;GPKMI<WEl)@(^Y^<USgc'
    b'A-CV|0VA14_J1o3I}hnSkuzKCWUFb}X3qM31LvL4kF~^^y-v4^)5sYv|bfN5f#MNUwNfi&q=J@N;@<KG%Lh$Bpi*dM5bWIT1'
    b'Eggk78$LM>gKeFDt+@M12&yyIZ5e4{O22iqns;!-S>mty<=V7643Gf$e%^Zv|h^cvp|y-DL|8sEmZko3DFuVnTZXUr<$4M$k'
    b'Qn@&{;NU}?eKCMI>a|h!IQ_xenkYwT;%&sha!x5DjO$Vw3Bv~bbzExu2&8Uu^E&)k)37h8rcz`7aL!)2D1G_|E*D)OuRlT9E'
    b'V>+}-ta=5i`c%hgWKh*NOZ2?4O%=C%(9?a@r<PMs4kuHhpzrB^nM@I%eedNs2b{O`iY!g`@v7;CgZsGu2WeY+<o^O=B&rwwD'
    b'3SRxSUjI>Q-0yqKjMq&<$tM?YJV2WO~C;Lx$tCxSN-~!T3#gOZd80%PZqJ9_Fg=vk6Tstm@s>vq<+QAy>72JJ-A<*Z|HA%r~'
    b'8gC&tJbz)i?SAisjqXisdE>{HzM7Z$FCy&+3G**ebb!i?+88tm&|8iu}M|`k~8Dd1*VfFABbeaVbr1tFegUHOTy_DwEjCy1u'
    b'4ttk5j`v^ras-tP+Kn`F16tPwOamtGia=oV8}S^OuvI9A5RpBlx#6tC=CXH5n}A%#81W3=@LgF$QlwhT2QD0bB)VMn7e5JV@'
    b'a{Gx3`j+_jlaI$-<K_FXP(a8bSD#k?1bF4w20PN%O1h8Ld>}eJDp~gPa*vB>4(*kxN`D&9Ssj37D_X1Hx;76Z9H{&reFT-eK'
    b'A)Z$C%u?4;jhN6Fc+)b)mnDinr7ubprzMJKRlU8`byOp^DQ+74L3gG$&e^3JL0DlW)w5MycQ6XTKAnsJd&}UDD(s=gex>`QS'
    b'4VmQ`|H;q3)~$RdSv2&nBlKejen-OGN`DEFdqt>UBrB;G3$N_R-b@*qir>LASYo3*`GseTE=))Cik;w+$MXp4qS&O!(I03>H'
    b'HL|sm6Ljkrx=NN55`wtBAd;`O+fRf0H&NckjYhVdcx`<Lw>q6x?gf4mkVDzn0n8BKs1vmz@2<zm(aRoJ}j#jo)+M`}8D)JyG'
    b'}@{kr1~D*0G?apTM7yDfGpP^vFp$-nbg?GNo6_l^Iqt)9B~SYN?O*=**WcdZA__xG<rXSoj_RQ2zET)&NiEk^VA`+jNbCHm#'
    b'L|K>rz^Sk>Pqs;;BYZWZZpuV%}d~n6<d#%#EK^u^xYPVPnr1te3)lhFTnl*j~mGRs0c-;88M+#}<XG(s@%G*2s0WfP+L*I3#'
    b'!`CtDu)jA96nB(F4V2Z%v_J*oR495(3u?U;m>?UGp*H;c>)0+?Q^cs4)>&08ZU($j<K1XFQYG;Fvfl+kOW~RL%gF6}@b`7Rg'
    b'tVUPjVZYU$vwA&7&-z)hj7rZWrwkJph#yXzK+}uVCoR64&mwuWS9e(N6?|TIRT`&F)7ow9u8z<4AtTP`6l5|!shG$Leg<Fh@'
    b'>NV$3{b_E1IWaCX9Q1VociX;SbH1fO3ue%FGNKK<I%_D8qY2F^fL0#u-nZ&L6Lk6{pXZ9f%X>AXIpVtdNkhfp&eLCMAs8%mQ'
    b'm{_tDyf9;B+9jtA~+2|UrK|9~aNz&cB2lZpHbjMMM(uUU8C_TV3}4zkn;e&9dgE%@sZtGV6;cyWkj`m-VY?F?oJGoC^n!_gG'
    b'L#(jYBu*7(Pr3d(TFr2xs<IZ60j$q@x>A6n3nYX9IR@+(jx_x)-qOV97ok=eHz_~~au=D_`hMI=%a5P+w5YY&u$wAKRcs86t'
    b'-f#lc_Q#_o5;Jx&$@982&45%ajsGSXl5XaD)O>F${w#+;GZgAz$<7SgjNL_zLc*tAG2y%^bkl&lVn+FW_ys*W^oG>JUJrkbf'
    b'r`!;=iaEtMf;EptsV`jr#ugTccxS9Ak2p95hYG${G0*>&@&zCnpvN^2D7oo5mDt0p;6e+!)2eU=ue>Q2Ka5{L5KBc(3bvSIY'
    b'wHcV(chyM1&7!WBBMpZ*@ka*>a3!L?^?r$}dyJy9I8VjEPsq%q#toJD|VFPw0pgagSYZu%v<SW6uGPKw6IoK~@|sXJZ-{`a9'
    b'`&(UUfINFTt_EAFH_T8^-t-V6|9{t)VeVf4_;?sz=xlFTH|8W8EogFV7V{DGcE(c!E+1b&13;Yjk?45M)ZM?)HqEj5I`4W>('
    b'!!8k)8Y2Z<35<(bEZ%W)Z#XduN#xvppNf1pErkDqK;9;vKJd0$rV6c(M0f7vWzA<)JhdTv0ftYOwJPI?^^N0&4H1&{`v2mW@'
    b'TTlF1(o}Vs`DkteTX{<wXC6u>lhY@Fk!Ng&r!3Fd&Ui|F1#=ca#~u$I3^9T5%q2dCISZ|v8ocQZY2FeOkGM;E$eqlRLuSwpP'
    b'1g?2SK`AC&sX3|oU$F7vmNRaoO?V!x?)*FAw$UM4~-Q{AEXo*R35=qT&8e<Qt64Zp)NAfBiEt1E*{i{bP2`BzH7Ys$ov=0E&'
    b'5%>v$Ge?oWE|$M|qh%$BT*wc3V2E!vI5pJbl~XY5gKl$8O}<HL#A^GWA;kMebw1rYN7i1qXdUEr1@2%>>lbk_Dfjg&`ThSQ`'
    b'gaz1!WMH(m(1SGZ5X06tdCpU`g-Xy>N&pkD)T)}#s9R1s3HtFmk)%ZmNWE7)cfFN5U8#*>Qo-)uNvaB1>fzXE@(pKDiNv21~'
    b'{&~oj`h5O37U^%!yrapRN|Gw(vQNN!q-e=_ZeHd-wCc$wQg$+&iHqD<05lGs&GAOF^9E2fEx^chX4%@kqV`9mkba6H4v*@9v'
    b'1sClp3jvr@BdIUqm!VkRc#S6^2orTllb)9IL3c`vKMJS`78zj?Si1UR4IeXZEI%F-fXBu04;Cf8lNREhi;|Nnfz`bCI?lWaz'
    b')V0s^Cz#&E-$JRj3qlnF#$^#uew7hHXthcQ`C&9zF{BN=<y8mhggL2y$K3RhUoV}er}w0_y8l$j$kTw=sFdOi9R8Upge2ayb'
    b'Z+vfmYQV_S)#a@7w8c)P<E5GBdzP-$C|cSL+`c>HzL=I(Fq39u|^l7%dl61s6qcXH42vk3|^vWd>O&B>PB+_?DsKFhpU8AE^'
    b'BSJ=4LE2;%GCb&1e*hm(GH4$a*CvSma;B*uEl_jh%i1<EI}R?icxB?blL>;MJw2*$v}9v{HsG$SRi>(LHoN<B`CH>{rnT!#o'
    b'?gzKM2n&b#`y?&31Bd_+bXGWgTj7TvU(NOfp6Fp|rp*d#MnG>mek_y+s6_Ury95Sy5%GH1dY=Z1KqT!NXVdY1Xr&ReAl_;DM'
    b'u(0)~1DG<%qQKeH88>e_(yM%b(5DH9%FK|J2AC%<FYyCpz=_Ap7;69sNBW@)974_GLHCd}uQx-jiPlU0wgh#J%8jR#NRx4d@'
    b'E)mIecad~<;Z(dYXROJGf~hGrD22i%vsO{$du||Ho*lPn2|o{*SP2MhhQ|Xhdl5fa$5h044V!bRE?5hG~R=*H?W4A*LsiV3o'
    b'ZAwu`3V*;}63Pg|p6d(8VTW_kiBwxzizPsWi@`32uBQ(03D3xBAnWI|Bh@4c5eEjb%c^)2Er*Crwuh$<9ElCI|~ki`WVD#om'
    b'YqWkSGx5G<X^a42@<IP$PX2Spji2qsn58GyP6Lr)(l8dy}5&VvGQLIXzdJuZe5sLxgOc}}9@h@&_2GWyly#9r11CY}IOh)<M'
    b'xW7MTcCG3xR7k~`Y$LRzc-sP0~p-(c`8zEzNhSO;mQIPB%!%Rn|dnB}C<oYQ~U04C8sHKyHh85<|<fK78Ck<h+09k)9n;Pq('
    b'$BHm^(v%jvNncK2SfoG~ozQIU^BkMfZi(l3Pk_k9k*Ay3)`6g(F~3ZB@c`jAVjUGl!)Qr+hMCG66HnoCh3qtP=~M@V&roeBa'
    b'k1m&8vkYDQ~jP0hQ2F4vk7sj=Za6j+6Tcg1AZ5uOf(PUV2n)K9}Te)liAYa-O7Yy1SGTIolFP_E9gz-VhL+}U(dv&^YC?C5W'
    b'B<f7+X8VT3}%pyV5RhWq`vvEKMhr4H>imd(%G8NL*m1bbN&;09e_KrXwyoovE0OJg_vCes5Ip*z5R@aoO+)mrZD8g!N;fxoj'
    b'}tt<Gdd>zfePlU@&3HTu?XP;Y{c0NiQFwE>U&z@5o`8SS|$L>qP}3=nqc3|VtHLD2>R40>uPyAxP$I_1uUC3Z+!+tlOlqfhN'
    b'hI$Dq$Mr8F4vhNHQ96r40&%_L&t=X7=fmp!e$OTq;*4Z%1hgh$8FG+uS=Lr>zhZ(^P7<5ovr=Zcq_ahc>@X7o?68sO^raaCW'
    b'U?=o@xNgHnyf<8qv3H?2SucU4K7AYF*+Xyq`zJ{z(f4&M^D5YEiX4LqzB(a+;YUyS#4ciFtu<>h>|<>y=+&LPgNd8Yurb7c%'
    b'MKpfb|;NazX1aYNgzftkUSn9DuZ4=fC)LmMCk5O!$Xz60h4zPgRz0Z2c2dq&VE6g>CEU`gSPtvDBB<QK+l)}q}OkB#=}9QGl'
    b'lXK9LCNNWfK5R>F<PacmUxIHt-KWCXg^1HRxl!9FH3w&Va$7;SFYup-0Ejuj4+R#g4tkrteMh%kQE`a@8AVUqfezj>e#{%Bm'
    b'3q$+86^Z^}d1dFkzUR39!QFBr~njTklKxRs0wnIX4@xMjDZpWyBt{gZrUny56{Kt)^*ql<HW-RwH&oGVNVoD0sCKk*9L)wjn'
    b'k-Tb=x(%02S2(G@}ccm$UD{uH+Wm4d(t9n=36}a-c9`mw2g`{x5wXJNFc}c<5g>8@GnwBJ%@rYY6J#mGh&#gA?akbBkY6GAu'
    b'8=sB#&8pqB@}0FSubN%^#@My5dR=*U?8@t2X#ESb^$#KI5n~B38V+NSUmpmE*vmKBCdk$d$HZNe`O#C3sDxq}7XCgcp@R$ey'
    b'VA)3StQ{rf`^9DKyKZd3losg*QCsWGI?2z_34Z2NjQ@i*Hl0d+ydql_Bpq=FXUJ0$^JRJ`8e?eL3k<qZ4u}h0zFO7^9uveey'
    b'+3GgxA4FpQ+6o_h^B;=fLdv8ahO}KiQ(B6t-<IyDy*o`0R&Q{8u^JtAwe`#WNU{w+YhgP}rJzm8t#8)InwH@NnV&@PCSQ@_p'
    b'n$jX(dt+F7NRe3KTVDF}MB?-{v122reS?GTVrmjKy3nqN{HHqumt>#Z!J4Kx<!gG7KT2Xc74TFG!x6cCG8ngiF|qXp|8MU|p'
    b'MWB0cz<7pY{De+Yh57^Olr2h`2y|qXlrZRL_qC-Jteu^R6tmcTAG(zJLX5?>hN`8Y8M>y<|qXIZcdyhz1i&)(*aDCj|uQ)r+'
    b'n7~%Sb^~8+Z62w3@|j$a({tH^j(2fze>!S(x%eNKa*?lu+u7IYTnW9ipVRpdsq$s^8w<Qhzc$ysv*B1b#|`+|PT%5K!!SQ`^'
    b'BR-;9Ifr4=!Sb)A)aeq7xj}&-tcQ-1@$bo$T_(Gyz8Ywy_L@WqInL`K6+4u-{n7q=~r7f&~Y&GPUNO-inOQFf(N5s@BXakx&'
    b'8Y=XQNvF6B4|1@m;^5_#gbx`7*i&UHPMOQ_1c$r5IvYLUjOr0ssuP5Ce&#7GfxIq$Ysy<7tyo3!&+#1<{1mf+$M1CIoo^wCS'
    b'kH*HqL1Xc}q&6a`jaq4yA*fa+{re|5gDzB+#h67-k<S2gD<C6T3>r7a4PWh;sQe=EFIN^C(fzqQqKb35=;?bhJw9UBGnch`!'
    b'8_p2G~Ey|uptJQ`hu<D?0e<`?cP%-L2$o|6OUcA4TYgKA#sp*+|R<86^T+_J_F6{iy{2sXzRnac)IrEBgQXyb90-@ZnrD^?F'
    b'DpZ~m;b^&}XMDN8o;O~t!-n!t-}nKaNmAsf_dyol1N~#a*?efw>&X==9QysHbup=*aFT3QGE~b-EfbaOQzA}FGA*!_Tv7r^O'
    b'ByXwr0kGFLP-NP?Jf5-pBMQAMq5SH2@9S{tkzd})FSEfkMvwWH@)EHR0yF$7N_#d<~egiwJ#xHTGB5G^33v<*_wZc-hU{^m9'
    b'9QSkQ>(kD|i($U1XeJETDoDRl(^gKS^y;)EtSKqUN*~B_AdU20&YgmCiRE{K`eg@~2#IRC2}c1Vl`Wd0lp`KQ$TNQ({ohdtW'
    b'r4VrOH27;fe2{FldN)SzSNJo^!|nmHmAt1%JE?g6XbC|5)=5^Pj<8&dLTBt;K4o})q{RmoK&EjkIa$~7{46rGUMF)oQNc_hU'
    b'JMlZFng4vmE|04V|h3u3;cuFe9%u)F4m-knHpsRF+u8uFUXt(38s^lqcKva<1vd<^4wdiz;1>6{26@*LbazQLY${kXcHsi7*'
    b'kz!BPb)+5Og8WD&F6N4f?-JBJ6Z4IJk!vqVqCTzhGDZS_QSDmSJ(miooQfC1nLldAeh>C87}op5Tga_r==T<(-XVs#Lxd#~J'
    b'+QXU0J4WM!IcV*$ucrehC3lUhwc8ORttA>*?qtGP}+?u(qdl<JM5gZ=jgpgHUos+KT+6Wj*C&wwV+cFFps5=3fvpDWXp2jTj'
    b'aWBx!(K6KAte)q=J^lEh9Q|S1JSxgz%y6L<|pBo^%s(?3F1Z@cA%qdqo3i3|p`p7i~~OU|2Mz5{}0D%y#LfGg|X}k@){qi9|'
    b'qZv|i#?n`GhTqvf*qD;W|XJrcCYW+lr(KDX*b4oj;XYDh;}!@*HO))x6ufp3gf-Z~hyaNixNhP&aD>oGeM*S8~n@Qhw}MK|W'
    b'EUti?tqR)1y&N`zR(1BC_1JSl%4Niz#TTT_FJl96PAklpcNV#RWLh&vntbSx$Ah=ehA5%p{7zLp*TY5<<I2}WvroN$;c*aDf'
    b'87k+M|B)+{fMP<;(NHnH%Mqg7HE0Sz<+wEo0!E?j1&4#1m(>|7bO(EOt#g%F;HS7gK2~o;j$?}uX&J(HiwPC*9=+ge@iMvjY'
    b'`>UmF7z2OnT%Dg-bH3q;UZI!T^Ya$HhR}&+A7vEUiqRsTxMFKnK&d;TnNbI@k*AUBrL5W>eOv3kgx>HV1AGje-O%5G2&&&R)'
    b'WKr>&iH`a6~1HeSh3<!dL5P`6dR%;z0yUD#lK#iI=>PVP~A(I$*6h))uheHb1gIsCjqn?&Bz*JG<ODaLbL%!ARtWH<Y`G9@w'
    b'r)XmiqR)pdxBqz9W%b{-P8<Aro+h_05Q^miia?t^|i)G?%KZKuNvvQpmMk1)zEzE}~Lniz^2Z?eDbHNJD&0hD$cy~budY79q'
    b'>@9>op!gQuR0+OZ>?p9u;3P%^*JMOwwf3Lb^3+U>*)#az&ttNi?T6KFt5ethB2sUVhZS>edMTd^uVDVsyahf;6g;)%V((P@@'
    b'DKdd@{I0o!hcp~bq6mjM3{=d48BG}+!VC1Y2auxcY#K+SMGm|I$P{?h&GAye;@TF?o-%kA?%GX9$seM%H(#Rtcw{x;SiM!#q'
    b'=ts!{iS)}+jG19Q74t*sOQbd-<)+ZU?kRW1$)=l&g6$#-Bc7GptjRC#jtvS&AX+R-C{Ge14ukJ$*KiIPJTl{n{uv1cjS8RCM'
    b'+FKPZQj*vGUf!aQ5<M14IxybfX+U1ogo(X@~9igMXi2-dr$`XpfW_Ac4jnXU4ypA8$Nhb;td{A2<EEgJI1fL4Ct5AqeaayEm'
    b'WXQZvMOj)zvueemy_v(Ko35!#jXj@Dou4g3lNBd0GTP_s8`!Q{*2GlY~3848Y@$XQci4#Kc!hb9g+%sHH;Jdp4&T-{ztPBkV'
    b'(Vs$=UHTkdvY$A-9ZRPzUoQf=4CTpLbLfdAbjbd<gyTGetr^^(OQ7B6Tqais0jDBXXh$d<~$1kP079*92Ql<1S*m}JS(ni2C'
    b'Lxfhdk+%M{h;><IG*yw=qo8ya-wUyMO9Eeyn-$7#4wK_QCQ#Gh9-+P%QjdfBF=G}bJoNoia;A<=E8-tWspXbSoovp`h_Yf)*'
    b'aSNjlW(r`Yr^yu6mhwS6?&{xk|NN`_Ny>6WO(aL1yg`MfKuDe&G37m|H`)@BKV&Sx5BhK^P*ZCCwV~5C0%E~TSdV&db-B*r8'
    b'l8tnXA2iefbGE@TC4Y7S<1r;XsH)-p|^FNwYgvcAccr@34r~)lTC77y{~Py9`N`V`YM6z7NufywTdVslk&GX><Y}0@f|vB^^'
    b'}Wsm672f1P&!e%eNzv%GVW&ZLh+$sJH#z6z}&0j%~J(L>cP;1B6Di@E?#EEeYt^k|i$3Q9`>MHn|F=D`|(n?JzzjVc~KC>P('
    b'Q1)$0)zXo#3YevaOf5p!6s$%On_@u4l|M(V?vb~z@4eT)c|H%#$gQTg^l>5!i;qDHz`^vgD5?%2bG1F|4?K2jpTvn-L9N$f&'
    b'U`E%FrLMuGu9A*_C_GVpv`@P=#nA9{lzlhxys7?!VOnfT+42Zos}d=vr$#V?^jI(hEcbKv5-Bl47*PCzds%~NY`jm>H(4WzH'
    b'y`kV1ka8N0SkJE*8g;8(US35-01zM+(=d0+$dW4x77c5&{yVvwOs6C4dh^#i03PrG}&b&8U#t80>gKG-s9r#%u;F!As=(q^0'
    b'+I$-QM=fO)dx5kVmV@q3%wj-40uSOV9C!;=<o7G7MIo<MBT3L*d7kenj%I@89jlhvHGj-?6-(A83!>$YWl#()}_|l&s9xQ3k'
    b'c%gsJ~7fu*MBQc9KwX>O&2@yhzmWkM4?*O2KU!?kDGKouvnqABHP6!CPEEb)w+KIoUgh;E<NuQSvb^SEgMxcr@JbQu&X<X|F'
    b'miTNPfoR-=wT1+1Jt>z?4>$eJeU?##=JIEhrKZcv&#?C-=p~jRS%I(Icq;V67j=P+T;BE-}A8i^DlQpPu=^d85ZA+meA`!5='
    b'dgLR#5k$&J=m3+bqRp0DLgN-uw9t*?nnHH?<Q^S=u7XS_Em|f<as6I``jonKWC|goVhV?0-6vFqgt2cGKB8I_#_!k?<<Ni?r'
    b'g0fI_F1@6lF^Ljf<_{478@p2*D$@&VXgRb-?F;d#M(+l?^HsJZO`Oe>GvJOz$J)HRRo=Nud_GN?#mE{DSRBS9-}D@x$B0l&R'
    b'>(9&bL6f$WXQD25E{x+UQuv4LIjz2;CCMy`_tu>w|rsyb7+}HJvQuM&ifhy}N~9;dYmQc&U>1KHk;eyx4D_U==%@VBy<1;jO'
    b'l~%)bn;qnJbUwA}UWZCDevC>8$CC9k#IlTFL88@BxEKgY6<Gn9l_*lZYE2(XuhytVN?b@aU97-e5?k^ql?0RQdn=3%c@83My'
    b'bii$JrMJpRc^Jaxpiiy4U&sP`j6(qQq_~(PqzT%c==NEIH+nN_{n$Sj+EtQ?}1@vb2o?_);tXRzl@1fWSnu(Pr6RWE)^57cw'
    b'yv+|hnJ^&YCAFSUn$y?>yJ%HMPSGH)kV!y9Yc6npnIxMK7;*_)dmxbZFqPDZR<uAKp)`ReyO-kN4QE1Yu`tKEY_8z0&oJ6Ty'
    b'b>XAV1zY0#4E{L^@CFvhBP$H5%L3h(Nmx*Vyp?t7X7IeAyz~&O)fyIS;>26wHqjiC%gbTC6jOx0Q=L^l~|b}ZBJ=1MFN{8Un'
    b'NOn8^j+Pd2rcS%E-srym5-#qZsg#`~23zPz1wciXf8mRs7D>0<n%Os178;ej8^EygZ1mA`p0}1+K%!N16@?0idFRbS=g3V90'
    b'SDuY<>q8x4%>dMCawn+>ah?CBE<=|RDAJ^F5hE(b6*b}A3_Gy&hU(yVeP&1>oH&;v%@-rhdOhIFHhK82w8aK;Qi|8V|6o7cB'
    b')Z((kp^nmoc;I1(+0x>qHK9se#z0=c66~shGWMNJ|!A_};U~<aQ#AH;lWy+r+Z;FjkXJvF@E}>3??Fgy@$B3vG<)~>b_e$LN'
    b'pXD4@j8vbN`@T3~U6gk2H7BiFF3f09nAINt$ka5_WF%o61$&n9(gnL$O9F31%t5rV!f4>oXWs<7Y<}fdEP=2!USlY+jFVxpn'
    b'qRu5)ovcRAl$O~2bY$u`OeUnma$<PD<V1b-D}VFT4yR+G|whS=o{<e^JUVh+{BOv26xg<pg^a1P@=;MAzv`7X7~aCI(}V$Hb'
    b'gy!M?!Q2UwPZK^}Po*2V}wv_vrqXSMXAYq1l~&OH|5kecTtq@ye%B#T(fN#XIZ!-Tq`UJxD*U&XcD#b)4zz;DLP^Joozdx4@'
    b'bM_<IdSP42nBg9W)exql5ya(8h5np*n7w-(}1zah#YCw~~~O;g^=AN647d}#aK{s^P^up_8lg4`2jPeD-)SMv}yPY1cmMPx*'
    b'q0;=?(HSB=Tj90>PE4|=Is{R!W^5tUlFc#2I>&BNq<ifJ{KQ6-!%md9G3Pj#D^;|lMhfk!k$6LY#$Vn7#pd`^Zd5`mEjQ+Gh'
    b'V_!ZL_l9`Z?}zeCKYJ*zr0{xAT{1$IT_Oic`=J=bE!r{h9F>jRL*-j2x%a<qZt)UpPo27N`!G2l?y0Ulr20KlNLUV>o))^V;'
    b'~`+%UtzW48X|+QeE+a?VT|x6G}`}wKbGyi8vqIZ+QmUh+unuT-oE!gJm|OU<CD1Wm<3_~4fE~|>UiXf*L^O=L3-2;yC_H~rh'
    b'9-(ZUSN$HXa}=vD29cGJD(uy8J`?%p~i)r$YiPEBh*1L_V!N8DE-!fWm0GGKAxt19+Tx1ObZWdyE`~W{+eqBA%i7u{1-J9VH'
    b'`H*Sj){#FbDY7#TRmeNJ2h>hkl4?+n&)+wJGAk9f!SO2k8ht?v2r3y?7<JrN2m<Jrs3(8Vg+eBv)#awUDjkK-1M=n3{FEYi*'
    b'2`6`+wF^)h;w?r_ZelNsAC|$dg4_g#^AUIm?8*b<Cz){XEu8Oe3mn4BWn-`L*Hv=l09w6Xmz$~&*Gl&hajJz{(<vMp(Bq2as'
    b';&!3!R<sN@9o;`FIA`(h_XC+{*Ilx%D|7k6H+1c2oxanf8-ULGL0=n-f9Skl#71OKN!w$*g@hV9=r~UFW7;}AZ?}zc_9(pEB'
    b'Y)GvFnLkD4;MI&U%GD>l+nW-U&RXj|5f)TY-$@xpnru?yf!i#49pE76JJTZiQ}y89J>tJ6++A?77~$!Jrjog?XRl3ySiHvFp'
    b'jg|dvA|1s;<7Oud41k+(?Q=%_l=SD}Fa-#qVtp051K7d;X-q^pL;9+o?NGipU&z_6|N0@%d_&V<?s*GDd16BPGN__e$^Av|t'
    b'B?Tw=<@ccBJMjhwgGw8RRz#FDkzh^8O)%DVtj<5G|Us32a8(G8?pUT+OTzHL)7u&7P|lAZ^$sY@gD!h4Nflw`VeJy_uT-JPT'
    b'LLXFmIqmtK1lY)lp#U~FJ1?77s<JJ}!;1hDaBdI>9aa?{cRKaS+RS|50xFk$CUn7aMWLWb|{z^a9i=9i?Wr(MNWb^UnMNI>B'
    b'`kyD8#yitnYJVeAXzaVMK59DPGy>w_Vewco0g@TF-qT;v_QHhwm?|ElN~)<GgqH$i3Qks-0lY`Ayo|y~%@g2I`i=<Y)o{F67'
    b'z4nstnUcBD`47|Wp-ITe!@;@D5n;%Z=$vg{L1z(K`<fp#Jco(yw3-v%Xi>8Ecm>{gXLNUK2khbKK5d@N$9)t$g~HDwZ@RH`s'
    b')Jt3L?eW#k&cU7l1;MgR|7~Wf{ohkl=BylT)`Jt4jn{z}B^8KuNhma-;vWNIdrU9}U|_#66oV@Oiu2Z+DMdHgvz+YAR0ayxl'
    b'qLcicf<*4<4%;OjQshTvLNM(++qWpG$lpg!Zjl~r0uHcDPGL&gM74lgAmj6%`Z<`oJh-Xr;1L#Id=FniKr7Y_0gVxdKGGl%d'
    b'z3YnsP$YqRbilZvbJM>FdI374c?`EhjBF>wBYd8m<SnQ!NC-Ibol9MzE8~DmU#AxGob5hv^g?lT2WtO19JV8TH!(Er@F0NWc'
    b'n>@b_@HvKPd^F0ZW2sh?x%cnaXT$w2hdH6L(VYYLuU2@o#|l|q9xsD<T_9dzE(z#l__yA<(+TI;sV@FAsBaWcBi$^#B@*rOi'
    b'w!M<qf6eWZx9K0RE8}>z0+-?>+Uj0KotdT7EX7ULdx&`U3gu@lDe)yeI@kO+!nNkSGZ8riaVFvY#-JbrCk|oLnFx@xL+ArjZ'
    b'R|=Viq1N6h2cVoXg4IM;O$6&a=3$Cbm2e%y}Br%v0D_<RlGvZac`1$0S_x?xuCM(2z6VsEQJ+Qb=|*_Bh;@%<s>;b_G&Lrnd'
    b'Mz$f8AoF)?DghDVLYwYMmrTl`#9deFJOB`+)k<6KLJPQHjEIoV}45dZLLN4@v2=#DE>H({u%BHm>BsbabC5lQ8S4qFd1n4x;'
    b'vRo6yXbM84WzrNfcJpHHx753z#g}fxIzM~O*?eUeE4316Kb^0D__Z9qTyvDNxH-3=J7|}cnG~4HVF9JxXGks@$l`i8czSQ{*'
    b'!PvYH8yHFU5})F-p>G=)J^FRj**_b%?(trFdn*cspP$=tt6`AwHV0&K#(>WC9!6_rAg~w1o<|G@EU4#xaMd)u@Yr^Ep|-;t_'
    b'B7LzXHC37^7uv^j$)`u-D`WAB(A7QF%3TL?tvR~$#iKH@}-fXd0H^|Q4h%weO_#MZ9o+Ty#4nZklJsA>SEeyMJ>l`!1`!hrU'
    b'gEXc_9)9RVHlCDJFFTsVw1Sc+LU%Z@uBLdk%ou>4$g$gc`;Pp6jGlpzej`v4{VFz$B!>e&R_%1Nd~UHeH$P!$3}IHKk#4!@3'
    b'>(J!{(6&PwM*e!^ZxKgJ$uar9TLg0i+1Ow^DyV=<@P^AY|t?z8q+X{=n3z!%n(P{R?SLWzDIii6D23zVHI)KZ0(i(oJc8&7j'
    b'29{=nErpVF1bB;dbux5wy=dYfDup#&k)ED!>T_a<zwy(O2dDxna-9_o8td$I5?t-Zon46qyTTNXhCC%U^$U)x55eD2Wx<>t2'
    b'{x#uWPx#k+{`C|85<ed5ybpEehdTE|o&BNC|FAllI%_jpx0(klflqHPo~)=ZoU^!4#)Kgw^1?Fqo-jl?VOaz8JwtEFy~x=6$'
    b'-pZG?Oc!ap&+AUqrw3k!!GlFfy3%vtJ(qWiWiwt-4xB%Vo!%c&Et+j{gREF0w}J-tvm&1bS6|ZKSW5&!qzldnW&^nQdJnSvv'
    b'_-@6_>di-s56?e+%#rERn)|fZJYU!FKgy$4JLZ1md%Y8ws~ok4D=Tg=u%*@!IXSJcV$(!5e6G2Dk%ryUdMerO0j1qBw5WHv^'
    b'h{zQLN`I=fMJMtgqR{e(3?>A4!F{DO^Gqp8FvEM(&1<=VXC(y~}9>rt_N1bmC&@pL5j8oijI^66K6THBYC{|643k2qgGa_=Y'
    b'K9QwK4R;p5+7y49quj&JLLHblEQHW9<tz5eG6zO`EaV)PBJ+5fkW?udv(~+~fz$Z4B9rS(5#6X4n9QhCRBV&mC^~qBmJ)>^k'
    b'sDT+%JXVK!EbX6iH!hkumA0nszCf(qqma8tos5#9h|+dMpjWxIY{bJuu<x{;ntkV!21pV>{QFS-zy}>E&21T<{v!F5s>R(0R'
    b'aI3h+&(MvM7U$*F}Z)=lt&A>#_DH8fZjy41X>j+xj9NW-6fN57a;3cz`~F!KYjdA3Q)g{G4zXV`|&4^UikY`naZf2JBO__Mt'
    b'3D|Zhja{cK81#;h#R+)mvFRb=s>tz~qg5y+-NX)k*910|)M^P&$$|s)thHuwI3!zC`WXrbhK|k2I)R`73j64LSJkRIap@NMD'
    b'G@C#geL&!yf$Va!s}6HGqquBJoOKd|3TirOcW0ysYT4seME-?r*ZWO^Ec9?r661e-i?_wq?22EMX2>~gh{L=a55$)5Ja`3)!'
    b'ahASjt55reTUJh*M9Z{X(ZQJ06QdeG;;PsI6`4^9WvJ*{`KVXt0H;~R6gpcJ(e}-Q^@Tf<nJCUAvcMx=!S`vbcMXNiAHRWPN'
    b'gN96yGUV_ch=;Ex#W2=tgfo#^5?WqHIc^3GA2_qL<!Gf*bb%J6^99t1w`5PBHOMg}oR?I}AjgYd;*@BV2#D2We?j>LUJ;dU1'
    b'6j*f49QK_M3*L^JGRD4sNM30WU1f6?j_pl<Bc3nMS4<b#8x+O9CEyP>YVcgL}MwsK0~PxFJ@W8;nOu4&54%UtZLUdQ<V<m2!'
    b';N!V9)f-Pc?VCMMQ&hT5)6T-VyU-@Ef+}nnN2l<ce;jQ75ad$|}jN7oQ7vItxNlE>BXSrYF3wPQ#f}1YdR5jz%KwaueOO4I|'
    b'Y`!!S&~;#;O6+D@!fW8tX)C?i*_?5rDJ!}n-IX#EhckB+7A5)`S^fWtS+QWAO`y6;3qifJ<ZHnTh#X(ZE0D4r42cHEISShrD'
    b'!$6IXyYmH9H5D!lhQ%hR99k11BG8);yHah4%TkY+B*33QUc?m_`uUn!J83<~fbl@ST4Kjt!$zow3PB+nad<PYFFRv!Q!QP3c'
    b'az%=5L@xE!hvWxf6T+;xX$LTF&1r((^Q_i?pEmMkaAme0h0jPgJ{(RH3`&BY35Z~irJ{Py?O4$T_8L9Mk{$<;Uzo?FivmZ)g'
    b'WburUX&bHKk~!~^fM52w0U3C2U!yxa2x&;>CDyM6VWRIpTFLGNVobGUy9b`A~iU&DnwnH%I*uR4vkXm9yJIGC(`6bocjP5Wl'
    b'(poSXK?zB?C!g!kRr#M7QxD+UxqcVHbIskYDj9lL?7tWVbE+I8V}es$s%U@+|Y;F^oRe#*#q|KFft1AZ5bSE&hxO{wa&YpGl'
    b'g{{g^-VnTa;xpMilx6>|p3BZ|!Yvk;{`F+0OOsI}l#Q>@Op-CazM+2Jm*J^0{qnVerHqh#DqYzbOW$s8br;9Iiv3C0GtTiuI'
    b'=q%a9h5$+c89Bs0byoUBXmbklo&}}d2YqKQVXCT>7TAL$KIlr01KZ5A<IwS54L}ksqsx`8z$fTKcM!Z08my+$+PG84t9A!vA'
    b'Fpl!4YS%+AT=OP&(&}Y-Qp*V~r<$B*az4wci*t4BdG-<0iw+9n6JI5?@n9KzV8{+eYMV5ro(}Mu8)sfFi9!rcD=E#rdbT?=u'
    b'6m-iv4_l(q3Vgywp5&#UHukYtU?Z9I9{6@%4L|qdJQc10RliaZR!{QD0Jc^N>b%yTm35Z&x;GcNEeI_Sm@`z^!<*$Yuy%p>S'
    b'jTgzq#;4D0Hr4(#V8U3;$6LY!dCdhT5V$s7$Rd>qF3>k80<(9~Ps_nA){dCBalF_Ln3aWrK#|YnDHBT&G-tCL*KKZJ$4+8@)'
    b'R8cQ*QnkjDyjOF;504ARRa`U4J1IZaSxnZsB(E<fip;m{Wr5&BBdR~@0Zw}~(n&wqxkk4z;U_xx(84TH+*aH^xV{ZKmO^kzw'
    b'AwpxcTOcS(Ynub6@SawPgdD;(P;^a^K44HU1p7G-J895S-J?SSEKYVZEXK3I>FN$y^xZ96~LPkhQU`xQ(e7~HNqO16N2K>6v'
    b'>&ORr$;c<soFknc)N&Bxgy&qkZ4fQj+8d|sqfJIgAdDSsP93l%gZlA#cn(xhI@ikZLH_%BcAoX+?{&G|Y^E8Db`zyVyRoCb2'
    b'ZZ+K-qKrmQ}5E7c@xneP5pE7C`MF9SAgH>Pc*{tNe3CJx39F1(Z^a9jCNfr6uo!FKiz&)ENV#ZkTv#h2ZLk`0QJMPebavS;>'
    b'{1=J^m*F&YSZ}0WeTqZ#fLU=$s!syy~AkydoR!axFlWF0=*F>I>;XAy<G=n|J_;P`bOR=pGPCPmImBID`4V9LW7<*lHg=3|u'
    b'l9nzx3_{bn^0@9fYaX|UgnB|-NWBFloZ)TFxZVEU>4)?%s0y{PlI=F3D?yIY5(EO7Sc<b=&~8)x*>X!jsgq<})3%YBe+7Wy~'
    b'ZYyK}gRcfz#5G$>=49%lOiywk$y2Bn2aH*SjLA@Mqn)B5O2Rmc8Z=jpe-Bg}>1IC(?di~l8dfrSLNeF-7v>#V|x&Nqqxl=Oi'
    b'FTfB~C{}gwJ{cMtDw&3jg^LpQ8*W#pT%(KnG{VjbRYuRSA;6jgzi85!KsM<u5TB8~(M)1W-Ukg$yva5fewAPv;t&)r`NSatT'
    b'jSSyYbFnUr7f5)6nNX!=5_X7JSfMvz@4{Hn<lEYC$98N+Sq~WbQagz$HbmF#=sLukfyWsj8T`#UDX8@AukOt3Ui8R4V{Y%Qq'
    b'e(bY>z0Se5qt~iOV0}=4Nhcg~t4}W@tz9+N*pizDShJQ0+={?&*7Zix^y`qzLQ?j#Mz|9A-*{q%y@PD#^O_lgfzRHkpZBvQ_'
    b'zLYDK>oDz@3?NoCVSx?p-sZV<7-BH7@lOD-a*FA7QX#8s?!7S=lx>u#Jck*0?Kwd5{25++wF2OoJ_kiCdh!lRdDr6$R6q%Z2'
    b'_1+LP>P(S47*t<!rSe4@a$YiraLt~>!j*w)4jN+W!GNE_$9de%n>4|Y%FO3=3u;TV!{UZY5sJ@tKTW@NOdx=9-`z}UDD7o=%'
    b'Pc(F39?W%sNK|^3VTAJb%%4Z`&G}8z6i%STCJ&Mh<?oJ>z2y9{%FiK(VxxfAEFcgb2zz7MFTv|xp(Apnd(e*ZC$XQr15pO!o'
    b'y>=U@C5<<=e#Yh=)dT=(wtl9Dnic=O1IC@f`wGr2Kd5OzR}zsG-aXHo3WL6%(#Q)aFUFqrgbP<KbE`mf-)J%o{7AW$ZG!FNy'
    b'bW~jFlQP7I~CoA?u1|sktF;MV7d=_LlCBjm)2<@54=3|1=uonmw!5AE5DG;mIwlbZ4_;@rr#28zjg=!Mj+wOiT?q@?w=IrR%'
    b'MlrDhgrIo_<SEV-oYMYQRDuW}0_uKHj}u`^B4S>a`Zl>5_a=r^l<TAiI@h8f!IZzwtxy#?#n6<99OFy;AgQ~C~5E8jS^BA!~'
    b'QF|}x68MBK-c)iPB>wB}3_^tvR4V-+r$d?lxnfJVjCmle~N!W!SW$q7QXoLe>g+!UupiBgcpg!_gc_<wRKcV9bBwUeXLz$RR'
    b'bSiQcl~}mUzLQ>ShOfa=7>@_!z$=CA=@lHr=T&)T7G-&XMG2vLn>3?+b-xQU6`-`;t(R>|j<GEV-6-$$t4$GxhmNP9&-}ut*'
    b'jDHUz1?l`ZN;`m?x^|BJ;<?<o)K)r42k41L4n^R2FIB%lPLvDLZt(w{$E7&u(rv)x+W(dLsu2xpxtXJ=N2vH_F`LmlDoIZbc'
    b'WveIP#gyiLo3?+^uF}DYyPqiSNR~PDa3=@^fqLTxN^v9JY$i^%FzOQZm9I+QFzxU#Zr;;&rk^-1`UnY1iR~l>7D#)KqTTY1e'
    b'RtiMe?eU1b=K)zA~bZ|nM9pFqnD-<Y7|mr+8`Eg$*FVw^!;K7;Vg#Dd*6{I%jO3Prmpf153HIBuFN$!qsjGd;+GM*rtlU(C@'
    b'!2>N;^_g82^zJ`<;zJ3-zCNgMxIDI;wHdUEBu6nIFd0ifbYD{&G1qb8vBZC$GFL;IOrKlZ(wW0}rw##Gx@Fz2{nj#d4BYA;t'
    b'oYNW1gWKDtA%;@iT8vFq4N@scC2FsPsVvV49G(rVmq_}v^Ppw|AP2H@^l2jE;3sPxI(Or^QsG)luk55Cv4R##L5ghJ|4ZT$i'
    b'BI?l0e$oHAcFl92FG!g){~)dL(v)L>?qLj@Gk}>V|x@!JE!_yDr&(8PWc{P+H~qCc)}ArFTbRc&u4RaZ}C<xHE1e>Sr>w?##'
    b'ywes>V~aYFr^q9nJM+TWlDMOTUUc=+3ljddkSk>!jk+SI_<*HWcZWw?Sx=MJM=vDqg_l*NZ9td?O<Rh%Y=!H|?02|KXE%Oj~'
    b'71U9eRA<zqE&TLfoSvDO6ZSMh3occ16jx98VInB=5`tsP!|^Y-O;AOy&iMwFvRU-YAadS4Au&_>VaK6=HLH$*Al5NUGveG$H'
    b'Dh>Atlj;}{Vs3GKQ`*KGP5)=?kYjfiWM$_7SVniV;NdmRG<)DkV^SUx<9`v=9^~zhr1~9C11%bxA$>+-1#~^`?CpVuwK?ZWq'
    b'$pwZ7ld?55jQsil!ds3}GCnu1C9!z74>>Sl!0^&mXE~qH@((&-!Pm+NIEL$2v!*i?x3p>5!>{gW<6U_3h-6#}NRF|{=Tfs3b'
    b'@k&Mgu~0kkX<r%0hyA3B0tj>ENy?}Orgwur<F@b1mj_}t_}fF*z?sXmiK)*4C}ky++i7&3%hz=Bi2j>?F{CgYc_y4w`enKk$'
    b'xOcflijB=Y7=NKzHN1MzJ_B_6Ln{yWOs)N*cDwx~BQ)B5yr%_4x-(ctXqu%?)|Oj#}7KvoK5`dx(}Vx?I<IWh5g-vBfw-M+p'
    b'6;Vhl5j!w5ZnWu0!|YfOfwvKxyW<vsaJ3Ert7vTLKQtE%Sg!el<XWG}=wx<Yh!k1HbBykgx6W>G|>5KZK$eXuWkF&N#iqy5('
    b'Syz#JkF>N(3+85J@?)feL+j5mxhL9~3rPt3^DBXTe7S4LWuPcw5RF*3XJMY`Y0eIx^|1n<m*6yQrcpU~!=?0?M;PMZ}LEIKX'
    b'+!iQSxD<jeK`g(Dg51%j9^5t}X$$X*T+vFr8YK(pz2_CvVpSC((hk(dpPTAyYh%OAQg$l0%b0EDB$H_>hmYu1&-_}(*w)8`6'
    b'(SG(R3G>`Q>3GOw9cJ1?3f(M^UQ?D_2|mEREE>myfe-!{DRyj&{lr1!HH{Oo)+k5L*<d@M2{k>NOgOzEgPXvDcXwEfuE5x65'
    b'(n_9Q}S=z_B|F&Er&t2+W5c$e7DWkpZRpQ8lSZ3qMxrv6Wsu9E=$R=a`s4(6lYn2UYKsg40H^)xne52YJCCVo*w--}@f@+I~'
    b'2?xOg;vgs*;-lrJZO{%|7tAk*i?3w8Rokv;X&T|!W0?xH{xXlb~TPTN~5&oH2e?}<2zxpI3Oc?K0xaUlz!K*+L5vZ^fW?FSt'
    b'`EX(UVc{fR3P1;7Sd{0b{O)!Iz2)Xt%<(D2aQv1y;DP{Q}JUAQI&?^oinY9vkJxB>VXD^EAF)op?Iv<{_rG>NSpLoHW1?Va%'
    b'#`yJWYhnGWOgoYFqB}p~U&c3!EA+-!eG|r2BC6A_N2|m{nh1xw_4G{Z=$TZm2Hr~>8R&WH)?FRQsBYgklU7RRefBWeiUevbD'
    b't(d><b?Ww$%>gSq_P0}4gXZ`D8$X9-mb+rDx&i*vVIpZ`nCP_$dYpZe26K<W|iK9gn55S+A>@+(OYDK*J1^HEQRAc;;-po8-'
    b'&2@-`;*)xGHS>kB~X9i&^Eg5)5T07PO37HDA(!ntP3e-gC?r0P<3lWyo~$8EWs(2GqJk3+<p<<XEX9TDtw>e^B!@4fEtdp=i'
    b'X^vBNf4S0G~QnX<w(nhP<xFNs-q{~>4pKSEU3Q5mY&f%av|Hzj2ALijZr6WHqoV}+K?L}G#|n-TLP5RpZ9hg|z;{p*rr1HYu'
    b'_t{r12_4uP5rWqobsY5LmjyqKmAV3d=tMAccD^jWLj#WHIh=9UyWc2G3?GktsHc)5eR0Vf$u@_^K(HgA4ON57bc_`TdqbYK{'
    b'_B2y+WUV5~!<v*1<x}=DpKo$Lt#HX-j(pEy&u2NlLz-@BucAa;4hl#1o*8gfpu9KJ?|iPw(6N<xv8eSxvu@8CHm&vL#5IHJ?'
    b'022Mv)^eSIaSRx8A%j2D{;DPJXlotQc_4OA1!P%p_dD?$M-(UUUI{`5^EkL<=b%dXOC+D<B$@!jKQ@;P*39;h1!DbHu~#uff'
    b'Dx|kPVVL`+LopB+9-nr|7D>^pq`dZ99>~v=fn+)z!c~QR%L^Y;x+-elq?Pveg>zg(CQ>BBiSz_uuQseJOFsw|&xX5F$szR&?'
    b'#Nx|CGC8c3wK&wqJa!XRe^GTG33)d)g6%SPFPDAARCV%1}bskA3wrIG#oX+SlJ4{NXM%eNk8fi<@tk`(&lgWfUJwTCC#Exvp'
    b'FZ9)%rUXHvM?muB2e1OBMaznD)f=(8o^87@l7<1vBm<#yq=AmVLgqcHiB7|dY!I0g*KW}D+<pQ})vx0jRUSiunD1{F+;25r?'
    b'7jiuQ`|MIIDJeKlqRUr{H^NVC*V~qQu&=^~tc3>wvng3kQzo)0{-0<{Z);Ny_c>AtwI#TJ-`6|zwwy;Tzm1Ji2bcVk0qxt^z'
    b'?M=#hz3`~JL)XdXnq(<2wNLeYgIt4@<fb>@6eADoSV5@r{EykT(9I%gC1-|4{GYHuX1~Ll!XHZ=<`kIf6}<2o~oq%`~Lw+*?'
    b't3W00V8_X!P^1m^}Y)45PeTS`J57qz>xezmJh1^85F7BuAxwB$ylud6J;Zd^1YPYsoIcLBC{9FDxnGkotGJ*pKm?z>Cc#87%'
    b'){M&Z1;Ag_H<f?TQhMxU!9Di>|3XwO)qE0+mfIZx=!O>Av9s2h#7(f7Mjbwd!}ub8@cu?M>`R)W5E>&;Z?*@5hffl_}6mt85'
    b'@?Yk5b31j*w3{A)2+(EU$-u~<GavA%mErZ`({?;#c!U;nfAt5MI9+pjBhyZ2*N%cbf)Rw*Cd+3M)6yHW4bcIL1(EG|yp=`Go'
    b'Dm`gw@*>7c8Bs&ip@Em-KB~kE&x-hybeiLKQ~;wKcv+3UFxr>5sGCl!38nPV-#pqEpqkoWWa8ARUCA;rIB$1O$=97G%f*G$%'
    b'Dh-STZK8#Sn4-Mdk`HO`fJ|kM65#HuMdR;i3m=E?r=ENuAI@kvY{aJ^Evu<#%md_6P+W8>iY4J*T>ky>2BMP8Hyv>usg^;mx'
    b'C=Y$+K3=4aKY6*J>~+PL*J(@HVnqfv}G4c=KV09HJ}-&DhlF`I$og^6XJFO;0}t!hGK&u7pGY(hEgWlG&`bcZ(%o@u;`IC3M'
    b'$ZMXXu!oN~{X7~evPr|0jIW=yPA;YHruLlZ{woGo`FHJOGEx)4KV6rIeAG&q1L_7aRQ>^d*W7sj&J>&$3LEV7goEdbK(5F|)'
    b'VSeIb=axMXZK!Pn5q=?H<LAwh2K4e%=!2)sBfv^LHEhWtFaEa+OEfkOoOc(JoS5Pi8EuwkI;6nvon#jirn*WZqnGAkvUC!tp'
    b'8$GbPXq9br$>_-iOb==3!=*{G*$uWjWp-OP)icinPYoB35)Taz36BDkp6Sg*X2LQpnR-kxQ*`{8D<?TmaWgQ20Vczj=p2NF-'
    b'n4Z4Bo0JUdGcYaD_-PpmWwyb35um+1I9FR?UIzPquwOjRQJTSygY_Vc4FU<$(}Gqz*gV|)gQ|Fq+HpRiw-;jXmDpf1_5z7P_'
    b'wvTDb-;pK`A$F20~;St69gFzUe9|ZN?Gm9Cwv*4_UH_!7vDSURFi)im}^&0t^QI+F-?&R&FX&`!;f$gnM@nC!0pUyJhsdrYf'
    b'nHH6N=@tS3DB=w;Fi<Uy|?Q{(8DppZbR0hvjN1|4)12w|Hy4?hcX=`9O>25GK-!kNN9FFBomUP?Pb@sG>~|AvA3rIHHu@1^='
    b'7Q*w!_5+r;YitlM8cAxNC>?l{#FUUJ-qZkXcSTr+E#iwe3>J<Ddik0nA-GYB5K;$Fyn9A;<ZRL35$TsSD^9(6IW`@xIkgd1P'
    b'>8aF7zYL~ws}{a&;z)UJx)%S+KL3!(?R)V*>r>}b`y^*7Pjx-Rd1CANtfsN2Rt>KfUL?FUct!9!sJ&3E@eXr0!q6#^=~E&bK'
    b'2A_++WUlA&@m_`_lUQ(GL#od0q5fdsgMFqkOw-{sIx^XD>2<<M3`C*@AbtyZRrt6+vnmkQu={bY7Rni0h)^K1TK}`+}k?P(j'
    b'Z>yK!bRVFE6MRW$EHdhh;8t@3y*PR->+13^CNnps=`PRMDn}4#huO@q!qW-XqW1cia;61fUNc*Inl(Tb?PtzhQw#X0*Nt+23'
    b'5UzsmVb$AbaL?S0u81=DQBfPX1gg8@Oxuw;H$M_zY*aI>3K@i29XQS@L*eeiGcZ>T~!wKumBIjn6<qQob?-O48o>(#Z@6cJJ'
    b'5?mkKI8rPgBuM5p66}I7o%kKp;RubwI0c#|@G0hY8LtMVd4_YRDLkS6(qrTyfMaj%K3|$Jj!LM*EpqkBiQONZ+3_p4r_Y-z6'
    b'#oDk45_36`(Pj->woJ#lC{lclX}WIvH`uCCJsx;D<_X>L+B9ZSJY(h#N-Cc;P9VibKJDz%$)J9_x(f@bo<O$qk+5+}K9YW|*'
    b'>kErzX9-NG()}WP1Co$7+r6nmoIx_+w@=$+j2OM@(7*wp8qRsmK1qKq-hmH)JmjK)7z{VezLJk2k-kZ6IO$^EnLZ|Yxj;O_L'
    b'WV7i?3`D$u{#1<BeRtP(022`68T(hYDK0=)XyouYEYBcXH6=<-<=hqs;2=W)B{w^>&msxG7#<4Jz?r)$_P91Q^q0hyF&Lvk+'
    b'Pl2cc8v7eqQizG2rOJc7EU_lCir5ckmFPd@{fw90~j{FW+LBZ)oB7BGA03SCb!1^NWlkS+6$E37-05_f%PEqXdSeh{TK-Mi{'
    b'ub4fqc*x6}q2Gz*S*mkZMDlSU$s}&P#^%<OjmTTtsHI#k-za{Pa&9qH+&~=A4j7@92yx5=>Q?qO`p=P8MW9~36m#Sh{ydBpK'
    b'iy7XSbx#uAf^@4cNNO*F%#ZT0WJAZW4MtzmLxVK6`xsTis$rz%O(GR$7uLw?23l%oH_9k5z2lCJ)FhQMbJkb4+g!W`Sg&gP$'
    b'&PIoU%2kH6>d-!BJLLC6ro|-qTi$yxk0<<eL9=vC}Jl<w?~roDNA5Hy5TGigJP@UBH3w!WFqY^8c(E=uU2yvA`O}D;6YaMxt'
    b'5gxTdhx-Rl;Bb>Q#izvZSsU`47Vgm!Ege$@RS94IJMe+Wuk@-!#oKyrSfGECcSuL>T3*?ig59htz!bY1*|4`Nr0)Vp@2ji!e'
    b'-xru@{U;#0b!OV^fehq1tnAuyJhhHlW>-Lbm*Pm_&oo3#a*71>&TJ2*6Rik9|_+Afh0v$I<zw=PeUl3SbtxDh|LNrf?5?xth'
    b'4;f|!_{6^JLT@j@VpllgiJScsen_`Wf%+4?a)s58r$8sBmi%?h*RCMVw;p?}wk9v{zdn5I-?ah=rWU0W>+huzHevTeh?Cn@u'
    b'Pbeo((uQKQny+=DTAZUK99=Saw%)v#+M52*sZa#pI7c}BLbo#=V>6^@wh2@aW~31Cv$S$ZNSWVAeVZ<RW(x=-4OJ(E7C|OWB'
    b'+yJiDx;>Y96p47!#@6;``0L^<J<DE+wS&`9;Oc~o+h)M%FNv3l5GRoK-cvceK}*i!8F4FKbN71S=MxKZ=Feuj^4b;h*Z50zI'
    b'37*nRNolDC%M80qAqI$x<P6tBHm+gsWA`swY}xFd$zHJLmDna#Glun_H8;aw-jmw%CX_orf~FJ`#UNThoh+B1&%L<@)uakn0'
    b'V;gkGQeoTX`zq(!LHMPNq?L4vYAC(!#8{rXP-di?t9=i=UfDM8=hkFjxMCXY6`rTm`!8Gr6&zwK2oh1OP)57Rp3*{rlPsFs;'
    b'FR3sDrp78mcxGniUl&^O=jFuXFP(o8Gr>fb3{DnAIvODm^btOTMwFqPL!!llbvXL34hz>N2-#5#gi(BE2Nz^e<00G!xD!FYW'
    b'wM}GVIzhmzb-dqFYC;(WI%j)E9nh0>g~X{4Rsw4ox8Q?TOT@elDSnplq{l(D91&vPP4T)>DWN$N(o}a>x;f&y4b_>}QbPrEk'
    b'bV<y1yx~8Qq4SOXx79Yr3fXAek#76;nLnYp=#|M$aWUygf8^{ef3Snvg;K2K5Zg4dB5+ViI>hqNn-ccDJoZ8>DA*-%@Sd9Ax'
    b'xu`1mW6)mX)te$r!cTR=#R2#@pI$qNzTx(DjLzOSN}HV-!9A3Tp{{<!JVNlWm?9X6^oG^{2)LBZ%KKOPQa(w=e9E;>>E61=U'
    b'zt6}d<IYrP;VHvOF!kAn_9@m*EscKO=zS`Zc=s=f_Lp<G|8+~I~&n*JmMWp)rvKbMvS(UdGOH&rO&0}PW^`sk2%In%3@TiNn'
    b'S$BT}oCT{t^k)OtViR&zUSl+w6pE8e4XDcG9Itq<&*hPCDC06Gu8e=$Mwg-I`R>Ko&^y;~g*aT9=5f7Weup1t<;QtTv)@jEJ'
    b'KkuHW!)_n`Z66Yq+9#+J+3dFR*6YlDlzKjW;IC--3Ob5azx45SJcgPu?f^1oLJ>Af1W)U9+NVbcxInLl?bCyUN2%Kvnlq)gb'
    b'WS@DpJk(js4~X7^p~hKwn4H{x+y}fQlxKdQMcXci3WACHK>e8r9qwW!wbX!%;FUGKqL<UU8In%=`C?wTRVMnsOo5Uj*byWPI'
    b'jNs*{m#fy%liANk8)Z>3e*JqGI}29*QL5cXQ{sLn|M`^5kXJk_QCz4J{Lad{8me0^N+-TrhMuYMZq}lUY&GA6$J=Ee?&{hRw'
    b'WnwXf3lGuCKSOLeYGVXPbqRN@Wpu&`)QV;d+CsBFtm-2Xq?=j6Vo*St2EL0qdcPv;Ds*R(HI_XXSgzVyKl8wBl=l!rJ=;Hui'
    b'k51UIxqNOq{tb1jtz4l9YIEh3}-^jcS&Om?=k0Z6$!SaY1Mc$H*%q=L(ozE*Oe%4U-nJ@Cqw?tHB!h=?M%TMU6?Q3?KjwFRK'
    b'Hmr+YB*T@ml46x~q3R$ud}So1#=$m{vPZ{M8mKWol=bB7bquS~U$Wrxw*lRHIkefk+g~YBbx;sNLq(}u=qY|~|48tu%?RzgU'
    b'nH(u<HPA)l72wXVrY(tK?~h&L9SBMcrmIK$n2@|A{j|5im`M`QI74Pxi7YYBz4ln_9^FS*=?u!_8ltSQ5$nqKN!*Cyu2dpk-'
    b'sgTtRttRUi`*lz6yv?Bps`1J94@eASd;7(pvXIn=ae$mXs?F--j`VvZY{sWk%GR;E}K~#I>d$<E3`g=0U9Im4vnwNf=TbL3J'
    b')-2I=_E+CmS26o|B?XvNOt_BLx*)neT9HwpQdE-BWS<yUjvr7qDb_x!CYU%u7qlIL)GVBg&Ht4!h;E@>H0NPNo)0i=uAYf&i'
    b'gi0B|&*issFY6$u=`z!U%QmK=cOPw@|Ta~b@Y0n=hFF7TyVW3PMmO~{QFNO(PUM9lVoEgZ-TnJ1nGPoR2z^Rn1C%nb&ZS#tw'
    b'S?z~qK4rOhmZgh_;jS<^6yhV_-2gPR9QfFPTsL5;0%o<U^-u0vKN4p;m@4xpb55KG*pY5uH6swBjQ}6?>7SkoELwSf@@Bx}w'
    b'2vMIDhk-6OEDA^??!|zC>syrFjF`Mp^Au~awf?SVFA@N?NW@Cl5vWWB0~ON(+y3@#-(s?&|iU!@Y1{nk}9*4>wd?j@rlXu^h'
    b'(PpSKic%*@N}b!$3=KOO3VkkV2E5j&geln6<(iRC1*(WcHgrGe+*`p}SfR!+o9Xwp21J$hS{*3rx&GLC$xsTEoIy?hoNun0%'
    b'_el?sKm^bD)N{>(H*ys2d<RkW!qJ7d|Bv`W6N$K){k=?*4$WEPh@I%GC|L*vL0G_JA-;%I2hml!VCe;+nNI3>dj{kAkx{AYs'
    b'h-|jYX?%JLGs~rY$)h<X+e#19KGx8h?P~hNG1#z|}JN$uIU}@_qjy-@(#+zRxS)Um1BzXnf$APtyftlFbQ_97hCXNRZz>#0G'
    b'!NA?@2>`S?l}<a1BJC5<PK8}ke%We$sI$yWqvfYu2I!Rcl6Yq3(zK4FE0E_Y=s`JYK}o3_4Xe)34Ko=i!<9c4Rek||0$7{^-'
    b'kvE|p=OhYGrLRzE9*8cpi~Z@{j$-j2k0;kt;P(S#5=ZkQ$;ll&7bOrstNY-YDL@+*@yoxJD|q^2Ef(`!vN}dbE;WYiY&BI`Q'
    b'YnNhUHQ5;>gUG{FzYHq47s(4U*}YQI_(mGZ7KvRgqd#<-0;KbrhF*UmUS?8CJa<wsd(Du>(5`WU|eo&nsGrAuMquJ*yS=MP#'
    b'Nuor!}j8895qjrxwt$jvYpliS8?5X0vY9G{{q-aP3my<?Bmv-xvv2oq#dhW81Z2I;quKJPXXQO!swwIxY#*fM1cndyeUzfMM'
    b'CVvcu`U);_wii)%OPw7ETqpc=O%85&3elV2QAv6gvm%dLC{(sPPrvGTv**_aAKeQsUltxS>7MZ3-b9p`?(z!LiGxIJc5&%z9'
    b'E>)CEcTg_Xv*ndg7<}n3nxhfDTGQ1A>8gugaL}SL9Nv+rB;#WY)jzk&XgJ<#Ei&=X|H2Q_mBQ<hJ{K*~q>Cz*OQtGlZN9N32'
    b'n)9nTO{G<3F<;9ssmH-$^r~bk1`msnCpZg7Y+~X0oEnUNu@PSl!h9ezcfp&J3HB{1iD(?_5?dal3!h>RN35^E}$N$t01HW`m'
    b'848hV!5$3U57d9&BNegfn4fTT9fSH{e$`sI2m<R7XC(C5lkP@)}`%?`yx~kV`r}OTS5mP5m?&8hNLVsYx4d&|^L7{0*P}%nh'
    b'&b@p9}Ads%^Q{AK$D02Ru=N$`cqYQ|5tg61X+sF)DO#uMfyUu@kDHQb=Y_heRkA<S%q37We8%;t{R1+%%n5}ip{K4Vx>NLW!'
    b'YOzvx(f9*Zf6-`tj6J5xBDq-+r?R}?_BUO5&OJ6P|40#3bt7^D0aZSL~IR7544bO!lHD0gc1>aY6C^jj{?LVu*_?aY@=CefY'
    b'q~UyVB|VJD7Lmmf*7fGfUxcOn1Z%2>y8Vr*P*8p9ccN-5h-BFXoDBTPV`dDMF+!z*@$^s_Bj9P88>IGB&+i!1<Ss7a@9yNa`'
    b'iNZ(>|z+!QH+<ZCNy7}UeQ<$Xs=WIULyOJc-i)I{;P_oG}aBW-~5SBFB=wV;9X1;YU08erEb-+zPp|}8WC}+e~A@Q)hSlCG0'
    b'>c45Oi15p$%q6*>;N}llK32Op5FvVt2JElDRe*7QIIlOgGG5Bxu7b-W))uyJg~WJ%7=(+u|`NrHJwT+??zy9jO*-g?^MZMf$'
    b'%rrU0&!iCwjB;)r3T5eu`4HC*^TzJqqA(gi9-k-_|p5GKjAF}cAX$U)&er+eYhr*HE0i2-FOGJJ$BcDD@H59sk9W2aES;MyD'
    b'fLRh&D$|`1Nm;<4QlZ^@$|4S$x)W!djF<5>Io4M!UfPToZmFz`KvAU^eZP^97RI7d678|;X6ra{aivK5CPk8C>82~Gv2x1vY'
    b'X#s81^0h5^=V_8k=9kv6+#Y^_vk=XONu837ksFUL@j|Zf(Z$)aci1D>O2psN#s5#qFi^MUUnfIn)VRnll8a*Oc)!7C6JyL`o'
    b'RP=-|N3&=a-G)KNy~ZYct0ZU#j<nI>Fnb_$1}+BPthx&^z-XQ^GCpJ-GfH{f(413E%z2nY+fX{R8ui-T_oqO{QdB@IojKY3N'
    b'J3k`?y2)PUx=>AJdcB>|!}Pqxxp>pRv=z8Xn$4Gp45mo=yOapMm%^v9Ma4;1?GUo1<UHZu{Z+#l<0m0rpUIQzjkVI<4OzCt-'
    b'|!)pcN@t}`GZtMPLp#y49AAo(lrxDi?hEtbVla>oQw&d)6%MGgach1V7sCRzY<)W#J>hNaSqN<qdD{E~RiCAyQd3SBvWbNby'
    b'w4^~pYd$3+ZSx-fp(E)`$3dh4@9f#LV<0X!p;}yeF^*U$<8TuQ3-%JTEgAM2TtFIiAPKu<7g`n34SBIb}murN$!3QbisquP>'
    b')JXP#r?x}U{Frb&<lxHBeE5}#=r^#3w44W`W@rX}ms`>5y2e9(HaZ?dO{0S`IujiA#^-de(03X_vAKJQEWJgubZoxev8C7r@'
    b'Ese;rB?qw`6M`RZbW2)dWLg-Yf{ws*T)DYaK1b)o})R1cCP=ohD>RItldb`2GD5^hdP#nw8ZUy&>$E5jSuNEo;GHF6gMVe;4'
    b'gC$v1NY5FRns$Y;Z_`2Ap!2?Z!Q~92;O`_kjF@N;K)#_N+<EW3`c-=)RNI=)Y^$s90A+3X#<ehbuT&86-kWB1jDCGQ-yqYbj'
    b'1(gNGj5>l4|l*_*---q~we<}XTF!%D8vcs&V$qxu?^=9_XI>U0^`65rjDsltKz*HP2YK$R=5v?L~ejIX=X2JsAZH3GAh2!pW'
    b'@8h%lL)CQ9_Z7kA7<1$VmhejSW7IBD9w8cO+u+j$18ox1FUOM_Ix6<wuTDl>w*}L#2#{*Fh2&~6*;4Q);Y?$-|aKH3TxJ(S-'
    b'{Z?4Vpq>_AbOtV^XyUEudWkP*Ws9`cCM3W5ko=J0yG3yWt2>UHW)r;7y}iAAE;aIyZA~|SpH%a?MT~IV|5FV)w*Xj-7zjKQ6'
    b'qg87j7KLUPfTc2n+a-bhOM6L#J-ussWD*i$Y;=o;lfo|&QOcG-yPE-WjO{dhIh_9Km&}sQO_d>wn*;bUovKbuI^eKXhrS34@'
    b'>i&&f_)mZugSmKeB=Fb&#ZR-eCYm&kRYY&mdTXq$$`mE-It^Ipx9IMd_)Dy4O~P;e#{4v(PUd=daPCm0b6(Xuk!SqcDUG?RG'
    b')r^=BmH-Muv;+bh_gns=z=OHu+(XT^uqu0+Y2B6o;vd%m29n7`R`(aNE&v^)g7>Bkq>tuHQ`0sB!P<FSbXW;q9_R44)He>z0'
    b'PEaP5s2~7w!sfa(okZsCa5q?kzH}p|N&#L%lquN~9jqGc9Z&Tw@Tu^>`R7V;QR~FTk0}YW01<Y63OY)GsyTd)zm-3^PZd+$V'
    b'n@a?jWcpV!Ty;R;?e@D~uix_y`Ul=&|Ij<?A9=_9WACJY;+^(Sy|ezA=l8oE?=k+F^t)Z}3H}NC-JbUp|4e}+_MYLN5Lx8s_'
    b'-EGd9(iBEpS4_U)q$mLkd!m@)b?cLFJLx#RNP{CT0&#(hX>9B6R&%`3DNnFQ6Rn>+0~-|W===V&Q$(xW4n<Zd|WiUuC|~<M%'
    b'<{`z7)Vb|KTXIzPzY~)4s0Mj#OBJ)^}vM27F2y{)^sJ0U1XRx4QK9HvM9_AyOsk9_%|U3~n`zE+dS2CEF(Kafp=|NUgLS3)h'
    b'Q8)B@XB{&_KQ(}%!zrL7?xbq0kbONsLv+rDyIY0FtVJe$7d7!|29d(5pFjMdPDB=#RY!q+lfuq%fjWN5$#d4tvtQ&4C|O+w('
    b'F=6;Z+4dE4#t&DQDqP-YJi2PYN16F+6j7a7{$LhuA+Mr7jbV)IE#G9wUwoR#!2Bi)f$iSng0-a9?G<#TjRxaJdQ{{iS3KJE&'
    b'SCmTexhPQC4H|o1M|q(tvZ|q|9%6lLpsk1dT+W-EGZF!oB~_FQNynj8A`Xx5Awew`nj_9wL_xzGPy|kN{swliC4X)xf`X~G>'
    b'B6h|=+}$<Aw8eB;K#j38zP?yUnt1ms&URyHNHRvRl4YWkrYkoE}*8*O4xw`fE9w_17K#5T-S{FM9g^y^fA_(qjn{OzFs)|;#'
    b'+)Q*z0<5hNIUrxlvoAt<gG!snLL~c^^%~T15OGBZ6p<`)D|BdaAT{`>w0sV;Y!{#vs9_mreAf_dW!oK>xMZI~-j0ct`zX#v;'
    b'r9Y;`2?esqQT6<)mBRT&#?CjDC0_RZtbLtZ~~y`S)54y0EKBrJc@4vp#)Mx_(!-p*O^=^84KI&l9h&HD{41%(^`4r%}4&kR2'
    b'w@Tc!n&aB}x=<#C-ddwhfR*)`kGHJGI1$kb{^e7ABaJmZC>y<nuF0TDE%@WuJXa-?bDt|2j75Lg}PX>~44lHFj-O(V^K|<rT'
    b')w+<2T=u%ct1UTzfefoBX4*l=N_%dlb*;3o%(TwLO8dr4>)K_$u+n-~+ILoc2UglkE1zB8YpZ`#PMc-^yVX;xp@E%fwX}C?C'
    b';m&1#FNS79VNn#)jJKhf)?V(N<A*yd}3A(Znyz2D91hAfJZ&2D1Dycu5H!S81(2hk^C&b+^IQd6_%jk*@N=j5*~v17qkWV20'
    b'&f&0#sAXIXF07Kj6~B-)m;7E)9wp((KeAF;ADh-f_olmM|d0@_38@qYT6XrDN*)Dp%+YdTk8RBN>9VJ72ChwWM>lHl_xA%AE'
    b'(R(%_@hE%gpX7}(W*H=M7vpSD$}eF%WnJ#)ccT+HB9P3~=M&(W#Xo};sxigT3jMLu+)a&bWc8rG}fT?_T^Qt10|YmvTP<Yar'
    b'1CIwLX+E((!E_r&F#tL7(E8zdu+lrprMTZ|z^n7c<p<VE7cfse|ikq3morpLV7mF}kXlUmkFgB1BM4deug?PWyKAkW3tZ5yn'
    b'X`MtGe9#r(WgMg#`dBnl1l5;WHAVW`smUX~$(QoLp(!2qtMif*v|Xuj`tio5YN&2mU#c`kzjUFtDD2c)Z&KWWx<axog-ne?;'
    b'w>$Sx3wfR3)#|=cw0+mMj`K~DMkWzAo;zY(U<Ulw(2Z^(dZs63Tq<D7%n0k7vg7M`@r||^ofu+A<)`HdKSh-35h@!(%Y-Bj6'
    b'@&{={tWunU=5w%tHGmvQd7CtmGd`$v@c1Z=%a`p9o|j{kX`ZI8AIc3A8J_D%W;ZYE527$ZdpI^YF6N9s+43w5=$}V;Hi<_%g'
    b'BrkdM9<*T8pv)&<TOWKo70tf%UOJfY=5*PAazZKN^3TART-S88`SfYZ?a!2kBwvW#OaLwNUNjy=7WT<HwYwR^U0IPw_xblak'
    b't*an`xdi&<d#l-+dI!!L)a8}Ubhn}P#0Ic!nXfc@A<k^Nlo!o4wrIKMf6@i6T!do@Snk(awhRw1YUawcCGqFay-G;5%;+>{{'
    b't{7d8y!iM#s&RT0Qw0|#@5>CqgS2IR4Xv-wt*<YvuYK$5q4m|bz9i*JFc|^L2Fh4a6`+U;RRN3mP!%vnimC#aWnfMH7(J?zQ'
    b'^t|1!s~0N*isd!h%#k^A1CGdV{0mw62IG)7@JeFCgY~GxUyh2WZsAX!hc?*HO1B@T&3x|jYYvP;K9qq7guQQQ-#9DQGx<wLa'
    b'`1_;Ojcz^~N?_Z=w`V;uj2yQ~<IPB&~v?I#4aF21q|EU~J1T47+yL-apLHmJ^a)kUnEqUTBHKymHMxlu4;$Jd%jpyT|zG^p$'
    b'xP%nIG;$KvnFc`^Ju-ox~jyhr!m!uv(XJ48b@M3lEQ_wWBh*dXXzn>Jm=xb#joWLmpk!MDd@3OktVpt_wi7XQE~Pcl5n<k#8'
    b')hy3N^wB@fLjAH&E??e10?t|3VJYC{Bd8M-w8!10vA!o*K{qZESe#?)01R02gw)n@xf7ZsR)IO3mO1JPx$S_4^nOHDlVq2#V'
    b'ecdH@`T9qrS=~^|g74plcA?_Wt{Q^sJlfUZF9WkY@nttz9talWh#&I}aIpsV>XJlIDei$v5^L9U%G`!HnCjub)W8d6yHmb}L'
    b'k8!w?q>(Y?I3FdAH8)syC-B2QAle=3{ks#e0&13yRPFZN^~u{4_}Cl4+~!%LQYQT+oc{-sXYuVWt2J(r@GKSg9morBa!#yq}'
    b'-z5ozAV<ojo)6{)YY};icWBJu}H_7nfAu2T_vQ6wbg6H}ONGQXHW6Q8;gysX$6`DB&+mEf3i5OMj{{x-|$kMz4xNhDm0a765'
    b'!1qZX|nix09GH}NIJb1dXl6S+HgZDGnZaj{Ou-o`~au8G-~*nrXti*%lW4@s0d;77d6X*g54tb09ur9MyfWvFKN)!kR!@1xR'
    b'hKGcz`oug*k`csNyf-Gdqv^`^;P|`g$gpzH)o1b-EUAF2rRI;+#jp!iA0G0G&`}l@*d%HfkMi2h?S=U$<AB25~b3y;Hr{CM~'
    b'7sEPfWnTxp73*SOu31yYfYksZ#`85}^k8}XvR*-XhJ;b1!|$8ljvRDU$V%bkk@MEEU$V!e?s&g_@~{T=aSf_sT1ZuLbMwDbj'
    b'XOBD8t=3Ue1u+Ej>8&{bzf+uV^3C8gCTl-yS>J5xrmcLTjRH-8n5&o#aqOe0;qp6_Ll}a?KlmT-0dM(xyAX#7dH53bEY(#Mv'
    b'0%@C~;pO5oAlDZvpHp{B%*uYDlq@SmxUD44@0QN)}W*qrF0-tZVM9dIu-QofV;lzyJQH>+H61wGC@)Iyy9JdU}_d6bU}Ao`V'
    b'zh1Tw95?Gt;Z;rr{F$`5>QMYmV&?I(41N7Y8ZOO*qacTJ?>=1O-=pQ9tArstnjlOjMJU8nW)HT-?|646xq)15pxvpV_L)VZa'
    b'*IDlIgsJ~cb7&+t<XR;_}`z$^@EzjaRRK9yAZ>-vy#a881Vvf;oKe5|T#`ZoR1m!;Os&;pW>npW-YgUUvY){0MjyeU56!Au*'
    b'=DzWibtrEJ<<@GX9LX%@JpPHu^R$!)j%JWg#e;-CzpSy)M_Rd!ss&3w&TCs_6kuRg+91+&J>}+`2pa2o8H>K=QL?leQ_~iW^'
    b'R$FRDP}|L8g#PRj7>CCE2y-hs))Ia^~`gSA0wi99YX=&J8f~5az45+-n+Ci4|SrhAd&KM4KqsnGeuULCtg#l54`|~KhY+IgT'
    b'P=k<A|I)87~>852=T%7zyS9ij_sh?cS0FP>d&;<Tn-KyEY4?Ll!_W#I`Kmo(B=incn-^t`6ylq1e!MIq%k<nd&W-%)9z1GwU'
    b'nnY7sm8tQw#$qf13xLeC1pe^%zimz)UG0@Yg#MY4EXzPOa{)R=#l8}d+sz(48Q_<8In#il%zSd%Z_R8Oqy>Dc(2>xu1p%zXJ'
    b'(nicbCTANQLihRApaz1nb7RO<{IkUo$B^;Y2EGKzbZ0aVbikzpXR?fxNoC_=GnKj`=x%GNfUz+JMw_l%oXLga_4<@DjiygHu'
    b'tlBr^-kDw19vB-E#ha_iM+u1E=^hv}@V8C)zcD_tw1&1GJx7Ga2CXf04YwNc8#wOpBZHL+6Jx_Z36D+vzHEERLKA0f6+PynM'
    b'XFi$iKXZh9!uRCRf&-#ST|ii_f`1X+|tpDi`Ct_dUkqXc6D39b^W}!Sa0m<+3BI#)2@HW@07DFog4a&60l9P(2i-iQqP^6bn'
    b'2iovu^06pt9(n0T*q`EV$sqQuEofbpN!nls&C0MN$jww7Z8#p<vZF0y^Ciq;t|E2y`RLg0+&*nVyp)t4|ZbjLXLIbq^d<@)7'
    b'q#BHm9q>9I_DR!Ms1Z%l%X`%;Elqo+L0*-Ln4!<VY4<-#s#sjC06l<=d!A>n&H1e4YGdYZmA;aWlX;$J4*(D3!)_?8#J;o`~'
    b'y=Qs13l*jSnO6&1t^VMTRUD#B9f+VeEeKA?eAa(e|Sg%hF#UjY@i>VU~8@3Q9@YgcZnSx|F&&>{LWoO{zF%bOqhSX>N<)yrK'
    b'oKD4Wo&u3SPm`%ASACeZqG`>I@^w|*<VG)^m-MKhCt8X8;<-(gZ;a<3G?)=Z;xGMBc%{a@q}RadYQlV1pZ<-%J^d>5vqE%L{'
    b'4_fX$Bv8*_+oBw%`>TP48MR53jQLsd-uXP*K*Eb++Hsh7XG)!Ic0Xbw9?G}+DYH}8<XN}2N~mR2OYo5b`bLA((KGjgB9|N@+'
    b'>u$>GxJPBjMFN3Nm{py)xvGc$v>d;$m)WH?oa2?c)x<uBN^AH>YKx^a1xev|#y${pIzxw5!zY&ku%lER@K^Gr5MsNk-5&8np'
    b'1IQY%jUH0LYDq>u`Pp#=^NxN+A2%$Q19*eP#)Yhd1*FU+vHsVxy0I{N0+4`GDr$e8wFp*P*qerWEy+2vfWj#}Xg(m|dC6g!K'
    b'5y)%n{ZO#{4faAL{Gv{hSf}4<~jXelhE3L1ILFZTMpu_jpTV0Fd|1{z)VyNS9ZI)QYH4Cj~c_JHu3s_;&(-2PCTfMEwViE0@'
    b')Aa$YREJV@np9Y!JL_x_lMV2613cdVkA{Wy+Kmm4OR$TJTXbuxV5UnD*=m0XEzY3PA-TymtOTJzFLJ$<%_i+sEZU1*71yp1C'
    b'XVQ>SK6<xpa4>dvJ_*e7Q^rjBWWrtTDD3&Py2~!o2`4G8s<02!caG^s5Z_=<A2u(S5imCD}W1yFKSWV9JSl++=D*>{xLjhaS'
    b'ht*2is9zX=^~M-8?nFq`8@;O~Wt}!MOr!Gsq`;BFl^`dx|1+>LCL<oyH-#oPs28Gt`={diK9DD#?vX;71>`4a$WLO3<G)w9m'
    b'B|QcYf|U?p2bcMGz?Mh0wPBJ_UoXT#W^Ch<~RFi=G3)n}g_AD$hBQ_}i19&#aT6?HcAk2)P2brB`+Y7jdee`X^pLqQX73U~*'
    b'l0xz>r>8%);y;DCtnUpd8n-bzBsiIr$TV{*6+Lg2E!Re&bYb4L(3T6<@{MoFE`J{q*+L;~KVLq+W{9t+(`c=$l70hne4}zL@'
    b'KCfVQf|J?VnT=V%nP2VE+2L$9JE~$<d(=A)r_&nDYL7bIX*cLrF`w07`e$b~WmbFC>zsB^>Sjl^OA{dcqz<)al>Axmv|EE(?'
    b'bOL(Fsp4q%{a~c$<bjAYRy1R&w2-?X>ysy)5kT-Xxj15s+dn|Fej6fZWZ%s4dz*JP*Y~LOJV0^+U-;^t3C2hj{MG1Wx3aMDD'
    b'cn99RkF<{+u43_9}=q-8nlxJ(^Y!YkG5fI6bIVUG2;y=pA=URnPq_O_RC-noPq^9d2Eh&U(Rd9d5N(;q++c*Wy+?7WfATCqW'
    b'%<wQpx(=io4`+aRlbn@-WgV;yd_Z^7~8Y!=kvR{Q4rryI(x_U+{K=+K|k;a2;0ba*g1IW5s4N{X6MJDQz^HJDFo#$nc*&Z?L'
    b'-Bi0Fzf}nan)zp4GJDHtSF>7W_XV#tds+cvkAB0EYaTT*l(dqH@bm~_zYu15334@y2zpC!2r>E1igDU3N)pM_Rd~|$P#r&p%'
    b'c`!K$4?9)N7u8du+dJ%)DF4oXQpf5XbPo@Y4$7!s*D@R@-6sbfHB8FkGg=b-N-FsOmU{Y9B9mW9s=wFLBWaqJy&VcqzneUe5'
    b'+9AxK%b_|l9$6cybd!9BA990t!c&KA$oGE_k3E^Rw=P&VK_x!!DdCM`}^;tc`xx4Bkvk@WM&IS<FzbdWi%cO_?aB7n+<W-`G'
    b'=?ou3nPMQ<;H^3~M|v*2rO|2SyXe@GWS%C9FdM_*w~B${v9|Ip&6kIuMf(gw7v@{H41mJY>2C8Ag@t``z(ci*BkWL2+5Vkze'
    b'>~T<gjdb#m>2GBncjIs_2G04EQ8_UmuB$np{jA4ZO$gkL+`@tNn0$g0C^pzzUR1jZal_kM<REOdyM4$Tr#WE?qTv?of2sgfN'
    b't_z7bmd<9~rypjp=bA?|Fsc#uN38K+kn<UhPvb@^D$fSW97=u`etK7~yj#ngYl6#uCLPaDxgAT%%<$&nqG&Ycg^t1<+;aeM>'
    b'G3;qZV{~+c5hK|pW_Ej9HwZeEf?21yHSi)>vGK4on5HZ8-c%Y1SgJG&V5vGQ^4TQX+G<7GgW1|hz*6@2Baa;4Mz+@f^Xnq>8'
    b'*2x{d2wdxuPm?eS7x~Vm5yl1O)*;bC_h`uF_YI|)CW$hiE+rz!%;l$qn91>5S)vM-jIM^*2vFlbbCv!ynkQx4Ktv#G39%YzO'
    b'h5CR_w*5^W#;~&KG27wht-pKzI%jodRmEKU_s|*zD|49UR+O)RHO(Rz$X9ROq#>?q>=y^gw*_UQfG%algS1^gNo&{S=O)DaK'
    b'zmJtvW8T=v?gD#37It*%Yxw^z_;N<|X0C-|Jae<cqJ;U%8@0b3ri@;%3J@Qi+%ln>DmCRC`MzK7`a-SXsVEOm5wHwwqNB<iM'
    b'sa7&DJivhRW1jl8A+@O(fe~eJEUaJ<3bbAtLPx5&<NT4B^n9${IqCn4!Nvk_n_fkeEv;a@G=jxm@w>$grYbiItHuwwMo%7tk'
    b'7M?mwqmZW1spO`?ne%>wo@HP#8gZIZq|F3QMfCm_B|%2sd%3iRtiXY_rJZkv8wt-2ujtxSISD<dFvo=Y@5|8WbY}SKMsH?#&'
    b'A3cLb~#Ek0jR`)=`9@>!n{EDcI0H7{oZA6d8G?yc2jh_Rb*)lz4?6^LJ!d4nhp+LK0IA-ue+8*X^l}=-HNKCf?e^I20m}5ZF'
    b'yhlrENX=MZjd0<gRi&IcUYZ#x&1)?GC~(V3O_cvt29t-zA5UpNSwrygQO#<g&xLfZjD>pvlw!Rf-V@8h)W2aO2#^U;vJcA&n'
    b'J4ZDY?WKUdolEOO8A3c2^_*HL%>Y<y3Ki<9VfE51()*jI(?J9ca+Ry@_J3-@|>GaHRsq8!+{VBM|j&(a*v4n?PRjb*RJ#TZj'
    b'p9=PhfT31~9sY4a}*Hz4xSwP9hy>X~&N42IuT=X6;_I}guaxH6qiDO{co0@!}@I8HQv_RcuE64Qi*j3(`ErjxkUSauYwv|a5'
    b'x`PVh7ujY~;N|vaV$@JUI}Pr7VaaK5m$`z_wZ^=(2GyB^_S6OmALnSeTG^RwBJ+YjFE%;%>lbT#iz=6BJj0lg2AKDx2a~RKt'
    b'Xug7S8jw(P-X0)SNZP2o-T_I9Y+}^!=gk-(s*{A7c_p*VA>DRoj+{ToSumtBNYBKF$O{qFQG!*Ib7YOOosva?6XIg9#BBI#V'
    b'o8GEnNDpI{*$XT&uiNaLP)86Gd_np(lrh5I>}+P3c=RmA*A)6qZ!Pe-em^W=ID1<B@(i3VBb+)_U{bwuJ8Jg8irD?(W*9x&K'
    b'FH5K$EGoFPRo@RkGa?Q_u(e)iDhy*wp~+uOQ{bbGs3Sch)^?!<yK_@@Nsa2un60=M)&RGd<t4&ih_CtIgJ{hiR?fc{SDZ%BV'
    b'T^tVfYd-V5!{vOibBl>$xe<{$*=_&m^!@p;g_l)wM0cLl4j3opoRLdFVI-^`?l<Q37qFT;;!l$~<sBLEfK@RCJ)q8e8zmMtf'
    b'3H?2#zi0H<A3yS<RC#%yWnuV7*jzcH&raWI;SCOT9X|HeM*tr~^?{C3n(6~R!MN(910Nmr(S?t$`sl$&Pkmsho&)u92p@;);'
    b'|M;E)W<P=9IKBL_&8A?r|@yAKF;9dOn#hU3w-r~EeO>I7EQXOL~;0tL<$fdK7LbZSm`_UftAMU11p`Y53Dp%A6QzdK9GnDRS'
    b')(wQz=N%Tzz1rg+fCbEY$}#^u0pE0<KjGwhdzvO?nD)q96wfaw;J~S3%AsWO}3^zQR!iJuyfA#2on(bL3CVkv}m<{)9*VdUB'
    b'@5{d&TqemybA{KOpd6LZW@%rQSP$Na<`^AkPh-DwY-_*i{ltDeY@18mhZ^?|eDx%$B7e62pPIp3%cY|abyfzA25`oQLVt3I$'
    b'f->DC5&P(-ywESLuVB21)4{X~H>H`CRzLp<{*wDA?0~`9Inw&`TpHvFc^Jn#Oz*8MQux%6hL31lmDcI1d`Z(hi(VhBO=}e_y'
    b'J*3LH?DQrbUMoQBF0U0x>G4{DJTqP^onF}GwbGp)b&R!gGUK%ZDSEBI$1$%JC|a)-D6Pk91<+26wQ_W7td*k)uN5c&(LSgG^'
    b'umD@%yT32Ofb(Ms(@*ENgajdC3Q4YkVCbAI+Z2WwU-oB!?{k;Uqg&n-V|00Fjpc}_{GHo5hfRZ`oXdYowX;BItJ2zG}2|}zt'
    b'>2o3h4pJmk$_;{FN!31EZk->ViZ}IfZm+APqGVdTLTA#|FyMKtaZbkpcthy+#VI!r&bv^$et6HPV%@8+~G+v^9#^FW*4PGzz'
    b'JoA^)K8CI;R&3feO(2%PoW^J#dlX%hQ#m~h()I}8xJTX@I-u^Dx`)0qZIauVhNgAXKTM;CvnKpIPId)?62<sIv$K*FS3O6aJ'
    b'Bqf&yZ5M40Igg`W^$AF%yPG{~})%ho-1XZ0sDJ972dOe-vM7F@7=!r=jU8yfIXS!71Y6p>MVwH+B#0hMyGX?6M7zuzoW!yl4'
    b'7*|rEgAT40=tQ*eM3<*<aA2lJNl@#AL2}`T-L3-lWJ$E?nv?330di7J7;vhFbjlz#nB4;g9jdxcG{~Yx_o$T6QHAIPHQ%~im'
    b'89IU!qv@^v&UzEnmx45g*CG$=#~=H><Nxa3EV&3nG-dk&UDjMYq7$aH5+DF;-yu4ugf5{(kSDl)j=BLOKWNnF)ppCVU(q6q+'
    b'pYmR=a5KUzr<2k7~TKitimUNKuf=y0WGab^FSkLfERvoI(Ku<P@4QKuw{Z0kT#E6(3o&?@bt_=+)D8Dtb*j43ZNfWPqa93~_'
    b'$5I!!FqZ&s&?8GC27ik8VcJL8E0@fsxVDz-8bTNT?G0|nyoA%1IaO%h_X=GMR-;`W|c#m~+dBuG8$GC+}!n5@Lw84iya#DyO'
    b'#kQ~+-4t;72>#WBBS=TWG)U2YRN-M_=ErQf&6*OV0L3YPyr35a!oI_0pqbRla9O=f}d(Tm~l%Vz=qgtL(y2D~d%?=r$=E_u;'
    b'XKyjngHnRpVx}jh1U{g23)B{Kpi4iox@>e~qI!ljUf7*IVUQYT!pf{UbacibH9rgxXFO3Lp6<lp=2nd)A4-swop{jPs)Xc23'
    b'9_r2a)KhWk)5$-3dGBZMB>8Yf2cu)UGIcJ+`0n=Qj;H7^3t6AXAF>&KVSg!TBjOhQFt1Z64a!d9+wi-_)h&&g4#Tebvx8bIx'
    b'#?2wbaq2wJuH!kWHs4C-C$?WRRTxI<wkPk7o>0tP+W2OKWP9ocP{qG+ncNu;vck$$YTZ3SGH;uo`-zL27c)^~(qAkfQsL4@S'
    b'EH3mb859YS=CbZzieGsL;ET1DI7jXCf$2B>mqa@<&p`9Nn=V@fv$H+IW)S=@467E=@_!yner(*4FC*0iPzr9Z6Eq5F|PjQ$b'
    b'd^VpcyL5BgNcflb8cv^=VWbEm|0fSkD(50~m(PI#c5W2jQ2+=7eun2KjN?;LU$_ZkM2D&;HA#~=F2yt9WU=hMECGZZdbFc`Z'
    b'+iZ#u#7921MF?GLNrb>QKe0s!tmvtYb*eyR5h7q*79ki{ix578<WxA(<tZE-m}j;K;WLPN+>Qdty)wWV``nyVrwovjYQg{(A'
    b'p!<5i``=oix9^eWQ!1or34lsbOMVITvb_w@EKRm9t~g-BGe#Tgy@tKScEt%C2;?AXIO+d)lE~a#R|W&MF=eMYg>fqFo;EnkU'
    b'=a$95aYTh=4&XLg=zsgy<=d79p^#Z)_1FWDtuGhYVs7qQ@XE>r{c%6vB1<!kj_@1LPE%F@QygP=jm{;*=9ugzy=}B809&(aW'
    b'erP6%Cvq844-zObl8SG0e(MF_<D))pa78N`ynu>xrk0vFSFwg@q05NDhykQO1ZsV{9#y2Bvm@^RDt-WDNF86-$eEbaHU2ywz'
    b'779kE9#DyOzkQ`RxfnOQJN*whov#w(XFq`jdkg=mqj~L7%M4&--$HP(rmtD@mB7{z05kj}MBtmpb2`obBYMF=XGDyrQVya%*'
    b'B7`olBtrB`2`oY!mlF7Z(k);SLYH0=A#?(Z5ZJgM?9LuDh#7ChdTomkrwn2dLW4Ntu>xrk0tfqzEkYb>kj<obIDuLDV-2#a3'
    b'OIp92%Vj=LIvUmQSEQ-N_++}R!@P{Oh=p_&6$4206EhG1~6lFqCvL2FexRlyl_-XV0q!Jl)&=BDJQVJV1R6SL3e`X1p{Qu3%'
    b'WZz*AEyZ=eq6&%L^fcn7i#Th~)(={3lyppw}f^Ug$80<%L5AvAm!`%yl0#NbR2VhWv{y9QX{9V~qRoKW*Wl#~?LCMBRVd;(-'
    b'RSc%U<~cyP=hZfM9LHS<W=`<E>XOc}(oK!-sr3rrX!%R2A_^$OHAdo?@h*l<bR*`#a34;8$Gf2_*q_p2kj{GJW(D13h1I<fH'
    b'&Z8+oeE7qBfe{91Qe&E}1IYkc6IwiPO+O$`fs%SFp+Np{r(^8p=CIQoYX1Dp&hI5-04TBO5eH+gB6C1Aj$YZ47R%v0cE>-n0'
    b'?AocSk721y?xPyVpfoOl4OeYGRrtQ$$I!RoJnj=4eyZ@#Y&f^`)P}2eo~rrd+wGkBHoW5~GoPSh=3|<%nASI!<X@-WAjh`$k'
    b'^W4cg^S|KN=lRlW!28d5w6F~noxBtP08|z@!Wb#pi-Tc>QLQjE!6Z`J6$vZH5Mrb%U(xmeTEpPSr|s_RrF<_8@m2s%X#26F4'
    b'F=(DtSru--Nn@LfYU+9pEM#Ybe{EQj2BAm7%mH6)YJM+ivGHNLY<U?UpFB*?`F$infKdrU2B<k}+8OGEOIcOg2f<7(`|^4W8'
    b'`bYm?R53)0F&N2!^lkIZO0bs${Z-VNPbWH+{UDK!p8DGPH_&D`tSD%^4wke*?=IUBoRczp=hvncW7_-0jyfYw;+d^r&$Dy!l'
    b'#*$4JCuxMR11zJ1q1{6}qWjlzE>HgX&PX$;@*}9ya7E8&j0gZ{oa#iZSIAiRoT`~ZYvr^7yR5lIBdg|3o*ChfZN$qf9kc7(S'
    b'_wOglD5lV0sP3m^BvQua?>5>kTuVgD`iQ#rxJc(sp~k+KN-QTziZFvQm3pT)3pCYHdZ$v2)WOp0It&z<e-~Z)s4AXgd~SkX!'
    b'Lp0P7uc5XQH2*0))kmGW*hEzS+zeMb_VI^N-=nlY73Tp7>-h**-yQc#!Jv%Vf)m@nDq8`l_v2`BTYgw=eTYt?>r6ow;_XpOA'
    b'9raCc~7h)TFZS&rxx;UURw7UD4dcO9$K@4tWI~kN7idNh64u^-YIeYOFFvbkb}p>PPUXGp25w<FEb-fA%$oht%#f-95BL=;v'
    b'!Mr81-XG9xZCx^tP)+KY!2NyML|@55OJ>?Dr3P_9RX)~{6Tr@|X0UWn1x)3G-iyZsF)BGntkW2KH9%6af+xp=djh>#hTjY2L'
    b'I9T}b`M(wUqUU1V?SR)S1X)iV}*t&&vn4a;_J*k`C%!jFaE)K=MoK<R_)SrMv$%c;x6?gz<;=tqWCSBvw@QWYOJL@9NAj{MF'
    b')V4a{NuR-_&-A3vVA4xPe>rkU1sFX!aAGxjq*H{Wu-3bvfnPt$#hR7I2~(acB~*g;?c*WLvNWS}N|b~Zul2T4F?iCHcVN-Y9'
    b'Z%Q-)KR}slrJRZO+$g|-lLO=c(5YX#=K<{4lA3mHUuYA1gz%v*0cv_u&_JBHMWp8KiJBmE%1gl1$jl=k3aY~`8URN2QEt+24'
    b'P*MyD(=@7i><r@33TLos~D{OX$li#qdI8i;1>_vJ|3lUA<)3xVf$Fy2O^TiRiB-5|p;rXX$bhhYfW5mWicIO^|1C%|drfRHL'
    b'4Mm+DD3)H5+^Nw?K9sn+q}PxVDj>DqOu*Nal`LmhwAP)vuTVyq2_n6v}Yj0(3ra8qd=Y42;8<Ja08Xa=b5DR<HuH`6<v0;w>'
    b'jr70SvBSEmS<P~^@=czDflk^n!EZALXup)Ukqku}k1|_>FiyW%<vF3TWN_S85LP>j>2uod~#%g8C;6hqpDFziz{o*{`Ko=>J'
    b'$>X-4-3d+G;sM8yp35xg4iSARmg(*1A(|2|7yS(WPJs6P>?885`**H^$}Yri%}2Dw-C754I?Ti_mJflq6inA%Zn+L9`5nOX8'
    b'1_fn==Vsd+{@b(1TCDtU<U^FlKUD&M`dh-M`#L1Xahv8t45;j%X6pIEY6*VpEp3t>^s1?3yat~mej2CZN;SdsgL%{OV|%8wM'
    b'KB5VCYIL#!(g2F!_6ekK;-(2IR+7Li;sWVp+zDlT`J(nR+?;G>qD4@9p{UBV80|+hxf!7R!}-vFCdknnYP}a$e$Lfh$$x3LE'
    b'!5oI3r`ql(30?-4KDPd!>)uM(1skdVl5#zV4WLej+d@5ge_$<(Z1Kr<p|DT9)UtEPv<gHtSnF;x<^?mEp&<l-_GVT8d}6B)u'
    b'}Ie)dla{QPKgwL~nha5#p+qYIakhZ>R!lDPyygveFRZtLqu9@}#j;pL#PCOVj!k6~Ul27!ZCMfSMEW4Z;Mjv<~cD9&(o(`H0'
    b'TYEV;x6nx=LzJy{4je%Kdsb+c;{JV{UztLPfp^Ccxo~Rh;?%UcDzjOyX4oiTwv+{Nk*aGN5Ocgh2%c^%cIYc@qbw>b&ExL@X'
    b'gGOT#ANgi!V$2tH1}{t`i;pFgGffIOtt+9l1EJOSaOPHL#0n0ak+xt5(20}@}qsrq8W1%<0;rlTseq}(a&oz-vHP~TxM|%-j'
    b'7*qqtSZr(@)x4s#(+=mHCj}C-kdHdhPUUZeq$AM8PFH#E^IK;wDCO0<n#U#;FYJ&ZEeJinRx6J1o8erVxD=_365dT9Wx6(Av'
    b'PbE!oNqOPrFyt@Af$JgBBajZXzi6EM7PPqRk`^~s`=32TyYTVWwO4;4n4k{H+%OWYB*ntdS>ntj%5qhG_=J#KKiHm(o+B!Sa'
    b's5(4uq_7bPAGfHqYZJ4)8gLp`VrE5<`0@2tsi!Sj7O-!_g+(35CG!SgVnZJySe#v{oyCnUP$l8%^wM|&pd&O}Y8+g$Y^Hdqv'
    b'vZ$x7No`j}HHP2BIS83889I(_^W3zV5Y?EZrI?Wt18Ez;GZ!MR%Mwp=JFmmb@LD!|6Tc(w7vK=dyr|l&w5D0;{fD^Gs3lCb-'
    b'GQ4AQ!i>se>&yHB0u3&9rL_Mgx3dPZCXyB?h*9uxn4e*ba9k>ksJ<VJFFGF?cv%q6p9#+er<j>^7m(*{j>4vz&l*u>d#~I<8'
    b'*y%Ccy7M#x3{Wqo@swI#Av%h+WL;dn#@g=*hr%Cy+xxPD^B`AN@LlYM?T#32PvFr+6NwL$tR~(I1b#!`LU@Wy4KID$gH%HW;'
    b'^$dtK&c>-S8r7>00GuXwpgG|P+B`6lA`b%HS&p?mwS@p<E6^SpoW_F~$?|6jB(rVnrFuiJP?)LnJF-n#Md-Xl+(cdoZ*k0G?'
    b'I!#;fE4bttc9EzdSK|wad_Z?=Y9yO_IsG$jULj9t`bE>$7U{P~#?LMlGYg#|9*tnWuRf5+_xHIXun(3%JZl$B%*!7l4RP@u)'
    b'0kC6O^7Cfy))!8QSa{yXpY~OoIy{j&<<W=i%dij2CVvw9$vcpl*7n)IVJoXS5S5FN4H*Bu(~)^#zhqRA{%fAA$<_tEv$o<6m'
    b'1W}G9S)P4jRE*>F{{`C#>GW*^x3_?-T&{ue_gG|E_%bb*L6xGf$Ua!AOOaT&_3I$yYjB%B&%s^qNOx`XEdJeN~EZ92A^(vK='
    b'Q;}*<jQ2S>=sP&-oqqEpZz3$;F+zdEq0i+JVIN^0mp6?3CSw*g^-7@P0;jo78iDCP?iYQ-eE{dCr|RKAm4c??k4-4l))S$%q'
    b'`Ded?MN4L4EKyBi(Z3m|Y7uDt2?@^BvI5&C!PSG#_DE4>2o6=80)-FU}9ZzRS#yqQnZxT%&+&i=Mi;cx4%eoInUJ2T@ecG~='
    b'2CC~Au{bT%erN}GD=GQ(uq?H>nIo0yD{es6{v_kD*%NC7`L93Oixt9#_{eiDz@M;v!qC{L{V@soC8J2bv4<GWfR+q9N{Zvl-'
    b'J<r)EszUm{>E%$Bg+=s$S7>!V$GcFw81;-P%Mj;tGgNN0GVj3K_ON12sKi6!(f#iJ{ytjQ7yHAW+c%5reN1t=E>4!#b=PpB?'
    b'TOz0hko4N=1K(vet&XZX)EQb6e}bra_uebKurR58~f;23WYEk{R*tX<Nbe)l|M7LvV{nrN`-t3J<@Me&O>npkI}^*7tM}vL5'
    b'N<j`011)M97)<dGGetKt^R0iM8J?@d=`BAp7L7#_)=1=0`ECmHqvTq*3!9`!?glm0w5)mw~^`(a#^=Z-}c6^=_$~=t1>r46s'
    b'^z0BGw8$v-4=8+Y0c{M0MNRnJ}$j(B4mj}SUmIxf6RpdcLJZEMX1<yT>ICDQwKnLPQ3hwTMU3fTa>-V+y@FX>*AFka@D{RfW'
    b'&jBg9%_)%brD^eT~KfqT{=Rpe_*m~eJh-&8j<^xX<7hNoiOT`c;;LuM+r&@&`ah@RmOeDK0#Q`-x3Y<;LiMRm3*Y<V<+u^nA'
    b'Igxy!9tth`F8K>#r6!@7d4jJWCAyBw>O0$L`ZCmxz6$rRrSW$@WZmyR`m0FHv@CbHO(!27!D@F11(l<Lkb9GiR8VqPUBHM(+'
    b'tOJ(^XE}~)6Zz5NyO$-tfhiMR(CQp5^yGwep>>W0=tQ1qUHU?u-iLzZSTajBh-9b&g!23uUEofH9Gx|Y82h=h;7<#Ocn;|Wd'
    b'ib!UYfOyYQj&OR4w$ORMRkI8}EFqgsvLWwa~Swz6E8jVj2oy8j8<Ru2<R(g{jhm3HVfsQzA}B`Pi+!;PPoJYCHg5CnxDI)UP'
    b'L$&6w=Bw*)aB5=$3?qAV&2BunO#aQLJmBbEb+;-QA#n$?=1yxTcYAW!U1!nhxS*a>1Ao~So{;DF%^pDJ&V^7Jy7@@rv=S9E2'
    b'SFj4`16Bf-8aN(ZGh06~eoDP27M-SaN9#`pbW<BxIa;vzy3*mHyF2g|uOeA~@0n<EK@s^%g(WdjQ|2F&uZ%rGdQbi0zLzGC@'
    b'j#G4=E1T_@o6rZ~SE&_)X-IBXvyysau*@<b=?FdU%)TuAOGi0I%-T}Z5(ja2Zu#>|@qjS)?vO)bmQiH*Fxeczr|%W*X#_l&e'
    b'}j8Nnfm~|#)mX}mkaX1M3Tr2VA6dYiQK_iG*WBKzpUXJfD7<1K_cY$Y30MzXw2AMkM*P>O3m%@rZ@#O`JV#+4PC_>O~tJ*a4'
    b'<6Tw2V4<O;Q(GEMO`<y^7)~NSW(_vs9u6Frsp<Ow)kGa5x{IzIoHdSfGA^e5y)l2LyUaJ8!eD2y8(bL#q7I{@`QJeulqztKS'
    b'eaOgWiGVj>xTUq(n(Bvrfd(tn2v8d=!5NtapULpaINL-Q3qL}BDY^vsBDB}NP6bCWW7;2UGMyE@0^({x#-FgXl0#kJepy{5I'
    b'E619}H{PQymsq@Z<vDv5X9OfPk@z20k=wNFpo(hi(SX`535rSY96&MN&c=1%M(I!QIvytd=2zCK+-HF%8T3rjU*WRMME0}Ba'
    b'5?L;-OI!@E#5_?=`nEth7yxU$@S?{$nX*h(%9=KxOV}#WIVn`rcRKCvS-Xn|4hFRE&r|v9t?WqOZjmf<AvO%??n^&=M^PZWA'
    b'Lu>-dj$N%o9A!ezWDy@H~)U~^qc1|ANQBuzyD1OroW?yvI5z;HDpumd6v%KW?|T0qz3Stw=cgV=p6_ri>}9fifZ@wNLT^}Ow'
    b'R%we<RA?zY37p3U}R<4ctL*XsLVyd>JD5keAx|?I;`%3q%u(QE(FdG-Qan<mfL8hLIoOewZLY5@q#f;V1pyyvIp!1q1M-CrZ'
    b'K{r2n(`=KUr8`Zo+xaYaAxdEY@v^z&b9eX9jrynMx4H*s<OA92P@&mb=Z00'
)


if __name__ == "__main__":
    raise SystemExit(main())
