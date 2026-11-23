"""Genera un archivio locale degli export di exportChatGPTFullPagination-v5.js."""

import argparse
import asyncio
import codecs
from collections import Counter, defaultdict
from contextlib import aclosing
from dataclasses import dataclass
from difflib import SequenceMatcher
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
    if not identifier and not title:
        return
    key = file_key(identifier) if identifier else f"nome:{title}"
    previous = refs.get(key, {})
    title = title or (previous["name"] if previous and not previous.get("generic") else "")
    mime = string(data.get("mime_type") or data.get("mimeType") or previous.get("mime"))
    image = image or previous.get("image", False) or mime.startswith("image/")
    refs[key] = {
        "key": key, "id": identifier, "name": title or ("Immagine" if image else "Allegato"),
        "image": image, "generic": not title, "mime": mime,
    }


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
        refs = {}

        for part in array(content.get("parts")):
            if isinstance(part, str):
                text_parts.append(part)
                continue
            if not isinstance(part, dict):
                continue
            text = string(part.get("text"))
            if text:
                text_parts.append(text)
            pointer = part.get("asset_pointer") or part.get("file_id")
            if pointer:
                asset = file_id(pointer, allow_bare=not part.get("asset_pointer"))
                add_file_ref(refs, part, asset, part.get("content_type") == "image_asset_pointer")

        body = "\n".join(text_parts) or string(content.get("text"))
        if in_path and body and content.get("content_type") in ("code", "execution_output"):
            fence = "`" * max(3, 1 + max((len(run) for run in re.findall(r"`+", body)), default=0))
            body = f"{fence}\n{body}\n{fence}"

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
            refs[path] = {"key": path, "name": path.rsplit("/", 1)[-1], "inline": True}
        app_data = obj(obj(metadata.get("model_dil_v2")).get("appData"))
        for component in obj(obj(app_data.get("opGenui")).get("componentResults")).values():
            path = sandbox_path(obj(obj(component).get("state")).get("url"))
            if path and path not in refs:
                refs[path] = {"key": path, "name": path.rsplit("/", 1)[-1]}

        for canvas_id, block in obj(metadata.get("writing_blocks")).items():
            if obj(block).get("content"):
                key = f"Canvas_{canvas_id}.md"
                refs[key] = {"key": key, "name": "Canvas.md", "match_name": key}

        for key, ref in refs.items():
            previous = combined_refs.get(key)
            if previous is None or (previous.get("generic") and not ref.get("generic")):
                combined_refs[key] = ref
        if in_path and body.strip():
            messages.append({"role": obj(message["author"])["role"], "text": body})
    if not messages and not obj(raw.get("mapping")) and fallback.strip():
        messages.append({"role": "transcript", "text": fallback})
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
        raise ValueError("Nessun export v5 riconosciuto nella cartella di origine.")

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
            "refs": [{"key": ref["key"], "name": ref["name"]} for ref in refs.values()],
        })
        search = "\n".join(
            [row["title"], *(message["text"] for message in messages)]
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
  <meta http-equiv="Content-Security-Policy" content="default-src 'none'; script-src 'self' file:; style-src 'self' file:; img-src 'self' file:; media-src 'self' file:; frame-src 'self' file:; base-uri 'none'; form-action 'none'">
  <title>Archivio chat</title>
  <link rel="stylesheet" href="stile.css">
  <script defer src="indice.js"></script>
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
.prose table { border-collapse: collapse; width: 100%; background: var(--surface); }
.prose th, .prose td { border: 1px solid var(--edge); text-align: left; padding: 7px 10px; }
.table-scroll { margin: 1em 0; overflow: auto; }
.file, .missing { display: inline-block; margin: 8px 9px 0 0; padding: 5px 9px; border-radius: 6px; border: 1px solid var(--edge); background: var(--surface); font-size: .86rem; overflow-wrap: anywhere; }
.missing { color: var(--muted); }
.in-line-missing { color: var(--muted); }
.message-image { display: block; max-width: 100%; max-height: 560px; width: auto; height: auto; margin-top: 9px; border-radius: 7px; object-fit: contain; }
.attachments { margin-top: 24px; border-top: 1px solid var(--edge); padding-top: 14px; }
.attachments h2 { margin: 0 0 8px; font-size: 1rem; }
.attachment-list { display: flex; flex-wrap: wrap; gap: 8px; list-style: none; margin: 0; padding: 0; }
.attachment-list li { min-width: 0; max-width: 100%; }
.attachment-list .file, .attachment-list .missing { margin: 0; max-width: 100%; }
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

function inline(parent, text, files) {
  // Precedenza a codice e link. L'HTML sorgente rimane testo.
  const token = /(\x60+)(.+?)\1|(!?)\[([^\]\n]+)\]\(((?:[^()\s]|\([^()\s]*\))+)\)|(\*\*|__|~~)([^\n]+?)\6|(\*|_)([^\n]+?)\8|\\([\\\x60*_{}\[\]()#+.!>|~-])/g;
  let offset = 0;
  for (const hit of text.matchAll(token)) {
    parent.append(text.slice(offset, hit.index));
    if (hit[1]) {
      parent.append(make("code", hit[2]));
    } else if (hit[5]) {
      const [, , , marker, label, href] = hit;
      if (href.startsWith("sandbox:/mnt/data/")) {
        let path = href.slice("sandbox:".length);
        try { path = decodeURIComponent(path); } catch { /* mantiene il percorso originale */ }
        const file = Object.hasOwn(files, path) ? files[path] : null;
        if (file) {
          parent.append(marker && isImage(file) ? linkedImage(file, label) : fileLink(file, label, true));
        } else {
          parent.append(make("span", label + " (file non disponibile)", "in-line-missing"));
        }
      } else {
        parent.append(externalLink(href, label) || label);
      }
    } else if (hit[6]) {
      parent.append(make(hit[6] === "~~" ? "del" : "strong", hit[7]));
    } else if (hit[8]) {
      parent.append(make("em", hit[9]));
    } else {
      parent.append(hit[10]);
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

function renderMarkdown(container, text, files, depth = 0) {
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
      inline(node, header[2].replace(/\s+#+\s*$/, ""), files);
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
        inline(cell, text, files);
        head.append(cell);
      }
      const body = table.createTBody();
      i += 2;
      while (i < lines.length && lines[i].includes("|") && lines[i].trim() && !startsBlock(lines[i])) {
        const row = body.insertRow();
        const parts = cells(lines[i++]);
        for (let col = 0; col < headerCells.length; col += 1) {
          inline(row.insertCell(), parts[col] || "", files);
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
        inline(li, next[3], files);
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
      renderMarkdown(block, body.join("\n"), files, depth + 1);
      container.append(block);
      continue;
    }

    const paragraph = make("p");
    inline(paragraph, line, files);
    i += 1;
    while (i < lines.length && lines[i].trim() && !startsBlock(lines[i])) {
      if (lines[i].includes("|") && i + 1 < lines.length && tableSeparator(lines[i + 1])) break;
      if (/ {2}$/.test(lines[i - 1])) paragraph.append(make("br"));
      else paragraph.append(" ");
      inline(paragraph, lines[i], files);
      i += 1;
    }
    container.append(paragraph);
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
  const content = document.createDocumentFragment();
  for (const message of chat.messages) {
    const section = make("section", undefined, "message " + message.role);
    const label = {user: "Utente", assistant: "ChatGPT", transcript: "Trascrizione"};
    section.append(make("p", label[message.role], "role"));
    const prose = make("div", undefined, "prose");
    renderMarkdown(prose, message.text, files);
    section.append(prose);
    content.append(section);
  }
  if (!content.childNodes.length) content.append(make("p", "Nessun messaggio testuale."));

  const missing = chat.refs.filter((ref) => !Object.hasOwn(files, ref.key));
  if (fileData.all.length || missing.length) {
    const attachments = make("section", undefined, "attachments");
    const title = make("h2", "Allegati");
    title.id = "attachments-title";
    attachments.setAttribute("aria-labelledby", title.id);
    const list = make("ul", undefined, "attachment-list");
    for (const file of fileData.all) {
      const item = make("li");
      item.append(fileLink(file));
      list.append(item);
    }
    for (const ref of missing) {
      const item = make("li");
      item.append(make("span", ref.name + " non disponibile", "missing"));
      list.append(item);
    }
    attachments.append(title, list);
    content.append(attachments);
  }
  article.replaceChildren(content);
}

async function openChat() {
  const key = location.hash.slice(1);
  const request = ++openRun;
  if (!validKey(key) || !rowByKey.has(key)) {
    activeKey = "";
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
    byId("main").scrollTop = 0;
    byId("conversation").focus({preventScroll: true});
  } catch {
    if (request === openRun) {
      byId("conversation").replaceChildren(make("p", "Impossibile aprire la conversazione.", "placeholder"));
    }
  }
}

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


if __name__ == "__main__":
    raise SystemExit(main())
