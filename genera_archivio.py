"""Genera un archivio locale degli export di exportChatGPTFullPagination-v5.js."""

import argparse
import asyncio
import codecs
from collections import Counter, defaultdict
from contextlib import aclosing
import json
from pathlib import Path
import re
from urllib.parse import unquote

CHUNK_SIZE = 256 * 1024
SEARCH_SHARD_SIZE = 256 * 1024
SITE_FOLDER = "archivio-chat"
BATCH_NAME = re.compile(r"chatgpt_business_export_COMPLETO_\d{4}-\d{2}-\d{2}\.json$")
NUMBERED_NAME = re.compile(r"^(\d{4,})_(.+)$")
UUID = re.compile(r"[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}$")
FILE_ID = re.compile(r"file[_-][a-zA-Z0-9_-]+")
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


def file_id(value):
    if not isinstance(value, str) or "user-" in value:
        return ""
    match = FILE_ID.search(value)
    return match[0] if match else ""


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
    for message in path_nodes(raw):
        content = obj(message.get("content"))
        metadata = obj(message.get("metadata"))
        role = obj(message["author"])["role"]
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
            asset = file_id(part.get("asset_pointer"))
            if asset:
                image = part.get("content_type") == "image_asset_pointer"
                label = "Immagine" if image else "Allegato"
                refs[asset] = {"key": asset, "name": label, "image": image, "generic": True}

        body = "\n".join(text_parts) or string(content.get("text"))
        if body and content.get("content_type") in ("code", "execution_output"):
            fence = "`" * max(3, 1 + max((len(run) for run in re.findall(r"`+", body)), default=0))
            body = f"{fence}\n{body}\n{fence}"

        for attachment in array(metadata.get("attachments")):
            attachment = obj(attachment)
            asset = file_id(attachment.get("id"))
            if asset:
                title = string(attachment.get("name") or attachment.get("file_name"))
                image = refs.get(asset, {}).get("image") or string(attachment.get("mime_type")).startswith("image/")
                refs[asset] = {
                    "key": asset, "name": title or ("Immagine" if image else "Allegato"),
                    "image": bool(image), "generic": not title,
                }

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
                refs[key] = {"key": key, "name": key}

        if body.strip() or refs:
            messages.append({"role": role, "text": body, "refs": list(refs.values())})
    if not messages and not obj(raw.get("mapping")) and fallback.strip():
        messages.append({"role": "transcript", "text": fallback, "refs": []})
    return messages


def scan_names(entries):
    batches = []
    individual_candidates = defaultdict(list)
    numbered = defaultdict(list)
    for name, kind in sorted(entries.items()):
        if kind != "file":
            continue
        if BATCH_NAME.fullmatch(name):
            batches.append(name)
            continue
        match = NUMBERED_NAME.fullmatch(name)
        if not match:
            continue
        number, rest = match.groups()
        numbered[number].append(name)
        if rest.endswith(".json") and "_" in rest:
            _, _, identifier = rest[:-5].rpartition("_")
            if identifier and not identifier.isdecimal():
                individual_candidates[identifier].append(name)
    return batches[::-1], individual_candidates, numbered


def individual_stem(identifier, title, candidates):
    for name in candidates.get(identifier, ()):
        suffix = f"_{identifier}.json"
        number = name.split("_", 1)[0]
        if name == f"{number}_{sanitize(title)}{suffix}":
            return name.removesuffix(suffix)
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


def match_attachment(ref, files):
    key = ref["key"]
    if key.startswith("/mnt/data/"):
        expected = [sanitize(key.rsplit("/", 1)[-1])]
    elif key.startswith("Canvas_"):
        expected = [key]
    else:
        expected = [] if ref.get("generic") else [sanitize(ref["name"])]
        expected.extend(name for name in files if name == key or name.startswith(f"{key}."))
    matches = {name for name in expected if name in files}
    if len(matches) == 1:
        return matches.pop()
    return None


async def attach_files(source, target, row, names, stem, excluded, terminal):
    prefix = stem + "_"
    files = {}
    for name in names:
        if name not in excluded and name.startswith(prefix):
            suffix = name[len(prefix):]
            if not suffix:
                continue
            files[suffix] = name
    if not files:
        return None

    folder = await target.mkdir(row["key"])
    available = {}
    used_names = set()
    for index, (display, original) in enumerate(files.items(), 1):
        name = sanitize(display).rstrip(".") or "file"
        if re.fullmatch(r"CON|PRN|AUX|NUL|COM[1-9]|LPT[1-9]", name.split(".")[0], re.IGNORECASE):
            name = f"_{name}"
        while name.casefold() in used_names:
            name = f"{index}-{name}"
        used_names.add(name.casefold())
        try:
            await folder.write(name, source.read(original))
        except Exception as exc:
            terminal.warning(f"File saltato: {original} ({exc})")
        else:
            available[display] = {
                "name": display,
                "url": f"file/{row['key']}/{name}",
            }

    linked = {}
    for ref in row["refs"].values():
        match = match_attachment(ref, available)
        if match:
            linked[ref["key"]] = available[match]
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
    batches, candidates, numbered = scan_names(entries)
    if not batches and not candidates:
        raise ValueError("Nessun export v5 riconosciuto nella cartella di origine.")

    site_name, site = await create_site_folder(destination)
    terminal.info(f"Destinazione: {site.name}")
    chat_dir = await site.mkdir("chat")
    search_dir = await site.mkdir("ricerca")
    files_dir = await site.mkdir("file")
    rows = []
    seen = set()
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
        if from_file:
            excluded.add(from_file)
        if identifier in seen:
            return True

        title = string(summary.get("title")) or string(raw.get("title"))
        stem = (
            from_file.removesuffix(f"_{file_identifier}.json") if from_file
            else individual_stem(identifier, title, candidates)
        )
        if stem and not from_file:
            excluded.add(f"{stem}_{identifier}.json")
        messages = prepare_messages(raw, fallback)
        key = str(len(rows) + 1)
        refs = {ref["key"]: ref for message in messages for ref in message["refs"]}
        row = {
            "key": key,
            "title": title or "Senza titolo",
            "safeTitle": sanitize(title),
            "stem": stem,
            "updated": last_update(summary, raw),
            "archived": raw.get("is_archived") is True or summary.get("is_archived") is True,
            "refs": refs,
        }
        await write_data(chat_dir, f"{key}.js", "registraChat", {"key": key, "messages": messages})
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
        seen.add(identifier)
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
            if not (canonical or explicit_id == identifier or name == expected_name):
                continue
            await add(raw, {}, from_file=name, file_identifier=identifier)

    if not rows:
        raise ValueError("L'export non contiene conversazioni leggibili.")

    await flush_search()
    terminal.info(f"{len(rows)} conversazioni. Copia dei file associabili in corso.")
    known_stems = {row["stem"] for row in rows if row["stem"]}
    asset_names = {
        number: [name for name in names if name not in excluded]
        for number, names in numbered.items()
    }
    inferred = group_unknown(asset_names, rows, known_stems)
    file_count = 0
    for row in rows:
        stem = row["stem"] or inferred.get(row["safeTitle"])
        if not stem:
            continue
        number = stem.split("_", 1)[0]
        files = await attach_files(source, files_dir, row, asset_names.get(number, ()), stem, excluded, terminal)
        if files and files["all"]:
            row["hasFiles"] = True
            file_count += len(files["all"])
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
    terminal.done(f"Creato {site_name}/index.html · {len(rows)} chat · {file_count} file")
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
  <meta http-equiv="Content-Security-Policy" content="default-src 'none'; script-src 'self' file:; style-src 'self' file:; img-src 'self' file:; base-uri 'none'; form-action 'none'">
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
.extras { margin-top: 24px; border-top: 1px solid var(--edge); padding-top: 14px; }
.extras summary { cursor: pointer; }
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
const isImage = (file) => /\.(?:png|jpe?g|gif|webp|avif|bmp)$/i.test(file.name);
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
  return link;
};
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

function inline(parent, text, files, used) {
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
          used.add(file.url);
          parent.append(marker && isImage(file) ? image(file, label) : fileLink(file, label, true));
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

function renderMarkdown(container, text, files, used, depth = 0) {
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
      inline(node, header[2].replace(/\s+#+\s*$/, ""), files, used);
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
        inline(cell, text, files, used);
        head.append(cell);
      }
      const body = table.createTBody();
      i += 2;
      while (i < lines.length && lines[i].includes("|") && lines[i].trim() && !startsBlock(lines[i])) {
        const row = body.insertRow();
        const parts = cells(lines[i++]);
        for (let col = 0; col < headerCells.length; col += 1) {
          inline(row.insertCell(), parts[col] || "", files, used);
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
        inline(li, next[3], files, used);
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
      renderMarkdown(block, body.join("\n"), files, used, depth + 1);
      container.append(block);
      continue;
    }

    const paragraph = make("p");
    inline(paragraph, line, files, used);
    i += 1;
    while (i < lines.length && lines[i].trim() && !startsBlock(lines[i])) {
      if (lines[i].includes("|") && i + 1 < lines.length && tableSeparator(lines[i + 1])) break;
      if (/ {2}$/.test(lines[i - 1])) paragraph.append(make("br"));
      else paragraph.append(" ");
      inline(paragraph, lines[i], files, used);
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
  const used = new Set();
  const content = document.createDocumentFragment();
  for (const message of chat.messages) {
    const section = make("section", undefined, "message " + message.role);
    const label = {user: "Utente", assistant: "ChatGPT", transcript: "Trascrizione"};
    section.append(make("p", label[message.role], "role"));
    const prose = make("div", undefined, "prose");
    renderMarkdown(prose, message.text, files, used);
    section.append(prose);
    for (const ref of message.refs) {
      const file = Object.hasOwn(files, ref.key) ? files[ref.key] : null;
      if (!file) {
        if (!ref.inline) section.append(make("span", ref.name + " non disponibile", "missing"));
      } else if (!used.has(file.url) || !ref.inline) {
        const name = ref.generic ? file.name : ref.name;
        if (ref.image && isImage(file)) section.append(image(file, name));
        section.append(fileLink(file, name));
        used.add(file.url);
      }
    }
    content.append(section);
  }
  const extraFiles = fileData.all.filter((file) => !used.has(file.url));
  if (extraFiles.length) {
    const details = make("details", undefined, "extras");
    details.append(make("summary", "File della conversazione (" + extraFiles.length + ")"));
    for (const file of extraFiles) details.append(fileLink(file));
    content.append(details);
  }
  if (!content.childNodes.length) content.append(make("p", "Nessun messaggio visibile."));
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
