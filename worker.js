"use strict";

const PYODIDE_URL = new URL(self.location.href).searchParams.has("locale")
  ? new URL("pyodide/", self.location.href).href
  : "https://cdn.jsdelivr.net/pyodide/v314.0.8/full/";
let loading;
let running = false;

function send(type, detail = {}) {
  self.postMessage({type, ...detail});
}

function loadRuntime() {
  if (!loading) {
    loading = (async () => {
      send("log", {line: "\x1b[36m INFO \x1b[0m  Caricamento di Python 3.14…"});
      const {loadPyodide} = await import(PYODIDE_URL + "pyodide.mjs");
      const runtime = await loadPyodide({
        indexURL: PYODIDE_URL,
        stdout: (line) => send("log", {line}),
        stderr: (line) => send("log", {line}),
      });
      const script = await fetch("genera_archivio.py");
      if (!script.ok) throw new Error("genera_archivio.py non è raggiungibile.");
      const source = await script.text();
      return {runtime, source};
    })().catch((error) => {
      loading = undefined;
      throw error;
    });
  }
  return loading;
}

self.addEventListener("message", async ({data}) => {
  if (data?.type !== "generate") return;
  if (running) {
    send("failure", {message: "Una generazione è già in corso."});
    return;
  }
  running = true;
  try {
    const {runtime, source} = await loadRuntime();
    const scope = runtime.runPython("dict(__name__='genera_archivio')");
    try {
      scope.set("origine", data.input);
      scope.set("destinazione", data.output);
      runtime.runPython(source, {globals: scope});
      const name = await runtime.runPythonAsync(
        "await genera_da_browser(origine, destinazione)", {globals: scope},
      );
      send("complete", {name});
    } finally {
      scope.destroy();
    }
  } catch (error) {
    send("failure", {message: error.message || String(error)});
  } finally {
    running = false;
  }
});
