# Generatore di archivio chat

[Scarica il pacchetto ZIP](https://raw.githubusercontent.com/74m15/codex/main/archivio_chat_pyodide.zip) oppure apri [lo script Python](genera_archivio.py).

`genera_archivio.py` legge la cartella creata da `exportChatGPTFullPagination-v5.js` e genera un sito statico. Usa la libreria standard di Python 3.14 e `asyncio` in un solo thread. I messaggi di avanzamento usano sequenze di colore ANSI compatibili con VT100.

## Con un worker Pyodide esistente

Carica il file Python nel namespace del worker e passa i due `FileSystemDirectoryHandle` ottenuti sulla pagina principale con `showDirectoryPicker()`. Usa il permesso di lettura per l’origine e quello di lettura e scrittura per la destinazione. Il worker può ricevere gli handle tramite `postMessage`.

```python
nome_sito = await genera_da_browser(handle_origine, handle_destinazione)
```

La funzione legge i file in blocchi attraverso gli handle, compresi gli allegati binari. Il nome restituito identifica la sottocartella creata. Per percorsi già disponibili nel filesystem virtuale del worker, usa `await genera_da_cartelle(orig, dest)`.

## Avvio di esempio nel browser

1. Estrai il pacchetto. Da quella cartella avvia `python3 -m http.server --bind 127.0.0.1 8000`.
2. Apri `http://localhost:8000/avvia.html` in Chrome o Edge. Scegli origine e destinazione e fai clic su **Genera archivio**.
3. Apri `archivio-chat/index.html` dalla destinazione scelta. Il generatore aggiunge un suffisso numerico quando la cartella esiste già.

L’avvio di esempio carica Pyodide 314.0.8 da jsDelivr. Per usare una copia locale di quella versione, colloca la distribuzione nella sottocartella `pyodide/` accanto ad `avvia.html` e apri `avvia.html?locale=1`. Il sito prodotto funziona offline.

## Da terminale con Python 3.14

```sh
python3.14 genera_archivio.py "/percorso/export" "/percorso/destinazione"
```

Aggiungi `--no-color` per un log senza sequenze ANSI. Entrambe le cartelle devono esistere.

## Contenuto dell’archivio

La cronologia è ordinata per data e la ricerca copre i titoli e il testo del ramo attivo. Il generatore legge il cumulativo e integra i JSON individuali, deduplicando per ID. Usa i nomi prodotti dallo script v5 e i riferimenti nei messaggi per associare gli allegati. I file riconducibili solo alla conversazione compaiono in **File della conversazione**. Un allegato scaduto o con associazione ambigua può comparire come non disponibile.

Il sito visualizza i costrutti Markdown più comuni, inclusi blocchi di codice, elenchi, citazioni e tabelle semplici. I blocchi Canvas esportati sono disponibili come file Markdown scaricabili.
