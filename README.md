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

La cronologia è ordinata per data e la ricerca copre i titoli e il testo del ramo attivo. Il generatore legge il cumulativo e integra i JSON individuali, deduplicando per ID.

Per collegare un allegato, il generatore cerca prima il suo ID ovunque nel nome del file, anche nelle sottocartelle e quando l’ID è attaccato ad altro testo. Se l’ID manca, prova i nomi normalizzati e le corrispondenze plausibili nella stessa conversazione. Gli abbinamenti ambigui restano da verificare. Al termine il terminale mostra quanti riferimenti sono stati collegati.

Le copie degli allegati usano il nome originale dei metadati quando è disponibile, oppure un nome ricavato dal file, ripulito dagli ID. L’estensione si basa sul contenuto, sul nome o sul tipo MIME. Il generatore usa `.bin` quando il formato resta sconosciuto. I file con lo stesso nome ricevono un suffisso numerico.

Gli allegati disponibili del ramo visibile compaiono nel punto indicato dal `mapping`, oppure accanto al messaggio quando la posizione è indicata solo nei metadati. In fondo alla conversazione, **Allegati** raccoglie tutti i file trovati. I riferimenti rimasti senza file compaiono solo nella sottosezione **Allegati non disponibili**. Entrambe le sezioni sono chiuse all’apertura della conversazione.

Il clic apre un popup sopra la chat. Immagini, PDF, file di testo e i formati audio e video supportati dal browser hanno un’anteprima. Il pulsante **Scarica allegato** è disponibile per tutti i formati. Per applicare un aggiornamento, rigenera il sito dalla cartella dell’export originale.

Il sito visualizza i costrutti Markdown più comuni, inclusi blocchi di codice, elenchi, citazioni e tabelle semplici. Riconosce anche `<WritingBlock>`, `<box>` e `<caption>` come elementi di visualizzazione. Quando l’export include il contenuto nei metadati, il testo del blocco di scrittura viene recuperato da lì. I blocchi Canvas esportati sono disponibili anche come file Markdown scaricabili. I tag negli esempi di codice restano letterali.
