# Generatore di archivio chat

[Scarica il pacchetto ZIP](https://raw.githubusercontent.com/74m15/codex/main/archivio_chat_pyodide.zip) oppure apri [lo script Python](genera_archivio.py).

`genera_archivio.py` legge gli export dalla v5 alla v7 e genera un sito statico. Usa la libreria standard di Python 3.14 e `asyncio` in un solo thread. I messaggi di avanzamento usano sequenze di colore ANSI compatibili con VT100.

## Esportazione delle chat

Apri il workspace ChatGPT da esportare, esegui [`exportChatGPTFullPagination-v7.js`](exportChatGPTFullPagination-v7.js) nella console del browser e scegli una cartella vuota. Usa quella cartella come origine per `genera_archivio.py`.

Lo script salva gli allegati con un nome come `0001_Titolo_file-abc123_Bilancio.pdf`. Il nome contiene l’ID completo e l’estensione, così il generatore può collegare anche allegati con lo stesso nome originale. La v7 cerca anche gli audio annidati e i file citati nei metadati e salva come Markdown i blocchi Canvas inclusi nei riferimenti. I download scaduti o non disponibili vengono segnalati nella console.

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

Il sito visualizza i costrutti Markdown più comuni, inclusi blocchi di codice, elenchi, citazioni e tabelle semplici. Riconosce `<WritingBlock>`, `<box>` e `<caption>` come elementi di visualizzazione e mostra il contenuto racchiuso in altre coppie di tag personalizzati. Quando disponibile, recupera dai metadati il testo dei blocchi di scrittura e gli URL delle citazioni. I tag negli esempi di codice restano letterali. I blocchi Canvas esportati sono disponibili anche come file Markdown scaricabili.

## Oggetti incorporati rilevati negli export di test

- I diagrammi SVG sono mostrati come immagini isolate. Il visualizzatore risolve i cicli, le condizioni e le coordinate calcolate a partire dai dati letterali del messaggio. I riferimenti interni per riempimenti, ritaglio e riuso di forme funzionano nell’immagine. [SVG 2](https://www.w3.org/TR/SVG/latest/conform.html)
- I blocchi `{#each … as p,i}` con una lista di dati sono mostrati come elenchi. `{p[0]}` inserisce una cella della riga e `{i+1}` indica la posizione a partire da uno. I componenti di presentazione danno struttura al contenuto. Il codice negli esempi e i costrutti non riconosciuti restano leggibili come sorgente.
- Quando `{@body}` contiene domande con opzioni, indice della risposta ed eventuale spiegazione, il sito ricrea un quiz con il pulsante **Verifica risposte**. Il quiz parte senza risposte e funziona offline.
- Le trascrizioni vocali sono mostrate nel messaggio. L’audio e i file citati si aprono nel popup quando il relativo file è presente nella cartella di origine. I risultati di immagini e citazioni con URL salvati compaiono come link. Per mappe, meteo e quotazioni si mostra il riepilogo registrato nell’export, quando disponibile.

Le formule LaTeX tra `\(...\)`, `\[...\]`, `$...$` e `$$...$$` vengono visualizzate come formule in linea o a blocco. Gli ambienti comuni, come `align`, sono supportati anche fuori dai delimitatori. Il sito include KaTeX 0.19.1 con licenza MIT e può visualizzare formule offline. [Licenza e sorgente KaTeX](https://www.npmjs.com/package/katex/v/0.19.1)

Apri una conversazione e seleziona **Stampa** per stampare o salvare in PDF tutti i messaggi del ramo visualizzato, anche su più pagine.
