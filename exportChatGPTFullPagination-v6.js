(async function exportChatGPTFullPagination() {
  const MAX_CHATS = 5000;
  const START_INDEX = 0;
  const NUM_CHATS_TO_EXPORT = MAX_CHATS;
  const DELAY_MS = 1200;
  const DOWNLOAD_INDIVIDUAL_JSON = true;
  const DOWNLOAD_FILES = true;

  // Configurazione Pausa Periodica Anti-Sovraccarico
  const PAUSE_EVERY_X_MINUTES = 5;
  const PAUSE_DURATION_Y_SECONDS = 10;

  // Configurazione Exponential Backoff
  const BACKOFF_INITIAL_SECONDS = 2;
  const BACKOFF_MAX_DOUBLINGS = 3;
  const BACKOFF_FALLBACK_Z_SECONDS = 10;

  const baseUrl = window.location.origin;
  const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
  const sanitize = (str) => (str || "untitled").replace(/[^a-z0-9_.-]/gi, "_").substring(0, 60);

  let consecutiveErrors = 0;
  let lastPauseTimestamp = Date.now();
  const retryQueue = [];
  const downloadedAssets = new Set();

  // Selezione cartella di destinazione (se supportata dal browser e non in Incognito)
  let dirHandle = null;
  if ("showDirectoryPicker" in window) {
    try {
      dirHandle = await window.showDirectoryPicker({ mode: "readwrite" });
      console.log(`[INFO] Cartella selezionata: "${dirHandle.name}". I file verranno salvati senza popup.`);
    } catch (e) {
      console.warn("[WARN] Selezione cartella annullata o non disponibile (es. finestra in Incognito). Uso download standard del browser.");
    }
  } else {
    console.warn("[WARN] File System Access API non supportata da questo browser. Uso download standard.");
  }

  async function saveBlobToDisk(blob, filename) {
    if (dirHandle) {
      try {
        const fileHandle = await dirHandle.getFileHandle(filename, { create: true });
        const writable = await fileHandle.createWritable();
        await writable.write(blob);
        await writable.close();
        return;
      } catch (fsErr) {
        console.warn(`[WARN] Scrittura diretta fallita per ${filename}, ripiego su download browser:`, fsErr.message);
      }
    }

    const link = document.createElement("a");
    link.href = URL.createObjectURL(blob);
    link.download = filename;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    setTimeout(() => URL.revokeObjectURL(link.href), 5000);
  }

  async function checkPeriodicPause() {
    const elapsedMs = Date.now() - lastPauseTimestamp;
    const intervalMs = PAUSE_EVERY_X_MINUTES * 60 * 1000;
    if (elapsedMs >= intervalMs) {
      console.log(`[PAUSE] Trascorsi ${PAUSE_EVERY_X_MINUTES} minuti. Pausa preventiva di ${PAUSE_DURATION_Y_SECONDS}s per evitare rate-limit...`);
      await sleep(PAUSE_DURATION_Y_SECONDS * 1000);
      lastPauseTimestamp = Date.now();
    }
  }

  async function handleErrorBackoff(contextLabel) {
    consecutiveErrors++;
    let waitSeconds;
    if (consecutiveErrors <= BACKOFF_MAX_DOUBLINGS) {
      waitSeconds = BACKOFF_INITIAL_SECONDS * Math.pow(2, consecutiveErrors - 1);
    } else {
      waitSeconds = BACKOFF_FALLBACK_Z_SECONDS;
    }
    console.warn(`[BACKOFF] Errore #${consecutiveErrors} su "${contextLabel}". Attesa di ${waitSeconds}s...`);
    await sleep(waitSeconds * 1000);
  }

  function resetErrorBackoff() {
    if (consecutiveErrors > 0) {
      console.log(`[OK] Chiamata riuscita: contatore backoff azzerato (era ${consecutiveErrors}).`);
      consecutiveErrors = 0;
    }
  }

  async function safeFetchJson(url, options = {}) {
    await checkPeriodicPause();
    const res = await fetch(url, options);
    const text = await res.text();
    if (!res.ok || text.trim().startsWith("<")) {
      const err = new Error(`HTTP ${res.status} su ${url}`);
      err.status = res.status;
      throw err;
    }
    resetErrorBackoff();
    return JSON.parse(text);
  }

  function getExtensionFromMime(mimeType) {
    if (!mimeType) return "bin";
    const map = {
      "image/png": "png",
      "image/jpeg": "jpg",
      "image/jpg": "jpg",
      "image/webp": "webp",
      "image/gif": "gif",
      "image/avif": "avif",
      "application/pdf": "pdf",
      "application/zip": "zip",
      "application/msword": "doc",
      "application/vnd.openxmlformats-officedocument.wordprocessingml.document": "docx",
      "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": "xlsx",
      "application/vnd.openxmlformats-officedocument.presentationml.presentation": "pptx",
      "audio/mpeg": "mp3",
      "audio/mp3": "mp3",
      "audio/wav": "wav",
      "audio/mp4": "m4a",
      "video/mp4": "mp4",
      "text/csv": "csv",
      "text/plain": "txt",
      "text/markdown": "md",
      "application/json": "json"
    };
    const mime = mimeType.split(";")[0].trim().toLowerCase();
    return Object.hasOwn(map, mime) ? map[mime] : "bin";
  }

  function assetFilename(indexPrefix, safeTitle, assetId, originalName, mimeType) {
    const basename = originalName.split(/[\\/]/).at(-1).trim();
    const extension = basename.match(/\.[a-z0-9]{1,12}$/i)?.[0] || "";
    const rawStem = extension ? basename.slice(0, -extension.length) : basename;
    const mimeExtension = getExtensionFromMime(mimeType);
    const suffix = (!extension || /^\.(bin|download|tmp)$/i.test(extension)) && mimeExtension !== "bin"
      ? `.${mimeExtension}`
      : extension || ".bin";
    const stem = rawStem && rawStem !== assetId
      ? sanitize(rawStem).slice(0, 60 - suffix.length).replace(/[._-]+$/, "") || "allegato"
      : "allegato";
    // L'ID resta intero, anche quando il nome descrittivo viene accorciato.
    return `${indexPrefix}_${safeTitle}_${assetId}_${stem}${suffix}`;
  }

  function extractAllChatResources(chatData) {
    const attachmentIds = new Set();
    const assetPointerIds = new Set();
    const assetNames = new Map();
    const sandboxFiles = new Map();
    const writingBlocks = [];

    const cleanId = (raw) => {
      if (typeof raw !== "string") return null;
      let value = raw;
      try { value = decodeURIComponent(raw); } catch { /* usa il valore originale */ }
      const path = value.trim().split(/[?#]/, 1)[0].split("://").at(-1);
      return path.split("/").findLast((part) => /^file[_-][a-z0-9][a-z0-9_-]*$/i.test(part)) || null;
    };

    function addAsset(ids, id, data) {
      if (!id) return;
      ids.add(id);
      const name = [data.name, data.filename, data.file_name, data.fileName]
        .find((value) => typeof value === "string" && value.trim());
      if (name && (ids === attachmentIds || !assetNames.has(id))) assetNames.set(id, name);
    }

    if (chatData && chatData.mapping) {
      Object.values(chatData.mapping).forEach((node) => {
        const msg = node.message;
        if (!msg) return;

        if (msg.content && Array.isArray(msg.content.parts)) {
          msg.content.parts.forEach((part) => {
            if (part && typeof part === "object") {
              addAsset(assetPointerIds, cleanId(part.asset_pointer) || cleanId(part.file_id), part);
            }
            if (typeof part === "string") {
              const sbMatches = part.match(/sandbox:(\/mnt\/data\/[^\s\)\"\]\'<>]+)/g);
              if (sbMatches) {
                sbMatches.forEach((m) => {
                  const cleanPath = decodeURIComponent(m.replace(/^sandbox:/, ""));
                  if (!sandboxFiles.has(cleanPath)) {
                    sandboxFiles.set(cleanPath, msg.id);
                  }
                });
              }
            }
          });
        }

        if (msg.metadata && Array.isArray(msg.metadata.attachments)) {
          msg.metadata.attachments.forEach((att) => {
            if (att) {
              addAsset(attachmentIds, cleanId(att.id) || cleanId(att.file_id) || cleanId(att.asset_pointer), att);
            }
          });
        }

        const compResults = msg.metadata?.model_dil_v2?.appData?.opGenui?.componentResults;
        if (compResults && typeof compResults === "object") {
          Object.values(compResults).forEach((comp) => {
            const url = comp?.state?.url;
            if (typeof url === "string" && url.startsWith("sandbox:/mnt/data/")) {
              const cleanPath = decodeURIComponent(url.replace(/^sandbox:/, ""));
              if (!sandboxFiles.has(cleanPath)) {
                sandboxFiles.set(cleanPath, msg.id);
              }
            }
          });
        }

        if (msg.metadata && msg.metadata.writing_blocks) {
          Object.entries(msg.metadata.writing_blocks).forEach(([wbId, wbData]) => {
            if (wbData && wbData.content) {
              writingBlocks.push({
                id: wbId,
                content: wbData.content
              });
            }
          });
        }
      });
    }

    attachmentIds.forEach((id) => assetPointerIds.delete(id));

    return {
      attachmentIds: Array.from(attachmentIds),
      assetPointerIds: Array.from(assetPointerIds),
      assetNames,
      sandboxFiles: Array.from(sandboxFiles.entries()).map(([path, messageId]) => ({ path, messageId })),
      writingBlocks
    };
  }

  console.log("[INFO] Avvio esportazione completa ChatGPT Business (v6)...");

  const sessionData = await safeFetchJson(`${baseUrl}/api/auth/session`);
  const token = sessionData.accessToken;

  const accountCookie = document.cookie.split("; ").find((row) => row.startsWith("_account="));
  const rawWorkspaceId = accountCookie ? decodeURIComponent(accountCookie.split("=")[1]) : (sessionData.account?.id || "");
  const workspaceId = /^[0-9a-f-]{36}$/i.test(rawWorkspaceId) ? rawWorkspaceId : "";

  if (!token) throw new Error("Access Token non trovato. Ricarica la pagina e verifica il login.");
  console.log(`[INFO] Autenticazione riuscita. Workspace ID: ${workspaceId || "Default"}`);

  const headers = {
    "Authorization": `Bearer ${token}`,
    "Content-Type": "application/json",
  };
  if (workspaceId) {
    headers["ChatGPT-Account-ID"] = workspaceId;
  }

  const seenIds = new Set();
  let conversations = [];
  const targetFetchLimit = Math.min(MAX_CHATS, START_INDEX + NUM_CHATS_TO_EXPORT);

  async function fetchAllByArchiveFlag(isArchived) {
    let offset = 0;
    let cursor = null;
    const pageSize = 50;
    let hasMore = true;

    console.log(`[INFO] Recupero elenco chat (${isArchived ? "Archiviate" : "Attive"})...`);

    while (hasMore && conversations.length < targetFetchLimit) {
      let listUrl = `${baseUrl}/backend-api/conversations?offset=${offset}&limit=${pageSize}&order=updated`;
      if (isArchived) listUrl += `&is_archived=true`;
      if (cursor) listUrl += `&cursor=${encodeURIComponent(cursor)}`;

      let listData;
      try {
        listData = await safeFetchJson(listUrl, { headers });
      } catch (err) {
        console.warn(`[WARN] Interruzione paginazione all'offset ${offset}: ${err.message}`);
        await handleErrorBackoff(`Paginazione offset ${offset}`);
        break;
      }

      const items = listData.items || [];
      if (items.length === 0) {
        hasMore = false;
        break;
      }

      let addedInThisPage = 0;
      for (const item of items) {
        if (!seenIds.has(item.id)) {
          seenIds.add(item.id);
          conversations.push(item);
          addedInThisPage++;
        }
      }

      console.log(`   [OK] Pagina caricata: +${addedInThisPage} chat (Totale: ${conversations.length})`);

      if (addedInThisPage === 0 || listData.has_more === false) {
        hasMore = false;
        break;
      }

      offset += items.length;
      cursor = listData.cursor || null;
      await sleep(600);
    }
  }

  await fetchAllByArchiveFlag(false);
  if (conversations.length < targetFetchLimit) {
    await fetchAllByArchiveFlag(true);
  }

  conversations = conversations.slice(START_INDEX, targetFetchLimit);
  const totalToProcess = Math.min(NUM_CHATS_TO_EXPORT, conversations.length);
  console.log(`[INFO] Totale conversazioni da elaborare: ${totalToProcess} (da indice ${START_INDEX}, limite: ${NUM_CHATS_TO_EXPORT})`);

  const exportedData = [];

  async function downloadSingleAsset(convId, assetId, isExplicitAttachment, indexPrefix, safeTitle, metadataName) {
    if (!assetId || assetId.startsWith("user-")) return;

    const dedupKey = `${convId}_${assetId}`;
    if (downloadedAssets.has(dedupKey)) {
      return;
    }

    const endpoints = isExplicitAttachment
      ? [
          `${baseUrl}/backend-api/files/${assetId}/download`,
          `${baseUrl}/backend-api/files/download/${assetId}`,
          `${baseUrl}/backend-api/conversation/${convId}/attachment/${assetId}/download`
        ]
      : [
          `${baseUrl}/backend-api/files/${assetId}/download`,
          `${baseUrl}/backend-api/files/download/${assetId}`
        ];

    let dlInfo = null;
    let lastError = null;
    let retryableError = null;

    for (const ep of endpoints) {
      try {
        dlInfo = await safeFetchJson(ep, { headers });
        if (dlInfo && dlInfo.download_url) break;
      } catch (err) {
        lastError = err;
        if (err.status === 410) break;
        if (err.status !== 404 && err.status !== 422) retryableError = err;
      }
    }

    if (dlInfo && dlInfo.download_url) {
      await checkPeriodicPause();
      const blobRes = await fetch(dlInfo.download_url);
      if (!blobRes.ok) {
        const err = new Error(`HTTP ${blobRes.status} su download_url per ${assetId}`);
        err.status = blobRes.status;
        throw err;
      }
      resetErrorBackoff();
      const blob = await blobRes.blob();
      const originalName = [dlInfo.file_name, metadataName]
        .find((value) => typeof value === "string" && value.trim() && value !== assetId) || "allegato";
      const finalName = assetFilename(indexPrefix, safeTitle, assetId, originalName, blob.type);
      await saveBlobToDisk(blob, finalName);
      downloadedAssets.add(dedupKey);
      console.log(`   [FILE] Scaricato: ${finalName}`);
      await sleep(700);
    } else {
      const errToThrow = retryableError || lastError || new Error(`Nessun download_url restituito per ${assetId}`);
      if (errToThrow.status === 404 || errToThrow.status === 410 || errToThrow.status === 422) {
        console.warn(`   [SKIP] Asset non disponibile (${errToThrow.status}): ${assetId}`);
        downloadedAssets.add(dedupKey);
        return;
      }
      throw errToThrow;
    }
  }

  async function downloadSingleSandboxFile(convId, sb, indexPrefix, safeTitle) {
    const dedupKey = `${convId}_sandbox_${sb.path}`;
    if (downloadedAssets.has(dedupKey)) {
      return;
    }

    const sbEndpoint = `${baseUrl}/backend-api/conversation/${convId}/interpreter/download?message_id=${encodeURIComponent(sb.messageId)}&sandbox_path=${encodeURIComponent(sb.path)}`;
    const dlInfo = await safeFetchJson(sbEndpoint, { headers });

    if (dlInfo && dlInfo.download_url) {
      await checkPeriodicPause();
      const blobRes = await fetch(dlInfo.download_url);
      if (!blobRes.ok) {
        const err = new Error(`HTTP ${blobRes.status} su sandbox download_url`);
        err.status = blobRes.status;
        throw err;
      }
      resetErrorBackoff();
      const blob = await blobRes.blob();
      const rawFileName = sb.path.split("/").pop() || "sandbox_file";
      const finalName = `${indexPrefix}_${safeTitle}_${sanitize(rawFileName)}`;
      await saveBlobToDisk(blob, finalName);
      downloadedAssets.add(dedupKey);
      console.log(`   [SANDBOX] Scaricato: ${finalName}`);
      await sleep(700);
    } else {
      const err = new Error(`Nessun download_url per sandbox: ${sb.path}`);
      err.status = 404;
      throw err;
    }
  }

  async function processConversation(item, globalNumber, isRetryPhase = false) {
    const indexPrefix = String(globalNumber).padStart(4, "0");
    const safeTitle = sanitize(item.title);
    const phaseLabel = isRetryPhase ? "[RETRY]" : "[MAIN]";
    console.log(`${phaseLabel} (#${globalNumber}) "${item.title}"`);

    let chatData;
    try {
      chatData = await safeFetchJson(`${baseUrl}/backend-api/conversation/${item.id}`, { headers });
    } catch (err) {
      console.warn(`[ERR] Errore lettura chat "${item.title}": ${err.message}`);
      await handleErrorBackoff(`Chat ${item.id}`);
      if (!isRetryPhase) {
        retryQueue.push({ type: "conversation", item, globalNumber });
      }
      return;
    }

    if (DOWNLOAD_INDIVIDUAL_JSON) {
      const singleChatBlob = new Blob([JSON.stringify(chatData, null, 2)], { type: "application/json" });
      await saveBlobToDisk(singleChatBlob, `${indexPrefix}_${safeTitle}_${item.id}.json`);
      await sleep(250);
    }

    let mdContent = `# ${chatData.title || "Senza titolo"}\n\n`;
    if (chatData.mapping) {
      Object.values(chatData.mapping).forEach((node) => {
        const msg = node.message;
        if (msg && msg.content && msg.content.parts && msg.author?.role !== "system") {
          const role = msg.author?.role === "user" ? "**Utente**" : "**ChatGPT**";
          const textParts = msg.content.parts
            .map((p) => {
              if (typeof p === "string") return p;
              if (p.content_type === "image_asset_pointer") return `[Immagine: ${p.asset_pointer}]`;
              return p.text || `[Allegato: ${p.asset_pointer || "file"}]`;
            })
            .join("\n");
          if (textParts.trim()) {
            mdContent += `### ${role}\n${textParts}\n\n---\n\n`;
          }
        }
      });
    }

    exportedData.push({
      id: item.id,
      title: item.title,
      create_time: item.create_time,
      update_time: item.update_time,
      markdown_text: mdContent,
      raw_data: chatData
    });

    if (DOWNLOAD_FILES) {
      const { attachmentIds, assetPointerIds, assetNames, sandboxFiles, writingBlocks } = extractAllChatResources(chatData);

      for (const assetId of attachmentIds) {
        const metadataName = assetNames.get(assetId);
        try {
          await downloadSingleAsset(item.id, assetId, true, indexPrefix, safeTitle, metadataName);
        } catch (fileErr) {
          console.warn(`   [ERR] Allegato ${assetId}: ${fileErr.message}`);
          await handleErrorBackoff(`Allegato ${assetId}`);
          if (!isRetryPhase) {
            retryQueue.push({ type: "asset", convId: item.id, assetId, isExplicitAttachment: true, indexPrefix, safeTitle, metadataName });
          }
        }
      }

      for (const assetId of assetPointerIds) {
        const metadataName = assetNames.get(assetId);
        try {
          await downloadSingleAsset(item.id, assetId, false, indexPrefix, safeTitle, metadataName);
        } catch (fileErr) {
          console.warn(`   [ERR] Asset ${assetId}: ${fileErr.message}`);
          await handleErrorBackoff(`Asset ${assetId}`);
          if (!isRetryPhase) {
            retryQueue.push({ type: "asset", convId: item.id, assetId, isExplicitAttachment: false, indexPrefix, safeTitle, metadataName });
          }
        }
      }

      for (const sb of sandboxFiles) {
        try {
          await downloadSingleSandboxFile(item.id, sb, indexPrefix, safeTitle);
        } catch (sbErr) {
          if (sbErr.status === 404 || sbErr.status === 410) {
            console.warn(`   [SKIP] Sandbox scaduta o non trovata (${sbErr.status}): ${sb.path}`);
            downloadedAssets.add(`${item.id}_sandbox_${sb.path}`);
          } else {
            console.warn(`   [ERR] Sandbox ${sb.path}: ${sbErr.message}`);
            await handleErrorBackoff(`Sandbox ${sb.path}`);
            if (!isRetryPhase) {
              retryQueue.push({ type: "sandbox", convId: item.id, sb, indexPrefix, safeTitle });
            }
          }
        }
      }

      for (const wb of writingBlocks) {
        const dedupKey = `${item.id}_canvas_${wb.id}`;
        if (!downloadedAssets.has(dedupKey)) {
          const wbBlob = new Blob([wb.content], { type: "text/markdown;charset=utf-8" });
          const finalName = `${indexPrefix}_${safeTitle}_Canvas_${wb.id}.md`;
          await saveBlobToDisk(wbBlob, finalName);
          downloadedAssets.add(dedupKey);
          console.log(`   [CANVAS] Salvato: ${finalName}`);
          await sleep(300);
        }
      }
    }
  }

  // 3. Fase principale: lettura conversazioni da 0 fino a NUM_CHATS_TO_EXPORT
  for (let i = 0; i < totalToProcess; i++) {
    const item = conversations[i];
    const globalNumber = START_INDEX + i + 1;
    console.log(`[PROGRESS] [${i + 1}/${totalToProcess}]`);
    await processConversation(item, globalNumber, false);
    await sleep(DELAY_MS);
  }

  // 4. Fase di svuotamento coda retry
  if (retryQueue.length > 0) {
    console.log(`\n[RETRY PHASE] Avvio recupero di ${retryQueue.length} elementi falliti nella fase principale...`);
    for (let r = 0; r < retryQueue.length; r++) {
      const task = retryQueue[r];
      console.log(`[RETRY ${r + 1}/${retryQueue.length}] Tipo: ${task.type}`);
      try {
        if (task.type === "conversation") {
          await processConversation(task.item, task.globalNumber, true);
        } else if (task.type === "asset") {
          await downloadSingleAsset(task.convId, task.assetId, task.isExplicitAttachment, task.indexPrefix, task.safeTitle, task.metadataName);
        } else if (task.type === "sandbox") {
          await downloadSingleSandboxFile(task.convId, task.sb, task.indexPrefix, task.safeTitle);
        }
      } catch (retryErr) {
        console.warn(`[RETRY FAILED] Elemento definitivamente saltato (${task.type}): ${retryErr.message}`);
        await handleErrorBackoff(`Retry ${task.type}`);
      }
      await sleep(DELAY_MS);
    }
    console.log("[RETRY PHASE] Coda di recupero completata.");
  }

  // 5. Salvataggio del file JSON cumulativo finale
  const jsonBlob = new Blob([JSON.stringify(exportedData, null, 2)], { type: "application/json" });
  const exportFilename = `chatgpt_business_export_COMPLETO_${new Date().toISOString().slice(0, 10)}.json`;
  await saveBlobToDisk(jsonBlob, exportFilename);

  console.log(`[DONE] Esportazione completata! Salvate ${exportedData.length} chat.`);
})();
