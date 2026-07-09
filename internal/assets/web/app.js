function apiPath(path) { const s = new URLSearchParams(window.location.search).get('s'); if (s) { return path.includes('?') ? path + '&s=' + s : path + '?s=' + s; } return path; }
/**
 * app.js — Gaze Receiver (Phase 4 & 5: Direct-to-Disk + Live Pipe)
 *
 * Connection strategy (tries in order):
 *   1. WebRTC P2P via RTCDataChannel (works through client-isolated Wi-Fi)
 *   2. Direct HTTP fallback (same LAN, no firewall issues)
 *
 * Direct-to-Disk:
 *   - Uses the FileSystem Writable Stream API if available.
 *   - Saves chunks directly to disk without bloating RAM.
 *   - Falls back to Blob memory buffers if picker is denied or unsupported.
 *
 * Live Pipe:
 *   - Renders a terminal simulator scrolling live output from standard input.
 *   - Implements both WebRTC text data pipe and SSE (Server-Sent Events) live log bridge.
 */

'use strict';

// ── Constants ─────────────────────────────────────────────────────────────────
const STUN_SERVERS    = [
  { urls: 'stun:stun.l.google.com:19302' },
  { urls: 'stun:stun1.l.google.com:19302' },
];
const CIRCUMFERENCE   = 2 * Math.PI * 42; // SVG progress ring

// ── State ──────────────────────────────────────────────────────────────────────
let currentFile      = null;
let transferMode     = 'http';   // 'webrtc' | 'http'
let startTime        = 0;
let receivedBytes    = 0;
let totalBytes       = 0;
let receivedChunks   = [];
let isLivePipeMode   = false;
let liveBacklogText  = "";

let initialOffset    = 0;
let lastSavedOffset  = 0;

function checkResumeState() {
  try {
    const saved = localStorage.getItem('beam_resume');
    if (saved) {
      const data = JSON.parse(saved);
      if (data.name === currentFile.name && data.size === currentFile.size) {
        if (confirm(`Resume partial download of ${currentFile.name} from ${formatBytes(data.offset)}?`)) {
          initialOffset = data.offset;
          receivedBytes = initialOffset;
          lastSavedOffset = initialOffset;
          return;
        }
      }
    }
  } catch (e) {}
  localStorage.removeItem('beam_resume');
  initialOffset = 0;
  lastSavedOffset = 0;
}

function maybeSaveProgress() {
  if (!currentFile || currentFile.size === -1) return;
  if (receivedBytes - lastSavedOffset >= 1024 * 1024) {
    localStorage.setItem('beam_resume', JSON.stringify({
      name: currentFile.name,
      size: currentFile.size,
      offset: receivedBytes
    }));
    lastSavedOffset = receivedBytes;
  }
}

// Direct-to-disk states
let diskWritableStream = null;
let diskFileHandle     = null;
let webrtcDataChannel  = null;
let currentShareURL    = "";
let useIndexedDB       = false;

// ── IndexedDB Buffering ────────────────────────────────────────────────────────
const IDB_NAME = 'GazeTransferDB';
const IDB_STORE = 'chunks';
let idb = null;
let idbChunkIndex = 0;

function initIDB() {
  return new Promise((resolve, reject) => {
    const req = indexedDB.open(IDB_NAME, 1);
    req.onupgradeneeded = (e) => {
      const db = e.target.result;
      if (db.objectStoreNames.contains(IDB_STORE)) {
        db.deleteObjectStore(IDB_STORE);
      }
      db.createObjectStore(IDB_STORE);
    };
    req.onsuccess = (e) => {
      idb = e.target.result;
      const tx = idb.transaction(IDB_STORE, 'readonly');
      const countReq = tx.objectStore(IDB_STORE).count();
      countReq.onsuccess = (e2) => {
        idbChunkIndex = e2.target.result;
        resolve();
      };
      countReq.onerror = () => resolve();
    };
    req.onerror = (e) => reject(e.target.error);
  });
}

async function clearIDB() {
  if (!idb) await initIDB();
  return new Promise((resolve, reject) => {
    const tx = idb.transaction(IDB_STORE, 'readwrite');
    tx.objectStore(IDB_STORE).clear();
    tx.oncomplete = () => {
      idbChunkIndex = 0;
      resolve();
    };
    tx.onerror = (e) => reject(e.target.error);
  });
}

function storeChunkIDB(chunk) {
  return new Promise((resolve, reject) => {
    const tx = idb.transaction(IDB_STORE, 'readwrite');
    const index = idbChunkIndex++;
    tx.objectStore(IDB_STORE).put(chunk, index);
    tx.oncomplete = () => resolve();
    tx.onerror = (e) => reject(e.target.error);
  });
}

function getAllChunksIDB() {
  return new Promise((resolve, reject) => {
    const tx = idb.transaction(IDB_STORE, 'readonly');
    const req = tx.objectStore(IDB_STORE).getAll();
    req.onsuccess = (e) => resolve(e.target.result);
    req.onerror = (e) => reject(e.target.error);
  });
}

// ── State machine ─────────────────────────────────────────────────────────────
const STATES = ['loading', 'ready', 'webrtc', 'downloading', 'livepipe', 'done', 'error'];

function setState(name) {
  STATES.forEach((s) => {
    const el = document.getElementById(`state-${s}`);
    if (el) el.classList.toggle('hidden', s !== name);
  });
  // Re-trigger entry animation
  const el = document.getElementById(`state-${name}`);
  if (el) { el.style.animation = 'none'; void el.offsetWidth; el.style.animation = ''; }
}

function setLoadingSub(text) {
  const el = document.getElementById('loading-sub');
  if (el) el.textContent = text;
}

function setWebRTCSub(text) {
  const el = document.getElementById('webrtc-sub');
  if (el) el.textContent = text;
}

function markStep(id, done = true) {
  const el = document.getElementById(id);
  if (!el) return;
  el.classList.toggle('step-done', done);
  el.classList.toggle('step-active', !done);
}

function setMode(mode, label) {
  transferMode = mode;
  const dot   = document.getElementById('mode-dot');
  const lbl   = document.getElementById('mode-label');
  const foot  = document.getElementById('transfer-mode-footer');
  if (dot)  dot.className  = `mode-dot mode-${mode}`;
  if (lbl)  lbl.textContent = label;
  if (foot) foot.textContent = label;
}

// ── Service Worker Pipe ───────────────────────────────────────────────────────
async function getSWPipe(fileMeta) {
  if (!('serviceWorker' in navigator)) return null;
  
  let reg = await navigator.serviceWorker.ready;
  let sw = reg.active || navigator.serviceWorker.controller;
  if (!sw) return null;

  const swUrl = `/sw-download-pipe/${Math.random().toString(36).substring(2)}`;
  const channel = new MessageChannel();
  const port = channel.port1;
  
  sw.postMessage({
    type: 'INIT_PORT',
    url: swUrl,
    filename: fileMeta.name,
    size: fileMeta.size,
    mime: fileMeta.mime
  }, [channel.port2]);

  const iframe = document.createElement('iframe');
  iframe.hidden = true;
  iframe.src = swUrl;
  document.body.appendChild(iframe);
  
  return port;
}

const RAM_WARNING_THRESHOLD = 500 * 1024 * 1024; // 500 MB

function checkRamWarning(size) {
  return new Promise((resolve) => {
    const useDiskStream = typeof window.showSaveFilePicker === 'function';
    const swSupported = 'serviceWorker' in navigator;
    // We shouldn't warn if disk stream, service worker stream, or IndexedDB are used.
    // Wait, IndexedDB buffering is always used as a fallback now. So maybe we don't need warning?
    // Actually, IndexedDB is used, but we'll stick to the original logic or improve it:
    // If we have swSupported, we don't need to warn.
    if (useDiskStream || swSupported || size <= RAM_WARNING_THRESHOLD || size === -1) {
      resolve(true);
      return;
    }
    
    const modal = document.getElementById('ram-warning-modal');
    if (!modal) {
      resolve(true);
      return;
    }
    
    modal.classList.remove('hidden');
    
    const abortBtn = document.getElementById('btn-ram-abort');
    const proceedBtn = document.getElementById('btn-ram-proceed');
    
    const cleanup = () => {
      modal.classList.add('hidden');
      abortBtn.removeEventListener('click', onAbort);
      proceedBtn.removeEventListener('click', onProceed);
    };
    
    const onAbort = () => { cleanup(); resolve(false); };
    const onProceed = () => { cleanup(); resolve(true); };
    
    abortBtn.addEventListener('click', onAbort);
    proceedBtn.addEventListener('click', onProceed);
  });
}

// ── Bootstrap ─────────────────────────────────────────────────────────────────
function init() {
  if ('serviceWorker' in navigator) {
    navigator.serviceWorker.register('/sw.js', { scope: '/' }).catch(err => {
      console.warn('Service Worker registration failed:', err);
    });
  }

  setState('loading');
  setMode('connecting', 'Connecting…');

  document.getElementById('btn-download')?.addEventListener('click', startHTTPDownload);
  document.getElementById('btn-upload-trigger')?.addEventListener('click', () => {
    document.getElementById('file-upload-input')?.click();
  });
  document.getElementById('file-upload-input')?.addEventListener('change', handleUploadFile);
  document.getElementById('btn-retry')?.addEventListener('click', () => {
    resetState();
    setTimeout(bootstrap, 400);
  });
  document.getElementById('btn-again')?.addEventListener('click', () => {
    resetState();
    setTimeout(bootstrap, 400);
  });

  document.getElementById('btn-copy-share-url')?.addEventListener('click', () => {
    if (currentShareURL) {
      navigator.clipboard.writeText(currentShareURL);
      const btn = document.getElementById('btn-copy-share-url');
      const old = btn.textContent;
      btn.textContent = "Copied URL!";
      setTimeout(() => btn.textContent = old, 1500);
    }
  });

  // Live pipe control listeners
  document.getElementById('btn-terminal-copy')?.addEventListener('click', () => {
    const pre = document.getElementById('terminal-pre');
    if (pre) {
      navigator.clipboard.writeText(pre.innerText);
      const btn = document.getElementById('btn-terminal-copy');
      const old = btn.textContent;
      btn.textContent = "Copied!";
      setTimeout(() => btn.textContent = old, 1500);
    }
  });

  document.getElementById('btn-terminal-download')?.addEventListener('click', async () => {
    if (useIndexedDB) {
      const chunks = await getAllChunksIDB();
      const blob = new Blob(chunks, { type: 'text/plain;charset=utf-8' });
      triggerSave(blob, currentFile ? currentFile.name : 'stream.log');
    } else {
      const pre = document.getElementById('terminal-pre');
      if (pre) {
        const blob = new Blob([pre.innerText], { type: 'text/plain;charset=utf-8' });
        triggerSave(blob, currentFile ? currentFile.name : 'stream.log');
      }
    }
  });

  initSpotlight();
  bootstrap();
}

function resetState() {
  currentFile = null;
  receivedChunks = [];
  receivedBytes = 0;
  isLivePipeMode = false;
  liveBacklogText = "";
  diskWritableStream = null;
  diskFileHandle = null;
  webrtcDataChannel = null;
  currentShareURL = "";
  useIndexedDB = false;
  if (idb) clearIDB().catch(console.error);
  document.getElementById('done-share-container')?.classList.add('hidden');
  const pre = document.getElementById('terminal-pre');
  if (pre) pre.innerHTML = '<span class="term-dim">// Waiting for stream input...</span>';
}

async function bootstrap() {
  const params = new URLSearchParams(window.location.search);
  const isWebRTCMode = params.get('mode') === 'webrtc' || params.get('sdp') || params.get('offer');
  const localURL = params.get('local');

  if (localURL) {
    try {
      setLoadingSub('Attempting direct local connection…');
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), 1500);
      
      const res = await fetch(`${localURL}/api/meta`, { signal: controller.signal });
      clearTimeout(timeoutId);
      
      if (res.ok) {
        console.log("Local connection successful, redirecting...");
        const newUrl = new URL(localURL);
        if (params.get('mode')) newUrl.searchParams.set('mode', params.get('mode'));
        if (params.get('sdp')) newUrl.searchParams.set('sdp', params.get('sdp'));
        newUrl.hash = window.location.hash;
        window.location.replace(newUrl.href);
        return;
      }
    } catch (e) {
      console.warn("Direct local connection failed, falling back to relay:", e);
    }
  }

  if (isWebRTCMode && typeof RTCPeerConnection !== 'undefined') {
    setLoadingSub('Connecting WebRTC signaling tunnel…');
    try {
      await startWebRTC();
      return;
    } catch (err) {
      console.warn('WebRTC failed, falling back to HTTP:', err);
    }
  }

  // Fallback: regular HTTP mode
  setLoadingSub('Fetching file metadata…');
  await fetchMetaAndShowReady();
}

// ── HTTP mode ─────────────────────────────────────────────────────────────────
async function fetchMetaAndShowReady() {
  try {
    const res = await fetch(apiPath('/api/meta'));
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    currentFile = await res.json();

    if (currentFile.size === -1) {
      // Live Pipe mode
      isLivePipeMode = true;
      setMode('http', 'Live Log (HTTP SSE)');
      startHTTPSSE();
    } else {
      checkResumeState();
      renderFileCard(currentFile);
      setMode('http', 'Direct LAN transfer');
      setState('ready');
    }
  } catch (err) {
    showError(`Could not reach the sender: ${err.message}`);
  }
}

async function startHTTPDownload() {
  if (!currentFile) return;

  const proceed = await checkRamWarning(currentFile.size);
  if (!proceed) {
    return;
  }

  startTime     = Date.now();
  receivedBytes = 0;
  totalBytes    = currentFile.size;

  // Try to use FileSystem API for streaming to disk if supported
  const useDiskStream = typeof window.showSaveFilePicker === 'function';
  const swSupported = 'serviceWorker' in navigator;
  useIndexedDB = !useDiskStream && !swSupported;

  if (!useDiskStream && !swSupported) {
    console.warn("Advanced streaming not supported. Large files may cause Out of Memory errors.");
    alert("Warning: Streaming direct-to-disk is not supported in this browser. Large files may fail due to RAM limits.");
  }

  let swPipePort = null;
  if (useDiskStream) {
    try {
      diskFileHandle = await window.showSaveFilePicker({
        suggestedName: currentFile.name,
      });
      diskWritableStream = await diskFileHandle.createWritable();
      useIndexedDB = false;
    } catch (pickerErr) {
      console.warn("Direct-to-disk picker cancelled/failed, falling back to SW or IndexedDB:", pickerErr);
      diskWritableStream = null;
      useIndexedDB = !swSupported;
    }
  }

  if (!diskWritableStream && swSupported) {
    swPipePort = await getSWPipe(currentFile);
  } else if (!diskWritableStream && useIndexedDB) {
    if (initialOffset === 0) {
      await clearIDB();
    }
    receivedChunks = [];
  }

  setState('downloading');
  updateProgress(initialOffset / totalBytes || 0);

  try {
    const headers = {};
    if (initialOffset > 0) {
      headers['Range'] = `bytes=${initialOffset}-`;
    }
    const res = await fetch(apiPath('/api/download'), { headers });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);

    const reader = res.body.getReader();
    let received = initialOffset;
    
    let decryptionKey = null;
    let encBuffer = new Uint8Array(0);
    if (window.location.hash.includes('k=')) {
      try {
        const b64 = window.location.hash.split('k=')[1].split('&')[0];
        const raw = Uint8Array.from(atob(b64.replace(/-/g, '+').replace(/_/g, '/')), c => c.charCodeAt(0));
        decryptionKey = await crypto.subtle.importKey(
          "raw", raw, { name: "AES-GCM" }, false, ["decrypt"]
        );
      } catch(e) {
        console.error("Failed to import decryption key", e);
      }
    }

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;


      if (decryptionKey) {
        let newBuffer = new Uint8Array(encBuffer.length + value.length);
        newBuffer.set(encBuffer, 0);
        newBuffer.set(value, encBuffer.length);
        encBuffer = newBuffer;

        while (encBuffer.length >= 4) {
          const dv = new DataView(encBuffer.buffer, encBuffer.byteOffset, encBuffer.byteLength);
          const frameLen = dv.getUint32(0, false);
          if (encBuffer.length >= 4 + frameLen) {
            const frame = encBuffer.slice(4, 4 + frameLen);
            encBuffer = encBuffer.slice(4 + frameLen);
            
            const nonce = frame.slice(0, 12);
            const ciphertext = frame.slice(12);
            const decrypted = await crypto.subtle.decrypt(
              { name: "AES-GCM", iv: nonce },
              decryptionKey,
              ciphertext
            );
            const decValue = new Uint8Array(decrypted);
            
            if (diskWritableStream) {
              await diskWritableStream.write(decValue);
            } else if (swPipePort) {
              swPipePort.postMessage(decValue);
            } else if (useIndexedDB) {
              await storeChunkIDB(decValue);
            } else {
              receivedChunks.push(decValue);
            }
            received += decValue.length;
          } else {
            break;
          }
        }
      } else {
        if (diskWritableStream) {
          await diskWritableStream.write(value);
        } else if (swPipePort) {
          swPipePort.postMessage(value);
        } else if (useIndexedDB) {
          await storeChunkIDB(value);
        } else {
          receivedChunks.push(value);
        }
        received += value.length;
      }


      receivedBytes = received;
      maybeSaveProgress();
      if (totalBytes > 0) {
        updateProgress(received / totalBytes);
        updateDLStats(received, totalBytes);
        updateSpeed(received);
      }
    }

    if (diskWritableStream) {
      await diskWritableStream.close();
    } else if (swPipePort) {
      swPipePort.postMessage('EOF');
    } else {
      let finalChunks = receivedChunks;
      if (useIndexedDB) {
        finalChunks = await getAllChunksIDB();
        await clearIDB();
      }
      triggerSave(new Blob(finalChunks, { type: currentFile.mime }), currentFile.name);
    }

    let modeDesc = diskWritableStream ? 'LAN HTTP (Direct Disk)' : (swPipePort ? 'LAN HTTP (SW Pipe)' : (useIndexedDB ? 'LAN HTTP (IndexedDB)' : 'LAN HTTP (RAM Blob)'));
    showDone(currentFile.name, currentFile.size, modeDesc);

  } catch (err) {
    showError(`Download failed: ${err.message}`);
  }
}

// ── HTTP SSE (Server-Sent Events) live log streamer ─────────────────────────
async function startHTTPSSE() {
  setState('livepipe');
  const source = new EventSource(apiPath('/api/live/stream'));
  const pre = document.getElementById('terminal-pre');
  if (pre) pre.textContent = "";

  const useDiskStream = typeof window.showSaveFilePicker === 'function';
  useIndexedDB = !useDiskStream;
  if (useIndexedDB) {
    await clearIDB();
  }

  source.onmessage = (event) => {
    try {
      const msg = JSON.parse(event.data);
      if (msg.type === "backlog" || msg.type === "data") {
        appendTerminalText(msg.payload);
        if (useIndexedDB) {
          storeChunkIDB(msg.payload);
        }
      } else if (msg.type === "eof") {
        source.close();
        // Change pulsing dot status
        const dot = document.querySelector('.live-pulse-dot');
        if (dot) {
          dot.style.background = 'var(--zinc-600)';
          dot.style.boxShadow = 'none';
          dot.style.animation = 'none';
        }
        showDone('Stream Log', receivedBytes, 'HTTP SSE Stream');
      }
    } catch (err) {
      console.error("SSE parse error:", err);
    }
  };

  source.onerror = () => {
    source.close();
    showError('SSE stream connection terminated.');
  };
}

// ── Decompression helper ──────────────────────────────────────────────────────
async function decompressOffer(base64Str) {
  // Decode URL-safe base64
  let b64 = base64Str.replace(/-/g, '+').replace(/_/g, '/');
  // Pad with '=' if necessary
  while (b64.length % 4 !== 0) {
    b64 += '=';
  }
  const binStr = atob(b64);
  const len = binStr.length;
  const bytes = new Uint8Array(len);
  for (let i = 0; i < len; i++) {
    bytes[i] = binStr.charCodeAt(i);
  }
  
  const decompressed = pako.inflate(bytes);
  return new TextDecoder().decode(decompressed);
}

// ── WebRTC P2P mode ───────────────────────────────────────────────────────────
async function startWebRTC() {
  setState('webrtc');
  setMode('webrtc', 'WebRTC P2P (optical handshake)');

  // 1. Fetch the full SDP offer from the server.
  let offer;
  const params = new URLSearchParams(window.location.search);
  const embeddedOffer = params.get('sdp') || params.get('offer');

  if (embeddedOffer) {
    setWebRTCSub('Decoding embedded SDP offer…');
    try {
      const decompressedSDP = await decompressOffer(embeddedOffer);
      offer = {
        type: 'offer',
        sdp: decompressedSDP,
        iceServers: STUN_SERVERS
      };
    } catch (err) {
      console.warn("Failed to decompress embedded offer, falling back to HTTP fetch", err);
    }
  }

  if (!offer) {
    setWebRTCSub('Fetching SDP offer…');
    const offerRes = await fetch(apiPath('/api/signal/offer'));
    if (!offerRes.ok) throw new Error(`offer fetch: HTTP ${offerRes.status}`);
    offer = await offerRes.json();
  }
  markStep('step-offer');

  // 2. Create peer connection and set remote description.
  const pc = new RTCPeerConnection({ iceServers: offer.iceServers || STUN_SERVERS });

  // Also fetch file meta in parallel.
  const metaPromise = fetch(apiPath('/api/meta')).then(r => r.json());

  await pc.setRemoteDescription(new RTCSessionDescription(offer));

  // 3. Create answer.
  setWebRTCSub('Creating answer…');
  const answer = await pc.createAnswer();
  await pc.setLocalDescription(answer);

  // Wait for ICE gathering.
  await new Promise((resolve) => {
    if (pc.iceGatheringState === 'complete') { resolve(); return; }
    pc.addEventListener('icegatheringstatechange', () => {
      if (pc.iceGatheringState === 'complete') resolve();
    });
    setTimeout(resolve, 3000); // safety timeout
  });

  // 4. POST answer to sender.
  setWebRTCSub('Sending answer to sender…');
  const answerRes = await fetch(apiPath('/api/signal/answer'), {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(pc.localDescription),
  });
  if (!answerRes.ok) throw new Error(`answer POST: HTTP ${answerRes.status}`);
  markStep('step-answer');

  // 5. Fetch and add ICE candidates from sender.
  setWebRTCSub('Exchanging ICE candidates…');
  try {
    const candRes  = await fetch(apiPath('/api/signal/candidates'));
    const cands    = await candRes.json();
    for (const c of cands) {
      await pc.addIceCandidate(new RTCIceCandidate(c));
    }
  } catch { /* non-fatal — trickle ICE will handle it */ }
  markStep('step-ice');

  // 6. Wait for data channel from sender.
  setWebRTCSub('Waiting for data channel…');
  const dc = await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('data channel timeout')), 30000);
    pc.ondatachannel = (e) => { clearTimeout(timer); resolve(e.channel); };
  });
  markStep('step-open');
  webrtcDataChannel = dc;

  // 7. Resolve file metadata.
  currentFile = await metaPromise;
  totalBytes  = currentFile.size;

  if (currentFile.size !== -1) {
    checkResumeState();
  }

  const proceed = await checkRamWarning(currentFile.size);
  if (!proceed) {
    if (webrtcDataChannel) webrtcDataChannel.close();
    pc.close();
    showError('Transfer aborted by user. File too large for memory.');
    return;
  }

  if (currentFile.size === -1) {
    // ── Live log stream over WebRTC data channel ─────────────────────────────
    isLivePipeMode = true;
    setState('livepipe');
    setMode('webrtc', 'WebRTC Live Log');
    const pre = document.getElementById('terminal-pre');
    if (pre) pre.textContent = "";

    const useDiskStream = typeof window.showSaveFilePicker === 'function';
    useIndexedDB = !useDiskStream;
    if (useIndexedDB) {
      clearIDB();
    }

    dc.onmessage = (e) => {
      if (typeof e.data === 'string') {
        if (e.data === "EOF") {
          // Finished
          const dot = document.querySelector('.live-pulse-dot');
          if (dot) {
            dot.style.background = 'var(--zinc-600)';
            dot.style.boxShadow = 'none';
            dot.style.animation = 'none';
          }
          showDone('Stream Log', receivedBytes, 'WebRTC Stream');
          pc.close();
        } else {
          appendTerminalText(e.data);
          if (useIndexedDB) {
            storeChunkIDB(e.data);
          }
        }
      }
    };
  } else {
    // ── Direct-to-Disk File transfer over WebRTC data channel ──────────────────
    const useDiskStream = typeof window.showSaveFilePicker === 'function';
    const swSupported = 'serviceWorker' in navigator;
    useIndexedDB = !useDiskStream && !swSupported;

    if (!useDiskStream && !swSupported) {
      console.warn("Advanced streaming not supported. Large files may cause Out of Memory errors.");
      alert("Warning: Streaming direct-to-disk is not supported in this browser. Large files may fail due to RAM limits.");
    }

    let swPipePort = null;
    if (useDiskStream) {
      try {
        diskFileHandle = await window.showSaveFilePicker({
          suggestedName: currentFile.name,
        });
        diskWritableStream = await diskFileHandle.createWritable({ keepExistingData: true });
        if (initialOffset > 0) {
          await diskWritableStream.seek(initialOffset);
        }
        useIndexedDB = false;
      } catch (pickerErr) {
        console.warn("WebRTC Direct-to-disk picker cancelled, falling back to SW or IndexedDB:", pickerErr);
        diskWritableStream = null;
        useIndexedDB = !swSupported;
      }
    }

    if (!diskWritableStream && swSupported) {
      swPipePort = await getSWPipe(currentFile);
    } else if (!diskWritableStream && useIndexedDB) {
      if (initialOffset === 0) {
        await clearIDB();
      }
      receivedChunks = [];
    }

    setState('downloading');
    startTime     = Date.now();
    updateProgress(initialOffset / totalBytes || 0);

    await new Promise((resolve, reject) => {
      dc.binaryType = 'arraybuffer';
      if (dc.readyState === 'open') {
        dc.send(`OFFSET:${initialOffset}`);
      } else {
        dc.onopen = () => dc.send(`OFFSET:${initialOffset}`);
      }

      dc.onmessage = async (e) => {
        if (typeof e.data === 'string') {
          if (e.data === "EOF") {
            if (diskWritableStream) {
              await diskWritableStream.close();
            } else if (swPipePort) {
              swPipePort.postMessage("EOF");
            } else {
              let finalChunks = receivedChunks;
              if (useIndexedDB) {
                finalChunks = await getAllChunksIDB();
                await clearIDB();
              }
              triggerSave(new Blob(finalChunks, { type: currentFile.mime }), currentFile.name);
            }
            resolve();
          }
          return;
        }

        const chunk = new Uint8Array(e.data);
        if (diskWritableStream) {
          // Direct disk write
          await diskWritableStream.write(chunk);
        } else if (swPipePort) {
          swPipePort.postMessage(chunk);
        } else if (useIndexedDB) {
          // IndexedDB buffer
          await storeChunkIDB(chunk);
        } else {
          // RAM buffer
          receivedChunks.push(chunk);
        }

        receivedBytes += chunk.byteLength;
        maybeSaveProgress();
        if (totalBytes > 0) {
          updateProgress(receivedBytes / totalBytes);
          updateDLStats(receivedBytes, totalBytes);
          updateSpeed(receivedBytes);
        }
      };

      dc.onerror = (e) => reject(new Error('data channel error: ' + e));
      dc.onclose = () => {
        if (receivedBytes < totalBytes) reject(new Error('channel closed early'));
      };
    });

    let modeDesc = diskWritableStream ? 'WebRTC P2P (Direct Disk)' : (swPipePort ? 'WebRTC P2P (SW Pipe)' : (useIndexedDB ? 'WebRTC P2P (IndexedDB)' : 'WebRTC P2P (RAM Blob)'));
    showDone(currentFile.name, currentFile.size, modeDesc);
    pc.close();
  }
}

// ── Phone-to-Laptop Upload Handler ───────────────────────────────────────────
async function handleUploadFile(e) {
  const file = e.target.files[0];
  if (!file) return;

  if (transferMode === 'webrtc' && webrtcDataChannel && webrtcDataChannel.readyState === 'open') {
    webrtcDataChannel.send("UPLOAD_META:" + file.name + ":" + file.size);
    document.getElementById('dl-label').textContent = "Uploading to Laptop…";
    setState('downloading');
    startTime = Date.now();
    receivedBytes = 0;
    updateProgress(0);

    const buffer = await file.arrayBuffer();
    const chunkSize = 65536;
    let offset = 0;

    while (offset < buffer.byteLength) {
      const chunk = buffer.slice(offset, offset + chunkSize);
      while (webrtcDataChannel.bufferedAmount > 1024 * 1024) {
        await new Promise(resolve => setTimeout(resolve, 10));
      }
      webrtcDataChannel.send(chunk);
      offset += chunk.byteLength;

      updateProgress(offset / buffer.byteLength);
      updateDLStats(offset, buffer.byteLength);
      updateSpeed(offset);
    }

    while (webrtcDataChannel.bufferedAmount > 0) {
      await new Promise(resolve => setTimeout(resolve, 10));
    }
    webrtcDataChannel.send("UPLOAD_EOF");
    showDone(file.name, file.size, "WebRTC P2P Upload");
  } else {
    const xhr = new XMLHttpRequest();
    const formData = new FormData();
    formData.append('file', file);

    document.getElementById('dl-label').textContent = "Uploading to Laptop…";
    setState('downloading');
    startTime = Date.now();
    updateProgress(0);

    xhr.upload.onprogress = (event) => {
      if (event.lengthComputable) {
        updateProgress(event.loaded / event.total);
        updateDLStats(event.loaded, event.total);
        updateSpeed(event.loaded);
      }
    };

    xhr.onload = () => {
      if (xhr.status === 200) {
        showDone(file.name, file.size, "HTTP Upload");
      } else {
        showError("Upload failed with status: " + xhr.status);
      }
    };

    xhr.onerror = () => {
      showError("Upload failed due to network error.");
    };

    xhr.open('POST', apiPath('/api/upload'), true);
    xhr.send(formData);
  }
}

// ── Helpers ───────────────────────────────────────────────────────────────────
function renderFileCard(meta) {
  document.getElementById('file-name').textContent = meta.name;
  document.getElementById('file-size').textContent = formatBytes(meta.size);
  document.getElementById('file-mime').textContent = mimeLabel(meta.mime);
  document.getElementById('file-icon-wrap').innerHTML = mimeIcon(meta.mime);
}

function triggerSave(blob, name) {
  const url = URL.createObjectURL(blob);
  const a   = Object.assign(document.createElement('a'), { href: url, download: name });
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  setTimeout(() => URL.revokeObjectURL(url), 2000);
}

function appendTerminalText(text) {
  const pre = document.getElementById('terminal-pre');
  if (!pre) return;
  
  pre.appendChild(document.createTextNode(text));
  receivedBytes += text.length;

  // Prune terminal buffer to prevent DOM memory leaks and UI lag
  const MAX_NODES = 10000;
  while (pre.childNodes.length > MAX_NODES) {
    pre.removeChild(pre.firstChild);
  }

  // Auto-scroll terminal body
  const body = document.querySelector('.terminal-body');
  if (body) {
    body.scrollTop = body.scrollHeight;
  }
}

function showDone(name, size, mode) {
  localStorage.removeItem('beam_resume');
  document.getElementById('done-sub').textContent = `${name} · ${formatBytes(size)}`;
  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  const speed   = formatBytes(size / (elapsed || 1)) + '/s';
  document.getElementById('done-meta').innerHTML =
    `<span>${mode}</span><span>${elapsed}s · avg ${speed}</span>`;

  const doneTitle = document.getElementById('done-title');
  const doneShare = document.getElementById('done-share-container');

  if (mode.includes("Upload")) {
    if (doneTitle) doneTitle.textContent = "File Shared Successfully!";
    
    // Create sharing link pointing to the laptop's file server
    let shareLink = window.location.origin;
    if (transferMode === 'webrtc') {
      shareLink += "?mode=webrtc";
    }
    currentShareURL = shareLink;

    // Load QR PNG dynamically from the server's newly added QR API
    const qrImg = document.getElementById('done-qr-img');
    if (qrImg) {
      qrImg.src = apiPath("/api/qr") + (apiPath("/api/qr").includes('?') ? '&' : '?') + "url=" + encodeURIComponent(shareLink);
    }
    
    if (doneShare) doneShare.classList.remove('hidden');
  } else {
    if (doneTitle) doneTitle.textContent = "Transfer complete";
    if (doneShare) doneShare.classList.add('hidden');
  }

  setState('done');
}

function showError(msg) {
  if (useIndexedDB) clearIDB().catch(console.error);
  document.getElementById('error-msg').textContent = msg;
  setState('error');
}

// ── Progress ──────────────────────────────────────────────────────────────────
function updateProgress(fraction) {
  const pct    = Math.round(fraction * 100);
  const circle = document.getElementById('progress-circle');
  const label  = document.getElementById('progress-pct');
  const wrap   = document.getElementById('progress-wrap');
  if (circle) circle.style.strokeDashoffset = CIRCUMFERENCE * (1 - fraction);
  if (label)  label.textContent = `${pct}%`;
  if (wrap)   wrap.setAttribute('aria-valuenow', pct);
}

function updateDLStats(received, total) {
  const el = document.getElementById('dl-stats');
  if (el) el.textContent = `${formatBytes(received)} / ${formatBytes(total)}`;
}

let lastSpeedSample = { t: 0, b: 0 };
function updateSpeed(received) {
  const now   = Date.now();
  const dt    = (now - lastSpeedSample.t) / 1000;
  if (dt < 0.5) return;
  const speed = (received - lastSpeedSample.b) / dt;
  lastSpeedSample = { t: now, b: received };
  const el = document.getElementById('dl-speed');
  if (el) el.textContent = formatBytes(speed) + '/s';
}

// ── Spotlight card effect ─────────────────────────────────────────────────────
function initSpotlight() {
  const card = document.getElementById('file-card');
  if (!card) return;
  card.addEventListener('mousemove', (e) => {
    const r = card.getBoundingClientRect();
    card.style.setProperty('--mouse-x', `${((e.clientX - r.left) / r.width * 100)}%`);
    card.style.setProperty('--mouse-y', `${((e.clientY - r.top)  / r.height * 100)}%`);
  });
}

// ── Format helpers ────────────────────────────────────────────────────────────
function formatBytes(bytes) {
  if (!bytes || bytes === 0) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(1024));
  return `${(bytes / Math.pow(1024, i)).toFixed(1)} ${units[i]}`;
}

function mimeLabel(mime) {
  const labels = {
    'application/pdf':'PDF','application/zip':'ZIP','application/gzip':'GZ',
    'application/x-tar':'TAR','video/mp4':'MP4','video/x-matroska':'MKV',
    'audio/mpeg':'MP3','image/png':'PNG','image/jpeg':'JPEG','image/gif':'GIF',
    'image/webp':'WEBP','text/plain':'TXT','text/markdown':'MD',
    'text/html':'HTML','application/json':'JSON','text/csv':'CSV',
  };
  return labels[mime] || 'FILE';
}

function mimeIcon(mime) {
  const a = `fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"`;
  if (mime?.startsWith('image/'))
    return `<svg width="32" height="32" viewBox="0 0 24 24" ${a} aria-hidden="true"><rect x="3" y="3" width="18" height="18" rx="2"/><circle cx="8.5" cy="8.5" r="1.5"/><polyline points="21 15 16 10 5 21"/></svg>`;
  if (mime?.startsWith('video/'))
    return `<svg width="32" height="32" viewBox="0 0 24 24" ${a} aria-hidden="true"><polygon points="23 7 16 12 23 17 23 7"/><rect x="1" y="5" width="15" height="14" rx="2"/></svg>`;
  if (mime?.startsWith('audio/'))
    return `<svg width="32" height="32" viewBox="0 0 24 24" ${a} aria-hidden="true"><path d="M9 18V5l12-2v13"/><circle cx="6" cy="18" r="3"/><circle cx="18" cy="16" r="3"/></svg>`;
  if (mime === 'application/pdf')
    return `<svg width="32" height="32" viewBox="0 0 24 24" ${a} aria-hidden="true"><path d="M14 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V8z"/><polyline points="14 2 14 8 20 8"/></svg>`;
  if (mime?.includes('zip')||mime?.includes('tar')||mime?.includes('gzip'))
    return `<svg width="32" height="32" viewBox="0 0 24 24" ${a} aria-hidden="true"><path d="M21 16V8a2 2 0 00-1-1.73l-7-4a2 2 0 00-2 0l-7 4A2 2 0 003 8v8a2 2 0 001 1.73l7 4a2 2 0 002 0l7-4A2 2 0 0021 16z"/><polyline points="3.27 6.96 12 12.01 20.73 6.96"/><line x1="12" y1="22.08" x2="12" y2="12"/></svg>`;
  return `<svg width="32" height="32" viewBox="0 0 24 24" ${a} aria-hidden="true"><path d="M14 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V8z"/><polyline points="14 2 14 8 20 8"/></svg>`;
}

// ── Entry point ───────────────────────────────────────────────────────────────
if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', init);
} else {
  init();
}

window.addEventListener('pagehide', () => {
  if (useIndexedDB && idb) {
    // Attempt best-effort synchronous-like clear
    try {
      const tx = idb.transaction(IDB_STORE, 'readwrite');
      tx.objectStore(IDB_STORE).clear();
    } catch (e) {
      // Ignore errors on unload
    }
  }
});
