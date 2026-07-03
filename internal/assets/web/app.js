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

// Direct-to-disk states
let diskWritableStream = null;
let diskFileHandle     = null;
let webrtcDataChannel  = null;
let currentShareURL    = "";

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

// ── Bootstrap ─────────────────────────────────────────────────────────────────
function init() {
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

  document.getElementById('btn-terminal-download')?.addEventListener('click', () => {
    const pre = document.getElementById('terminal-pre');
    if (pre) {
      const blob = new Blob([pre.innerText], { type: 'text/plain;charset=utf-8' });
      triggerSave(blob, currentFile ? currentFile.name : 'stream.log');
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
  document.getElementById('done-share-container')?.classList.add('hidden');
  const pre = document.getElementById('terminal-pre');
  if (pre) pre.innerHTML = '<span class="term-dim">// Waiting for stream input...</span>';
}

async function bootstrap() {
  const params = new URLSearchParams(window.location.search);
  const isWebRTCMode = params.get('mode') === 'webrtc' || params.get('sdp');

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
    const res = await fetch('/api/meta');
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    currentFile = await res.json();

    if (currentFile.size === -1) {
      // Live Pipe mode
      isLivePipeMode = true;
      setMode('http', 'Live Log (HTTP SSE)');
      startHTTPSSE();
    } else {
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

  startTime     = Date.now();
  receivedBytes = 0;
  totalBytes    = currentFile.size;

  // Try to use FileSystem API for streaming to disk if supported
  const useDiskStream = typeof window.showSaveFilePicker === 'function';

  if (useDiskStream) {
    try {
      diskFileHandle = await window.showSaveFilePicker({
        suggestedName: currentFile.name,
      });
      diskWritableStream = await diskFileHandle.createWritable();
    } catch (pickerErr) {
      console.warn("Direct-to-disk picker cancelled/failed, falling back to RAM Blob:", pickerErr);
      diskWritableStream = null;
    }
  }

  setState('downloading');
  updateProgress(0);

  try {
    const res = await fetch('/api/download');
    if (!res.ok) throw new Error(`HTTP ${res.status}`);

    const total  = parseInt(res.headers.get('Content-Length') || '0', 10);
    const reader = res.body.getReader();
    let received = 0;

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;

      if (diskWritableStream) {
        await diskWritableStream.write(value);
      } else {
        receivedChunks.push(value);
      }

      received += value.length;
      receivedBytes = received;
      if (total > 0) {
        updateProgress(received / total);
        updateDLStats(received, total);
        updateSpeed(received);
      }
    }

    if (diskWritableStream) {
      await diskWritableStream.close();
    } else {
      triggerSave(new Blob(receivedChunks, { type: currentFile.mime }), currentFile.name);
    }

    showDone(currentFile.name, currentFile.size, diskWritableStream ? 'LAN HTTP (Direct Disk)' : 'LAN HTTP (RAM Blob)');

  } catch (err) {
    showError(`Download failed: ${err.message}`);
  }
}

// ── HTTP SSE (Server-Sent Events) live log streamer ─────────────────────────
function startHTTPSSE() {
  setState('livepipe');
  const source = new EventSource('/api/live/stream');
  const pre = document.getElementById('terminal-pre');
  if (pre) pre.textContent = "";

  source.onmessage = (event) => {
    try {
      const msg = JSON.parse(event.data);
      if (msg.type === "backlog" || msg.type === "data") {
        appendTerminalText(msg.payload);
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

// ── WebRTC P2P mode ───────────────────────────────────────────────────────────
async function startWebRTC() {
  setState('webrtc');
  setMode('webrtc', 'WebRTC P2P (optical handshake)');

  // 1. Fetch the full SDP offer from the server.
  setWebRTCSub('Fetching SDP offer…');
  const offerRes = await fetch('/api/signal/offer');
  if (!offerRes.ok) throw new Error(`offer fetch: HTTP ${offerRes.status}`);
  const offer = await offerRes.json();
  markStep('step-offer');

  // 2. Create peer connection and set remote description.
  const pc = new RTCPeerConnection({ iceServers: STUN_SERVERS });

  // Also fetch file meta in parallel.
  const metaPromise = fetch('/api/meta').then(r => r.json());

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
  const answerRes = await fetch('/api/signal/answer', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(pc.localDescription),
  });
  if (!answerRes.ok) throw new Error(`answer POST: HTTP ${answerRes.status}`);
  markStep('step-answer');

  // 5. Fetch and add ICE candidates from sender.
  setWebRTCSub('Exchanging ICE candidates…');
  try {
    const candRes  = await fetch('/api/signal/candidates');
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

  if (currentFile.size === -1) {
    // ── Live log stream over WebRTC data channel ─────────────────────────────
    isLivePipeMode = true;
    setState('livepipe');
    setMode('webrtc', 'WebRTC Live Log');
    const pre = document.getElementById('terminal-pre');
    if (pre) pre.textContent = "";

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
        }
      }
    };
  } else {
    // ── Direct-to-Disk File transfer over WebRTC data channel ──────────────────
    const useDiskStream = typeof window.showSaveFilePicker === 'function';

    if (useDiskStream) {
      try {
        diskFileHandle = await window.showSaveFilePicker({
          suggestedName: currentFile.name,
        });
        diskWritableStream = await diskFileHandle.createWritable();
      } catch (pickerErr) {
        console.warn("WebRTC Direct-to-disk picker cancelled, falling back to RAM Blob:", pickerErr);
        diskWritableStream = null;
      }
    }

    setState('downloading');
    startTime     = Date.now();
    receivedBytes = 0;
    receivedChunks = [];
    updateProgress(0);

    await new Promise((resolve, reject) => {
      dc.binaryType = 'arraybuffer';

      dc.onmessage = async (e) => {
        if (typeof e.data === 'string') {
          if (e.data === "EOF") {
            if (diskWritableStream) {
              await diskWritableStream.close();
            } else {
              triggerSave(new Blob(receivedChunks, { type: currentFile.mime }), currentFile.name);
            }
            resolve();
          }
          return;
        }

        const chunk = new Uint8Array(e.data);
        if (diskWritableStream) {
          // Direct disk write
          await diskWritableStream.write(chunk);
        } else {
          // RAM buffer
          receivedChunks.push(chunk);
        }

        receivedBytes += chunk.byteLength;
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

    showDone(currentFile.name, currentFile.size, diskWritableStream ? 'WebRTC P2P (Direct Disk)' : 'WebRTC P2P (RAM Blob)');
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

    xhr.open('POST', '/api/upload', true);
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

  // Auto-scroll terminal body
  const body = document.querySelector('.terminal-body');
  if (body) {
    body.scrollTop = body.scrollHeight;
  }
}

function showDone(name, size, mode) {
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
      qrImg.src = "/api/qr?url=" + encodeURIComponent(shareLink);
    }
    
    if (doneShare) doneShare.classList.remove('hidden');
  } else {
    if (doneTitle) doneTitle.textContent = "Transfer complete";
    if (doneShare) doneShare.classList.add('hidden');
  }

  setState('done');
}

function showError(msg) {
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
