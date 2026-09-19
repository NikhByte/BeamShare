// Stream-Saver Service Worker Pipe
const streamMap = new Map();
const STREAM_TTL_MS = 300 * 1000; // 5 minutes TTL

function formatContentDisposition(filename) {
  const safeFilename = filename || 'download';
  // Fallback: ASCII characters only, strip/replace non-ASCII and sanitize quotes/backslashes
  const fallback = safeFilename
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^\x20-\x7E]/g, '_')
    .replace(/["\\]/g, '\\$&');
  
  // Extended parameter: RFC 5987 / RFC 8187 UTF-8 percent-encoded
  const encoded = encodeURIComponent(safeFilename)
    .replace(/['()*]/g, c => '%' + c.charCodeAt(0).toString(16).toUpperCase());
    
  return `attachment; filename="${fallback}"; filename*=UTF-8''${encoded}`;
}

self.addEventListener('install', (event) => {
  self.skipWaiting();
});

self.addEventListener('activate', (event) => {
  event.waitUntil(self.clients.claim());
});

self.addEventListener('message', (event) => {
  if (event.data && event.data.type === 'INIT_PORT') {
    const { url, filename, size, mime } = event.data;
    const port = event.ports && event.ports[0];
    if (!port) return;

    let streamController = null;
    let isCleanedUp = false;

    const cleanup = () => {
      if (isCleanedUp) return;
      isCleanedUp = true;

      if (ttlTimer) {
        clearTimeout(ttlTimer);
        ttlTimer = null;
      }

      if (streamMap.has(url)) {
        streamMap.delete(url);
      }

      try {
        port.close();
      } catch (_) {}
    };

    let ttlTimer = setTimeout(() => {
      if (streamController) {
        try {
          streamController.error(new Error('Stream TTL expired'));
        } catch (_) {}
      }
      cleanup();
    }, STREAM_TTL_MS);

    const stream = new ReadableStream({
      start(controller) {
        streamController = controller;
      },
      cancel(reason) {
        try {
          port.postMessage({ type: 'CANCEL' });
        } catch (_) {}
        cleanup();
      }
    });

    port.onmessage = (e) => {
      if (!e || !e.data) return;
      if (e.data === 'EOF' || (typeof e.data === 'object' && e.data.type === 'EOF')) {
        if (streamController) {
          try { streamController.close(); } catch (_) {}
        }
        cleanup();
      } else if (typeof e.data === 'object' && e.data.type === 'ABORT') {
        if (streamController) {
          try { streamController.error(e.data.reason || 'Stream aborted'); } catch (_) {}
        }
        cleanup();
      } else {
        if (streamController) {
          try {
            streamController.enqueue(e.data); // data is Uint8Array
          } catch (err) {
            cleanup();
          }
        }
      }
    };

    port.onmessageerror = () => {
      if (streamController) {
        try { streamController.error(new Error('Port message error')); } catch (_) {}
      }
      cleanup();
    };

    streamMap.set(url, { stream, filename, size, mime, cleanup, ttlTimer, port });
  }
});

self.addEventListener('fetch', (event) => {
  const url = new URL(event.request.url);
  
  // Intercept synthetic download URLs used by the service worker pipe
  if (url.pathname.startsWith('/sw-download-pipe/')) {
    if (streamMap.has(url.pathname)) {
      const entry = streamMap.get(url.pathname);
      const { stream, filename, size, mime, ttlTimer } = entry;
      
      streamMap.delete(url.pathname); // Only download once per URL
      if (ttlTimer) {
        clearTimeout(ttlTimer);
        entry.ttlTimer = null;
      }
      
      const headers = new Headers({
        'Content-Type': mime || 'application/octet-stream',
        'Content-Disposition': formatContentDisposition(filename)
      });
      
      if (size && size > 0) {
        headers.set('Content-Length', size);
      }

      event.respondWith(new Response(stream, { headers }));
    } else {
      // If stream not found, could be an expired link or reload, just return 404
      event.respondWith(new Response('Stream not found or already downloaded.', { status: 404 }));
    }
  }
});

if (typeof module !== 'undefined' && module.exports) {
  module.exports = { streamMap, formatContentDisposition, STREAM_TTL_MS };
}

