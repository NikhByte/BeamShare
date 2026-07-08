// Stream-Saver Service Worker Pipe
const streamMap = new Map();

self.addEventListener('install', (event) => {
  self.skipWaiting();
});

self.addEventListener('activate', (event) => {
  event.waitUntil(self.clients.claim());
});

self.addEventListener('message', (event) => {
  if (event.data && event.data.type === 'INIT_PORT') {
    const { url, filename, size, mime } = event.data;
    const port = event.ports[0];

    let streamController;
    const stream = new ReadableStream({
      start(controller) {
        streamController = controller;
      },
      cancel() {
        port.postMessage({ type: 'CANCEL' });
      }
    });

    port.onmessage = (e) => {
      if (e.data === 'EOF') {
        streamController.close();
        port.close();
      } else if (e.data.type === 'ABORT') {
        streamController.error(e.data.reason);
        port.close();
      } else {
        streamController.enqueue(e.data); // data is Uint8Array
      }
    };

    streamMap.set(url, { stream, filename, size, mime });
  }
});

self.addEventListener('fetch', (event) => {
  const url = new URL(event.request.url);
  
  // Intercept synthetic download URLs used by the service worker pipe
  if (url.pathname.startsWith('/sw-download-pipe/')) {
    if (streamMap.has(url.pathname)) {
      const { stream, filename, size, mime } = streamMap.get(url.pathname);
      streamMap.delete(url.pathname); // Only download once per URL
      
      const headers = new Headers({
        'Content-Type': mime || 'application/octet-stream',
        'Content-Disposition': `attachment; filename="${encodeURIComponent(filename)}"`
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
