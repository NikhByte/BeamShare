const { test, describe, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('fs');
const path = require('path');
const { JSDOM } = require('jsdom');
const pako = require('pako');

// Read index.html for DOM fixture
const htmlPath = path.join(__dirname, 'index.html');
const htmlContent = fs.readFileSync(htmlPath, 'utf8');

describe('Gaze Web Receiver Test Suite', () => {
  let dom;
  let window;
  let document;
  let app;

  beforeEach(() => {
    dom = new JSDOM(htmlContent, {
      url: 'http://localhost:8080/?b=http://localhost:8080'
    });

    window = dom.window;
    document = window.document;

    window.HTMLCanvasElement.prototype.getContext = function() {
      return {
        fillRect: () => {}, clearRect: () => {}, getImageData: () => ({ data: [] }), putImageData: () => {},
        createImageData: () => [], setTransform: () => {}, drawImage: () => {}, save: () => {}, fillText: () => {},
        restore: () => {}, beginPath: () => {}, moveTo: () => {}, lineTo: () => {}, closePath: () => {}, stroke: () => {},
        translate: () => {}, scale: () => {}, rotate: () => {}, arc: () => {}, fill: () => {}, measureText: () => ({ width: 0 }),
        transform: () => {}, rect: () => {}, clip: () => {}
      };
    };

    // Set up mock indexedDB to prevent unhandled rejection in JSDOM
    window.indexedDB = {
      open: () => {
        const req = {};
        setTimeout(() => {
          const db = {
            objectStoreNames: { contains: () => false },
            createObjectStore: () => {},
            transaction: () => {
              const tx = {
                objectStore: () => ({
                  count: () => {
                    const cReq = {};
                    setTimeout(() => {
                      cReq.result = 0;
                      if (cReq.onsuccess) cReq.onsuccess({ target: cReq });
                    }, 0);
                    return cReq;
                  },
                  clear: () => {}
                }),
                oncomplete: null,
                onerror: null
              };
              setTimeout(() => {
                if (tx.oncomplete) tx.oncomplete();
              }, 0);
              return tx;
            }
          };
          req.result = db;
          if (req.onsuccess) req.onsuccess({ target: { result: db } });
        }, 0);
        return req;
      }
    };
    global.indexedDB = window.indexedDB;

    window.localStorage = {
      getItem: () => null,
      setItem: () => {},
      removeItem: () => {}
    };
    global.localStorage = window.localStorage;

    global.HTMLCanvasElement = window.HTMLCanvasElement;
    global.HTMLImageElement = window.HTMLImageElement;

    if (!window.HTMLCanvasElement.prototype.getContext) {
      window.HTMLCanvasElement.prototype.getContext = function() {
        return {
          fillRect: () => {},
          clearRect: () => {},
          getImageData: (x, y, w, h) => ({ data: new Array(w * h * 4) }),
          putImageData: () => {},
          createImageData: () => ([]),
          setTransform: () => {},
          drawImage: () => {},
          save: () => {},
          fillText: () => {},
          restore: () => {},
          beginPath: () => {},
          moveTo: () => {},
          lineTo: () => {},
          closePath: () => {},
          stroke: () => {},
          translate: () => {},
          scale: () => {},
          rotate: () => {},
          arc: () => {},
          fill: () => {},
          measureText: () => ({ width: 0 }),
          transform: () => {},
          rect: () => {},
          clip: () => {},
          fillStyle: '',
          strokeStyle: '',
          lineWidth: 1
        };
      };
    }
    if (!window.HTMLCanvasElement.prototype.toDataURL) {
      window.HTMLCanvasElement.prototype.toDataURL = () => 'data:image/png;base64,mock';
    }

    let QRious;
    try {
      QRious = require('./qrious.min.js');
    } catch (_) {
      QRious = require('qrious');
    }
    global.QRious = QRious;
    window.QRious = QRious;

    // Set up global environment for app.js
    const { webcrypto } = require('node:crypto');
    window.crypto = webcrypto;
    global.crypto = webcrypto;
    global.window = window;
    global.document = document;
    global.HTMLCanvasElement = window.HTMLCanvasElement;
    global.HTMLImageElement = window.HTMLImageElement;
    global.navigator = window.navigator;
    global.location = window.location;
    global.URLSearchParams = window.URLSearchParams;
    global.TextDecoder = require('util').TextDecoder;
    global.atob = (str) => Buffer.from(str, 'base64').toString('binary');
    global.btoa = (str) => Buffer.from(str, 'binary').toString('base64');
    window.atob = global.atob;
    window.btoa = global.btoa;
    if (typeof global.MessageChannel !== 'undefined') {
      window.MessageChannel = global.MessageChannel;
    }
    window.showSaveFilePicker = async () => {}; // mock showSaveFilePicker
    try { QRious = require('./qrious.min.js'); } catch (_) {}
    window.QRious = QRious;
    global.QRious = QRious;
    if (window.HTMLCanvasElement && !window.HTMLCanvasElement.prototype.getContext) {
      window.HTMLCanvasElement.prototype.getContext = () => ({
        fillRect: () => {}, clearRect: () => {}, getImageData: () => ({ data: [] }), putImageData: () => {},
        createImageData: () => [], setTransform: () => {}, drawImage: () => {}, save: () => {}, fillText: () => {},
        restore: () => {}, beginPath: () => {}, moveTo: () => {}, lineTo: () => {}, closePath: () => {}, stroke: () => {},
        translate: () => {}, scale: () => {}, rotate: () => {}, arc: () => {}, fill: () => {}, measureText: () => ({ width: 0 }),
        transform: () => {}, rect: () => {}, clip: () => {}
      });
    }
    global.pako = pako;
    window.pako = pako;
    window.crypto = webcrypto;
    global.crypto = webcrypto;
    window.__BEAM_TEST_ENV__ = true;

    // Load qrcode.min.js and app.js
    delete require.cache[require.resolve('./qrcode.min.js')];
    const qrcodeLib1 = require('./qrcode.min.js');
    global.generateQRCodeSVGDataURL = qrcodeLib1.generateQRCodeSVGDataURL;
    window.generateQRCodeSVGDataURL = qrcodeLib1.generateQRCodeSVGDataURL;
    window.qrcode = qrcodeLib1;

    delete require.cache[require.resolve('./app.js')];
    app = require('./app.js');
  });

  test('SDP Decompression (zlib + URL-safe Base64)', async () => {
    const originalSDP = "v=0\r\no=- 123456 2 IN IP4 127.0.0.1\r\ns=BeamShare Test Offer\r\nt=0 0\r\n";
    
    // Compress with zlib
    const compressedBytes = pako.deflate(originalSDP);
    
    // Convert to URL-safe Base64
    const b64 = Buffer.from(compressedBytes).toString('base64url');

    const decompressed = await app.decompressOffer(b64);
    assert.equal(decompressed, originalSDP);
  });

  test('UI State Transitions', () => {
    app.setState('loading');
    assert.equal(document.getElementById('state-loading').classList.contains('hidden'), false);
    assert.equal(document.getElementById('state-ready').classList.contains('hidden'), true);

    app.setState('ready');
    assert.equal(document.getElementById('state-loading').classList.contains('hidden'), true);
    assert.equal(document.getElementById('state-ready').classList.contains('hidden'), false);

    app.setState('done');
    assert.equal(document.getElementById('state-done').classList.contains('hidden'), false);
  });

  test('Render File Metadata Card', () => {
    const fileMeta = {
      name: 'document.pdf',
      size: 2097152, // 2 MB
      mime: 'application/pdf'
    };

    app.renderFileCard(fileMeta);

    assert.equal(document.getElementById('file-name').textContent, 'document.pdf');
    assert.equal(document.getElementById('file-size').textContent, '2.0 MB');
    assert.equal(document.getElementById('file-mime').textContent, 'PDF');
  });

  test('Update Progress Ring and Percentage', () => {
    app.updateProgress(0.75);
    assert.equal(document.getElementById('progress-pct').textContent, '75%');
    assert.equal(document.getElementById('progress-wrap').getAttribute('aria-valuenow'), '75');
  });

  test('Virtual Log Viewer Append Lines', () => {
    const viewer = new app.VirtualLogViewer('.terminal-body', 1000);
    viewer.append("Log line 1\nLog line 2\n");

    assert.equal(viewer.lines.length, 2);
    assert.equal(viewer.lines[0], "Log line 1");
    assert.equal(viewer.lines[1], "Log line 2");
    assert.equal(viewer.getText().includes("Log line 1"), true);
  });

  test('Virtual Log Viewer ANSI Stripping', () => {
    const viewer = new app.VirtualLogViewer('.terminal-body', 1000);
    viewer.append("\x1b[32mSuccess\x1b[0m\n\x1b[31mError\x1b[0m\n");

    assert.equal(viewer.lines.length, 2);
    assert.equal(viewer.lines[0], "\x1b[32mSuccess\x1b[0m");
    assert.equal(app.stripAnsi(viewer.lines[0]), "Success");
    assert.equal(viewer.getText().includes("Success\nError"), true);
  });

  test('Virtual Log Viewer Filtering', () => {
    const viewer = new app.VirtualLogViewer('.terminal-body', 1000);
    viewer.append("apple\nbanana\ncherry\n");

    assert.equal(viewer.lines.length, 3);

    viewer.setFilter("ban");
    assert.equal(viewer.filteredIndices.length, 1);
    assert.equal(viewer.filteredIndices[0], 1); // index of "banana"

    // Test that new appends are filtered correctly
    viewer.append("bandana\ndate\n");
    assert.equal(viewer.filteredIndices.length, 2);
    assert.equal(viewer.filteredIndices[1], 3); // index of "bandana"
  });

  test('EventSource Live Stream Parsing & Handlers', async () => {
    let mockSourceInstance = null;

    class MockEventSource {
      constructor(url) {
        this.url = url;
        mockSourceInstance = this;
      }
      close() {
        this.closed = true;
      }
    }

    window.EventSource = MockEventSource;
    global.EventSource = MockEventSource;

    // Start SSE streaming
    const ssePromise = app.startHTTPSSE();

    // Allow microtasks to run so clearIDB completes and onmessage is assigned
    await new Promise(resolve => setTimeout(resolve, 10));

    assert.notEqual(mockSourceInstance, null);
    assert.equal(document.getElementById('state-livepipe').classList.contains('hidden'), false);

    // Simulate backlog SSE message
    mockSourceInstance.onmessage({
      data: JSON.stringify({ type: 'backlog', payload: 'Backlog log entry\n' })
    });

    // Simulate data SSE message
    mockSourceInstance.onmessage({
      data: JSON.stringify({ type: 'data', payload: 'Realtime log entry\n' })
    });

    // Simulate EOF message
    mockSourceInstance.onmessage({
      data: JSON.stringify({ type: 'eof' })
    });

    assert.equal(mockSourceInstance.closed, true);
    assert.equal(document.getElementById('state-done').classList.contains('hidden'), false);

    await ssePromise;
  });

  test('SequentialChunkQueue — Sequential Order and Watermark Backpressure Flow Control', async () => {
    const sentMessages = [];
    const mockDataChannel = {
      readyState: 'open',
      send: (msg) => sentMessages.push(msg)
    };

    const writtenChunks = [];
    let concurrentWrites = 0;
    let maxConcurrentWrites = 0;

    const queue = new app.SequentialChunkQueue({
      highWatermark: 100, // 100 bytes HWM
      lowWatermark: 30,   // 30 bytes LWM
      dataChannel: mockDataChannel,
      writeHandler: async (chunk) => {
        concurrentWrites++;
        if (concurrentWrites > maxConcurrentWrites) {
          maxConcurrentWrites = concurrentWrites;
        }
        // Simulate async write delay
        await new Promise(r => setTimeout(r, 10));
        writtenChunks.push(chunk[0]); // record chunk byte identifier
        concurrentWrites--;
      }
    });

    // Enqueue 4 chunks of 30 bytes each while writeHandler is pending on chunk 1
    const c1 = new Uint8Array(30).fill(1);
    const c2 = new Uint8Array(30).fill(2);
    const c3 = new Uint8Array(30).fill(3);
    const c4 = new Uint8Array(30).fill(4);

    queue.enqueue(c1); // Total bytes = 30
    assert.equal(queue.isPaused, false);

    queue.enqueue(c2); // Total bytes = 60
    assert.equal(queue.isPaused, false);

    queue.enqueue(c3); // Total bytes = 90
    assert.equal(queue.isPaused, false);

    queue.enqueue(c4); // Total bytes = 120 -> exceeds HWM (100)!
    assert.equal(queue.isPaused, true);
    assert.deepEqual(sentMessages, ['PAUSE']);

    queue.enqueueEOF();

    await queue.drain();

    assert.equal(maxConcurrentWrites, 1, 'Writes must be processed sequentially one at a time');
    assert.deepEqual(writtenChunks, [1, 2, 3, 4], 'Chunks must be written in exact FIFO order');
    assert.deepEqual(sentMessages, ['PAUSE', 'RESUME'], 'RESUME signal must be sent when queue drops below LWM');
  });

  test('SequentialChunkQueue — Error Propagation in Write Handler', async () => {
    let capturedError = null;
    const queue = new app.SequentialChunkQueue({
      writeHandler: async () => {
        throw new Error('Disk write failed');
      },
      onError: (err) => {
        capturedError = err;
      }
    });

    queue.enqueue(new Uint8Array([1, 2, 3]));
    queue.enqueueEOF();

    await assert.rejects(
      async () => await queue.drain(),
      { message: 'Disk write failed' }
    );
    assert.equal(capturedError.message, 'Disk write failed');

    // Subsequent drain() call on already errored queue should reject without unhandled rejections
    await assert.rejects(
      async () => await queue.drain(),
      { message: 'Disk write failed' }
    );
  });

  test('SequentialChunkQueue — Default Watermarks (16 MB / 4 MB) and In-Flight Write Memory Tracking', async () => {
    const sentMessages = [];
    const mockDataChannel = {
      readyState: 'open',
      send: (msg) => sentMessages.push(msg)
    };

    let finishWrite;
    const writePromise = new Promise(r => { finishWrite = r; });

    const queue = new app.SequentialChunkQueue({
      dataChannel: mockDataChannel,
      writeHandler: async () => {
        await writePromise;
      }
    });

    assert.equal(queue.highWatermark, 16 * 1024 * 1024);
    assert.equal(queue.lowWatermark, 4 * 1024 * 1024);

    const chunkSize = 5 * 1024 * 1024; // 5 MB
    const c1 = new Uint8Array(chunkSize);
    const c2 = new Uint8Array(chunkSize);
    const c3 = new Uint8Array(chunkSize);
    const c4 = new Uint8Array(chunkSize);

    queue.enqueue(c1); // 5MB enqueued, processLoop starts writeHandler(c1)
    queue.enqueue(c2); // 10MB total
    queue.enqueue(c3); // 15MB total
    assert.equal(queue.isPaused, false);

    queue.enqueue(c4); // 20MB total >= 16MB HWM -> PAUSE
    assert.equal(queue.isPaused, true);
    assert.deepEqual(sentMessages, ['PAUSE']);

    // Total bytes should reflect in-flight chunk + queued chunks (20MB)
    assert.equal(queue.totalBytes, 20 * 1024 * 1024);

    queue.enqueueEOF();

    // Release writeHandler
    finishWrite();
    await queue.drain();

    assert.equal(queue.totalBytes, 0);
    assert.deepEqual(sentMessages, ['PAUSE', 'RESUME']);
  });

  test('SequentialChunkQueue — QuotaExceededError and DataChannel Closure', async () => {
    let capturedError = null;
    let channelClosed = false;
    const mockDC = {
      readyState: 'open',
      close: () => { channelClosed = true; }
    };

    const quotaError = new Error('Storage quota exceeded');
    quotaError.name = 'QuotaExceededError';

    const queue = new app.SequentialChunkQueue({
      dataChannel: mockDC,
      writeHandler: async () => {
        throw quotaError;
      },
      onError: (err) => {
        capturedError = err;
        mockDC.close();
      }
    });

    queue.enqueue(new Uint8Array([1, 2, 3]));
    queue.enqueueEOF();

    await assert.rejects(
      async () => await queue.drain(),
      { name: 'QuotaExceededError' }
    );

    assert.equal(capturedError.name, 'QuotaExceededError');
    assert.equal(channelClosed, true, 'DataChannel must be closed on storage write error');
  });

  test('WebRTC Receiver DataChannel Chunk Queueing', async () => {
    const receivedData = [];
    const mockDC = {
      readyState: 'open',
      send: () => {},
      close: () => {}
    };

    const queue = new app.SequentialChunkQueue({
      highWatermark: 1024,
      lowWatermark: 256,
      dataChannel: mockDC,
      writeHandler: async (chunk) => {
        await new Promise(r => setTimeout(r, 2));
        receivedData.push(...chunk);
      }
    });

    // Simulate dc.onmessage handler logic
    const onmessage = (e) => {
      if (typeof e.data === 'string') {
        if (e.data === 'EOF') {
          queue.enqueueEOF();
        }
        return;
      }
      queue.enqueue(new Uint8Array(e.data));
    };

    // Synchronously fire 5 binary messages with interleaved PAUSE/RESUME control messages
    onmessage({ data: new Uint8Array([1]).buffer });
    onmessage({ data: 'PAUSE' });
    onmessage({ data: new Uint8Array([2]).buffer });
    onmessage({ data: 'RESUME' });
    for (let i = 3; i <= 5; i++) {
      onmessage({ data: new Uint8Array([i]).buffer });
    }

    // Fire EOF message
    onmessage({ data: 'EOF' });

    // Await drain
    await queue.drain();

    assert.deepEqual(receivedData, [1, 2, 3, 4, 5]);
  });

  test('WebRTC Receiver AES-GCM Decryption and Chunk Queueing', async () => {
    const { webcrypto } = require('node:crypto');
    const key = await webcrypto.subtle.generateKey(
      { name: 'AES-GCM', length: 256 },
      true,
      ['encrypt', 'decrypt']
    );
    const rawKey = await webcrypto.subtle.exportKey('raw', key);
    const keyB64 = Buffer.from(rawKey).toString('base64url');

    window.location.hash = `#k=${keyB64}`;
    const importedKey = await app.parseDecryptionKeyFromHash(window.location.hash);
    assert.notEqual(importedKey, null);

    const receivedData = [];
    const mockDC = {
      readyState: 'open',
      send: () => {},
      close: () => {}
    };

    const queue = new app.SequentialChunkQueue({
      highWatermark: 1024,
      lowWatermark: 256,
      dataChannel: mockDC,
      writeHandler: async (chunk) => {
        receivedData.push(...chunk);
      }
    });

    const plaintext1 = new Uint8Array([10, 20, 30, 40]);
    const plaintext2 = new Uint8Array([50, 60, 70, 80]);

    async function createEncryptedFrame(pt) {
      const nonce = webcrypto.getRandomValues(new Uint8Array(12));
      const ciphertext = await webcrypto.subtle.encrypt(
        { name: 'AES-GCM', iv: nonce },
        key,
        pt
      );
      const frameLen = 12 + ciphertext.byteLength;
      const payload = new Uint8Array(4 + frameLen);
      const dv = new DataView(payload.buffer);
      dv.setUint32(0, frameLen, false);
      payload.set(nonce, 4);
      payload.set(new Uint8Array(ciphertext), 16);
      return payload;
    }

    const frame1 = await createEncryptedFrame(plaintext1);
    const frame2 = await createEncryptedFrame(plaintext2);

    let encBuffer = new Uint8Array(0);
    let decryptChain = Promise.resolve();

    const onmessage = (e) => {
      decryptChain = decryptChain.then(async () => {
        if (typeof e.data === 'string') {
          if (e.data === 'EOF') {
            queue.enqueueEOF();
            await queue.drain();
          }
          return;
        }

        const value = new Uint8Array(e.data);
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

            const nonce = new Uint8Array(frame.subarray(0, 12));
            const ciphertext = new Uint8Array(frame.subarray(12));
            const decrypted = await webcrypto.subtle.decrypt(
              { name: 'AES-GCM', iv: nonce },
              importedKey,
              ciphertext
            );
            queue.enqueue(new Uint8Array(decrypted));
          } else {
            break;
          }
        }
      });
    };

    onmessage({ data: frame1.buffer });
    onmessage({ data: frame2.buffer });
    onmessage({ data: 'EOF' });

    await decryptChain;

    assert.deepEqual(receivedData, [10, 20, 30, 40, 50, 60, 70, 80]);
  });

  test('WebRTC Receiver AES-GCM Decryption with Chunk Fragmentation', async () => {
    const { webcrypto } = require('node:crypto');
    const key = await webcrypto.subtle.generateKey(
      { name: 'AES-GCM', length: 256 },
      true,
      ['encrypt', 'decrypt']
    );
    const rawKey = await webcrypto.subtle.exportKey('raw', key);
    const keyB64 = Buffer.from(rawKey).toString('base64url');

    window.location.hash = `#k=${keyB64}`;
    const importedKey = await app.parseDecryptionKeyFromHash(window.location.hash);

    const receivedData = [];
    const queue = new app.SequentialChunkQueue({
      writeHandler: async (chunk) => {
        receivedData.push(...chunk);
      }
    });

    const plaintext = new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    const nonce = webcrypto.getRandomValues(new Uint8Array(12));
    const ciphertext = await webcrypto.subtle.encrypt(
      { name: 'AES-GCM', iv: nonce },
      key,
      plaintext
    );
    const frameLen = 12 + ciphertext.byteLength;
    const fullPayload = new Uint8Array(4 + frameLen);
    const dv = new DataView(fullPayload.buffer);
    dv.setUint32(0, frameLen, false);
    fullPayload.set(nonce, 4);
    fullPayload.set(new Uint8Array(ciphertext), 16);

    const chunkA = new Uint8Array(fullPayload.subarray(0, 5));
    const chunkB = new Uint8Array(fullPayload.subarray(5, 15));
    const chunkC = new Uint8Array(fullPayload.subarray(15));

    let encBuffer = new Uint8Array(0);
    let decryptChain = Promise.resolve();

    const onmessage = (e) => {
      decryptChain = decryptChain.then(async () => {
        if (typeof e.data === 'string') {
          if (e.data === 'EOF') {
            queue.enqueueEOF();
            await queue.drain();
          }
          return;
        }

        const value = new Uint8Array(e.data);
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

            const nonce = new Uint8Array(frame.subarray(0, 12));
            const ciphertext = new Uint8Array(frame.subarray(12));
            const decrypted = await webcrypto.subtle.decrypt(
              { name: 'AES-GCM', iv: nonce },
              importedKey,
              ciphertext
            );
            queue.enqueue(new Uint8Array(decrypted));
          } else {
            break;
          }
        }
      });
    };

    onmessage({ data: chunkA.buffer });
    onmessage({ data: chunkB.buffer });
    onmessage({ data: chunkC.buffer });
    onmessage({ data: 'EOF' });

    await decryptChain;

    assert.deepEqual(receivedData, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
  });
});

describe('Gaze Web Sender Test Suite', () => {
  let dom;
  let window;
  let document;
  let app;

  beforeEach(() => {
    dom = new JSDOM(htmlContent, {
      url: 'http://localhost:8080/'
    });

    window = dom.window;
    document = window.document;

    window.HTMLCanvasElement.prototype.toDataURL = function() {
      return 'data:image/png;base64,mock';
    };

    window.HTMLCanvasElement.prototype.getContext = function() {
      return {
        fillRect: () => {}, clearRect: () => {}, getImageData: () => ({ data: [] }), putImageData: () => {},
        createImageData: () => [], setTransform: () => {}, drawImage: () => {}, save: () => {}, fillText: () => {},
        restore: () => {}, beginPath: () => {}, moveTo: () => {}, lineTo: () => {}, closePath: () => {}, stroke: () => {},
        translate: () => {}, scale: () => {}, rotate: () => {}, arc: () => {}, fill: () => {}, measureText: () => ({ width: 0 }),
        transform: () => {}, rect: () => {}, clip: () => {}
      };
    };

    // Node's WebCrypto
    const { webcrypto } = require('node:crypto');
    window.crypto = webcrypto;

    global.window = window;
    global.document = document;
    global.HTMLCanvasElement = window.HTMLCanvasElement;
    global.HTMLImageElement = window.HTMLImageElement;
    global.crypto = window.crypto;
    global.navigator = window.navigator;
    global.location = window.location;
    global.URLSearchParams = window.URLSearchParams;
    global.TextDecoder = require('util').TextDecoder;
    global.atob = (str) => Buffer.from(str, 'base64').toString('binary');
    global.btoa = (str) => Buffer.from(str, 'binary').toString('base64');
    window.atob = global.atob;
    window.btoa = global.btoa;

    const QRious = require('./qrious.min.js');
    window.QRious = QRious;
    global.QRious = QRious;

    if (window.HTMLCanvasElement && !window.HTMLCanvasElement.prototype.getContext) {
      window.HTMLCanvasElement.prototype.getContext = () => ({
        fillRect: () => {},
        clearRect: () => {},
        getImageData: () => ({ data: [] }),
        putImageData: () => {},
        createImageData: () => [],
        setTransform: () => {},
        drawImage: () => {},
        save: () => {},
        fillText: () => {},
        restore: () => {},
        beginPath: () => {},
        moveTo: () => {},
        lineTo: () => {},
        closePath: () => {},
        stroke: () => {},
        translate: () => {},
        scale: () => {},
        rotate: () => {},
        arc: () => {},
        fill: () => {},
        measureText: () => ({ width: 0 }),
        transform: () => {},
        rect: () => {},
        clip: () => {},
      });
    }

    global.fetch = async (url) => {
      if (url.includes('/poll')) {
          return { ok: false, status: 404 };
      }
      return {
        ok: true,
        json: async () => ({ session: 'mock-session-123' })
      };
    };
    window.fetch = global.fetch;

    class RTCPeerConnection {
      constructor() {}
      createDataChannel() { return { onopen: () => {}, onmessage: () => {}, onclose: () => {} }; }
      async createOffer() { return { sdp: 'v=0...' }; }
      async setLocalDescription(d) { this.localDescription = { sdp: 'v=0...' }; }
    }
    window.RTCPeerConnection = RTCPeerConnection;
    global.RTCPeerConnection = RTCPeerConnection;
    window.__BEAM_TEST_ENV__ = true;

    delete require.cache[require.resolve('./qrcode.min.js')];
    const qrcodeLib2 = require('./qrcode.min.js');
    global.generateQRCodeSVGDataURL = qrcodeLib2.generateQRCodeSVGDataURL;
    window.generateQRCodeSVGDataURL = qrcodeLib2.generateQRCodeSVGDataURL;
    window.qrcode = qrcodeLib2;

    delete require.cache[require.resolve('./app.js')];
    app = require('./app.js');

    // Set mock file
    app.handleSenderFileSelect({ name: 'test.txt', size: 1024, type: 'text/plain' });

  });

  test('startSenderSharing generates AES-GCM key and appends #k fragment with client-side QR generation', async () => {
    // Intercept fetch / network calls to verify no external requests are made
    let externalRequests = [];
    window.fetch = async (url) => {
      externalRequests.push(url.toString());
      return { ok: true, json: async () => ({}) };
    };

    await app.startSenderSharing();

    const urlInput = document.getElementById('send-url-input');
    const shareURL = urlInput.value || "http://localhost/";
    const hash = new URL(shareURL).hash;

    assert.equal(hash.startsWith('#k='), true);

    // Verify the fragment is valid base64url and resolves to 32 bytes (256-bit)
    const b64 = hash.substring(3).replace(/-/g, '+').replace(/_/g, '/');
    const raw = Buffer.from(b64, 'base64');
    assert.equal(raw.length, 32);

    // Ensure global encryption key was created
    assert.notEqual(app.get_senderEncryptionKey(), null);

    // Verify QR code image src uses inline SVG data URI instead of third-party api.qrserver.com
    const qrImg = document.getElementById('send-qr-img');
    const srcAttr = qrImg.getAttribute('src') || qrImg.src;
    assert.equal(srcAttr.startsWith('data:image/svg+xml'), true);
    assert.equal(srcAttr.includes('api.qrserver.com'), false);
  });

  test('startSenderSharing generates local QR code with full URL and #k fragment on canvas without external API calls', async () => {
    let externalCallMade = false;
    const origFetch = global.fetch;
    global.fetch = async (url, opts) => {
      if (typeof url === 'string' && (url.includes('qrserver.com') || url.includes('/api/qr'))) {
        externalCallMade = true;
      }
      return origFetch(url, opts);
    };

    await app.startSenderSharing();

    assert.equal(externalCallMade, false, 'No external QR API requests should be made');

    const sendCanvas = document.getElementById('send-qr-canvas');
    assert.notEqual(sendCanvas, null);

    const urlInput = document.getElementById('send-url-input');
    assert.ok(urlInput.value.includes('#k='));
  });

  test('startSenderSharing renders QR code locally in memory without outbound network calls to api.qrserver.com or /api/qr', async () => {
    const fetchedURLs = [];
    global.fetch = async (url, opts) => {
      fetchedURLs.push(url);
      if (url.includes('/poll')) {
        return { ok: false, status: 404 };
      }
      return {
        ok: true,
        json: async () => ({ session: 'mock-session-123' })
      };
    };
    window.fetch = global.fetch;

    await app.startSenderSharing();

    // Verify 0 requests were sent to api.qrserver.com
    const qrServerCalls = fetchedURLs.filter(u => u.includes('qrserver.com'));
    assert.equal(qrServerCalls.length, 0, 'Must not make HTTP requests to api.qrserver.com');

    // Verify 0 requests were sent to /api/qr
    const localQRCalls = fetchedURLs.filter(u => u.includes('/api/qr'));
    assert.equal(localQRCalls.length, 0, 'Must not make HTTP requests to /api/qr');

    // Verify canvas element was updated locally
    const canvas = document.getElementById('send-qr-canvas');
    assert.notEqual(canvas, null);
    assert.equal(canvas.width > 0, true);

    // Verify img element has inline SVG data URI
    const sendImg = document.getElementById('send-qr-img');
    assert.notEqual(sendImg, null);
    const srcAttr = sendImg.getAttribute('src') || sendImg.src;
    assert.equal(srcAttr.startsWith('data:image/svg+xml'), true);
    assert.equal(decodeURIComponent(srcAttr).includes('<path d='), true);
  });

  test('renderQRCode generates local QR SVG and Canvas elements containing Base64 AES keys (#k=)', () => {
    const shareURL = "http://localhost:8080/?s=test-session-123&mode=webrtc#k=dGVzdC1zZWNyZXQta2V5LTAxMjM0NTY3ODkwMTI=";
    
    // SVG element target
    const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
    app.renderQRCode(shareURL, svg);
    assert.equal(svg.getAttribute('viewBox').length > 0, true);
    assert.equal(svg.innerHTML.includes('<path d='), true);

    // Image element target (data URI)
    const img = document.createElement('img');
    app.renderQRCode(shareURL, img);
    const srcAttr = img.getAttribute('src') || img.src;
    assert.equal(srcAttr.startsWith('data:image/svg+xml'), true);
    assert.equal(decodeURIComponent(srcAttr).includes('<path d='), true);

    // Container element target
    const div = document.createElement('div');
    app.renderQRCode(shareURL, div);
    assert.equal(div.innerHTML.includes('<svg'), true);
  });

  test('createOPFSWriter uses createWritable when available', async () => {
    let written = [];
    const mockFileHandle = {
      createWritable: async () => ({
        write: async (c) => written.push(c),
        close: async () => {}
      })
    };

    const writer = await app.createOPFSWriter(mockFileHandle, 0);
    await writer.write(new Uint8Array([10, 20]));
    assert.equal(written.length, 1);
    assert.equal(written[0][0], 10);
  });

  test('createOPFSWriter falls back to OPFSStreamWriter when createWritable is missing', async () => {
    let messagesSent = [];
    let terminated = false;

    class MockWorker {
      constructor(url) {
        this.url = url;
      }
      addEventListener(type, listener) {
        if (type === 'message') {
          this.listener = listener;
        }
      }
      removeEventListener() {}
      postMessage(msg) {
        messagesSent.push(msg);
        if (msg.type === 'INIT') {
          setTimeout(() => this.listener({ data: { type: 'INIT_OK' } }), 0);
        } else if (msg.type === 'WRITE') {
          setTimeout(() => this.listener({ data: { type: 'WRITE_OK', written: msg.chunk ? msg.chunk.byteLength : 0 } }), 0);
        } else if (msg.type === 'CLOSE') {
          setTimeout(() => this.listener({ data: { type: 'CLOSE_OK' } }), 0);
        }
      }
      terminate() {
        terminated = true;
      }
    }

    global.Worker = MockWorker;
    window.Worker = MockWorker;

    const mockFileHandleWithoutWritable = {}; // no createWritable method (simulates Safari / Firefox)

    const writer = await app.createOPFSWriter(mockFileHandleWithoutWritable, 0);
    assert.equal(writer instanceof app.OPFSStreamWriter, true);

    await writer.write(new Uint8Array([100, 200]));
    await writer.close();

    assert.equal(messagesSent.length, 3);
    assert.equal(messagesSent[0].type, 'INIT');
    assert.equal(messagesSent[1].type, 'WRITE');
    assert.equal(messagesSent[2].type, 'CLOSE');
    assert.equal(terminated, true);
  });

  test('checkRamWarning resolves true when streaming support or size is within limits', async () => {
    const resultSmall = await app.checkRamWarning(100 * 1024 * 1024); // 100 MB
    assert.equal(resultSmall, true);
  });

  test('extractKeyFragment decodes standard, URL-safe, and percent-encoded keys', () => {
    // Standard base64
    assert.equal(app.extractKeyFragment('#k=dGVzdGtleQ=='), 'dGVzdGtleQ==');
    // Without padding
    assert.equal(app.extractKeyFragment('#k=dGVzdGtleQ'), 'dGVzdGtleQ==');
    // Percent-encoded padding (%3D)
    assert.equal(app.extractKeyFragment('#k=dGVzdGtleQ%3D%3D'), 'dGVzdGtleQ==');
    // Percent-encoded key parameter name (#k%3D)
    assert.equal(app.extractKeyFragment('#k%3DdGVzdGtleQ%3D%3D'), 'dGVzdGtleQ==');
    // Double percent-encoded (%253D)
    assert.equal(app.extractKeyFragment('#k%3DdGVzdGtleQ%253D%253D'), 'dGVzdGtleQ==');
    // Embedded inside query params in hash
    assert.equal(app.extractKeyFragment('#mode=webrtc&k=dGVzdGtleQ%3D%3D'), 'dGVzdGtleQ==');
    assert.equal(app.extractKeyFragment('#mode=webrtc&k%3DdGVzdGtleQ'), 'dGVzdGtleQ==');
    // URL-safe base64 hyphens and underscores
    assert.equal(app.extractKeyFragment('#k=dGVzdC1rZXlfMDEyMzQ1Njc4OTA='), 'dGVzdC1rZXlfMDEyMzQ1Njc4OTA=');
    // Missing or invalid hash
    assert.equal(app.extractKeyFragment(''), null);
    assert.equal(app.extractKeyFragment('#mode=webrtc'), null);
  });

  test('parseDecryptionKeyFromHash successfully imports AES-GCM CryptoKey for percent-encoded key', async () => {
    // 32-byte key in base64: 32 bytes of 0x01
    const raw32 = new Uint8Array(32).fill(1);
    const b64 = Buffer.from(raw32).toString('base64');
    const encodedHash = `#k%3D${encodeURIComponent(b64)}`;

    const key = await app.parseDecryptionKeyFromHash(encodedHash);
    assert.notEqual(key, null);
    assert.equal(key.algorithm.name, 'AES-GCM');
  });

  test('parseDecryptionKeyFromHash rejects non-32-byte key length', async () => {
    // 16-byte key in base64
    const raw16 = new Uint8Array(16).fill(1);
    const b64 = Buffer.from(raw16).toString('base64');
    const encodedHash = `#k=${b64}`;

    await assert.rejects(
      async () => {
        await app.parseDecryptionKeyFromHash(encodedHash);
      },
      {
        message: /Invalid decryption key length: expected 32 bytes, got 16/
      }
    );
  });

  test('parseSessionInput handles full URLs, relative paths, and raw session IDs/passphrases', () => {
    const origin = (typeof window !== 'undefined' && window.location && window.location.origin) 
      ? window.location.origin 
      : 'https://beamshare.app';

    // Full URL
    assert.equal(app.parseSessionInput('https://beamshare.app/?s=abc12345'), 'https://beamshare.app/?s=abc12345');
    // Relative query
    assert.equal(app.parseSessionInput('/?s=xyz789'), `${origin}/?s=xyz789`);
    assert.equal(app.parseSessionInput('?s=xyz789'), `${origin}/?s=xyz789`);
    // Raw session code or passphrase
    assert.equal(app.parseSessionInput('0123456789abcdef0123456789abcdef'), `${origin}/?s=0123456789abcdef0123456789abcdef`);
    assert.equal(app.parseSessionInput('my-secret-passphrase'), `${origin}/?s=my-secret-passphrase`);
    // Whitespace trimming
    assert.equal(app.parseSessionInput('  test-code  '), `${origin}/?s=test-code`);
    // Empty inputs
    assert.equal(app.parseSessionInput(''), null);
    assert.equal(app.parseSessionInput('   '), null);
    assert.equal(app.parseSessionInput(null), null);
  });

  test('getIceServers extracts TURN server and credentials from URL parameters during embedded offer handling', () => {
    // Mock location with turn query parameters
    dom.reconfigure({
      url: 'http://localhost:8080/?mode=webrtc&turn_server=turn%3Aturn.example.com%3A3478&turn_username=alice&turn_credential=secret'
    });

    const offer = { type: 'offer', sdp: 'v=0...', iceServers: [{ urls: 'stun:stun.l.google.com:19302' }] };
    const servers = app.getIceServers(offer);

    assert.equal(servers.length, 1);
    assert.deepEqual(servers[0].urls, ['turn:turn.example.com:3478']);
    assert.equal(servers[0].username, 'alice');
    assert.equal(servers[0].credential, 'secret');
  });

  describe('WebRTC Buffer Drain Helper Tests (waitForBufferDrain)', () => {
    class MockDataChannel {
      constructor(bufferedAmount = 0, readyState = 'open') {
        this.bufferedAmount = bufferedAmount;
        this.bufferedAmountLowThreshold = 0;
        this.readyState = readyState;
        this.listeners = new Map();
      }

      addEventListener(type, listener) {
        if (!this.listeners.has(type)) {
          this.listeners.set(type, new Set());
        }
        this.listeners.get(type).add(listener);
      }

      removeEventListener(type, listener) {
        if (this.listeners.has(type)) {
          this.listeners.get(type).delete(listener);
        }
      }

      emit(type, eventData) {
        if (this.listeners.has(type)) {
          for (const listener of Array.from(this.listeners.get(type))) {
            listener(eventData);
          }
        }
      }
    }

    test('waitForBufferDrain resolves immediately when bufferedAmount is already <= targetThreshold', async () => {
      const dc = new MockDataChannel(500 * 1024, 'open');
      await app.waitForBufferDrain(dc, 1024 * 1024, 512 * 1024, 50);
      assert.equal(dc.bufferedAmountLowThreshold, 512 * 1024);
      assert.equal(dc.listeners.get('bufferedamountlow')?.size || 0, 0);
    });

    test('waitForBufferDrain resolves when bufferedamountlow event fires and cleans up listeners', async () => {
      const dc = new MockDataChannel(2 * 1024 * 1024, 'open');
      const promise = app.waitForBufferDrain(dc, 1024 * 1024, 512 * 1024, 50);

      assert.equal(dc.listeners.get('bufferedamountlow').size, 1);

      dc.bufferedAmount = 400 * 1024;
      dc.emit('bufferedamountlow');

      await promise;
      assert.equal(dc.listeners.get('bufferedamountlow').size, 0);
    });

    test('waitForBufferDrain resolves via fallback polling within 50ms when event is missed', async () => {
      const dc = new MockDataChannel(2 * 1024 * 1024, 'open');
      const promise = app.waitForBufferDrain(dc, 1024 * 1024, 512 * 1024, 50);

      // Simulate buffer drain WITHOUT firing bufferedamountlow event
      dc.bufferedAmount = 100 * 1024;

      const start = Date.now();
      await promise;
      const elapsed = Date.now() - start;

      assert.ok(elapsed <= 300, `Expected polling resolution around 50ms, took ${elapsed}ms`);
      assert.equal(dc.listeners.get('bufferedamountlow').size, 0);
    });

    test('waitForBufferDrain rejects and cleans up when channel closes', async () => {
      const dc = new MockDataChannel(2 * 1024 * 1024, 'open');
      const promise = app.waitForBufferDrain(dc, 1024 * 1024, 512 * 1024, 50);

      dc.readyState = 'closed';
      dc.emit('close');

      await assert.rejects(promise, {
        message: 'Data channel is no longer open'
      });
      assert.equal(dc.listeners.get('bufferedamountlow').size, 0);
    });

    test('waitForBufferDrain rejects immediately if channel is closed or null', async () => {
      const dcClosed = new MockDataChannel(0, 'closed');
      await assert.rejects(app.waitForBufferDrain(dcClosed, 0, 0, 50), {
        message: 'Data channel is no longer open'
      });
      await assert.rejects(app.waitForBufferDrain(null, 0, 0, 50), {
        message: 'Data channel is no longer open'
      });
    });

  });
});

describe('WebRTC Buffer Backpressure Suite', () => {
  let app;

  beforeEach(() => {
    delete require.cache[require.resolve('./app.js')];
    app = require('./app.js');
  });

  class MockDataChannel {
    constructor(bufferedAmount = 0, readyState = 'open') {
      this.bufferedAmount = bufferedAmount;
      this.bufferedAmountLowThreshold = 0;
      this.readyState = readyState;
      this.listeners = new Map();
    }

    addEventListener(event, fn) {
      if (!this.listeners.has(event)) {
        this.listeners.set(event, new Set());
      }
      this.listeners.get(event).add(fn);
    }

    removeEventListener(event, fn) {
      if (this.listeners.has(event)) {
        this.listeners.get(event).delete(fn);
      }
    }

    emit(event) {
      if (this.listeners.has(event)) {
        for (const fn of this.listeners.get(event)) {
          fn();
        }
      }
    }

    getListenerCount(event) {
      return this.listeners.has(event) ? this.listeners.get(event).size : 0;
    }
  }

  test('resolves immediately if bufferedAmount is already <= targetThreshold', async () => {
    const dc = new MockDataChannel(256 * 1024, 'open');
    let resolved = false;

    await app.waitForBufferedAmountLow(dc, 512 * 1024);
    resolved = true;

    assert.equal(resolved, true);
    assert.equal(dc.getListenerCount('bufferedamountlow'), 0);
  });

  test('resolves via bufferedamountlow event when buffer drops', async () => {
    const dc = new MockDataChannel(2 * 1024 * 1024, 'open');
    
    const waitPromise = app.waitForBufferedAmountLow(dc, 512 * 1024);
    assert.equal(dc.getListenerCount('bufferedamountlow'), 1);

    // Simulate buffer drop and event fire
    dc.bufferedAmount = 256 * 1024;
    dc.emit('bufferedamountlow');

    await waitPromise;
    assert.equal(dc.getListenerCount('bufferedamountlow'), 0);
  });

  test('resolves on immediate recheck if buffer dropped during listener attachment', async () => {
    const dc = new MockDataChannel(2 * 1024 * 1024, 'open');

    // Intercept addEventListener to simulate buffer drop right before recheck
    const origAddEventListener = dc.addEventListener.bind(dc);
    dc.addEventListener = (event, fn) => {
      origAddEventListener(event, fn);
      dc.bufferedAmount = 100; // Drops buffer below threshold!
    };

    await app.waitForBufferedAmountLow(dc, 512 * 1024);
    assert.equal(dc.getListenerCount('bufferedamountlow'), 0);
  });

  test('resolves via polling fallback if event is lost/missed', async () => {
    const dc = new MockDataChannel(2 * 1024 * 1024, 'open');

    const waitPromise = app.waitForBufferedAmountLow(dc, 512 * 1024, 10);
    assert.equal(dc.getListenerCount('bufferedamountlow'), 1);

    // Simulate buffer drop WITHOUT emitting event
    dc.bufferedAmount = 100 * 1024;

    await waitPromise;
    assert.equal(dc.getListenerCount('bufferedamountlow'), 0);
  });

  test('rejects cleanly and cleans up listeners if data channel closes', async () => {
    const dc = new MockDataChannel(2 * 1024 * 1024, 'open');

    const waitPromise = app.waitForBufferedAmountLow(dc, 512 * 1024, 10);
    assert.equal(dc.getListenerCount('bufferedamountlow'), 1);

    // Simulate channel close
    dc.readyState = 'closed';

    await assert.rejects(
      async () => await waitPromise,
      { message: 'Data channel is no longer open' }
    );

    assert.equal(dc.getListenerCount('bufferedamountlow'), 0);
  });
});

describe('WebRTC Backpressure & Flow Control Suite', () => {
  let dom;
  let window;
  let document;
  let app;

  beforeEach(() => {
    dom = new JSDOM(htmlContent, {
      url: 'http://localhost:8080/'
    });

    window = dom.window;
    document = window.document;

    const { webcrypto } = require('node:crypto');
    window.crypto = webcrypto;

    global.window = window;
    global.document = document;
    global.crypto = window.crypto;
    global.navigator = window.navigator;
    global.location = window.location;
    global.URLSearchParams = window.URLSearchParams;
    global.TextDecoder = require('util').TextDecoder;
    global.FileReader = window.FileReader;

    window.__BEAM_TEST_ENV__ = true;

    delete require.cache[require.resolve('./app.js')];
    app = require('./app.js');
  });

  class MockDataChannel {
    constructor(bufferedAmount = 0, readyState = 'open') {
      this.bufferedAmount = bufferedAmount;
      this.bufferedAmountLowThreshold = 0;
      this.readyState = readyState;
      this.listeners = new Map();
    }

    addEventListener(type, listener) {
      if (!this.listeners.has(type)) {
        this.listeners.set(type, new Set());
      }
      this.listeners.get(type).add(listener);
    }

    removeEventListener(type, listener) {
      if (this.listeners.has(type)) {
        this.listeners.get(type).delete(listener);
      }
    }

    emit(type, event) {
      if (this.listeners.has(type)) {
        for (const listener of Array.from(this.listeners.get(type))) {
          listener(event);
        }
      }
    }

    send(data) {}
  }

  test('waitForDataChannelBuffer resolves immediately if bufferedAmount <= targetAmount', async () => {
    const dc = new MockDataChannel(500 * 1024, 'open');
    await app.waitForDataChannelBuffer(dc, 1024 * 1024, 512 * 1024);
    assert.equal(dc.bufferedAmountLowThreshold, 512 * 1024);
  });

  test('waitForDataChannelBuffer resolves when bufferedamountlow event fires', async () => {
    const dc = new MockDataChannel(2 * 1024 * 1024, 'open');
    const promise = app.waitForDataChannelBuffer(dc, 1024 * 1024, 512 * 1024);

    assert.equal(dc.listeners.get('bufferedamountlow').size, 1);
    dc.bufferedAmount = 500 * 1024;
    dc.emit('bufferedamountlow');

    await promise;
    assert.equal(dc.listeners.get('bufferedamountlow').size, 0, 'Listeners must be cleaned up on resolve');
  });

  test('waitForDataChannelBuffer post-registration re-check resolves without waiting', async () => {
    class FastDrainDataChannel extends MockDataChannel {
      addEventListener(type, listener) {
        super.addEventListener(type, listener);
        if (type === 'bufferedamountlow') {
          // Buffer drained immediately before event loop fired event
          this.bufferedAmount = 300 * 1024;
        }
      }
    }

    const dc = new FastDrainDataChannel(2 * 1024 * 1024, 'open');
    await app.waitForDataChannelBuffer(dc, 1024 * 1024, 512 * 1024);
    assert.equal(dc.listeners.get('bufferedamountlow')?.size || 0, 0, 'Listeners must be cleaned up');
  });

  test('waitForDataChannelBuffer periodic fallback timer resolves when event is missed', async () => {
    const dc = new MockDataChannel(2 * 1024 * 1024, 'open');
    const start = Date.now();
    const promise = app.waitForDataChannelBuffer(dc, 1024 * 1024, 512 * 1024);

    // Drains buffer without firing 'bufferedamountlow'
    dc.bufferedAmount = 100 * 1024;

    await promise;
    const elapsed = Date.now() - start;
    assert.equal(elapsed >= 200, true, 'Resolved via 250ms periodic timer fallback');
    assert.equal(dc.listeners.get('bufferedamountlow')?.size || 0, 0, 'Listeners must be cleaned up');
  });

  test('waitForDataChannelBuffer rejects when channel closes or errors', async () => {
    const dc = new MockDataChannel(2 * 1024 * 1024, 'open');
    const promise = app.waitForDataChannelBuffer(dc, 1024 * 1024, 512 * 1024);

    dc.readyState = 'closed';
    dc.emit('close');

    await assert.rejects(promise, { message: /closed|closing|no longer open/i });
    assert.equal(dc.listeners.get('bufferedamountlow')?.size || 0, 0, 'Listeners must be cleaned up on rejection');
  });

  test('waitForDataChannelBuffer rejects immediately if channel is already closed', async () => {
    const dc = new MockDataChannel(2 * 1024 * 1024, 'closed');
    await assert.rejects(
      app.waitForDataChannelBuffer(dc, 1024 * 1024, 512 * 1024),
      { message: /closed|closing|no longer open/i }
    );
  });

  test('uploadFileP2P streams file chunks and sends UPLOAD_EOF using backpressure helper', async () => {
    const sent = [];
    const mockDC = new MockDataChannel(0, 'open');
    mockDC.send = (msg) => sent.push(msg);

    delete require.cache[require.resolve('./app.js')];
    const testApp = require('./app.js');

    const fileContent = new Uint8Array(150 * 1024).fill(65);
    const mockFile = new window.File([fileContent], 'test-p2p.bin', { type: 'application/octet-stream' });

    await testApp.uploadFileP2P(mockFile, mockDC);

    assert.equal(sent.length, 5, 'Should send UPLOAD_META, 3 chunks, and UPLOAD_EOF');
    assert.equal(sent[0], 'UPLOAD_META:test-p2p.bin:153600');
    assert.equal(sent[4], 'UPLOAD_EOF');
  });

  test('sendWebRTCFile streams chunks and sends EOF using backpressure helper', async () => {
    const sent = [];
    const mockDC = new MockDataChannel(0, 'open');
    mockDC.send = (msg) => sent.push(msg);

    delete require.cache[require.resolve('./app.js')];
    const testApp = require('./app.js');

    const fileContent = new Uint8Array(150 * 1024).fill(66);
    const mockFile = new window.File([fileContent], 'sender-test.bin', { type: 'application/octet-stream' });

    testApp.handleSenderFileSelect(mockFile);

    await testApp.sendWebRTCFile(0, mockDC);

    assert.equal(sent.length, 4, 'Should send 3 chunks and EOF');
    assert.equal(sent[3], 'EOF');
  });
});

