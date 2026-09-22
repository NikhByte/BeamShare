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

    // Set up global environment for app.js
    global.window = window;
    global.document = document;
    global.navigator = window.navigator;
    global.location = window.location;
    global.URLSearchParams = window.URLSearchParams;
    global.TextDecoder = require('util').TextDecoder;
    global.atob = (str) => Buffer.from(str, 'base64').toString('binary');
    global.btoa = (str) => Buffer.from(str, 'binary').toString('base64');
    window.atob = global.atob;
    window.btoa = global.btoa;
    window.showSaveFilePicker = async () => {}; // mock showSaveFilePicker
    global.pako = pako;
    window.pako = pako;
    window.__BEAM_TEST_ENV__ = true;

    // Load app.js
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

    // Enqueue 4 chunks of 40 bytes each while writeHandler is pending on chunk 1
    const c1 = new Uint8Array(40).fill(1);
    const c2 = new Uint8Array(40).fill(2);
    const c3 = new Uint8Array(40).fill(3);
    const c4 = new Uint8Array(40).fill(4);

    queue.enqueue(c1); // Dequeued immediately for writing (queue totalBytes = 0)
    assert.equal(queue.isPaused, false);

    queue.enqueue(c2); // Queue totalBytes = 40
    assert.equal(queue.isPaused, false);

    queue.enqueue(c3); // Queue totalBytes = 80
    assert.equal(queue.isPaused, false);

    queue.enqueue(c4); // Queue totalBytes = 120 -> exceeds HWM (100)!
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

  test('WebRTC DataChannel String Control Messages (PAUSE/RESUME/EOF)', async () => {
    let paused = false;
    let eofEnqueued = false;
    const receivedChunks = [];

    const mockQueue = {
      enqueue: (chunk) => receivedChunks.push(chunk[0]),
      enqueueEOF: () => { eofEnqueued = true; }
    };

    const handleDataChannelMessage = (e) => {
      if (typeof e.data === 'string') {
        if (e.data === 'EOF') {
          mockQueue.enqueueEOF();
        } else if (e.data === 'PAUSE') {
          paused = true;
        } else if (e.data === 'RESUME') {
          paused = false;
        }
        return;
      }
      mockQueue.enqueue(new Uint8Array(e.data));
    };

    handleDataChannelMessage({ data: new Uint8Array([10]).buffer });
    assert.equal(paused, false);
    assert.deepEqual(receivedChunks, [10]);

    handleDataChannelMessage({ data: 'PAUSE' });
    assert.equal(paused, true);
    assert.deepEqual(receivedChunks, [10], 'Control message must not be enqueued as data chunk');

    handleDataChannelMessage({ data: new Uint8Array([20]).buffer });
    assert.deepEqual(receivedChunks, [10, 20]);

    handleDataChannelMessage({ data: 'RESUME' });
    assert.equal(paused, false);

    handleDataChannelMessage({ data: 'EOF' });
    assert.equal(eofEnqueued, true);
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

    // Node's WebCrypto
    const { webcrypto } = require('node:crypto');
    window.crypto = webcrypto;

    global.window = window;
    global.document = document;
    global.crypto = window.crypto;
    global.navigator = window.navigator;
    global.location = window.location;
    global.URLSearchParams = window.URLSearchParams;
    global.TextDecoder = require('util').TextDecoder;
    global.atob = (str) => Buffer.from(str, 'base64').toString('binary');
    global.btoa = (str) => Buffer.from(str, 'binary').toString('base64');
    window.atob = global.atob;
    window.btoa = global.btoa;
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

    delete require.cache[require.resolve('./app.js')];
    app = require('./app.js');

    // Set mock file
    app.handleSenderFileSelect({ name: 'test.txt', size: 1024, type: 'text/plain' });

  });

  test('startSenderSharing generates AES-GCM key and appends #k fragment', async () => {
    await app.startSenderSharing();

    const urlInput = document.getElementById('send-url-input');
    const hash = new URL(urlInput.value || "http://localhost/").hash;

    assert.equal(hash.startsWith('#k='), true);

    // Verify the fragment is valid base64url and resolves to 32 bytes (256-bit)
    const b64 = hash.substring(3).replace(/-/g, '+').replace(/_/g, '/');
    const raw = Buffer.from(b64, 'base64');
    assert.equal(raw.length, 32);

    // Ensure global encryption key was created
    assert.notEqual(app.get_senderEncryptionKey(), null);
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
});
