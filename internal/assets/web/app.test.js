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
});
