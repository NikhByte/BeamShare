const { test, describe, beforeEach, afterEach } = require('node:test');
const assert = require('node:assert/strict');

describe('Service Worker Stream Cleanup & RFC 6266 Tests', () => {
  let listeners = {};
  let mockSelf;

  beforeEach(() => {
    listeners = {};
    mockSelf = {
      addEventListener: (event, cb) => {
        listeners[event] = cb;
      },
      skipWaiting: () => {},
      clients: { claim: async () => {} }
    };
    global.self = mockSelf;

    // Reset module cache so sw.js re-registers event listeners
    delete require.cache[require.resolve('./sw.js')];
  });

  afterEach(() => {
    delete global.self;
  });

  test('formatContentDisposition formats RFC 6266 dual parameters correctly', () => {
    const { formatContentDisposition } = require('./sw.js');

    // Case 1: Standard filename with spaces and parentheses
    const header1 = formatContentDisposition('Project Report (2026).pdf');
    assert.equal(
      header1,
      'attachment; filename="Project Report (2026).pdf"; filename*=UTF-8\'\'Project%20Report%20%282026%29.pdf'
    );

    // Case 2: Non-ASCII characters (unicode)
    const header2 = formatContentDisposition('résumé.pdf');
    assert.equal(
      header2,
      'attachment; filename="resume.pdf"; filename*=UTF-8\'\'r%C3%A9sum%C3%A9.pdf'
    );

    // Case 3: Quotes in filename
    const header3 = formatContentDisposition('file "test".txt');
    assert.equal(
      header3,
      'attachment; filename="file \\"test\\".txt"; filename*=UTF-8\'\'file%20%22test%22.txt'
    );
  });

  test('StreamMap entry deleted upon stream completion (EOF)', () => {
    const { streamMap } = require('./sw.js');
    const url = '/sw-download-pipe/test-eof';

    let closed = false;
    const mockPort = {
      onmessage: null,
      onmessageerror: null,
      close: () => { closed = true; },
      postMessage: () => {}
    };

    // Trigger INIT_PORT message
    listeners['message']({
      data: {
        type: 'INIT_PORT',
        url,
        filename: 'test.bin',
        size: 100,
        mime: 'application/octet-stream'
      },
      ports: [mockPort]
    });

    assert.equal(streamMap.has(url), true);

    // Send EOF
    mockPort.onmessage({ data: 'EOF' });

    assert.equal(streamMap.has(url), false);
    assert.equal(closed, true);
  });

  test('StreamMap entry deleted upon stream abort (ABORT)', () => {
    const { streamMap } = require('./sw.js');
    const url = '/sw-download-pipe/test-abort';

    let closed = false;
    const mockPort = {
      onmessage: null,
      onmessageerror: null,
      close: () => { closed = true; },
      postMessage: () => {}
    };

    listeners['message']({
      data: {
        type: 'INIT_PORT',
        url,
        filename: 'test.bin',
        size: 100,
        mime: 'application/octet-stream'
      },
      ports: [mockPort]
    });

    assert.equal(streamMap.has(url), true);

    // Send ABORT
    mockPort.onmessage({ data: { type: 'ABORT', reason: 'User canceled' } });

    assert.equal(streamMap.has(url), false);
    assert.equal(closed, true);
  });

  test('StreamMap entry deleted upon stream cancellation (ReadableStream cancel)', async () => {
    const { streamMap } = require('./sw.js');
    const url = '/sw-download-pipe/test-cancel';

    let postedMessage = null;
    let closed = false;
    const mockPort = {
      onmessage: null,
      onmessageerror: null,
      close: () => { closed = true; },
      postMessage: (msg) => { postedMessage = msg; }
    };

    listeners['message']({
      data: {
        type: 'INIT_PORT',
        url,
        filename: 'test.bin',
        size: 100,
        mime: 'application/octet-stream'
      },
      ports: [mockPort]
    });

    assert.equal(streamMap.has(url), true);
    const entry = streamMap.get(url);

    // Cancel the ReadableStream
    await entry.stream.cancel();

    assert.equal(streamMap.has(url), false);
    assert.deepEqual(postedMessage, { type: 'CANCEL' });
    assert.equal(closed, true);
  });

  test('StreamMap entry deleted upon port error (onmessageerror)', () => {
    const { streamMap } = require('./sw.js');
    const url = '/sw-download-pipe/test-error';

    let closed = false;
    const mockPort = {
      onmessage: null,
      onmessageerror: null,
      close: () => { closed = true; },
      postMessage: () => {}
    };

    listeners['message']({
      data: {
        type: 'INIT_PORT',
        url,
        filename: 'test.bin',
        size: 100,
        mime: 'application/octet-stream'
      },
      ports: [mockPort]
    });

    assert.equal(streamMap.has(url), true);

    // Trigger onmessageerror
    mockPort.onmessageerror();

    assert.equal(streamMap.has(url), false);
    assert.equal(closed, true);
  });

  test('StreamMap entry cleared automatically when TTL expires', async () => {
    const { streamMap, STREAM_TTL_MS } = require('./sw.js');
    const url = '/sw-download-pipe/test-ttl';

    // Mock setTimeout to capture TTL callback
    const originalSetTimeout = global.setTimeout;
    let timerCb = null;
    let timerDelay = 0;
    global.setTimeout = (cb, delay) => {
      timerCb = cb;
      timerDelay = delay;
      return 12345;
    };

    let closed = false;
    const mockPort = {
      onmessage: null,
      onmessageerror: null,
      close: () => { closed = true; },
      postMessage: () => {}
    };

    try {
      listeners['message']({
        data: {
          type: 'INIT_PORT',
          url,
          filename: 'test.bin',
          size: 100,
          mime: 'application/octet-stream'
        },
        ports: [mockPort]
      });

      assert.equal(streamMap.has(url), true);
      assert.equal(timerDelay, STREAM_TTL_MS);
      assert.notEqual(timerCb, null);

      // Trigger TTL timer expiration callback
      timerCb();

      assert.equal(streamMap.has(url), false);
      assert.equal(closed, true);
    } finally {
      global.setTimeout = originalSetTimeout;
    }
  });

  test('Fetch interceptor creates Response with RFC 6266 Content-Disposition', () => {
    const { streamMap } = require('./sw.js');
    const url = '/sw-download-pipe/test-fetch';
    const filename = 'Project Report (2026).pdf';

    const mockPort = {
      onmessage: null,
      onmessageerror: null,
      close: () => {},
      postMessage: () => {}
    };

    listeners['message']({
      data: {
        type: 'INIT_PORT',
        url,
        filename,
        size: 1024,
        mime: 'application/pdf'
      },
      ports: [mockPort]
    });

    assert.equal(streamMap.has(url), true);

    let responseResult = null;
    listeners['fetch']({
      request: { url: `http://localhost${url}` },
      respondWith: (resp) => {
        responseResult = resp;
      }
    });

    // Check that entry is deleted from streamMap upon fetch
    assert.equal(streamMap.has(url), false);
    assert.notEqual(responseResult, null);
    assert.equal(
      responseResult.headers.get('Content-Disposition'),
      'attachment; filename="Project Report (2026).pdf"; filename*=UTF-8\'\'Project%20Report%20%282026%29.pdf'
    );
    assert.equal(responseResult.headers.get('Content-Type'), 'application/pdf');
    assert.equal(responseResult.headers.get('Content-Length'), '1024');
  });

  test('INIT_PORT message handler posts READY message over message port', () => {
    require('./sw.js');
    const url = '/sw-download-pipe/test-ready';
    let readyPosted = false;

    const mockPort = {
      onmessage: null,
      onmessageerror: null,
      close: () => {},
      postMessage: (msg) => {
        if (msg && msg.type === 'READY') {
          readyPosted = true;
        }
      }
    };

    listeners['message']({
      data: {
        type: 'INIT_PORT',
        url,
        filename: 'test.bin',
        size: 100,
        mime: 'application/octet-stream'
      },
      ports: [mockPort]
    });

    assert.equal(readyPosted, true);
  });
});
