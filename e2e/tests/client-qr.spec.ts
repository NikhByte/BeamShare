import { test, expect } from '@playwright/test';
import * as path from 'path';
import * as fs from 'fs';
import jsQR from 'jsqr';
import {
  startBeamSender,
  startRelayServer,
  BeamSenderInstance,
  RelayServerInstance,
  stopAllProcesses,
} from '../harness/cli-runner';
import {
  createTestFile,
  cleanupTempDir,
} from '../harness/test-helpers';

test.describe('Client-Side QR Code Rendering & Privacy E2E Tests', () => {
  let relayServer: RelayServerInstance | null = null;
  let testFilePath: string;

  test.beforeAll(() => {
    const buffer = Buffer.from('BeamShare Local QR Test Payload Data', 'utf8');
    testFilePath = createTestFile('qr-test-payload.txt', buffer);
  });

  test.afterAll(() => {
    stopAllProcesses();
    cleanupTempDir();
  });

  test.afterEach(() => {
    stopAllProcesses();
  });

  test('Web Sender generates QR code client-side without third-party requests', async ({ page }) => {
    relayServer = await startRelayServer();
    const relayAddr = relayServer.url;

    let thirdPartyQRRequestMade = false;

    // Monitor network requests
    page.on('request', (request) => {
      const url = request.url();
      if (url.includes('qrserver.com') || (request.resourceType() === 'image' && !url.startsWith('http://localhost') && !url.startsWith('http://127.0.0.1') && !url.startsWith('data:'))) {
        thirdPartyQRRequestMade = true;
      }
    });

    // Abort any external third party calls if attempted
    await page.route('**/*', (route) => {
      const url = route.request().url();
      if (url.includes('qrserver.com')) {
        return route.abort();
      }
      return route.continue();
    });

    // Open Gaze web receiver/sender page on relay server
    await page.goto(`${relayAddr}/`);

    // Switch to "Send" tab
    await page.evaluate(() => {
      const tabHeader = document.getElementById('tab-header');
      if (tabHeader) tabHeader.classList.remove('hidden');
      if (typeof (window as any).switchTab === 'function') {
        (window as any).switchTab('send');
      } else {
        const sendBtn = document.getElementById('tab-send');
        sendBtn?.click();
      }
    });

    // Upload test file
    const fileInput = page.locator('#sender-file-input');
    await fileInput.setInputFiles(testFilePath);

    // Wait for "Start P2P Sharing" button to be visible and click it
    await page.waitForSelector('#btn-start-share:not([disabled])');
    await page.click('#btn-start-share');

    // Wait for QR code image to appear
    const qrImg = page.locator('#send-qr-img');
    await expect(qrImg).toBeVisible();

    // Verify image source is a local data URI
    const src = await qrImg.getAttribute('src');
    expect(src).toBeTruthy();
    expect(src?.startsWith('data:image/')).toBe(true);
    expect(src?.includes('api.qrserver.com')).toBe(false);

    // Confirm no network requests were sent to qrserver.com or third-party image services
    expect(thirdPartyQRRequestMade).toBe(false);

    // Verify share link input contains the #k= encryption key fragment
    const urlInputValue = await page.inputValue('#send-url-input');
    expect(urlInputValue).toContain('#k=');

    // Capture screenshot for visual verification
    await page.screenshot({ path: '/tmp/client_qr_verification.png', fullPage: true });

    // Validate that screenshot exists
    expect(fs.existsSync('/tmp/client_qr_verification.png')).toBe(true);
  });
});
