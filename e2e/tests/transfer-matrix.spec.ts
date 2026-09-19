import { test, expect } from '@playwright/test';
import * as path from 'path';
import * as fs from 'fs';
import {
  startBeamSender,
  startRelayServer,
  BeamSenderInstance,
  RelayServerInstance,
  stopAllProcesses,
} from '../harness/cli-runner';
import {
  createTestFile,
  computeHash,
  cleanupTempDir,
} from '../harness/test-helpers';

test.describe('BeamShare Full Network Matrix File Transfer E2E', () => {
  let testFilePath: string;
  let testFileContent: Buffer;
  let expectedHash: string;

  test.beforeAll(() => {
    // Generate a reproducible test file payload (64KB payload with deterministic data)
    const buffer = Buffer.alloc(64 * 1024);
    for (let i = 0; i < buffer.length; i++) {
      buffer[i] = (i % 256);
    }
    testFileContent = buffer;
    testFilePath = createTestFile('test-transfer-payload.bin', buffer);
    expectedHash = computeHash(buffer);
  });

  test.afterAll(() => {
    stopAllProcesses();
    cleanupTempDir();
  });

  test.afterEach(async () => {
    stopAllProcesses();
  });

  test('Mode 1: Direct WebRTC P2P Transfer', async ({ page }) => {
    let beamInstance: BeamSenderInstance | null = null;
    try {
      beamInstance = startBeamSender({ filePath: testFilePath });
      const parsed = await beamInstance.parsedPromise;

      expect(parsed.webrtcURL).toBeTruthy();
      const url = new URL(parsed.webrtcURL);
      url.searchParams.set('no_stun', '1');
      const webrtcURL = url.toString();

      const downloadPromise = page.waitForEvent('download', { timeout: 30000 });

      await page.goto(webrtcURL);

      // Assert DOM lifecycle states
      // WebRTC mode starts at loading/webrtc -> downloading -> done
      await expect(page.locator('#state-done')).toBeVisible({ timeout: 30000 });
      await expect(page.locator('#done-title')).toHaveText(/Transfer complete|File Shared/i);

      const download = await downloadPromise;
      const downloadPath = await download.path();
      expect(downloadPath).toBeTruthy();

      if (downloadPath) {
        const receivedContent = fs.readFileSync(downloadPath);
        const receivedHash = computeHash(receivedContent);
        expect(receivedHash).toBe(expectedHash);
      }
    } finally {
      if (beamInstance) {
        await beamInstance.stop();
      }
    }
  });

  test('Mode 2: Direct LAN HTTP Streaming Fallback', async ({ page }) => {
    let beamInstance: BeamSenderInstance | null = null;
    try {
      beamInstance = startBeamSender({ filePath: testFilePath });
      const parsed = await beamInstance.parsedPromise;

      expect(parsed.localURL).toBeTruthy();

      await page.goto(parsed.localURL);

      // Should enter ready state displaying file metadata and Download button
      await expect(page.locator('#state-ready')).toBeVisible({ timeout: 15000 });
      await expect(page.locator('#file-name')).toHaveText(path.basename(testFilePath));

      const downloadPromise = page.waitForEvent('download', { timeout: 30000 });

      // Click Download File button
      await page.click('#btn-download');

      // State transition: downloading -> done
      await expect(page.locator('#state-done')).toBeVisible({ timeout: 30000 });
      await expect(page.locator('#done-title')).toHaveText(/Transfer complete|File Shared/i);

      const download = await downloadPromise;
      const downloadPath = await download.path();
      expect(downloadPath).toBeTruthy();

      if (downloadPath) {
        const receivedContent = fs.readFileSync(downloadPath);
        const receivedHash = computeHash(receivedContent);
        expect(receivedHash).toBe(expectedHash);
      }
    } finally {
      if (beamInstance) {
        await beamInstance.stop();
      }
    }
  });

  test('Mode 3: Global HTTP Relay Tunneling Fallback', async ({ page }) => {
    let relayInstance: RelayServerInstance | null = null;
    let beamInstance: BeamSenderInstance | null = null;

    try {
      relayInstance = await startRelayServer();
      beamInstance = startBeamSender({
        filePath: testFilePath,
        relayURL: relayInstance.url,
      });

      const parsed = await beamInstance.parsedPromise;
      expect(parsed.relayDisplayURL).toBeTruthy();

      // Modify local parameter to point to an unreachable port (simulate restricted firewalled LAN)
      const relayURL = new URL(parsed.relayDisplayURL!);
      relayURL.searchParams.set('local', 'http://127.0.0.1:0');

      await page.goto(relayURL.toString());

      // Should connect via relay and reach state-ready
      await expect(page.locator('#state-ready')).toBeVisible({ timeout: 20000 });
      await expect(page.locator('#file-name')).toHaveText(path.basename(testFilePath));

      const downloadPromise = page.waitForEvent('download', { timeout: 30000 });

      // Click Download button
      await page.click('#btn-download');

      // State transition to downloading -> done via relay stream
      await expect(page.locator('#state-done')).toBeVisible({ timeout: 30000 });
      await expect(page.locator('#done-title')).toHaveText(/Transfer complete|File Shared/i);

      const download = await downloadPromise;
      const downloadPath = await download.path();
      expect(downloadPath).toBeTruthy();

      if (downloadPath) {
        const receivedContent = fs.readFileSync(downloadPath);
        const receivedHash = computeHash(receivedContent);
        expect(receivedHash).toBe(expectedHash);
      }
    } finally {
      if (beamInstance) await beamInstance.stop();
      if (relayInstance) await relayInstance.stop();
    }
  });

  test('Mode 4: Seamless Automatic Fallback on WebRTC Failure', async ({ page }) => {
    let beamInstance: BeamSenderInstance | null = null;
    try {
      beamInstance = startBeamSender({ filePath: testFilePath });
      const parsed = await beamInstance.parsedPromise;

      // Abort WebRTC signaling API requests to simulate WebRTC connection failure
      await page.route('**/api/signal/**', (route) => route.abort());

      const fallbackURL = `${parsed.localURL}/?mode=webrtc`;

      await page.goto(fallbackURL);

      // WebRTC attempt will fail due to aborted signaling, and app should seamlessly transition to HTTP ready state
      await expect(page.locator('#state-ready')).toBeVisible({ timeout: 15000 });
      await expect(page.locator('#file-name')).toHaveText(path.basename(testFilePath));

      const downloadPromise = page.waitForEvent('download', { timeout: 30000 });

      await page.click('#btn-download');

      await expect(page.locator('#state-done')).toBeVisible({ timeout: 30000 });

      const download = await downloadPromise;
      const downloadPath = await download.path();
      expect(downloadPath).toBeTruthy();

      if (downloadPath) {
        const receivedContent = fs.readFileSync(downloadPath);
        const receivedHash = computeHash(receivedContent);
        expect(receivedHash).toBe(expectedHash);
      }
    } finally {
      if (beamInstance) {
        await beamInstance.stop();
      }
    }
  });
});
