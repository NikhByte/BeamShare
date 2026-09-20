import * as fs from 'fs';
import * as path from 'path';
import * as crypto from 'crypto';

export const TEMP_DIR = path.join(__dirname, '..', '..', 'tmp-test-data');

export function ensureTempDir(): string {
  if (!fs.existsSync(TEMP_DIR)) {
    fs.mkdirSync(TEMP_DIR, { recursive: true });
  }
  return TEMP_DIR;
}

export function createTestFile(filename: string, content: string | Buffer): string {
  ensureTempDir();
  const filePath = path.join(TEMP_DIR, filename);
  fs.writeFileSync(filePath, content);
  return filePath;
}

export function computeHash(data: Buffer | string): string {
  return crypto.createHash('sha256').update(data).digest('hex');
}

export function cleanupTempDir(): void {
  if (fs.existsSync(TEMP_DIR)) {
    fs.rmSync(TEMP_DIR, { recursive: true, force: true });
  }
}
