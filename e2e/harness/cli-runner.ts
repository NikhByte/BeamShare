import { spawn, ChildProcess, execSync } from 'child_process';
import * as path from 'path';
import * as fs from 'fs';
import * as net from 'net';

export const ROOT_DIR = path.join(__dirname, '..', '..');
export const BIN_DIR = path.join(ROOT_DIR, 'bin');
export const BEAM_BIN = path.join(BIN_DIR, 'beam');
export const RELAY_BIN = path.join(BIN_DIR, 'relay');

const activeProcesses = new Set<ChildProcess>();

function cleanupAllProcesses() {
  for (const proc of activeProcesses) {
    if (!proc.killed) {
      try {
        proc.kill('SIGKILL');
      } catch (e) {
        // ignore
      }
    }
  }
  activeProcesses.clear();
}

process.on('exit', cleanupAllProcesses);
process.on('SIGINT', cleanupAllProcesses);
process.on('SIGTERM', cleanupAllProcesses);

export function ensureBinariesExist(): void {
  if (!fs.existsSync(BIN_DIR)) {
    fs.mkdirSync(BIN_DIR, { recursive: true });
  }

  if (!fs.existsSync(BEAM_BIN) || !fs.existsSync(RELAY_BIN)) {
    console.log('Building beam and relay Go binaries...');
    execSync(`go build -o ${BEAM_BIN} ./cmd/beam`, { cwd: ROOT_DIR, stdio: 'inherit' });
    execSync(`go build -o ${RELAY_BIN} ./cmd/relay`, { cwd: ROOT_DIR, stdio: 'inherit' });
  }
}

export function findFreePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const server = net.createServer();
    server.listen(0, '127.0.0.1', () => {
      const port = (server.address() as net.AddressInfo).port;
      server.close(() => resolve(port));
    });
    server.on('error', reject);
  });
}

export interface RelayServerInstance {
  port: number;
  url: string;
  process: ChildProcess;
  stop: () => Promise<void>;
}

export async function startRelayServer(port?: number): Promise<RelayServerInstance> {
  ensureBinariesExist();
  const relayPort = port || (await findFreePort());

  const proc = spawn(RELAY_BIN, [], {
    cwd: ROOT_DIR,
    env: { ...process.env, PORT: relayPort.toString() },
    stdio: ['ignore', 'pipe', 'pipe'],
  });

  activeProcesses.add(proc);

  const relayURL = `http://127.0.0.1:${relayPort}`;

  // Wait for port to be open
  let connected = false;
  for (let i = 0; i < 50; i++) {
    try {
      await new Promise<void>((resolve, reject) => {
        const socket = net.connect(relayPort, '127.0.0.1', () => {
          socket.end();
          resolve();
        });
        socket.on('error', reject);
      });
      connected = true;
      break;
    } catch (e) {
      await new Promise((r) => setTimeout(r, 100));
    }
  }

  if (!connected) {
    proc.kill('SIGKILL');
    activeProcesses.delete(proc);
    throw new Error(`Relay server failed to start on port ${relayPort}`);
  }

  const stop = async () => {
    if (!proc.killed) {
      proc.kill('SIGTERM');
      await new Promise((r) => setTimeout(r, 200));
      if (!proc.killed) {
        proc.kill('SIGKILL');
      }
    }
    activeProcesses.delete(proc);
  };

  return {
    port: relayPort,
    url: relayURL,
    process: proc,
    stop,
  };
}

export interface ParsedBeamOutput {
  localURL: string;
  webrtcURL: string;
  relayDisplayURL?: string;
  fullOutput: string;
}

export interface BeamSenderInstance {
  process: ChildProcess;
  getOutput: () => string;
  parsedPromise: Promise<ParsedBeamOutput>;
  stop: () => Promise<void>;
}

export function startBeamSender(options: {
  filePath: string;
  relayURL?: string;
  receiverURL?: string;
  extraArgs?: string[];
  env?: Record<string, string>;
}): BeamSenderInstance {
  ensureBinariesExist();

  const args: string[] = ['send', options.filePath, '--discovery-timeout=1'];
  if (options.relayURL) {
    args.push(`--relay=${options.relayURL}`);
  } else {
    args.push('--relay=none');
  }
  if (options.receiverURL) {
    args.push(`--receiver=${options.receiverURL}`);
  }
  if (options.extraArgs) {
    args.push(...options.extraArgs);
  }

  const proc = spawn(BEAM_BIN, args, {
    cwd: ROOT_DIR,
    env: { ...process.env, ...options.env },
    stdio: ['ignore', 'pipe', 'pipe'],
  });

  activeProcesses.add(proc);

  let output = '';
  let resolveParsed!: (val: ParsedBeamOutput) => void;
  let rejectParsed!: (err: Error) => void;

  const parsedPromise = new Promise<ParsedBeamOutput>((resolve, reject) => {
    resolveParsed = resolve;
    rejectParsed = reject;
  });

  let resolved = false;
  let isCheckingPort = false;

  const checkOutput = () => {
    if (resolved || isCheckingPort) return;

    // Check if output contains URLs
    const directMatch = output.match(/http:\/\/(127\.0\.0\.1|localhost|192\.\d+\.\d+\.\d+|10\.\d+\.\d+\.\d+):\d+/);
    const webrtcMatch = output.match(/http:\/\/[^\s]+[\?&]mode=webrtc[^\s]*/);
    const relayMatch = output.match(/http:\/\/[^\s]+[\?&]s=[^&\s#]+[^\s]*/);

    const isRelayRequested = options.relayURL && options.relayURL !== 'none' && options.relayURL !== 'disabled';
    const hasCompleteOutput = isRelayRequested
      ? !!relayMatch
      : (output.includes('Waiting for receiver') || output.includes('sdp='));

    if (directMatch && hasCompleteOutput) {
      isCheckingPort = true;
      let localURL = directMatch[0];
      localURL = localURL.replace(/http:\/\/(192\.\d+\.\d+\.\d+|10\.\d+\.\d+\.\d+|0\.0\.0\.0)/, 'http://127.0.0.1');
      const port = parseInt(new URL(localURL).port, 10);

      // Verify server TCP port is open and listening before returning
      const checkPort = async () => {
        let open = false;
        for (let i = 0; i < 50; i++) {
          try {
            await new Promise<void>((res, rej) => {
              const socket = net.connect(port, '127.0.0.1', () => {
                socket.end();
                res();
              });
              socket.on('error', rej);
            });
            open = true;
            break;
          } catch (e) {
            await new Promise((r) => setTimeout(r, 100));
          }
        }

        if (open && !resolved) {
          let webrtcURL = `${localURL}/?mode=webrtc`;
          if (webrtcMatch) {
            webrtcURL = webrtcMatch[0].replace(/http:\/\/(192\.\d+\.\d+\.\d+|10\.\d+\.\d+\.\d+|0\.0\.0\.0)/, 'http://127.0.0.1');
          } else {
            const sdpMatch = output.match(/sdp=([^&\s#]+)/);
            if (sdpMatch) {
              webrtcURL = `${localURL}/?mode=webrtc&sdp=${sdpMatch[1]}`;
            }
          }

          resolved = true;
          resolveParsed({
            localURL,
            webrtcURL,
            relayDisplayURL: relayMatch ? relayMatch[0] : undefined,
            fullOutput: output,
          });
        } else {
          isCheckingPort = false;
        }
      };

      checkPort();
    }
  };

  proc.stdout?.on('data', (data) => {
    output += data.toString();
    checkOutput();
  });

  proc.stderr?.on('data', (data) => {
    output += data.toString();
    checkOutput();
  });

  proc.on('error', (err) => {
    if (!resolved) {
      resolved = true;
      rejectParsed(err);
    }
  });

  proc.on('exit', (code) => {
    if (!resolved) {
      resolved = true;
      rejectParsed(new Error(`beam process exited unexpectedly with code ${code}\nOutput: ${output}`));
    }
  });

  // Set 15s timeout for URL parsing
  setTimeout(() => {
    if (!resolved) {
      resolved = true;
      rejectParsed(new Error(`Timeout waiting for beam CLI output.\nOutput so far:\n${output}`));
    }
  }, 15000);

  const stop = async () => {
    if (!proc.killed) {
      proc.kill('SIGTERM');
      await new Promise((r) => setTimeout(r, 200));
      if (!proc.killed) {
        proc.kill('SIGKILL');
      }
    }
    activeProcesses.delete(proc);
  };

  return {
    process: proc,
    getOutput: () => output,
    parsedPromise,
    stop,
  };
}

export function stopAllProcesses(): void {
  cleanupAllProcesses();
}
