import http from 'node:http';
import fs from 'node:fs';
import os from 'node:os';
import type { Logger } from 'pino';
import type { Connection } from './connection.js';
import type { ExecutorStatus } from './executor.js';
import { isSandboxAvailable } from './sandbox.js';
import { getActiveCredentialFileCount } from './credentialInjector.js';

interface DiskInfo {
  path: string;
  availableBytes: number;
  totalBytes: number;
}

function getDiskInfo(dirPath: string): DiskInfo | null {
  try {
    const stats = fs.statfsSync(dirPath);
    return {
      path: dirPath,
      availableBytes: stats.bavail * stats.bsize,
      totalBytes: stats.blocks * stats.bsize,
    };
  } catch {
    return null;
  }
}

function buildHealthPayload(
  connection: Connection,
  executor: { getStatus(): ExecutorStatus },
  workerId: string,
  startTime: number,
) {
  const wsState = connection.getConnectionState();
  const execStatus = executor.getStatus();

  const tmpdir = getDiskInfo(os.tmpdir());
  const devShm = process.platform === 'linux' && fs.existsSync('/dev/shm')
    ? getDiskInfo('/dev/shm')
    : null;

  return {
    status: wsState === 'connected' ? 'healthy' as const : 'degraded' as const,
    workerId,
    uptimeMs: Date.now() - startTime,
    websocket: wsState,
    execution: execStatus,
    sandbox: {
      bwrapAvailable: isSandboxAvailable(),
    },
    disk: {
      tmpdir,
      devShm,
    },
    credentials: {
      activeFileCount: getActiveCredentialFileCount(),
    },
  };
}

export function startHealthProbe(
  port: number,
  connection: Connection,
  executor: { getStatus(): ExecutorStatus },
  workerId: string,
  logger: Logger,
): http.Server {
  const startTime = Date.now();

  const server = http.createServer((req, res) => {
    if (req.method !== 'GET') {
      res.writeHead(405);
      res.end();
      return;
    }

    const payload = buildHealthPayload(connection, executor, workerId, startTime);

    if (req.url === '/healthz') {
      // Liveness: alive unless WS is permanently disconnected (shutting down)
      const alive = payload.websocket !== 'disconnected';
      res.writeHead(alive ? 200 : 503, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(payload));
      return;
    }

    if (req.url === '/readyz') {
      // Readiness: ready to accept work when connected and idle
      const ready = payload.websocket === 'connected' && payload.execution.state === 'idle';
      res.writeHead(ready ? 200 : 503, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(payload));
      return;
    }

    res.writeHead(404);
    res.end();
  });

  server.listen(port, '0.0.0.0', () => {
    logger.info({ port }, 'Health probe listening');
  });

  server.on('error', (err) => {
    logger.error({ err, port }, 'Health probe server error');
  });

  return server;
}
