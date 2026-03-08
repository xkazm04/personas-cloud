import 'dotenv/config';
import { nanoid } from 'nanoid';

export interface WorkerConfig {
  orchestratorUrl: string;
  workerToken: string;
  workerId: string;
  /** Maximum concurrent executions this worker can handle (default 1). */
  maxConcurrentExecutions: number;
  /** Grace period (ms) for SIGTERM before force-killing active executions (default 30000). */
  shutdownGracePeriodMs: number;
}

function required(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return value;
}

export function loadConfig(): WorkerConfig {
  const orchestratorUrl = required('ORCHESTRATOR_URL');

  // Enforce WSS (TLS) for production — credentials are sent over this connection.
  // Allow plain ws:// only when ALLOW_INSECURE_WS=1 is explicitly set (local dev).
  if (orchestratorUrl.startsWith('ws://')) {
    if (process.env['ALLOW_INSECURE_WS'] === '1') {
      console.warn(
        '⚠️  WARNING: Connecting to orchestrator over plaintext WebSocket (ws://). ' +
        'Credentials will be transmitted without encryption. ' +
        'Do NOT use this in production.',
      );
    } else {
      throw new Error(
        'ORCHESTRATOR_URL uses plaintext ws:// which exposes credentials on the network. ' +
        'Use wss:// for TLS-encrypted connections. ' +
        'To override for local development, set ALLOW_INSECURE_WS=1.',
      );
    }
  }

  const maxConcurrent = parseInt(process.env['MAX_CONCURRENT_EXECUTIONS'] || '1', 10);
  const gracePeriod = parseInt(process.env['SHUTDOWN_GRACE_PERIOD_MS'] || '30000', 10);
  return {
    orchestratorUrl,
    workerToken: required('WORKER_TOKEN'),
    workerId: process.env['WORKER_ID'] || `worker-${nanoid(8)}`,
    maxConcurrentExecutions: Number.isFinite(maxConcurrent) && maxConcurrent >= 1 ? maxConcurrent : 1,
    shutdownGracePeriodMs: Number.isFinite(gracePeriod) && gracePeriod >= 0 ? gracePeriod : 30_000,
  };
}
