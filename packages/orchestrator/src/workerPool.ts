import { WebSocketServer, WebSocket } from 'ws';
import https from 'node:https';
import crypto from 'node:crypto';
import fs from 'node:fs';
import {
  parseWorkerMessage,
  serializeMessage,
  type WorkerMessage,
  type OrchestratorMessage,
  type ExecAssign,
  type ExecStdout,
  type ExecStderr,
  type ExecComplete,
  type ExecProgress,
  type WorkerEvent,
  type WorkerBusy,
  type WorkerInfo,
} from '@dac-cloud/shared';
import type { Logger } from 'pino';
import type { IncomingMessage } from 'node:http';
import type { TlsConfig } from './tls.js';

/** Typed event map for WorkerPool → Dispatcher communication. */
export interface WorkerPoolEvents {
  'worker-connected': (info: WorkerInfo) => void;
  'worker-disconnected': (workerId: string, executionId: string | undefined) => void;
  'worker-ready': (workerId: string) => void;
  'stdout': (workerId: string, msg: ExecStdout) => void;
  'stderr': (workerId: string, msg: ExecStderr) => void;
  'complete': (workerId: string, msg: ExecComplete) => void;
  'persona-event': (workerId: string, msg: WorkerEvent) => void;
  'progress': (workerId: string, msg: ExecProgress) => void;
  'busy': (workerId: string, msg: WorkerBusy) => void;
}

const HEARTBEAT_INTERVAL_MS = 30_000;
const HEARTBEAT_TIMEOUT_MS = 90_000;
const HEARTBEAT_NEAR_MISS_THRESHOLD_MS = HEARTBEAT_TIMEOUT_MS * 0.5;

/** In-memory counters for worker pool connection lifecycle. */
export interface WorkerPoolMetrics {
  totalConnections: number;
  totalDisconnections: number;
  parseFailures: number;
  heartbeatNearMisses: number;
  /** Recent connection durations (ms) for the last N disconnects. */
  recentConnectionDurations: number[];
}

/** Constant-time string comparison to prevent timing side-channel attacks on token validation. */
function timingSafeTokenEqual(a: string, b: string): boolean {
  const bufA = Buffer.from(a, 'utf8');
  const bufB = Buffer.from(b, 'utf8');
  if (bufA.length !== bufB.length) {
    // Compare against self to maintain constant timing, then return false
    crypto.timingSafeEqual(bufA, bufA);
    return false;
  }
  return crypto.timingSafeEqual(bufA, bufB);
}

interface WorkerEntry {
  ws: WebSocket;
  info: WorkerInfo;
  heartbeatTimer?: ReturnType<typeof setInterval>;
}

export class WorkerPool {
  private workers = new Map<string, WorkerEntry>();
  private idleWorkers = new Set<string>();
  private wss: WebSocketServer | null = null;
  private allowedDigests: string[];
  private rejectUnverified: boolean;

  // Typed listener storage — one array per event name
  private listeners: { [K in keyof WorkerPoolEvents]?: WorkerPoolEvents[K][] } = {};

  /** Connection lifecycle metrics. */
  private metrics: WorkerPoolMetrics = {
    totalConnections: 0,
    totalDisconnections: 0,
    parseFailures: 0,
    heartbeatNearMisses: 0,
    recentConnectionDurations: [],
  };
  private static readonly MAX_DURATION_HISTORY = 100;

  constructor(
    private expectedWorkerToken: string,
    private logger: Logger,
    opts?: { allowedDigests?: string[]; rejectUnverified?: boolean },
  ) {
    this.allowedDigests = opts?.allowedDigests ?? [];
    this.rejectUnverified = opts?.rejectUnverified ?? false;
  }

  /** Register a typed event listener. */
  on<K extends keyof WorkerPoolEvents>(event: K, listener: WorkerPoolEvents[K]): this {
    if (!this.listeners[event]) {
      this.listeners[event] = [];
    }
    this.listeners[event]!.push(listener);
    return this;
  }

  /** Emit a typed event. */
  private emit<K extends keyof WorkerPoolEvents>(event: K, ...args: Parameters<WorkerPoolEvents[K]>): void {
    const fns = this.listeners[event];
    if (fns) {
      for (const fn of fns) {
        (fn as (...a: Parameters<WorkerPoolEvents[K]>) => void)(...args);
      }
    }
  }

  listen(port: number, tlsConfig?: TlsConfig): void {
    if (tlsConfig?.enabled) {
      const httpsServer = https.createServer({
        cert: fs.readFileSync(tlsConfig.certPath),
        key: fs.readFileSync(tlsConfig.keyPath),
        ca: tlsConfig.caPath ? fs.readFileSync(tlsConfig.caPath) : undefined,
        requestCert: tlsConfig.requireClientCert,
        rejectUnauthorized: tlsConfig.requireClientCert,
      });
      this.wss = new WebSocketServer({ server: httpsServer });
      httpsServer.listen(port);
      this.logger.info({ port, tls: true, mTLS: tlsConfig.requireClientCert }, 'Worker pool listening (WSS)');
    } else {
      this.wss = new WebSocketServer({ port });
      this.logger.info({ port, tls: false }, 'Worker pool listening (WS — plaintext)');
    }

    this.wss.on('connection', (ws: WebSocket, req: IncomingMessage) => {
      this.logger.info({ remoteAddr: req.socket.remoteAddress }, 'Worker connection attempt');

      // Validate worker token from Authorization header (timing-safe comparison)
      const authHeader = req.headers.authorization;
      const token = authHeader?.startsWith('Bearer ') ? authHeader.slice(7) : null;
      if (!token || !timingSafeTokenEqual(token, this.expectedWorkerToken)) {
        this.logger.warn({ remoteAddr: req.socket.remoteAddress }, 'Worker rejected: invalid token');
        ws.close(1008, 'Invalid worker token');
        return;
      }

      this.handleConnection(ws);
    });

    this.wss.on('error', (err) => {
      this.logger.error({ err }, 'WebSocket server error');
    });
  }

  private handleConnection(ws: WebSocket): void {
    let workerId: string | null = null;

    const connectionTimeout = setTimeout(() => {
      if (!workerId) {
        this.logger.warn('Worker did not send hello within timeout, closing');
        ws.close(1008, 'Hello timeout');
      }
    }, 10_000);

    ws.on('message', (raw: Buffer) => {
      const msg = parseWorkerMessage(raw.toString());
      if (!msg) {
        this.metrics.parseFailures++;
        this.logger.warn({ parseFailures: this.metrics.parseFailures }, 'Invalid message from worker');
        return;
      }

      if (msg.type === 'hello') {
        clearTimeout(connectionTimeout);
        workerId = msg.workerId;
        this.registerWorker(workerId, ws, msg.version, msg.capabilities, msg.imageDigest, msg.claudeCliVersion);
        return;
      }

      if (!workerId) {
        this.logger.warn({ type: msg.type }, 'Message before hello');
        return;
      }

      this.handleWorkerMessage(workerId, msg);
    });

    ws.on('close', () => {
      clearTimeout(connectionTimeout);
      if (workerId) {
        this.unregisterWorker(workerId);
      }
    });

    ws.on('error', (err) => {
      this.logger.error({ err, workerId }, 'Worker WebSocket error');
    });
  }

  private registerWorker(workerId: string, ws: WebSocket, version: string, capabilities: string[], imageDigest?: string, claudeCliVersion?: string): void {
    // I10: If a worker with this ID already exists, disconnect the old one first
    const existing = this.workers.get(workerId);
    if (existing) {
      this.logger.warn({ workerId }, 'Duplicate worker ID — disconnecting previous connection');
      if (existing.heartbeatTimer) clearInterval(existing.heartbeatTimer);
      existing.ws.close(1001, 'Replaced by new connection');
      this.workers.delete(workerId);
      this.idleWorkers.delete(workerId);
    }

    const verified = this.allowedDigests.length === 0 || (!!imageDigest && this.allowedDigests.includes(imageDigest));

    if (this.rejectUnverified && !verified) {
      this.logger.warn({ workerId, imageDigest }, 'Rejecting unverified worker — digest not in allowlist');
      ws.close(1008, 'Unverified image digest');
      return;
    }

    const info: WorkerInfo = {
      workerId,
      status: 'idle',
      version,
      capabilities,
      connectedAt: Date.now(),
      lastHeartbeat: Date.now(),
      imageDigest,
      claudeCliVersion,
      verified,
    };

    const heartbeatTimer = setInterval(() => {
      const entry = this.workers.get(workerId);
      if (!entry) return;

      const heartbeatAge = Date.now() - entry.info.lastHeartbeat;

      if (heartbeatAge > HEARTBEAT_TIMEOUT_MS) {
        this.logger.warn({ workerId }, 'Worker heartbeat timeout');
        entry.ws.close(1001, 'Heartbeat timeout');
        return;
      }

      if (heartbeatAge > HEARTBEAT_NEAR_MISS_THRESHOLD_MS) {
        this.metrics.heartbeatNearMisses++;
        this.logger.warn({ workerId, heartbeatAge, thresholdMs: HEARTBEAT_NEAR_MISS_THRESHOLD_MS }, 'Heartbeat near-miss');
      }

      this.send(workerId, { type: 'heartbeat', timestamp: Date.now() });
    }, HEARTBEAT_INTERVAL_MS);

    this.workers.set(workerId, { ws, info, heartbeatTimer });
    this.idleWorkers.add(workerId);
    this.metrics.totalConnections++;

    // Send ack
    this.send(workerId, {
      type: 'ack',
      workerId,
      sessionToken: `session-${Date.now()}`,
    });

    this.logger.info({ workerId, version, capabilities, imageDigest, claudeCliVersion, verified }, 'Worker registered');
    this.emit('worker-connected', info);
  }

  private unregisterWorker(workerId: string): void {
    const entry = this.workers.get(workerId);
    if (!entry) return;

    if (entry.heartbeatTimer) {
      clearInterval(entry.heartbeatTimer);
    }

    const hadExecution = entry.info.currentExecutionId;
    const connectionDurationMs = Date.now() - entry.info.connectedAt;

    this.workers.delete(workerId);
    this.idleWorkers.delete(workerId);
    this.metrics.totalDisconnections++;

    // Track recent connection durations (ring buffer)
    this.metrics.recentConnectionDurations.push(connectionDurationMs);
    if (this.metrics.recentConnectionDurations.length > WorkerPool.MAX_DURATION_HISTORY) {
      this.metrics.recentConnectionDurations.shift();
    }

    this.logger.info({ workerId, hadExecution, connectionDurationMs }, 'Worker disconnected');
    this.emit('worker-disconnected', workerId, hadExecution);
  }

  private handleWorkerMessage(workerId: string, msg: WorkerMessage): void {
    const entry = this.workers.get(workerId);
    if (!entry) return;

    switch (msg.type) {
      case 'heartbeat':
        entry.info.lastHeartbeat = Date.now();
        break;

      case 'ready':
        entry.info.status = 'idle';
        entry.info.currentExecutionId = undefined;
        this.idleWorkers.add(workerId);
        this.emit('worker-ready', workerId);
        break;

      case 'stdout':
        this.emit('stdout', workerId, msg);
        break;

      case 'stderr':
        this.emit('stderr', workerId, msg);
        break;

      case 'complete':
        entry.info.status = 'idle';
        entry.info.currentExecutionId = undefined;
        this.idleWorkers.add(workerId);
        this.logger.info({
          workerId,
          executionId: msg.executionId,
          status: msg.status,
          durationMs: msg.durationMs,
          ...(msg.phaseTimings ? { phaseTimings: msg.phaseTimings } : {}),
        }, 'Execution complete');
        this.emit('complete', workerId, msg);
        break;

      case 'event':
        this.emit('persona-event', workerId, msg);
        break;

      case 'progress':
        this.emit('progress', workerId, msg);
        break;

      case 'busy':
        this.logger.warn({ workerId, executionId: msg.executionId, reason: msg.reason }, 'Worker rejected assignment (busy)');
        // Mark worker as idle again so it can receive future work once it finishes
        entry.info.status = 'idle';
        entry.info.currentExecutionId = undefined;
        this.idleWorkers.add(workerId);
        this.emit('busy', workerId, msg);
        break;

      default:
        this.logger.warn({ workerId, type: (msg as WorkerMessage).type }, 'Unknown message type');
    }
  }

  send(workerId: string, msg: OrchestratorMessage): boolean {
    const entry = this.workers.get(workerId);
    if (!entry || entry.ws.readyState !== WebSocket.OPEN) return false;

    entry.ws.send(serializeMessage(msg));
    return true;
  }

  assign(workerId: string, assignment: ExecAssign): boolean {
    const entry = this.workers.get(workerId);
    if (!entry || entry.info.status !== 'idle') return false;

    entry.info.status = 'executing';
    entry.info.currentExecutionId = assignment.executionId;
    this.idleWorkers.delete(workerId);
    return this.send(workerId, assignment);
  }

  getIdleWorker(): string | null {
    const first = this.idleWorkers.values().next();
    return first.done ? null : first.value;
  }

  getWorkers(): WorkerInfo[] {
    return Array.from(this.workers.values()).map(e => ({ ...e.info }));
  }

  getWorkerCount(): { total: number; idle: number; executing: number } {
    const idle = this.idleWorkers.size;
    return { total: this.workers.size, idle, executing: this.workers.size - idle };
  }

  getMetrics(): WorkerPoolMetrics {
    return { ...this.metrics, recentConnectionDurations: [...this.metrics.recentConnectionDurations] };
  }

  /** Return worker IDs whose last heartbeat exceeds the near-miss threshold. */
  getStaleWorkers(): Array<{ workerId: string; heartbeatAgeMs: number }> {
    const now = Date.now();
    const stale: Array<{ workerId: string; heartbeatAgeMs: number }> = [];
    for (const [workerId, entry] of this.workers) {
      const age = now - entry.info.lastHeartbeat;
      if (age > HEARTBEAT_NEAR_MISS_THRESHOLD_MS) {
        stale.push({ workerId, heartbeatAgeMs: age });
      }
    }
    return stale;
  }

  shutdown(): void {
    for (const [workerId] of this.workers) {
      this.send(workerId, {
        type: 'shutdown',
        reason: 'Orchestrator shutting down',
        gracePeriodMs: 30_000,
      });
    }
    this.wss?.close();
    for (const entry of this.workers.values()) {
      if (entry.heartbeatTimer) clearInterval(entry.heartbeatTimer);
    }
  }
}
