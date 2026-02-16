import { WebSocketServer, WebSocket } from 'ws';
import { EventEmitter } from 'node:events';
import {
  parseMessage,
  serializeMessage,
  type WorkerMessage,
  type OrchestratorMessage,
  type ExecAssign,
  type WorkerInfo,
} from '@dac-cloud/shared';
import type { Logger } from 'pino';
import type { IncomingMessage } from 'node:http';

const HEARTBEAT_INTERVAL_MS = 30_000;
const HEARTBEAT_TIMEOUT_MS = 90_000;

interface WorkerEntry {
  ws: WebSocket;
  info: WorkerInfo;
  heartbeatTimer?: ReturnType<typeof setInterval>;
}

export class WorkerPool extends EventEmitter {
  private workers = new Map<string, WorkerEntry>();
  private wss: WebSocketServer | null = null;

  constructor(
    private expectedWorkerToken: string,
    private logger: Logger,
  ) {
    super();
  }

  listen(port: number): void {
    this.wss = new WebSocketServer({ port });

    this.wss.on('connection', (ws: WebSocket, req: IncomingMessage) => {
      this.logger.info({ remoteAddr: req.socket.remoteAddress }, 'Worker connection attempt');

      // C3: Validate worker token from query string (?token=...)
      const url = new URL(req.url || '/', `http://${req.headers.host || 'localhost'}`);
      const token = url.searchParams.get('token');
      if (!token || token !== this.expectedWorkerToken) {
        this.logger.warn({ remoteAddr: req.socket.remoteAddress }, 'Worker rejected: invalid token');
        ws.close(1008, 'Invalid worker token');
        return;
      }

      this.handleConnection(ws);
    });

    this.wss.on('error', (err) => {
      this.logger.error({ err }, 'WebSocket server error');
    });

    this.logger.info({ port }, 'Worker pool listening');
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
      const msg = parseMessage<WorkerMessage>(raw.toString());
      if (!msg) {
        this.logger.warn('Invalid message from worker');
        return;
      }

      if (msg.type === 'hello') {
        clearTimeout(connectionTimeout);
        workerId = msg.workerId;
        this.registerWorker(workerId, ws, msg.version, msg.capabilities);
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

  private registerWorker(workerId: string, ws: WebSocket, version: string, capabilities: string[]): void {
    // I10: If a worker with this ID already exists, disconnect the old one first
    const existing = this.workers.get(workerId);
    if (existing) {
      this.logger.warn({ workerId }, 'Duplicate worker ID — disconnecting previous connection');
      if (existing.heartbeatTimer) clearInterval(existing.heartbeatTimer);
      existing.ws.close(1001, 'Replaced by new connection');
      this.workers.delete(workerId);
    }

    const info: WorkerInfo = {
      workerId,
      status: 'idle',
      version,
      capabilities,
      connectedAt: Date.now(),
      lastHeartbeat: Date.now(),
    };

    const heartbeatTimer = setInterval(() => {
      const entry = this.workers.get(workerId);
      if (!entry) return;

      if (Date.now() - entry.info.lastHeartbeat > HEARTBEAT_TIMEOUT_MS) {
        this.logger.warn({ workerId }, 'Worker heartbeat timeout');
        entry.ws.close(1001, 'Heartbeat timeout');
        return;
      }

      this.send(workerId, { type: 'heartbeat', timestamp: Date.now() });
    }, HEARTBEAT_INTERVAL_MS);

    this.workers.set(workerId, { ws, info, heartbeatTimer });

    // Send ack
    this.send(workerId, {
      type: 'ack',
      workerId,
      sessionToken: `session-${Date.now()}`,
    });

    this.logger.info({ workerId, version, capabilities }, 'Worker registered');
    this.emit('worker-connected', info);
  }

  private unregisterWorker(workerId: string): void {
    const entry = this.workers.get(workerId);
    if (!entry) return;

    if (entry.heartbeatTimer) {
      clearInterval(entry.heartbeatTimer);
    }

    const hadExecution = entry.info.currentExecutionId;
    this.workers.delete(workerId);

    this.logger.info({ workerId, hadExecution }, 'Worker disconnected');
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
        this.logger.info({
          workerId,
          executionId: msg.executionId,
          status: msg.status,
          durationMs: msg.durationMs,
        }, 'Execution complete');
        this.emit('complete', workerId, msg);
        break;

      case 'event':
        this.emit('persona-event', workerId, msg);
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
    return this.send(workerId, assignment);
  }

  getIdleWorker(): string | null {
    for (const [workerId, entry] of this.workers) {
      if (entry.info.status === 'idle') {
        return workerId;
      }
    }
    return null;
  }

  getWorkers(): WorkerInfo[] {
    return Array.from(this.workers.values()).map(e => ({ ...e.info }));
  }

  getWorkerCount(): { total: number; idle: number; executing: number } {
    let idle = 0;
    let executing = 0;
    for (const entry of this.workers.values()) {
      if (entry.info.status === 'idle') idle++;
      if (entry.info.status === 'executing') executing++;
    }
    return { total: this.workers.size, idle, executing };
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
