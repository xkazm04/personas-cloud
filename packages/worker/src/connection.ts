import WebSocket from 'ws';
import {
  parseMessage,
  serializeMessage,
  PROTOCOL_VERSION,
  type OrchestratorMessage,
  type WorkerMessage,
} from '@dac-cloud/shared';
import type { Logger } from 'pino';

const HEARTBEAT_INTERVAL_MS = 30_000;
const MAX_RECONNECT_DELAY_MS = 30_000;
const INITIAL_RECONNECT_DELAY_MS = 1_000;

export interface ConnectionCallbacks {
  onAssign(msg: OrchestratorMessage & { type: 'assign' }): void;
  onCancel(msg: OrchestratorMessage & { type: 'cancel' }): void;
  onShutdown(msg: OrchestratorMessage & { type: 'shutdown' }): void;
}

export class Connection {
  private ws: WebSocket | null = null;
  private connected = false;
  private reconnectDelay = INITIAL_RECONNECT_DELAY_MS;
  private heartbeatTimer?: ReturnType<typeof setInterval>;
  private reconnectTimer?: ReturnType<typeof setTimeout>;
  private shuttingDown = false;

  constructor(
    private orchestratorUrl: string,
    private workerId: string,
    private workerToken: string,
    private callbacks: ConnectionCallbacks,
    private logger: Logger,
  ) {}

  connect(): void {
    if (this.shuttingDown) return;

    this.logger.info({ url: this.orchestratorUrl, workerId: this.workerId }, 'Connecting to orchestrator');

    // Append worker token as query parameter for authentication
    const urlWithToken = `${this.orchestratorUrl}?token=${encodeURIComponent(this.workerToken)}`;
    this.ws = new WebSocket(urlWithToken);

    this.ws.on('open', () => {
      this.connected = true;
      this.reconnectDelay = INITIAL_RECONNECT_DELAY_MS;

      // Send hello
      this.send({
        type: 'hello',
        workerId: this.workerId,
        version: PROTOCOL_VERSION,
        capabilities: ['claude-cli', 'node', 'git'],
      });

      // Start heartbeat
      this.heartbeatTimer = setInterval(() => {
        this.send({ type: 'heartbeat', timestamp: Date.now() });
      }, HEARTBEAT_INTERVAL_MS);

      this.logger.info('Connected to orchestrator');
    });

    this.ws.on('message', (raw: Buffer) => {
      const msg = parseMessage<OrchestratorMessage>(raw.toString());
      if (!msg) {
        this.logger.warn('Invalid message from orchestrator');
        return;
      }

      this.handleMessage(msg);
    });

    this.ws.on('close', (code: number, reason: Buffer) => {
      this.logger.info({ code, reason: reason.toString() }, 'Disconnected from orchestrator');
      this.cleanup();
      this.scheduleReconnect();
    });

    this.ws.on('error', (err: Error) => {
      this.logger.error({ err }, 'WebSocket error');
      // 'close' will follow, which handles reconnection
    });
  }

  private handleMessage(msg: OrchestratorMessage): void {
    switch (msg.type) {
      case 'ack':
        this.logger.info({ workerId: msg.workerId }, 'Received ack from orchestrator');
        // Send ready after ack
        this.sendReady();
        break;

      case 'assign':
        this.logger.info({ executionId: msg.executionId, personaId: msg.personaId }, 'Received execution assignment');
        this.callbacks.onAssign(msg as any);
        break;

      case 'cancel':
        this.logger.info({ executionId: msg.executionId }, 'Received cancel');
        this.callbacks.onCancel(msg as any);
        break;

      case 'shutdown':
        this.logger.info({ reason: msg.reason }, 'Received shutdown');
        this.shuttingDown = true;
        this.callbacks.onShutdown(msg as any);
        break;

      case 'heartbeat':
        // Orchestrator heartbeat — no action needed
        break;
    }
  }

  send(msg: WorkerMessage): boolean {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
      this.logger.warn({ type: msg.type }, 'Cannot send — not connected');
      return false;
    }

    this.ws.send(serializeMessage(msg));
    return true;
  }

  sendReady(): void {
    this.send({ type: 'ready' });
  }

  sendStdout(executionId: string, chunk: string): void {
    this.send({
      type: 'stdout',
      executionId,
      chunk,
      timestamp: Date.now(),
    });
  }

  sendStderr(executionId: string, chunk: string): void {
    this.send({
      type: 'stderr',
      executionId,
      chunk,
      timestamp: Date.now(),
    });
  }

  sendComplete(
    executionId: string,
    status: 'completed' | 'failed' | 'cancelled',
    exitCode: number,
    durationMs: number,
    sessionId?: string,
    totalCostUsd?: number,
  ): void {
    this.send({
      type: 'complete',
      executionId,
      status,
      exitCode,
      durationMs,
      sessionId,
      totalCostUsd,
    });
  }

  sendEvent(executionId: string, eventType: string, payload: unknown): void {
    this.send({
      type: 'event',
      executionId,
      eventType: eventType as any,
      payload,
    });
  }

  private cleanup(): void {
    this.connected = false;
    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = undefined;
    }
  }

  private scheduleReconnect(): void {
    if (this.shuttingDown) return;

    this.logger.info({ delayMs: this.reconnectDelay }, 'Scheduling reconnect');

    this.reconnectTimer = setTimeout(() => {
      this.connect();
    }, this.reconnectDelay);

    // Exponential backoff
    this.reconnectDelay = Math.min(this.reconnectDelay * 2, MAX_RECONNECT_DELAY_MS);
  }

  disconnect(): void {
    this.shuttingDown = true;
    this.cleanup();
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
    }
    if (this.ws) {
      this.ws.close(1000, 'Worker shutting down');
    }
  }

  isConnected(): boolean {
    return this.connected;
  }
}
