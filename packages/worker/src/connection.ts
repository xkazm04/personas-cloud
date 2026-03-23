import WebSocket from 'ws';
import fs from 'node:fs';
import {
  parseOrchestratorMessage,
  serializeMessage,
  PROTOCOL_VERSION,
  type OrchestratorMessage,
  type ExecAssign,
  type ExecCancel,
  type OrchestratorShutdown,
  type WorkerMessage,
  type PhaseTimings,
} from '@dac-cloud/shared';
import type { Logger } from 'pino';

export interface WorkerTlsConfig {
  caPath: string;
  certPath: string;
  keyPath: string;
  rejectUnauthorized: boolean;
}

const HEARTBEAT_INTERVAL_MS = 30_000;
const MAX_RECONNECT_DELAY_MS = 30_000;
const INITIAL_RECONNECT_DELAY_MS = 1_000;

export interface ConnectionCallbacks {
  onAssign(msg: ExecAssign): void;
  onCancel(msg: ExecCancel): void;
  onShutdown(msg: OrchestratorShutdown): void;
}

export class Connection {
  private ws: WebSocket | null = null;
  private connected = false;
  private reconnectDelay = INITIAL_RECONNECT_DELAY_MS;
  private heartbeatTimer?: ReturnType<typeof setInterval>;
  private reconnectTimer?: ReturnType<typeof setTimeout>;
  private shuttingDown = false;

  private tlsConfig?: WorkerTlsConfig;
  private cachedTlsOptions?: Pick<WebSocket.ClientOptions, 'ca' | 'cert' | 'key' | 'rejectUnauthorized'>;

  private imageDigest: string;
  private claudeCliVersion: string;

  constructor(
    private orchestratorUrl: string,
    private workerId: string,
    private workerToken: string,
    private callbacks: ConnectionCallbacks,
    private logger: Logger,
    tlsConfig?: WorkerTlsConfig,
    opts?: { imageDigest?: string; claudeCliVersion?: string },
  ) {
    this.tlsConfig = tlsConfig;
    this.imageDigest = opts?.imageDigest || '';
    this.claudeCliVersion = opts?.claudeCliVersion || '';

    if (tlsConfig) {
      this.cachedTlsOptions = {
        rejectUnauthorized: tlsConfig.rejectUnauthorized,
      };
      if (tlsConfig.caPath) {
        this.cachedTlsOptions.ca = fs.readFileSync(tlsConfig.caPath);
      }
      if (tlsConfig.certPath && tlsConfig.keyPath) {
        this.cachedTlsOptions.cert = fs.readFileSync(tlsConfig.certPath);
        this.cachedTlsOptions.key = fs.readFileSync(tlsConfig.keyPath);
      }
    }
  }

  connect(): void {
    if (this.shuttingDown) return;

    this.logger.info({ url: this.orchestratorUrl, workerId: this.workerId }, 'Connecting to orchestrator');

    const wsOptions: WebSocket.ClientOptions = {
      ...this.cachedTlsOptions,
      headers: {
        Authorization: `Bearer ${this.workerToken}`,
      },
    };

    this.ws = new WebSocket(this.orchestratorUrl, wsOptions);

    this.ws.on('open', () => {
      this.connected = true;
      this.reconnectDelay = INITIAL_RECONNECT_DELAY_MS;

      // Send hello
      this.send({
        type: 'hello',
        workerId: this.workerId,
        version: PROTOCOL_VERSION,
        capabilities: ['claude-cli', 'node', 'git', 'network-policy'],
        ...(this.imageDigest ? { imageDigest: this.imageDigest } : {}),
        ...(this.claudeCliVersion ? { claudeCliVersion: this.claudeCliVersion } : {}),
      });

      // Start heartbeat
      this.heartbeatTimer = setInterval(() => {
        this.send({ type: 'heartbeat', timestamp: Date.now() });
      }, HEARTBEAT_INTERVAL_MS);

      this.logger.info('Connected to orchestrator');
    });

    this.ws.on('message', (raw: Buffer) => {
      const msg = parseOrchestratorMessage(raw.toString());
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
        this.callbacks.onAssign(msg);
        break;

      case 'cancel':
        this.logger.info({ executionId: msg.executionId }, 'Received cancel');
        this.callbacks.onCancel(msg);
        break;

      case 'shutdown':
        this.logger.info({ reason: msg.reason }, 'Received shutdown');
        this.shuttingDown = true;
        this.callbacks.onShutdown(msg);
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

  sendBusy(executionId: string, reason: string): void {
    this.send({ type: 'busy', executionId, reason });
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
    phaseTimings?: PhaseTimings,
  ): void {
    this.send({
      type: 'complete',
      executionId,
      status,
      exitCode,
      durationMs,
      sessionId,
      totalCostUsd,
      phaseTimings,
    });
  }

  sendProgress(executionId: string, phase: string, percent: number, detail?: string): void {
    this.send({
      type: 'progress',
      executionId,
      phase,
      percent,
      detail,
      timestamp: Date.now(),
    });
  }

  sendEvent(executionId: string, eventType: import('@dac-cloud/shared').ProtocolEventType, payload: unknown): void {
    this.send({
      type: 'event',
      executionId,
      eventType,
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

  getConnectionState(): 'connected' | 'reconnecting' | 'disconnected' {
    if (this.connected) return 'connected';
    if (this.shuttingDown) return 'disconnected';
    return 'reconnecting';
  }
}
