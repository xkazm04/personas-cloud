import WebSocket from 'ws';
import fs from 'node:fs';
import {
  parseOrchestratorMessage,
  serializeMessage,
  signWorkerPayload,
  PROTOCOL_VERSION,
  checkProtocolCompatibility,
  type OrchestratorMessage,
  type ExecAssign,
  type ExecCancel,
  type OrchestratorShutdown,
  type WorkerMessage,
  type PhaseTimings,
  type WorkerHealthMetrics,
  type SignedWorkerEnvelope,
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

/** How long to accumulate output chunks before flushing as a single message. */
const OUTPUT_BATCH_INTERVAL_MS = 50;

/**
 * Micro-batching buffer for stdout/stderr output.
 *
 * Accumulates output chunks for up to OUTPUT_BATCH_INTERVAL_MS before flushing
 * them as a single combined WebSocket message.  Under heavy output (e.g. verbose
 * builds producing hundreds of lines/sec), this reduces WebSocket frame overhead
 * and network syscalls by ~99x compared to per-line messaging.
 */
class OutputBatcher {
  /** Buffered chunks keyed by executionId, then by stream type. */
  private buffers = new Map<string, { stdout: string[]; stderr: string[] }>();
  /** Pending flush timers keyed by executionId. */
  private timers = new Map<string, ReturnType<typeof setTimeout>>();

  constructor(
    private sendFn: (executionId: string, type: 'stdout' | 'stderr', combined: string) => void,
  ) {}

  /** Buffer a chunk for batched sending. */
  push(executionId: string, type: 'stdout' | 'stderr', chunk: string): void {
    let buf = this.buffers.get(executionId);
    if (!buf) {
      buf = { stdout: [], stderr: [] };
      this.buffers.set(executionId, buf);
    }
    buf[type].push(chunk);

    // Schedule a flush if not already pending for this execution
    if (!this.timers.has(executionId)) {
      this.timers.set(
        executionId,
        setTimeout(() => {
          this.timers.delete(executionId);
          this.flushExecution(executionId);
        }, OUTPUT_BATCH_INTERVAL_MS),
      );
    }
  }

  /** Immediately flush all buffered output for an execution (e.g. before complete). */
  flush(executionId: string): void {
    // Cancel pending timer
    const timer = this.timers.get(executionId);
    if (timer) {
      clearTimeout(timer);
      this.timers.delete(executionId);
    }
    this.flushExecution(executionId);
  }

  /** Flush all buffered output for all executions and cancel all timers. */
  flushAll(): void {
    for (const timer of this.timers.values()) {
      clearTimeout(timer);
    }
    this.timers.clear();
    for (const executionId of [...this.buffers.keys()]) {
      this.flushExecution(executionId);
    }
  }

  private flushExecution(executionId: string): void {
    const buf = this.buffers.get(executionId);
    if (!buf) return;

    if (buf.stdout.length > 0) {
      this.sendFn(executionId, 'stdout', buf.stdout.join('\n'));
      buf.stdout.length = 0;
    }
    if (buf.stderr.length > 0) {
      this.sendFn(executionId, 'stderr', buf.stderr.join('\n'));
      buf.stderr.length = 0;
    }

    // Clean up if both buffers are empty
    if (buf.stdout.length === 0 && buf.stderr.length === 0) {
      this.buffers.delete(executionId);
    }
  }
}

export interface ConnectionCallbacks {
  onAssign(msg: ExecAssign): void;
  onCancel(msg: ExecCancel): void;
  onShutdown(msg: OrchestratorShutdown): void;
  onReviewResponse(msg: OrchestratorMessage & { type: 'review_response' }): void;
  /** Called after ack to get IDs of executions still running locally (for reconnect recovery). */
  getActiveExecutionIds(): string[];
}

export class Connection {
  private ws: WebSocket | null = null;
  private connected = false;
  private reconnectDelay = INITIAL_RECONNECT_DELAY_MS;
  private heartbeatTimer?: ReturnType<typeof setInterval>;
  private reconnectTimer?: ReturnType<typeof setTimeout>;
  private shuttingDown = false;
  /** Monotonic sequence number for HMAC-signed outbound messages. */
  private seq = 0;

  /** Optional function that returns current health metrics to include in heartbeats. */
  private getHealthMetrics?: () => WorkerHealthMetrics;

  /** Micro-batcher for stdout/stderr output to reduce WebSocket frame overhead. */
  private outputBatcher: OutputBatcher;

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
    private maxSlots: number = 1,
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

    this.outputBatcher = new OutputBatcher((executionId, type, combined) => {
      this.send({ type, executionId, chunk: combined, timestamp: Date.now() });
    });
  }

  /** Set the health metrics provider for heartbeat reporting. */
  setHealthMetricsProvider(provider: () => WorkerHealthMetrics): void {
    this.getHealthMetrics = provider;
  }

  connect(): void {
    if (this.shuttingDown) return;

    this.logger.info({ url: this.orchestratorUrl, workerId: this.workerId }, 'Connecting to orchestrator');

    // Auth is sent in the hello message rather than in the URL to avoid
    // exposing the token in proxy/CDN/load-balancer access logs.
    // TLS options are applied when configured.
    const wsOptions: WebSocket.ClientOptions = {
      ...this.cachedTlsOptions,
    };

    this.ws = new WebSocket(this.orchestratorUrl, wsOptions);

    this.ws.on('open', () => {
      this.connected = true;
      this.reconnectDelay = INITIAL_RECONNECT_DELAY_MS;
      this.seq = 0; // Reset sequence counter for new connection

      // Send hello with auth token (replaces URL query string auth)
      this.send({
        type: 'hello',
        workerId: this.workerId,
        token: this.workerToken,
        version: PROTOCOL_VERSION,
        capabilities: ['claude-cli', 'node', 'git', 'network-policy'],
        ...(this.imageDigest ? { imageDigest: this.imageDigest } : {}),
        ...(this.claudeCliVersion ? { claudeCliVersion: this.claudeCliVersion } : {}),
      });

      // Start heartbeat with optional system health metrics
      this.heartbeatTimer = setInterval(() => {
        const metrics = this.getHealthMetrics?.();
        this.send({ type: 'heartbeat', timestamp: Date.now(), metrics });
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

      // 4409 = duplicate worker ID rejected — another instance with this ID
      // is already connected. Do not auto-reconnect as it would loop.
      if (code === 4409) {
        this.logger.error(
          'Duplicate worker ID rejected by orchestrator — another worker with this ID is already connected. Exiting.',
        );
        this.shuttingDown = true;
        return;
      }

      this.scheduleReconnect();
    });

    this.ws.on('error', (err: Error) => {
      this.logger.error({ err }, 'WebSocket error');
      // 'close' will follow, which handles reconnection
    });
  }

  private handleMessage(msg: OrchestratorMessage): void {
    switch (msg.type) {
      case 'ack': {
        const compat = checkProtocolCompatibility(msg.protocolVersion, PROTOCOL_VERSION);
        if (compat === 'incompatible' || compat === 'invalid') {
          this.logger.error(
            { orchestratorVersion: msg.protocolVersion, workerVersion: PROTOCOL_VERSION },
            'Orchestrator protocol version incompatible — disconnecting',
          );
          this.disconnect();
          return;
        }
        if (compat === 'warn') {
          this.logger.warn(
            { orchestratorVersion: msg.protocolVersion, workerVersion: PROTOCOL_VERSION },
            'Orchestrator protocol version mismatch (near-compatible) — consider upgrading',
          );
        }
        this.logger.info({ workerId: msg.workerId, protocolVersion: msg.protocolVersion }, 'Received ack from orchestrator');

        // Report any executions still running locally before announcing capacity.
        // On a fresh start this list is empty; after a reconnect it lets the
        // orchestrator reconcile its view of active work.
        const activeIds = this.callbacks.getActiveExecutionIds();
        if (activeIds.length > 0) {
          this.logger.info(
            { activeExecutionIds: activeIds, count: activeIds.length },
            'Re-reporting active executions after reconnect',
          );
          this.send({ type: 'active_executions', executionIds: activeIds });
        }

        // Report accurate available slots (accounts for in-flight executions)
        const availableSlots = this.maxSlots - activeIds.length;
        this.sendReady(availableSlots, this.maxSlots);
        break;
      }

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

      case 'review_response':
        this.logger.info({ executionId: (msg as any).executionId, reviewId: (msg as any).reviewId }, 'Received review response');
        this.callbacks.onReviewResponse(msg as any);
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

    const payload = serializeMessage(msg);
    const seq = ++this.seq;
    const sig = signWorkerPayload(payload, seq, this.workerToken);
    const envelope: SignedWorkerEnvelope = { _signed: true, seq, sig, payload };
    this.ws.send(JSON.stringify(envelope));
    return true;
  }

  sendReady(availableSlots: number, maxSlots: number): void {
    this.send({ type: 'ready', availableSlots, maxSlots });
  }

  sendBusy(executionId: string, reason: string): void {
    this.send({ type: 'busy', executionId, reason });
  }

  sendStdout(executionId: string, chunk: string): void {
    this.outputBatcher.push(executionId, 'stdout', chunk);
  }

  sendStderr(executionId: string, chunk: string): void {
    this.outputBatcher.push(executionId, 'stderr', chunk);
  }

  /**
   * Immediately flush all buffered output for a given execution.
   * Call this after post-exit line flushes and before sendComplete to
   * guarantee output ordering.
   */
  flushOutput(executionId: string): void {
    this.outputBatcher.flush(executionId);
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
    // Flush any buffered output before sending the completion signal,
    // ensuring the orchestrator receives all output before the status change.
    this.outputBatcher.flush(executionId);

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

  sendProgress(executionId: string, phaseOrProgress: string | import('@dac-cloud/shared').ExecutionProgress, percent?: number, detail?: string): void {
    if (typeof phaseOrProgress === 'string') {
      this.send({
        type: 'progress',
        executionId,
        phase: phaseOrProgress,
        percent: percent ?? 0,
        detail,
        timestamp: Date.now(),
      });
    } else {
      const progress = phaseOrProgress;
      this.send({
        type: 'progress',
        executionId,
        stage: progress.stage,
        percentEstimate: progress.percentEstimate,
        activeTool: progress.activeTool,
        message: progress.message,
        toolCallsCompleted: progress.toolCallsCompleted,
        timestamp: Date.now(),
      });
    }
  }

  sendEvent(executionId: string, eventType: import('@dac-cloud/shared').ProtocolEventType, payload: unknown): void {
    this.send({
      type: 'event',
      executionId,
      eventType,
      payload,
    });
  }

  sendReviewRequest(executionId: string, reviewId: string, payload: unknown): void {
    this.send({
      type: 'review_request',
      executionId,
      reviewId,
      payload,
      timestamp: Date.now(),
    });
  }

  private cleanup(): void {
    this.connected = false;
    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = undefined;
    }
    // Cancel pending batch timers (output can't be sent on a closed connection)
    this.outputBatcher.flushAll();
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
