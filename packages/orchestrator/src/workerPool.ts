import { WebSocketServer, WebSocket } from 'ws';
import { randomBytes } from 'node:crypto';
import { EventEmitter } from 'node:events';
import {
  parseMessage,
  parseSignedEnvelope,
  serializeMessage,
  verifyWorkerPayload,
  PROTOCOL_VERSION,
  checkProtocolCompatibility,
  type WorkerMessage,
  type OrchestratorMessage,
  type ExecAssign,
  type ExecStdout,
  type ExecStderr,
  type ExecComplete,
  type ExecProgress,
  type WorkerEvent,
  type WorkerReviewRequest,
  type WorkerActiveExecutions,
  type WorkerInfo,
} from '@dac-cloud/shared';
import type { Logger } from 'pino';
import type { IncomingMessage } from 'node:http';
import { safeCompare } from './auth.js';

// ---------------------------------------------------------------------------
// Typed event emitter — compiler-enforced contract for WorkerPool events
// ---------------------------------------------------------------------------

/**
 * Event map defining every event the WorkerPool can emit and the exact
 * callback signature for each. Adding, removing, or changing a payload
 * shape produces compile-time errors at all emit and subscribe sites.
 */
export interface WorkerPoolEvents {
  'worker-connected': (info: WorkerInfo) => void;
  'worker-ready': (workerId: string) => void;
  'worker-disconnected': (workerId: string, executionId: string | undefined) => void;
  'stdout': (workerId: string, msg: ExecStdout) => void;
  'stderr': (workerId: string, msg: ExecStderr) => void;
  'complete': (workerId: string, msg: ExecComplete) => void;
  'persona-event': (workerId: string, msg: WorkerEvent) => void;
  'progress': (workerId: string, msg: ExecProgress) => void;
  'review-request': (workerId: string, msg: WorkerReviewRequest) => void;
  'executions-reclaimed': (workerId: string, msg: WorkerActiveExecutions) => void;
}

/**
 * Thin typed wrapper around Node's EventEmitter. Constrains `on`, `emit`,
 * and `off` to the signatures declared in `WorkerPoolEvents` so the
 * compiler enforces correctness at both emit and subscribe sites.
 */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export interface TypedEventEmitter<T> {
  on<K extends keyof T & string>(event: K, listener: T[K] extends (...args: any[]) => void ? T[K] : never): this;
  emit<K extends keyof T & string>(event: K, ...args: T[K] extends (...args: infer A) => void ? A : never): boolean;
  off<K extends keyof T & string>(event: K, listener: T[K] extends (...args: any[]) => void ? T[K] : never): this;
  removeAllListeners<K extends keyof T & string>(event?: K): this;
}

const HEARTBEAT_INTERVAL_MS = 30_000;
const HEARTBEAT_TIMEOUT_MS = 90_000;
/**
 * Grace period for the old connection to flush final **output chunks** before
 * the socket is forcibly closed. This does NOT preserve the execution — all
 * in-flight executions on the old connection are immediately marked as failed
 * by the Dispatcher's `worker-disconnected` handler. The grace period exists
 * solely to let already-buffered WebSocket frames drain so that the tail
 * buffer captures as much output as possible before the execution is failed.
 *
 * **Contract**: Worker reconnect always means execution loss. The new connection
 * starts with zero active executions. There is no execution handoff mechanism.
 */
const RECONNECT_GRACE_MS = 5_000;

interface WorkerEntry {
  ws: WebSocket;
  info: WorkerInfo;
  heartbeatTimer?: ReturnType<typeof setInterval>;
  /** Last verified sequence number from this worker (for replay protection). */
  lastSeq: number;
}

/** Custom close code: duplicate worker ID rejected (WebSocket equivalent of HTTP 409). */
const WS_CLOSE_DUPLICATE = 4409;

export class WorkerPool extends (EventEmitter as new () => TypedEventEmitter<WorkerPoolEvents> & EventEmitter) {
  private workers = new Map<string, WorkerEntry>();
  /** Index of workers by their available capacity: availableSlots -> Set of workerIds. */
  private idleByCapacity = new Map<number, Set<string>>();
  private wss: WebSocketServer | null = null;

  constructor(
    private expectedWorkerToken: string,
    private logger: Logger,
  ) {
    super();
  }

  listen(port: number): void {
    this.wss = new WebSocketServer({ port });

    this.wss.on('connection', (ws: WebSocket, _req: IncomingMessage) => {
      // Token is no longer extracted from the URL query string — it is sent
      // inside the hello message to avoid exposing secrets in proxy/CDN logs.
      // Authentication happens in handleConnection() when the hello arrives.
      this.handleConnection(ws);
    });

    this.wss.on('error', (err) => {
      this.logger.error({ err }, 'WebSocket server error');
    });

    this.logger.info({ port }, 'Worker pool listening');
  }

  private handleConnection(ws: WebSocket): void {
    // The workerId is bound to this specific WebSocket connection once the
    // hello handshake completes.  All subsequent messages on this socket are
    // validated against this identity — a worker cannot impersonate another.
    let workerId: string | null = null;

    const connectionTimeout = setTimeout(() => {
      if (!workerId) {
        this.logger.warn('Worker did not send hello within timeout, closing');
        ws.close(1008, 'Hello timeout');
      }
    }, 10_000);

    ws.on('message', (raw: Buffer) => {
      const rawStr = raw.toString();

      // Try to parse as a signed envelope first
      const envelope = parseSignedEnvelope(rawStr);
      if (envelope) {
        // Signed message — verify HMAC using the shared worker token
        if (!verifyWorkerPayload(envelope.payload, envelope.seq, envelope.sig, this.expectedWorkerToken)) {
          this.logger.warn({ workerId, seq: envelope.seq }, 'Worker message HMAC verification failed — dropping');
          return;
        }

        // Verify monotonic sequence number (per-worker)
        if (workerId) {
          const entry = this.workers.get(workerId);
          if (entry && envelope.seq <= entry.lastSeq) {
            this.logger.warn({ workerId, seq: envelope.seq, lastSeq: entry.lastSeq }, 'Worker message sequence out of order — dropping');
            return;
          }
          if (entry) {
            entry.lastSeq = envelope.seq;
          }
        }

        const msg = parseMessage<WorkerMessage>(envelope.payload);
        if (!msg) {
          this.logger.warn({ workerId }, 'Invalid message payload inside signed envelope');
          return;
        }

        if (msg.type === 'hello') {
          clearTimeout(connectionTimeout);
          if (!this.validateHelloToken(msg.token, ws)) return;
          workerId = msg.workerId;
          this.registerWorker(workerId, ws, msg.version, msg.capabilities);
          return;
        }

        if (!workerId) {
          this.logger.warn({ type: msg.type }, 'Message before hello');
          return;
        }

        // Source binding: verify the message came on the socket that owns this workerId
        if (!this.validateMessageSource(workerId, ws)) return;

        this.handleWorkerMessage(workerId, msg);
        return;
      }

      // Unsigned messages are rejected — all worker messages must be HMAC-signed.
      this.logger.warn({ workerId }, 'Rejected unsigned worker message — HMAC signing is required');
      ws.close(1008, 'Unsigned messages are not accepted — HMAC signing is required');
    });

    ws.on('close', () => {
      clearTimeout(connectionTimeout);
      if (workerId) {
        // Only unregister if this socket is still the active one for this worker.
        // During reconnection the old socket closes after the new one is registered,
        // so we must not accidentally remove the replacement entry.
        const current = this.workers.get(workerId);
        if (current && current.ws === ws) {
          this.unregisterWorker(workerId);
        }
      }
    });

    ws.on('error', (err) => {
      this.logger.error({ err, workerId }, 'Worker WebSocket error');
    });
  }

  /**
   * Validate the worker token sent in the hello message.
   * Returns true if the token matches; closes the socket and returns false otherwise.
   */
  private validateHelloToken(token: string | undefined, ws: WebSocket): boolean {
    if (!token || !safeCompare(token, this.expectedWorkerToken)) {
      this.logger.warn('Worker rejected: invalid token in hello message');
      ws.close(1008, 'Invalid worker token');
      return false;
    }
    return true;
  }

  /**
   * Validate that a message arriving on `ws` belongs to the registered worker
   * entry for `workerId`.  Prevents a connection from claiming to be a different
   * worker after initial registration.
   */
  private validateMessageSource(workerId: string, ws: WebSocket): boolean {
    const entry = this.workers.get(workerId);
    if (!entry || entry.ws !== ws) {
      this.logger.warn({ workerId }, 'Message source mismatch — socket is not the registered connection for this worker, dropping');
      return false;
    }
    return true;
  }

  private registerWorker(workerId: string, ws: WebSocket, version: string, capabilities: string[]): void {
    // Validate protocol version compatibility
    const compat = checkProtocolCompatibility(version);
    if (compat === 'incompatible' || compat === 'invalid') {
      this.logger.error(
        { workerId, workerVersion: version, orchestratorVersion: PROTOCOL_VERSION },
        'Worker rejected: incompatible protocol version',
      );
      ws.close(1008, `Incompatible protocol version: worker=${version}, orchestrator=${PROTOCOL_VERSION}`);
      return;
    }
    if (compat === 'warn') {
      this.logger.warn(
        { workerId, workerVersion: version, orchestratorVersion: PROTOCOL_VERSION },
        'Worker protocol version mismatch (near-compatible) — consider upgrading',
      );
    }

    // Duplicate worker ID handling: reject if the existing connection is still
    // alive and responsive (session hijacking attempt), but allow replacement
    // when the old connection is dead or closing (legitimate reconnect).
    const existing = this.workers.get(workerId);
    if (existing) {
      const oldAlive = existing.ws.readyState === WebSocket.OPEN;
      const heartbeatFresh = (Date.now() - existing.info.lastHeartbeat) < HEARTBEAT_TIMEOUT_MS;

      if (oldAlive && heartbeatFresh) {
        // Old connection is still alive and responsive — reject the new one
        // to prevent session hijacking.  4409 is a custom close code
        // (WebSocket equivalent of HTTP 409 Conflict).
        this.logger.warn(
          { workerId, existingConnectedAt: existing.info.connectedAt },
          'Duplicate worker ID rejected — existing connection is still active',
        );
        ws.close(WS_CLOSE_DUPLICATE, 'Duplicate worker ID: existing connection is still active');
        return;
      }

      // Old connection is dead/closing — this is a legitimate reconnect.
      //
      // Active executions are initially marked as failed via the
      // 'worker-disconnected' event. If the worker is still running those
      // executions, it will re-report them via an 'active_executions'
      // message after the handshake completes. The Dispatcher then
      // reclaims them (moves from completed-failed back to active-running)
      // and the WorkerPool re-registers the execution IDs.
      const lostExecutions = [...existing.info.currentExecutionIds];
      if (lostExecutions.length > 0) {
        this.logger.warn(
          { workerId, lostExecutions, lostCount: lostExecutions.length },
          'Worker reconnect — all in-flight executions are lost (no handoff). Executions will be marked failed.',
        );
      } else {
        this.logger.info({ workerId }, 'Worker reconnecting (no active executions to fail)');
      }

      // Fail active executions before tearing down the old entry
      for (const executionId of lostExecutions) {
        this.emit('worker-disconnected', workerId, executionId);
      }

      if (existing.heartbeatTimer) clearInterval(existing.heartbeatTimer);
      const oldWs = existing.ws;
      // Remove from idle index before deleting
      this.syncIdleIndex(workerId, existing.info.availableSlots, 0);
      this.workers.delete(workerId);
      setTimeout(() => {
        if (oldWs.readyState === WebSocket.OPEN || oldWs.readyState === WebSocket.CONNECTING) {
          oldWs.close(1001, 'Replaced by reconnection');
        }
      }, RECONNECT_GRACE_MS);
    }

    const info: WorkerInfo = {
      workerId,
      status: 'idle',
      version,
      capabilities,
      currentExecutionIds: [],
      maxConcurrentSlots: 1,
      availableSlots: 1,
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

    this.workers.set(workerId, { ws, info, heartbeatTimer, lastSeq: 0 });

    // Invariant: a newly registered worker always starts with zero active
    // executions.  This assertion guards against future code paths that might
    // accidentally carry over execution state from a previous connection.
    if (info.currentExecutionIds.length !== 0) {
      this.logger.error(
        { workerId, currentExecutionIds: info.currentExecutionIds },
        'BUG: newly registered worker has non-empty currentExecutionIds — clearing',
      );
      info.currentExecutionIds = [];
      info.availableSlots = info.maxConcurrentSlots;
    }

    // Add to idle index
    this.syncIdleIndex(workerId, 0, info.availableSlots);

    // Send ack with orchestrator's protocol version
    this.send(workerId, {
      type: 'ack',
      workerId,
      sessionToken: randomBytes(32).toString('hex'),
      protocolVersion: PROTOCOL_VERSION,
    });

    this.logger.info({ workerId, version, capabilities }, 'Worker registered');
    this.emit('worker-connected', info);
  }

  /**
   * Remove a worker from the pool when its WebSocket closes (non-reconnect path).
   * Emits 'worker-disconnected' for every active execution, causing the Dispatcher
   * to mark them as failed. There is no automatic re-queue — the event processor's
   * stale recovery sweep handles retry semantics at the event level.
   */
  private unregisterWorker(workerId: string): void {
    const entry = this.workers.get(workerId);
    if (!entry) return;

    if (entry.heartbeatTimer) {
      clearInterval(entry.heartbeatTimer);
    }

    const activeExecutionIds = [...entry.info.currentExecutionIds];
    // Remove from idle index
    this.syncIdleIndex(workerId, entry.info.availableSlots, 0);
    this.workers.delete(workerId);

    this.logger.info({ workerId, activeExecutionIds }, 'Worker disconnected');
    // Emit disconnect for each active execution so dispatcher can handle them
    for (const executionId of activeExecutionIds) {
      this.emit('worker-disconnected', workerId, executionId);
    }
    // If no active executions, still emit disconnect with undefined
    if (activeExecutionIds.length === 0) {
      this.emit('worker-disconnected', workerId, undefined);
    }
  }

  private handleWorkerMessage(workerId: string, msg: WorkerMessage): void {
    const entry = this.workers.get(workerId);
    if (!entry) return;

    switch (msg.type) {
      case 'heartbeat':
        entry.info.lastHeartbeat = Date.now();
        if (msg.metrics) {
          entry.info.healthMetrics = msg.metrics;
        }
        break;

      case 'ready': {
        // Update capacity info from worker
        const availableSlots = msg.availableSlots ?? 1;
        const maxSlots = msg.maxSlots ?? 1;
        const oldAvailable = entry.info.availableSlots;

        entry.info.maxConcurrentSlots = maxSlots;
        entry.info.availableSlots = availableSlots;
        entry.info.status = availableSlots >= maxSlots ? 'idle' : (availableSlots > 0 ? 'executing' : 'executing');

        this.syncIdleIndex(workerId, oldAvailable, availableSlots);
        this.emit('worker-ready', workerId);
        break;
      }

      case 'stdout':
        if (!this.validateExecutionOwnership(workerId, entry, msg.executionId)) return;
        this.emit('stdout', workerId, msg);
        break;

      case 'stderr':
        if (!this.validateExecutionOwnership(workerId, entry, msg.executionId)) return;
        this.emit('stderr', workerId, msg);
        break;

      case 'complete': {
        if (!this.validateExecutionOwnership(workerId, entry, msg.executionId)) return;
        // Remove completed execution from tracking
        const oldAvailable = entry.info.availableSlots;
        const idx = entry.info.currentExecutionIds.indexOf(msg.executionId);
        if (idx !== -1) entry.info.currentExecutionIds.splice(idx, 1);
        entry.info.availableSlots = entry.info.maxConcurrentSlots - entry.info.currentExecutionIds.length;
        entry.info.status = entry.info.currentExecutionIds.length === 0 ? 'idle' : 'executing';

        this.syncIdleIndex(workerId, oldAvailable, entry.info.availableSlots);

        this.logger.info({
          workerId,
          executionId: msg.executionId,
          status: msg.status,
          durationMs: msg.durationMs,
          remainingExecutions: entry.info.currentExecutionIds.length,
          availableSlots: entry.info.availableSlots,
        }, 'Execution complete');
        this.emit('complete', workerId, msg);
        break;
      }

      case 'event':
        if (!this.validateExecutionOwnership(workerId, entry, msg.executionId)) return;
        this.emit('persona-event', workerId, msg);
        break;

      case 'progress':
        if (!this.validateExecutionOwnership(workerId, entry, msg.executionId)) return;
        this.emit('progress', workerId, msg);
        break;

      case 'review_request':
        if (!this.validateExecutionOwnership(workerId, entry, msg.executionId)) return;
        this.emit('review-request', workerId, msg);
        break;

      case 'active_executions': {
        // Worker is re-reporting executions still running locally after a
        // reconnect. Re-register them so the orchestrator's view of capacity
        // matches reality and validateExecutionOwnership passes for subsequent
        // messages on these executions.
        const executionIds = msg.executionIds ?? [];
        const oldAvailable = entry.info.availableSlots;

        for (const execId of executionIds) {
          if (!entry.info.currentExecutionIds.includes(execId)) {
            entry.info.currentExecutionIds.push(execId);
          }
        }

        entry.info.availableSlots = entry.info.maxConcurrentSlots - entry.info.currentExecutionIds.length;
        entry.info.status = entry.info.currentExecutionIds.length === 0 ? 'idle' : 'executing';
        this.syncIdleIndex(workerId, oldAvailable, entry.info.availableSlots);

        this.logger.info(
          {
            workerId,
            reclaimedExecutions: executionIds,
            count: executionIds.length,
            availableSlots: entry.info.availableSlots,
          },
          'Worker re-reported active executions after reconnect',
        );

        this.emit('executions-reclaimed', workerId, msg);
        break;
      }

      default:
        this.logger.warn({ workerId, type: (msg as WorkerMessage).type }, 'Unknown message type');
    }
  }

  /**
   * Sync a worker's position in the idle index when its available capacity changes.
   */
  private syncIdleIndex(workerId: string, oldAvailable: number, newAvailable: number): void {
    if (oldAvailable > 0) {
      const set = this.idleByCapacity.get(oldAvailable);
      if (set) {
        set.delete(workerId);
        if (set.size === 0) {
          this.idleByCapacity.delete(oldAvailable);
        }
      }
    }

    if (newAvailable > 0) {
      let set = this.idleByCapacity.get(newAvailable);
      if (!set) {
        set = new Set();
        this.idleByCapacity.set(newAvailable, set);
      }
      set.add(workerId);
    }
  }

  /**
   * Validate that the executionId in a worker message matches one of the executions
   * currently assigned to that worker.  Prevents a worker from claiming to
   * complete or stream output for an execution it was never assigned.
   */
  private validateExecutionOwnership(workerId: string, entry: WorkerEntry, executionId: string): boolean {
    if (!entry.info.currentExecutionIds.includes(executionId)) {
      this.logger.warn(
        { workerId, claimedExecution: executionId, assignedExecutions: entry.info.currentExecutionIds },
        'Execution ownership mismatch — worker sent message for an execution it is not assigned to, dropping',
      );
      return false;
    }
    return true;
  }

  send(workerId: string, msg: OrchestratorMessage): boolean {
    const entry = this.workers.get(workerId);
    if (!entry || entry.ws.readyState !== WebSocket.OPEN) return false;

    entry.ws.send(serializeMessage(msg));
    return true;
  }

  assign(workerId: string, assignment: ExecAssign): boolean {
    const entry = this.workers.get(workerId);
    if (!entry || entry.info.availableSlots <= 0) return false;

    // Try sending first
    const sent = this.send(workerId, assignment);
    if (!sent) return false;

    // Only update state if message was successfully handed to the WebSocket
    const oldAvailable = entry.info.availableSlots;
    entry.info.currentExecutionIds.push(assignment.executionId);
    entry.info.availableSlots = entry.info.maxConcurrentSlots - entry.info.currentExecutionIds.length;
    entry.info.status = 'executing';

    this.syncIdleIndex(workerId, oldAvailable, entry.info.availableSlots);
    return true;
  }

  /**
   * Returns a worker with available execution capacity.
   * Prefers workers with the most available slots (least loaded first).
   * 
   * Implementation is O(C) where C is the number of distinct capacity levels,
   * which is much smaller than O(N) workers scan.
   */
  /**
   * Check if a specific worker has available capacity.
   * Used for affinity routing — allows the dispatcher to prefer a known
   * worker without falling back to a full scan when it has capacity.
   * Returns the workerId if it has slots, null otherwise.
   */
  getWorkerIfIdle(workerId: string): string | null {
    const entry = this.workers.get(workerId);
    if (!entry) return null;
    if (entry.info.availableSlots > 0) return workerId;
    return null;
  }

  getIdleWorker(): string | null {
    const capacities = Array.from(this.idleByCapacity.keys());
    if (capacities.length === 0) return null;

    let max = -1;
    for (const cap of capacities) {
      if (cap > max) max = cap;
    }

    if (max <= 0) return null;

    const set = this.idleByCapacity.get(max)!;
    // Return the first worker from the set
    return set.values().next().value || null;
  }

  getWorkers(): WorkerInfo[] {
    return Array.from(this.workers.values()).map(e => ({ ...e.info }));
  }

  getWorkerCount(): { total: number; idle: number; executing: number; totalSlots: number; availableSlots: number } {
    let idle = 0;
    let executing = 0;
    let totalSlots = 0;
    let availableSlots = 0;
    for (const entry of this.workers.values()) {
      if (entry.info.status === 'idle') idle++;
      if (entry.info.status === 'executing') executing++;
      totalSlots += entry.info.maxConcurrentSlots;
      availableSlots += entry.info.availableSlots;
    }
    return { total: this.workers.size, idle, executing, totalSlots, availableSlots };
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
