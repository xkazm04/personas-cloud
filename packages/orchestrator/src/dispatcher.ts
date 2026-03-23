import { type ExecRequest, type ExecStdout, type ExecStderr, type ExecComplete, type ExecProgress, type WorkerEvent, type WorkerBusy, type ProgressInfo, type PhaseTimings, type EncryptedPayload, TOPICS, decrypt, DecryptionError, deriveMasterKeyAsync } from '@dac-cloud/shared';
import type Database from 'better-sqlite3';
import type { WorkerPool } from './workerPool.js';
import type { TokenManager } from './tokenManager.js';
import type { OAuthManager } from './oauth.js';
import type { KafkaClient } from './kafka.js';
import type { TenantDbManager } from './tenantDbManager.js';
import type { TenantKeyManager } from './tenantKeyManager.js';
import type { AuditLog } from './auditLog.js';
import type { Logger } from 'pino';
import * as db from './db/index.js';
import { resolveToken, ExecutionPreparer, type Decryptor } from './dispatchPipeline.js';

interface QueuedExecution {
  request: ExecRequest;
  receivedAt: number;
}

// Maximum number of times an execution will be re-queued after worker disconnect
const MAX_DISCONNECT_RETRIES = 2;

/** Backpressure configuration for the dispatch queue. */
export interface QueueBackpressureConfig {
  /** Maximum total queue depth. Submissions beyond this are rejected. 0 = unlimited. */
  maxQueueDepth: number;
  /** Queue depth at which a warning metric is emitted. 0 = disabled. */
  warningThreshold: number;
  /** Queue depth at which a critical metric is emitted. 0 = disabled. */
  criticalThreshold: number;
  /** Maximum queued items per tenant. 0 = unlimited (no per-tenant quota). */
  perTenantQuota: number;
  /** Retry-After value (seconds) included in rejection results. */
  retryAfterSeconds: number;
}

/** Result of a submit() call — either accepted or rejected with a reason. */
export type SubmitResult =
  | { accepted: true }
  | { accepted: false; reason: 'queue_full' | 'tenant_quota_exceeded'; retryAfterSeconds: number };

/** Which threshold band the queue is currently in. */
type ThresholdLevel = 'normal' | 'warning' | 'critical';

/** All valid execution statuses. */
type ExecutionStatus = 'queued' | 'running' | 'completed' | 'failed' | 'cancelled';

/** What triggered a state transition. */
type TransitionTrigger =
  | 'submit'
  | 'dispatch'
  | 'complete'
  | 'busy-nack'
  | 'disconnect-retry'
  | 'disconnect-exhausted'
  | 'assign-failure';

// In-memory tracking for active executions (real-time output streaming)
interface ActiveExecution {
  workerId: string;
  startedAt: number;
  output: string[];
  status: 'running' | 'completed' | 'failed' | 'cancelled';
  progress?: ProgressInfo;
  completedAt?: number;
  exitCode?: number;
  durationMs?: number;
  sessionId?: string;
  totalCostUsd?: number;
  phaseTimings?: PhaseTimings;
  /** Original request kept for re-queuing on worker disconnect. Only present for in-flight executions. */
  request?: ExecRequest;
  /** How many times this execution has been re-queued after a worker disconnect. */
  disconnectRetries?: number;
}

const OUTPUT_FLUSH_INTERVAL_MS = 500;

export class Dispatcher {
  private queue: QueuedExecution[] = [];
  // In-memory map for real-time output + cancel routing (active executions only)
  private active = new Map<string, ActiveExecution>();
  private tenantDbManager: TenantDbManager | null = null;
  private masterKey: Buffer | null = null;
  private tenantKeyManager: TenantKeyManager | null = null;
  private auditLog: AuditLog | null = null;
  private preparer: ExecutionPreparer | null = null;
  // Maps executionId → projectId for DB resolution during callbacks
  private executionTenant = new Map<string, string>();
  // Batched output buffers: executionId → pending chunks
  private outputBuffers = new Map<string, string[]>();
  private outputFlushTimer: NodeJS.Timeout;
  // Guard flag to prevent concurrent processQueue runs from racing on getIdleWorker/assign
  private dispatching = false;
  // Backpressure state
  private backpressure: QueueBackpressureConfig;
  private tenantQueueCounts = new Map<string, number>();
  private currentThresholdLevel: ThresholdLevel = 'normal';
  // Interval counters — reset each time snapshotAndResetCounters() is called
  private _intervalCompleted = 0;
  private _intervalFailed = 0;
  private _intervalCostUsd = 0;

  constructor(
    private pool: WorkerPool,
    private tokenManager: TokenManager,
    private oauth: OAuthManager | null,
    private kafka: KafkaClient,
    private logger: Logger,
    backpressureConfig?: Partial<QueueBackpressureConfig>,
  ) {
    this.backpressure = {
      maxQueueDepth: backpressureConfig?.maxQueueDepth ?? 1000,
      warningThreshold: backpressureConfig?.warningThreshold ?? 500,
      criticalThreshold: backpressureConfig?.criticalThreshold ?? 800,
      perTenantQuota: backpressureConfig?.perTenantQuota ?? 0,
      retryAfterSeconds: backpressureConfig?.retryAfterSeconds ?? 30,
    };
    // Periodic flush of batched output to DB
    this.outputFlushTimer = setInterval(() => this.flushOutputBuffers(), OUTPUT_FLUSH_INTERVAL_MS);

    // When a worker becomes ready, try to dispatch queued work
    this.pool.on('worker-ready', () => {
      this.processQueue();
    });

    // When a worker connects, try to dispatch
    this.pool.on('worker-connected', () => {
      this.processQueue();
    });

    // Track stdout for execution output
    this.pool.on('stdout', (_workerId: string, msg: ExecStdout) => {
      this.handleOutputChunk(msg.executionId, msg.chunk, msg.timestamp);
    });

    // Track stderr for debugging failed executions
    this.pool.on('stderr', (_workerId: string, msg: ExecStderr) => {
      this.handleOutputChunk(msg.executionId, msg.chunk, msg.timestamp, '[STDERR] ');
      this.logger.warn({ executionId: msg.executionId, stderr: msg.chunk }, 'Execution stderr');
    });

    // Handle execution completion
    this.pool.on('complete', (_workerId: string, msg: ExecComplete) => {
      this.kafka.produce(TOPICS.EXEC_LIFECYCLE, JSON.stringify({
        executionId: msg.executionId,
        status: msg.status,
        exitCode: msg.exitCode,
        durationMs: msg.durationMs,
        sessionId: msg.sessionId,
        totalCostUsd: msg.totalCostUsd,
        ...(msg.phaseTimings ? { phaseTimings: msg.phaseTimings } : {}),
      }), msg.executionId).catch(err => {
        this.logger.error({ err, executionId: msg.executionId }, 'Failed to produce lifecycle to Kafka');
      });

      // Flush any remaining buffered output before persisting completion
      this.flushOutputForExecution(msg.executionId);

      // Update in-memory
      const exec = this.active.get(msg.executionId);
      const terminalStatus: ExecutionStatus = msg.status === 'completed' ? 'completed' : msg.status === 'cancelled' ? 'cancelled' : 'failed';
      this.logTransition({
        executionId: msg.executionId,
        previousState: 'running',
        newState: terminalStatus,
        trigger: 'complete',
        workerId: exec?.workerId,
        disconnectRetries: exec?.disconnectRetries ?? 0,
      });
      if (exec) {
        exec.status = msg.status;
        exec.completedAt = Date.now();
        exec.exitCode = msg.exitCode;
        exec.durationMs = msg.durationMs;
        exec.sessionId = msg.sessionId;
        exec.totalCostUsd = msg.totalCostUsd;
        exec.phaseTimings = msg.phaseTimings;
        // Remove from active after a short delay (allow polling to pick up final state)
        setTimeout(() => this.active.delete(msg.executionId), 30_000);
      }

      // Increment interval counters
      if (terminalStatus === 'completed') {
        this._intervalCompleted++;
      } else {
        this._intervalFailed++;
      }
      this._intervalCostUsd += msg.totalCostUsd ?? 0;

      // Persist to DB
      const completeDb = this.resolveDbForExecution(msg.executionId);
      if (completeDb) {
        try {
          db.updateExecution(completeDb, msg.executionId, {
            status: msg.status === 'completed' ? 'completed' : msg.status === 'cancelled' ? 'cancelled' : 'failed',
            completedAt: new Date().toISOString(),
            durationMs: msg.durationMs,
            claudeSessionId: msg.sessionId,
            costUsd: msg.totalCostUsd ?? 0,
          });
        } catch (err) {
          this.logger.error({ err, executionId: msg.executionId }, 'Failed to persist execution completion');
        }
      }

      // Clean up tenant mapping after completion delay
      setTimeout(() => this.executionTenant.delete(msg.executionId), 35_000);

      // Process next queued item
      this.processQueue();
    });

    // Track execution progress
    this.pool.on('progress', (_workerId: string, msg: ExecProgress) => {
      const exec = this.active.get(msg.executionId);
      if (exec) {
        exec.progress = { phase: msg.phase, percent: msg.percent, detail: msg.detail, updatedAt: msg.timestamp };
      }
      this.kafka.produce(TOPICS.EXEC_PROGRESS, JSON.stringify({
        executionId: msg.executionId,
        phase: msg.phase,
        percent: msg.percent,
        detail: msg.detail,
        timestamp: msg.timestamp,
      }), msg.executionId).catch(err => {
        this.logger.error({ err, executionId: msg.executionId }, 'Failed to produce progress to Kafka');
      });
    });

    // Forward persona events to Kafka
    this.pool.on('persona-event', (_workerId: string, msg: WorkerEvent) => {
      this.kafka.produce(TOPICS.EVENTS, JSON.stringify({
        executionId: msg.executionId,
        eventType: msg.eventType,
        payload: msg.payload,
        timestamp: Date.now(),
      }), msg.executionId).catch(err => {
        this.logger.error({ err, executionId: msg.executionId }, 'Failed to produce event to Kafka');
      });
    });

    // Handle worker busy nack — re-queue the rejected execution
    this.pool.on('busy', (_workerId: string, msg: WorkerBusy) => {
      this.logger.warn({ workerId: _workerId, executionId: msg.executionId, reason: msg.reason }, 'Worker rejected assignment, re-queuing');
      const exec = this.active.get(msg.executionId);
      if (exec?.request) {
        const request = exec.request;
        this.logTransition({
          executionId: msg.executionId,
          previousState: 'running',
          newState: 'queued',
          trigger: 'busy-nack',
          workerId: _workerId,
          personaId: request.personaId,
          disconnectRetries: exec.disconnectRetries ?? 0,
        });
        this.active.delete(msg.executionId);
        // Mark as queued in DB
        const reqDb = this.resolveDbForExecution(msg.executionId);
        if (reqDb) {
          try { db.updateExecution(reqDb, msg.executionId, { status: 'queued' }); } catch (err) { this.logger.warn({ err, executionId: msg.executionId, op: 'updateExecution:queued' }, 'Failed to update execution status on busy nack'); }
        }
        this.queue.unshift({ request, receivedAt: Date.now() });
        this.processQueue();
      } else {
        // Fallback: reconstruct from DB if request was not stored (should not happen)
        this.logTransition({
          executionId: msg.executionId,
          previousState: 'running',
          newState: 'queued',
          trigger: 'busy-nack',
          workerId: _workerId,
        });
        this.active.delete(msg.executionId);
        const projectId = this.executionTenant.get(msg.executionId) || 'default';
        const reqDb = this.resolveDb(projectId);
        if (reqDb) {
          try { db.updateExecution(reqDb, msg.executionId, { status: 'queued' }); } catch (err) { this.logger.warn({ err, executionId: msg.executionId, op: 'updateExecution:queued' }, 'Failed to update execution status on busy nack fallback'); }
        }
        const dbExec = reqDb ? db.getExecution(reqDb, msg.executionId) : null;
        if (dbExec) {
          this.queue.unshift({
            request: {
              executionId: msg.executionId,
              personaId: dbExec.personaId,
              prompt: dbExec.inputData || '',
              projectId,
              config: { timeoutMs: 300_000 },
            },
            receivedAt: Date.now(),
          });
          this.processQueue();
        }
      }
    });

    // Handle worker disconnect during execution — re-queue if retries remain
    this.pool.on('worker-disconnected', (_workerId: string, executionId: string | undefined) => {
      if (executionId) {
        const exec = this.active.get(executionId);
        const retries = exec?.disconnectRetries ?? 0;

        if (exec?.request && retries < MAX_DISCONNECT_RETRIES) {
          // Re-queue for retry
          const nextRetry = retries + 1;
          this.logTransition({
            executionId,
            previousState: 'running',
            newState: 'queued',
            trigger: 'disconnect-retry',
            workerId: _workerId,
            personaId: exec.request.personaId,
            disconnectRetries: nextRetry,
            errorMessage: `Worker disconnected (retry ${nextRetry}/${MAX_DISCONNECT_RETRIES})`,
          });

          // Clear the active entry so dispatchToWorker can create a fresh one
          this.active.delete(executionId);

          // Mark as queued in DB
          const disconnDb = this.resolveDbForExecution(executionId);
          if (disconnDb) {
            try {
              db.updateExecution(disconnDb, executionId, {
                status: 'queued',
                errorMessage: `Worker disconnected (retry ${nextRetry}/${MAX_DISCONNECT_RETRIES})`,
              });
            } catch (err) { this.logger.warn({ err, executionId, op: 'updateExecution:queued' }, 'Failed to update execution status on worker disconnect re-queue'); }
          }

          // Temporarily store the retry count so dispatchToWorker picks it up
          const request = exec.request;
          // Create a placeholder active entry to carry retry count across re-dispatch
          this.active.set(executionId, {
            workerId: '',
            startedAt: 0,
            output: [],
            status: 'running',
            request,
            disconnectRetries: nextRetry,
          });

          this.queue.unshift({ request, receivedAt: Date.now() });
          this.processQueue();
        } else {
          // Retries exhausted or no request stored — permanent failure
          this.logTransition({
            executionId,
            previousState: 'running',
            newState: 'failed',
            trigger: 'disconnect-exhausted',
            workerId: _workerId,
            disconnectRetries: retries,
            errorMessage: `Worker disconnected (retries exhausted: ${retries}/${MAX_DISCONNECT_RETRIES})`,
          });
          if (exec) {
            exec.status = 'failed';
            exec.completedAt = Date.now();
            setTimeout(() => this.active.delete(executionId), 30_000);
          }

          const disconnDb = this.resolveDbForExecution(executionId);
          if (disconnDb) {
            try {
              db.updateExecution(disconnDb, executionId, {
                status: 'failed',
                errorMessage: `Worker disconnected (retries exhausted: ${retries}/${MAX_DISCONNECT_RETRIES})`,
                completedAt: new Date().toISOString(),
              });
            } catch (err) { this.logger.warn({ err, executionId, op: 'updateExecution:failed' }, 'Failed to persist execution failure on worker disconnect'); }
          }
          this.executionTenant.delete(executionId);

          this.kafka.produce(TOPICS.EXEC_LIFECYCLE, JSON.stringify({
            executionId,
            status: 'failed',
            error: 'Worker disconnected (retries exhausted)',
            durationMs: 0,
          }), executionId).catch(err => { this.logger.warn({ err, executionId, op: 'kafka:exec_lifecycle' }, 'Failed to produce disconnect failure lifecycle to Kafka'); });
        }
      }
    });
  }

  setTenantDbManager(mgr: TenantDbManager): void {
    this.tenantDbManager = mgr;
  }

  async setMasterKey(masterKeyHex: string): Promise<void> {
    this.masterKey = await deriveMasterKeyAsync(masterKeyHex);
  }

  setTenantKeyManager(mgr: TenantKeyManager): void {
    this.tenantKeyManager = mgr;
  }

  setAuditLog(log: AuditLog): void {
    this.auditLog = log;
  }

  /** Resolve the database for a given projectId via tenant DB manager. */
  private resolveDb(projectId?: string): Database.Database | null {
    if (this.tenantDbManager && projectId) {
      return this.tenantDbManager.getTenantDb(projectId);
    }
    return null;
  }

  /** Resolve the database for an in-flight execution by its ID. */
  private resolveDbForExecution(executionId: string): Database.Database | null {
    const projectId = this.executionTenant.get(executionId);
    return this.resolveDb(projectId);
  }

  /**
   * Emit a single structured log entry for every execution state transition.
   * Creates a queryable timeline: grep for "Execution state transition" to see
   * the full lifecycle of any execution.
   */
  private logTransition(opts: {
    executionId: string;
    previousState: ExecutionStatus | null;
    newState: ExecutionStatus;
    trigger: TransitionTrigger;
    workerId?: string;
    personaId?: string;
    queueWaitMs?: number;
    disconnectRetries?: number;
    errorMessage?: string;
  }): void {
    this.logger.info({
      executionId: opts.executionId,
      previousState: opts.previousState,
      newState: opts.newState,
      trigger: opts.trigger,
      ...(opts.workerId ? { workerId: opts.workerId } : {}),
      ...(opts.personaId ? { personaId: opts.personaId } : {}),
      ...(opts.queueWaitMs !== undefined ? { queueWaitMs: opts.queueWaitMs } : {}),
      ...(opts.disconnectRetries !== undefined ? { disconnectRetries: opts.disconnectRetries } : {}),
      ...(opts.errorMessage ? { errorMessage: opts.errorMessage } : {}),
    }, 'Execution state transition');
  }

  /**
   * Resolve the tenant/master key, decrypt a payload, and emit audit events.
   * Returns the plaintext on success or undefined on failure (key missing or decrypt error).
   */
  private async decryptWithAudit(
    payload: EncryptedPayload,
    tenantId: string,
    resourceId: string,
    logContext: Record<string, unknown>,
    logMessage: string,
  ): Promise<string | undefined> {
    const decryptKey = this.tenantKeyManager
      ? await this.tenantKeyManager.getKey(tenantId)
      : this.masterKey;
    if (!decryptKey) return undefined;

    try {
      const decrypted = decrypt(payload, decryptKey, { tenantId, credentialId: resourceId, resourceType: 'credential' });
      try { this.auditLog?.record({ action: 'key:decrypt', tenantId, resourceType: 'credential', resourceId }); } catch (err) { this.logger.error({ err, tenantId, resourceId, op: 'auditLog:key:decrypt' }, 'Failed to record decrypt audit event'); }
      return decrypted;
    } catch (err) {
      const reason = err instanceof DecryptionError ? err.message : (err instanceof Error ? err.message : String(err));
      this.logger.error({ err, ...logContext, tenantId }, logMessage);
      try { this.auditLog?.record({ action: 'key:decrypt_failed', tenantId, resourceType: 'credential', resourceId, detail: { reason } }); } catch (err) { this.logger.error({ err, tenantId, resourceId, op: 'auditLog:key:decrypt_failed' }, 'Failed to record decrypt failure audit event'); }
      return undefined;
    }
  }

  submit(request: ExecRequest): SubmitResult {
    const projectId = request.projectId || 'default';

    // --- Backpressure checks ---
    const { maxQueueDepth, perTenantQuota, retryAfterSeconds } = this.backpressure;

    // Global queue depth limit
    if (maxQueueDepth > 0 && this.queue.length >= maxQueueDepth) {
      this.logger.warn({
        executionId: request.executionId,
        personaId: request.personaId,
        queueDepth: this.queue.length,
        maxQueueDepth,
        projectId,
      }, 'Submission rejected: queue depth limit exceeded');
      return { accepted: false, reason: 'queue_full', retryAfterSeconds };
    }

    // Per-tenant quota
    if (perTenantQuota > 0) {
      const tenantCount = this.tenantQueueCounts.get(projectId) ?? 0;
      if (tenantCount >= perTenantQuota) {
        this.logger.warn({
          executionId: request.executionId,
          personaId: request.personaId,
          projectId,
          tenantQueueCount: tenantCount,
          perTenantQuota,
        }, 'Submission rejected: per-tenant queue quota exceeded');
        return { accepted: false, reason: 'tenant_quota_exceeded', retryAfterSeconds };
      }
    }

    this.logger.info({
      executionId: request.executionId,
      personaId: request.personaId,
      ...(request.eventId ? { eventId: request.eventId } : {}),
    }, 'Execution submitted');

    // Track which tenant owns this execution (for callback DB resolution)
    this.executionTenant.set(request.executionId, projectId);

    // Create DB record immediately
    const submitDb = this.resolveDb(projectId);
    if (submitDb) {
      try {
        db.createExecution(submitDb, {
          id: request.executionId,
          personaId: request.personaId,
          inputData: request.prompt,
          projectId,
          eventId: request.eventId ?? null,
        });
      } catch (err) {
        this.logger.error({ err, executionId: request.executionId }, 'Failed to create execution record');
      }
    }

    this.logTransition({
      executionId: request.executionId,
      previousState: null,
      newState: 'queued',
      trigger: 'submit',
      personaId: request.personaId,
      disconnectRetries: 0,
    });

    this.queue.push({ request, receivedAt: Date.now() });
    this.incrementTenantCount(projectId);
    this.checkThresholds();
    this.processQueue();
    return { accepted: true };
  }

  private async processQueue(): Promise<void> {
    // Guard: only one processQueue loop runs at a time to prevent concurrent
    // calls from racing on getIdleWorker()/assign() and double-claiming a worker.
    if (this.dispatching) return;
    this.dispatching = true;
    try {
      while (this.queue.length > 0) {
        const workerId = this.pool.getIdleWorker();
        if (!workerId) {
          this.logger.debug({ queueLength: this.queue.length }, 'No idle worker, execution queued');
          return;
        }

        const item = this.queue.shift()!;
        const tenantId = item.request.projectId || 'default';
        this.decrementTenantCount(tenantId);
        this.checkThresholds();
        await this.dispatchToWorker(workerId, item.request, item.receivedAt);
      }
    } finally {
      this.dispatching = false;
    }
  }

  /** Exposes decryptWithAudit as the Decryptor interface for pipeline stages. */
  private getDecryptor(): Decryptor {
    return {
      decrypt: (payload, tenantId, resourceId, logContext, logMessage) =>
        this.decryptWithAudit(payload, tenantId, resourceId, logContext, logMessage),
    };
  }

  /** Lazily initialise the ExecutionPreparer (depends on mutable setter state). */
  private ensurePreparer(): ExecutionPreparer {
    if (!this.preparer) {
      this.preparer = new ExecutionPreparer(
        (projectId) => this.resolveDb(projectId),
        this.getDecryptor(),
        this.logger,
      );
    }
    return this.preparer;
  }

  private async dispatchToWorker(workerId: string, request: ExecRequest, queuedAt?: number): Promise<void> {
    const dispatchStartMs = Date.now();
    const queueWaitMs = queuedAt !== undefined ? dispatchStartMs - queuedAt : undefined;

    // Stage 1: Resolve token (stays in Dispatcher)
    const tokenStartMs = Date.now();
    const token = await resolveToken(this.oauth, this.tokenManager);
    const tokenResolveMs = Date.now() - tokenStartMs;
    if (!token) {
      this.logger.error({ executionId: request.executionId, tokenResolveMs }, 'No Claude token available, re-queuing execution');
      this.queue.unshift({ request, receivedAt: Date.now() });
      return;
    }

    // Stages 2-4: Persona resolution, payload assembly, assignment building
    const prepareStartMs = Date.now();
    const assignment = await this.ensurePreparer().prepare(request, token);
    const prepareDurationMs = Date.now() - prepareStartMs;
    const tenantId = request.projectId || 'default';
    const dispatchDb = this.resolveDb(tenantId);

    // Track in-memory for real-time streaming (preserve retry count across re-dispatches)
    const prevRetries = this.active.get(request.executionId)?.disconnectRetries ?? 0;
    this.active.set(request.executionId, {
      workerId,
      startedAt: Date.now(),
      output: [],
      status: 'running',
      request,
      disconnectRetries: prevRetries,
    });

    // Mark running in DB
    if (dispatchDb) {
      try {
        db.updateExecution(dispatchDb, request.executionId, {
          status: 'running',
          startedAt: new Date().toISOString(),
        });
      } catch (err) { this.logger.warn({ err, executionId: request.executionId, op: 'updateExecution:running' }, 'Failed to mark execution as running in DB'); }
    }

    const sent = this.pool.assign(workerId, assignment);
    const totalDispatchMs = Date.now() - dispatchStartMs;
    if (!sent) {
      this.logger.error({ workerId, executionId: request.executionId }, 'Failed to assign to worker');
      this.active.delete(request.executionId);
      if (dispatchDb) {
        try { db.updateExecution(dispatchDb, request.executionId, { status: 'queued' }); } catch (err) { this.logger.warn({ err, executionId: request.executionId, op: 'updateExecution:queued' }, 'Failed to re-queue execution in DB after assign failure'); }
      }
      this.logTransition({
        executionId: request.executionId,
        previousState: 'running',
        newState: 'queued',
        trigger: 'assign-failure',
        workerId,
        personaId: request.personaId,
        disconnectRetries: prevRetries,
      });
      this.queue.unshift({ request, receivedAt: Date.now() });
      this.processQueue();
    } else {
      this.logTransition({
        executionId: request.executionId,
        previousState: 'queued',
        newState: 'running',
        trigger: 'dispatch',
        workerId,
        personaId: request.personaId,
        queueWaitMs,
        disconnectRetries: prevRetries,
      });
    }

    // Emit dispatch latency metrics for observability
    this.logger.info({
      executionId: request.executionId,
      personaId: request.personaId,
      workerId,
      queueWaitMs,
      tokenResolveMs,
      prepareDurationMs,
      totalDispatchMs,
    }, 'Dispatch latency');
  }

  cancelExecution(executionId: string): boolean {
    const exec = this.active.get(executionId);
    if (!exec || exec.status !== 'running') return false;

    return this.pool.send(exec.workerId, {
      type: 'cancel',
      executionId,
    });
  }

  getQueueLength(): number {
    return this.queue.length;
  }

  getBackpressureStatus(): { depth: number; maxDepth: number; level: ThresholdLevel; tenantCounts: Record<string, number> } {
    const tenantCounts: Record<string, number> = {};
    for (const [k, v] of this.tenantQueueCounts) {
      if (v > 0) tenantCounts[k] = v;
    }
    return { depth: this.queue.length, maxDepth: this.backpressure.maxQueueDepth, level: this.currentThresholdLevel, tenantCounts };
  }

  getActiveExecutions(): Array<{ executionId: string; workerId: string; startedAt: number }> {
    return Array.from(this.active.entries())
      .filter(([, e]) => e.status === 'running')
      .map(([executionId, e]) => ({
        executionId,
        workerId: e.workerId,
        startedAt: e.startedAt,
      }));
  }

  /**
   * Get execution state. Tries in-memory first (for real-time output),
   * then falls back to DB for completed/historical executions.
   */
  getExecution(executionId: string): ActiveExecution | null {
    // Check in-memory active executions first
    const active = this.active.get(executionId);
    if (active) return active;

    const execDb = this.resolveDbForExecution(executionId);
    if (execDb) {
      const dbExec = db.getExecution(execDb, executionId);
      if (dbExec) {
        return {
          workerId: '',
          startedAt: dbExec.startedAt ? new Date(dbExec.startedAt).getTime() : 0,
          output: dbExec.outputData ? dbExec.outputData.split('\n').filter(Boolean) : [],
          status: dbExec.status as ActiveExecution['status'],
          completedAt: dbExec.completedAt ? new Date(dbExec.completedAt).getTime() : undefined,
          durationMs: dbExec.durationMs ?? undefined,
          sessionId: dbExec.claudeSessionId ?? undefined,
          totalCostUsd: dbExec.costUsd ?? undefined,
        };
      }
    }

    return null;
  }

  getExecutionOutput(executionId: string): string[] | null {
    const exec = this.getExecution(executionId);
    return exec?.output ?? null;
  }

  /** Shared handler for stdout/stderr output chunks. */
  private handleOutputChunk(executionId: string, chunk: string, timestamp: number, prefix?: string): void {
    const line = prefix ? `${prefix}${chunk}` : chunk;

    const exec = this.active.get(executionId);
    if (exec) {
      exec.output.push(line);
    }

    this.bufferOutput(executionId, line + '\n');

    // Forward stdout to Kafka (stderr is not forwarded)
    if (!prefix) {
      this.kafka.produce(TOPICS.EXEC_OUTPUT, JSON.stringify({
        executionId,
        chunk,
        timestamp,
      }), executionId).catch(err => {
        this.logger.error({ err, executionId }, 'Failed to produce output to Kafka');
      });
    }
  }

  /** Buffer an output chunk for batched DB write. */
  private bufferOutput(executionId: string, chunk: string): void {
    let buf = this.outputBuffers.get(executionId);
    if (!buf) {
      buf = [];
      this.outputBuffers.set(executionId, buf);
    }
    buf.push(chunk);
  }

  /** Flush buffered output for a specific execution to DB. */
  private flushOutputForExecution(executionId: string): void {
    const buf = this.outputBuffers.get(executionId);
    if (!buf || buf.length === 0) return;

    const combined = buf.join('');
    this.outputBuffers.delete(executionId);

    const outputDb = this.resolveDbForExecution(executionId);
    if (outputDb) {
      try { db.appendOutput(outputDb, executionId, combined); } catch (err) { this.logger.warn({ err, executionId, op: 'appendOutput' }, 'Failed to flush buffered output to DB'); }
    }
  }

  /** Flush all buffered output across all executions to DB. */
  private flushOutputBuffers(): void {
    for (const executionId of this.outputBuffers.keys()) {
      this.flushOutputForExecution(executionId);
    }
  }

  private incrementTenantCount(projectId: string): void {
    this.tenantQueueCounts.set(projectId, (this.tenantQueueCounts.get(projectId) ?? 0) + 1);
  }

  private decrementTenantCount(projectId: string): void {
    const count = this.tenantQueueCounts.get(projectId) ?? 0;
    if (count <= 1) {
      this.tenantQueueCounts.delete(projectId);
    } else {
      this.tenantQueueCounts.set(projectId, count - 1);
    }
  }

  /**
   * Check queue depth against warning/critical thresholds and emit Kafka
   * metrics when the level changes.
   */
  private checkThresholds(): void {
    const depth = this.queue.length;
    const { warningThreshold, criticalThreshold } = this.backpressure;

    let newLevel: ThresholdLevel = 'normal';
    if (criticalThreshold > 0 && depth >= criticalThreshold) {
      newLevel = 'critical';
    } else if (warningThreshold > 0 && depth >= warningThreshold) {
      newLevel = 'warning';
    }

    if (newLevel !== this.currentThresholdLevel) {
      const previousLevel = this.currentThresholdLevel;
      this.currentThresholdLevel = newLevel;

      this.logger.warn({
        queueDepth: depth,
        previousLevel,
        newLevel,
        warningThreshold,
        criticalThreshold,
      }, 'Queue threshold level changed');

      this.kafka.produce(TOPICS.QUEUE_METRICS, JSON.stringify({
        event: 'threshold_crossed',
        previousLevel,
        newLevel,
        queueDepth: depth,
        maxQueueDepth: this.backpressure.maxQueueDepth,
        timestamp: Date.now(),
      })).catch(err => {
        this.logger.error({ err }, 'Failed to produce queue metrics to Kafka');
      });
    }
  }

  /** Return cumulative counters since last call and reset them. */
  snapshotAndResetCounters(): { completed: number; failed: number; costUsd: number } {
    const snap = {
      completed: this._intervalCompleted,
      failed: this._intervalFailed,
      costUsd: Math.round(this._intervalCostUsd * 1_000_000) / 1_000_000,
    };
    this._intervalCompleted = 0;
    this._intervalFailed = 0;
    this._intervalCostUsd = 0;
    return snap;
  }

  shutdown(): void {
    clearInterval(this.outputFlushTimer);
    this.flushOutputBuffers();
  }
}
