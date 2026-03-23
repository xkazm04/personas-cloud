import { type ExecAssign, type ExecRequest, type ExecStdout, type ExecStderr, type ExecComplete, type ExecProgress, type WorkerEvent, type WorkerBusy, type WorkerReviewRequest, type PermissionPolicy, type ReviewRequest, type WorkerInfo, type ProgressInfo, type PhaseTimings, type EncryptedPayload, TOPICS, projectTopic, assemblePrompt, parseModelProfile, parsePermissionPolicy, decrypt, DecryptionError, deriveMasterKeyAsync } from '@dac-cloud/shared';
import { OrchestratorMetrics } from './metrics.js';
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
  /** Number of times this execution has been re-queued due to transient failures. */
  retryCount: number;
  /** Earliest time (ms since epoch) at which this item may be retried. */
  nextRetryAt: number;
}

/**
 * Rolling tail buffer size per execution (64 KB).
 * Only the most recent output is kept in memory for real-time polling.
 * Full output is persisted to DB and streamed to Kafka.
 */
const TAIL_BUFFER_BYTES = 64 * 1024;

/** Maximum pending executions in the in-memory queue before rejecting new submissions. */
const MAX_QUEUE_DEPTH = 500;

/** Maximum total output size in bytes allowed from a single execution (10 MB). */
const MAX_OUTPUT_BYTES = 10 * 1024 * 1024;

/** Maximum number of entries in the LRU completed-execution cache. */
const COMPLETED_CACHE_MAX_SIZE = 1_000;

/** Maximum seconds to wait for in-flight executions during graceful shutdown. */
const DRAIN_TIMEOUT_MS = 60_000;

/** Maximum number of re-queue retries before permanently failing an execution. */
const MAX_REQUEUE_RETRIES = 5;

/** Base backoff delay in ms (doubled on each retry: 5s, 10s, 20s, 40s, 80s). */
const REQUEUE_BASE_DELAY_MS = 5_000;

// ---------------------------------------------------------------------------
// Ring buffer — O(1) FIFO push/shift without array element copying
// ---------------------------------------------------------------------------

/**
 * Fixed-capacity ring buffer with automatic doubling when full.
 * push() is O(1) amortized, shift() is O(1). No element copying on dequeue
 * (unlike Array.shift which copies n-1 elements).
 */
class RingBuffer<T> {
  private buf: (T | undefined)[];
  private head = 0;
  private tail = 0;
  private count = 0;

  constructor(initialCapacity = 64) {
    this.buf = new Array(initialCapacity);
  }

  get length(): number { return this.count; }

  push(item: T): void {
    if (this.count === this.buf.length) this.grow();
    this.buf[this.tail] = item;
    this.tail = (this.tail + 1) % this.buf.length;
    this.count++;
  }

  shift(): T | undefined {
    if (this.count === 0) return undefined;
    const item = this.buf[this.head]!;
    this.buf[this.head] = undefined; // allow GC
    this.head = (this.head + 1) % this.buf.length;
    this.count--;
    return item;
  }

  clear(): void {
    this.buf = new Array(64);
    this.head = 0;
    this.tail = 0;
    this.count = 0;
  }

  private grow(): void {
    const newCap = this.buf.length * 2;
    const newBuf = new Array<T | undefined>(newCap);
    for (let i = 0; i < this.count; i++) {
      newBuf[i] = this.buf[(this.head + i) % this.buf.length];
    }
    this.buf = newBuf;
    this.head = 0;
    this.tail = this.count;
  }
}

// ---------------------------------------------------------------------------
// Min-heap — O(log n) insert/extract for backoff items keyed on nextRetryAt
// ---------------------------------------------------------------------------

/**
 * Binary min-heap keyed on `nextRetryAt`. Insert and extractMin are O(log n).
 * peek is O(1). Used for the backoff segment where items must be ordered by
 * their retry time.
 */
class MinHeap {
  private heap: QueuedExecution[] = [];

  get length(): number { return this.heap.length; }

  /** O(1) — view the item with the smallest nextRetryAt without removing. */
  peek(): QueuedExecution | undefined {
    return this.heap[0];
  }

  /** O(log n) — insert an item while maintaining heap invariant. */
  push(item: QueuedExecution): void {
    this.heap.push(item);
    this.siftUp(this.heap.length - 1);
  }

  /** O(log n) — remove and return the item with the smallest nextRetryAt. */
  pop(): QueuedExecution | undefined {
    if (this.heap.length === 0) return undefined;
    const min = this.heap[0];
    const last = this.heap.pop()!;
    if (this.heap.length > 0) {
      this.heap[0] = last;
      this.siftDown(0);
    }
    return min;
  }

  clear(): void {
    this.heap.length = 0;
  }

  private siftUp(i: number): void {
    while (i > 0) {
      const parent = (i - 1) >>> 1;
      if (this.heap[parent].nextRetryAt <= this.heap[i].nextRetryAt) break;
      [this.heap[parent], this.heap[i]] = [this.heap[i], this.heap[parent]];
      i = parent;
    }
  }

  private siftDown(i: number): void {
    const n = this.heap.length;
    while (true) {
      let smallest = i;
      const left = 2 * i + 1;
      const right = 2 * i + 2;
      if (left < n && this.heap[left].nextRetryAt < this.heap[smallest].nextRetryAt) smallest = left;
      if (right < n && this.heap[right].nextRetryAt < this.heap[smallest].nextRetryAt) smallest = right;
      if (smallest === i) break;
      [this.heap[smallest], this.heap[i]] = [this.heap[i], this.heap[smallest]];
      i = smallest;
    }
  }
}

// ---------------------------------------------------------------------------
// Two-segment execution queue: ready ring buffer + backoff min-heap
// ---------------------------------------------------------------------------

/**
 * Two-segment execution queue:
 * - **Ready segment**: Ring buffer — O(1) push, O(1) shift. No array copying.
 * - **Backoff segment**: Min-heap keyed on nextRetryAt — O(log n) push, O(1) peek, O(log n) pop.
 *
 * processQueue becomes a two-liner: promote ready backoff items, then pop from the ring buffer.
 */
class ExecutionQueue {
  private ready = new RingBuffer<QueuedExecution>();
  private backoff = new MinHeap();

  /** Total number of items across both segments. */
  get length(): number {
    return this.ready.length + this.backoff.length;
  }

  /** Push a ready item (nextRetryAt <= now or 0). O(1) amortized. */
  pushReady(item: QueuedExecution): void {
    this.ready.push(item);
  }

  /** Push a backoff item into the min-heap. O(log n). */
  pushBackoff(item: QueuedExecution): void {
    this.backoff.push(item);
  }

  /** Dequeue the first ready item. O(1). */
  shift(): QueuedExecution | undefined {
    return this.ready.shift();
  }

  /** Promote backoff items whose nextRetryAt has passed to the ready ring buffer. */
  promoteReady(): void {
    const now = Date.now();
    while (this.backoff.length > 0 && this.backoff.peek()!.nextRetryAt <= now) {
      this.ready.push(this.backoff.pop()!);
    }
  }

  /** Check if there are any items ready for dispatch (including promotable backoff items). */
  hasReady(): boolean {
    if (this.ready.length > 0) return true;
    if (this.backoff.length > 0 && this.backoff.peek()!.nextRetryAt <= Date.now()) return true;
    return false;
  }

  /** Clear all items from both segments. */
  clear(): void {
    this.ready.clear();
    this.backoff.clear();
  }
}

/** Execution terminal status as stored in the DB and used across the dispatcher. */
type DbExecutionStatus = 'completed' | 'failed' | 'cancelled';

/**
 * Map a worker-reported status string to a canonical DB status.
 * Centralises the mapping that was previously duplicated in 3+ locations,
 * so adding a new worker status produces a compiler error here instead of
 * silently falling through to 'failed' in scattered ternaries.
 */
function toDbStatus(workerStatus: string): DbExecutionStatus {
  switch (workerStatus) {
    case 'completed': return 'completed';
    case 'cancelled': return 'cancelled';
    default: return 'failed';
  }
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
  /** Persona that owns this execution — carried forward from ExecRequest to avoid DB re-reads. */
  personaId: string;
  projectId?: string;
  startedAt: number;
  /**
   * Rolling tail buffer — only the most recent ~64 KB of output lines.
   * Full output is persisted to DB and streamed to Kafka.
   */
  output: string[];
  /** Running total of bytes in the tail buffer. */
  outputBytes: number;
  /** Total number of output lines received (including evicted lines). */
  totalOutputLines: number;
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
  /** Latest structured progress snapshot from the worker. */
  progress?: {
    stage: string;
    percentEstimate: number | null;
    activeTool: string | null;
    message: string;
    toolCallsCompleted: number;
    stageStartedAt: number;
  };
  /** Pending human review requests for this execution. */
  pendingReviews?: ReviewRequest[];
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

/**
 * Lightweight cache entry for recently-completed executions.
 * Holds only scalar final state — no output buffer — so polling can read
 * terminal status without hitting the DB, while avoiding the memory cost
 * of retaining the full ActiveExecution (which includes the 64 KB tail buffer).
 */
interface CompletedExecution {
  workerId: string;
  projectId?: string;
  startedAt: number;
  status: 'completed' | 'failed' | 'cancelled';
  completedAt: number;
  exitCode?: number;
  durationMs?: number;
  sessionId?: string;
  totalCostUsd?: number;
}

/**
 * Normalized execution state returned by `Dispatcher.getExecution()`.
 *
 * Discriminated union on `source` so consumers can distinguish between:
 * - **live**: real-time data with rolling tail buffer and progress tracking
 * - **cached**: recently-completed lightweight entry — output was intentionally
 *   evicted to save memory (outputEvicted = true)
 * - **historical**: full historical data reconstructed from the DB — may
 *   include status values like 'queued' that don't exist in the live pipeline
 */
interface ExecutionSnapshotBase {
  workerId: string | null;
  projectId?: string;
  startedAt: number | null;
  output: string[];
  outputBytes: number;
  totalOutputLines: number;
  completedAt?: number;
  exitCode?: number;
  durationMs?: number;
  sessionId?: string;
  totalCostUsd?: number;
}

export interface LiveExecutionSnapshot extends ExecutionSnapshotBase {
  source: 'live';
  workerId: string;
  startedAt: number;
  status: 'running' | 'completed' | 'failed' | 'cancelled';
  /** Output is the real-time rolling tail buffer (~64 KB). */
  outputEvicted: false;
  progress?: ActiveExecution['progress'];
  pendingReviews?: ReviewRequest[];
}

export interface CachedExecutionSnapshot extends ExecutionSnapshotBase {
  source: 'cached';
  workerId: string;
  startedAt: number;
  status: 'completed' | 'failed' | 'cancelled';
  /**
   * Always true — the completed cache intentionally evicts output to save
   * memory. Callers that need output should fall through to the DB.
   */
  outputEvicted: true;
}

export interface HistoricalExecutionSnapshot extends ExecutionSnapshotBase {
  source: 'historical';
  workerId: null;
  /** DB status — may include 'queued' which doesn't exist in live pipeline. */
  status: 'queued' | 'running' | 'completed' | 'failed' | 'cancelled';
  outputEvicted: false;
}

export type ExecutionSnapshot =
  | LiveExecutionSnapshot
  | CachedExecutionSnapshot
  | HistoricalExecutionSnapshot;

/**
 * Simple bounded LRU cache backed by a Map (which preserves insertion order).
 * On `get`, the entry is promoted to most-recent. On `set`, the oldest entry
 * is evicted when the cache exceeds `maxSize`.
 */
class LRUCache<K, V> {
  private map = new Map<K, V>();
  constructor(private maxSize: number) {}

  get(key: K): V | undefined {
    const value = this.map.get(key);
    if (value === undefined) return undefined;
    // Promote to most-recent
    this.map.delete(key);
    this.map.set(key, value);
    return value;
  }

  set(key: K, value: V): void {
    // If key already exists, delete first so it moves to the end
    if (this.map.has(key)) this.map.delete(key);
    this.map.set(key, value);
    // Evict oldest entries if over capacity
    while (this.map.size > this.maxSize) {
      const oldest = this.map.keys().next().value!;
      this.map.delete(oldest);
    }
  }

  has(key: K): boolean {
    return this.map.has(key);
  }

  delete(key: K): boolean {
    return this.map.delete(key);
  }

  clear(): void {
    this.map.clear();
  }
}

// ---------------------------------------------------------------------------
// ExecutionSink — decouples output/lifecycle persistence from the Dispatcher.
// Each sink handles its own errors independently, eliminating the scattered
// try/catch blocks that previously polluted every event handler.
// ---------------------------------------------------------------------------

/** Lifecycle event payload for terminal state transitions. */
interface LifecyclePayload {
  executionId: string;
  status: string;
  exitCode?: number;
  durationMs?: number;
  sessionId?: string;
  totalCostUsd?: number;
  error?: string;
}

/**
 * Abstraction over the triple-write pattern (DB + Kafka + Metrics).
 * Each method represents a single concern; CompositeSink fans out calls
 * to all registered sinks with independent error isolation.
 */
export interface ExecutionSink {
  /** Persist an output chunk (stdout or stderr line). */
  onOutput(executionId: string, dbLine: string, projectId: string | undefined, kafkaPayload: { executionId: string; chunk: string; timestamp: number }): void;
  /** Record a terminal lifecycle event (completion, failure, disconnect). */
  onLifecycle(projectId: string | undefined, payload: LifecyclePayload): void;
  /** Persist execution completion to DB (status, duration, cost, session). */
  onComplete(executionId: string, status: DbExecutionStatus, msg: { durationMs: number; sessionId?: string; totalCostUsd?: number }): void;
  /** Persist execution failure to DB (status + error message). */
  onFailed(executionId: string, errorMessage: string): void;
  /** Record completion metrics (status, duration, cost, persona). */
  onMetrics(executionId: string, status: DbExecutionStatus, durationMs: number, totalCostUsd: number, personaId?: string): void;
  /** Revert a prematurely-failed execution back to running after worker reconnect. */
  onReclaimed(executionId: string): void;
}

class DbSink implements ExecutionSink {
  constructor(
    private getDatabase: () => Database.Database | null,
    private logger: Logger,
  ) {}

  onOutput(executionId: string, dbLine: string): void {
    const database = this.getDatabase();
    if (!database) return;
    try {
      db.appendOutput(database, executionId, dbLine);
    } catch {
      // DB output errors are best-effort — counted by MetricsSink
    }
  }

  onLifecycle(): void {
    // Lifecycle events are Kafka-only; DB persistence is handled by onComplete/onFailed
  }

  onComplete(executionId: string, status: DbExecutionStatus, msg: { durationMs: number; sessionId?: string; totalCostUsd?: number }): void {
    const database = this.getDatabase();
    if (!database) return;
    try {
      db.updateExecution(database, executionId, {
        status,
        completedAt: new Date().toISOString(),
        durationMs: msg.durationMs,
        claudeSessionId: msg.sessionId,
        costUsd: msg.totalCostUsd ?? 0,
      });
      // Resolve trigger firing outcome if this execution was trigger-sourced
      if (status === 'completed' || status === 'failed') {
        try {
          db.resolveTriggerFiring(database, executionId, {
            status: status as 'completed' | 'failed',
            costUsd: msg.totalCostUsd,
            durationMs: msg.durationMs,
          });
        } catch { /* best effort — firing may not exist for non-trigger executions */ }
      }
      // Track cost against deployment budget (if this persona has a deployment)
      if (msg.totalCostUsd && msg.totalCostUsd > 0) {
        try {
          const exec = db.getExecution(database, executionId);
          if (exec) {
            const deployment = db.getDeploymentByPersona(database, exec.personaId);
            if (deployment) {
              db.addDeploymentCost(database, deployment.id, msg.totalCostUsd);
            }
          }
        } catch { /* best effort — budget tracking is non-critical */ }
      }
    } catch (err) {
      this.logger.error({ err, executionId }, 'Failed to persist execution completion');
    }
  }

  onFailed(executionId: string, errorMessage: string): void {
    const database = this.getDatabase();
    if (!database) return;
    try {
      db.updateExecution(database, executionId, {
        status: 'failed',
        errorMessage,
        completedAt: new Date().toISOString(),
      });
      db.resolveTriggerFiring(database, executionId, {
        status: 'failed',
        errorMessage,
      });
    } catch { /* best effort */ }
  }

  onMetrics(): void {
    // Metrics are handled by MetricsSink
  }

  onReclaimed(executionId: string): void {
    const database = this.getDatabase();
    if (!database) return;
    try {
      db.updateExecution(database, executionId, {
        status: 'running',
        errorMessage: null,
        completedAt: null,
      });
    } catch { /* best effort */ }
  }
}

class KafkaSink implements ExecutionSink {
  constructor(
    private kafka: KafkaClient,
    private logger: Logger,
  ) {}

  onOutput(_executionId: string, _dbLine: string, projectId: string | undefined, kafkaPayload: { executionId: string; chunk: string; timestamp: number }): void {
    const outputTopic = projectTopic(TOPICS.EXEC_OUTPUT, projectId);
    this.kafka.produce(outputTopic, JSON.stringify(kafkaPayload), kafkaPayload.executionId).catch(err => {
      this.logger.error({ err, executionId: kafkaPayload.executionId }, 'Failed to produce output to Kafka');
    });
  }

  onLifecycle(projectId: string | undefined, payload: LifecyclePayload): void {
    const topic = projectTopic(TOPICS.EXEC_LIFECYCLE, projectId);
    this.kafka.produce(topic, JSON.stringify(payload), payload.executionId).catch(err => {
      this.logger.error({ err, executionId: payload.executionId }, 'Failed to produce lifecycle to Kafka');
    });
  }

  onComplete(): void {
    // DB-only operation
  }

  onFailed(): void {
    // DB-only operation
  }

  onMetrics(): void {
    // Handled by MetricsSink
  }

  onReclaimed(): void {
    // DB-only operation
  }
}

class MetricsSink implements ExecutionSink {
  constructor(private metrics: OrchestratorMetrics) {}

  onOutput(): void {
    // Output metrics (error counts) are tracked via Promise.allSettled in CompositeSink
  }

  onLifecycle(): void {}
  onComplete(): void {}
  onFailed(): void {}
  onReclaimed(): void {}

  onMetrics(executionId: string, status: DbExecutionStatus, durationMs: number, totalCostUsd: number, personaId?: string): void {
    this.metrics.recordCompletion(executionId, status, durationMs, totalCostUsd, personaId);
  }
}

/**
 * Fans out sink calls to all registered sinks with independent error isolation.
 * Each sink catches its own errors without polluting the others.
 */
class CompositeSink implements ExecutionSink {
  constructor(
    private sinks: ExecutionSink[],
    private metrics: OrchestratorMetrics,
  ) {}

  onOutput(executionId: string, dbLine: string, projectId: string | undefined, kafkaPayload: { executionId: string; chunk: string; timestamp: number }): void {
    // Fan out to all sinks — DB is sync (wrapped in try/catch inside DbSink),
    // Kafka is async (fire-and-forget with .catch inside KafkaSink).
    // Track per-sink error metrics for observability.
    for (const sink of this.sinks) {
      try {
        sink.onOutput(executionId, dbLine, projectId, kafkaPayload);
      } catch {
        this.metrics.recordOutputDbError();
      }
    }
  }

  onLifecycle(projectId: string | undefined, payload: LifecyclePayload): void {
    for (const sink of this.sinks) {
      try { sink.onLifecycle(projectId, payload); } catch { /* isolated */ }
    }
  }

  onComplete(executionId: string, status: DbExecutionStatus, msg: { durationMs: number; sessionId?: string; totalCostUsd?: number }): void {
    for (const sink of this.sinks) {
      try { sink.onComplete(executionId, status, msg); } catch { /* isolated */ }
    }
  }

  onFailed(executionId: string, errorMessage: string): void {
    for (const sink of this.sinks) {
      try { sink.onFailed(executionId, errorMessage); } catch { /* isolated */ }
    }
  }

  onMetrics(executionId: string, status: DbExecutionStatus, durationMs: number, totalCostUsd: number, personaId?: string): void {
    for (const sink of this.sinks) {
      try { sink.onMetrics(executionId, status, durationMs, totalCostUsd, personaId); } catch { /* isolated */ }
    }
  }

  onReclaimed(executionId: string): void {
    for (const sink of this.sinks) {
      try { sink.onReclaimed(executionId); } catch { /* isolated */ }
    }
  }
}

// ---------------------------------------------------------------------------
// PersonaResolver — separates "what to run" from "where to run it".
// Decouples credential decryption, model profile parsing, and prompt assembly
// from the dispatch path, making persona configuration logic unit-testable
// without standing up a full Dispatcher.
// ---------------------------------------------------------------------------

/** Fully-resolved persona execution configuration returned by PersonaResolver. */
export interface ResolvedPersona {
  prompt: string;
  env: Record<string, string>;
  permPolicy: PermissionPolicy | null;
}

/**
 * Resolves a persona ID + exec request into the complete execution configuration:
 * assembled prompt, environment variables (including decrypted credentials and
 * provider-specific overrides), and the parsed permission policy.
 *
 * This is a pure "what to run" concern — the Dispatcher handles the "where to run"
 * concern (worker selection, queue management, retry logic).
 */
export class PersonaResolver {
  constructor(
    private getDatabase: () => Database.Database | null,
    private getMasterKey: () => string | null,
    private logger: Logger,
  ) {}

  /**
   * Resolve a persona into the full execution configuration.
   *
   * Returns the resolved config, or `{ error }` if the persona can't be resolved
   * (e.g. persona deleted). The caller decides how to surface the failure.
   */
  resolve(
    request: ExecRequest,
    claudeToken: string,
  ): ResolvedPersona | { error: string } {
    const env: Record<string, string> = {
      ANTHROPIC_API_KEY: claudeToken,
    };

    let prompt = request.prompt;
    let permPolicy: PermissionPolicy | null = null;

    const database = this.getDatabase();
    if (!database) {
      return { prompt, env, permPolicy };
    }

    const persona = db.getPersona(database, request.personaId);
    if (!persona) {
      return { error: `Persona ${request.personaId} not found — it may have been deleted` };
    }

    permPolicy = parsePermissionPolicy(persona.permissionPolicy);
    const tools = db.getToolsForPersona(database, persona.id);
    const creds = db.listCredentialsForPersona(database, persona.id);

    // Decrypt credentials and inject as env vars
    const credentialHints: string[] = [];
    const masterKeyHex = this.getMasterKey();
    for (const cred of creds) {
      const envName = `CONNECTOR_${cred.name.toUpperCase().replace(/[^A-Z0-9]/g, '_')}`;
      credentialHints.push(envName);

      if (masterKeyHex) {
        try {
          const credSalt = cred.salt ? Buffer.from(cred.salt, 'hex') : undefined;
          const { key } = deriveMasterKey(masterKeyHex, credSalt, cred.iter);
          const payload: EncryptedPayload = {
            encrypted: cred.encryptedData,
            iv: cred.iv,
            tag: cred.tag,
            salt: cred.salt,
            iter: cred.iter,
          };
          const decrypted = decrypt(payload, key);

          // Try to parse as JSON with multiple fields
          try {
            const fields = JSON.parse(decrypted) as Record<string, string>;
            for (const [fieldKey, fieldValue] of Object.entries(fields)) {
              const fieldEnvName = `CONNECTOR_${cred.name.toUpperCase().replace(/[^A-Z0-9]/g, '_')}_${fieldKey.toUpperCase().replace(/[^A-Z0-9]/g, '_')}`;
              env[fieldEnvName] = String(fieldValue);
            }
          } catch {
            // Not JSON — inject as single value
            env[envName] = decrypted;
          }
        } catch (err) {
          this.logger.error({ err, credentialId: cred.id, credentialName: cred.name }, 'Failed to decrypt credential');
        }
      }
    }

    const inputData = request.inputData ? (request.inputData as Record<string, unknown>) : undefined;
    prompt = assemblePrompt(persona, tools, inputData, credentialHints.length > 0 ? credentialHints : undefined);

    // Apply model profile env overrides (strategy pattern per provider)
    const profile = parseModelProfile(persona.modelProfile);
    if (profile?.provider) {
      switch (profile.provider) {
        case 'ollama':
          if (profile.baseUrl) env['OLLAMA_BASE_URL'] = profile.baseUrl;
          if (profile.authToken) env['OLLAMA_API_KEY'] = profile.authToken;
          delete env['ANTHROPIC_API_KEY'];
          break;
        case 'litellm':
          if (profile.baseUrl) env['ANTHROPIC_BASE_URL'] = profile.baseUrl;
          if (profile.authToken) env['ANTHROPIC_AUTH_TOKEN'] = profile.authToken;
          delete env['ANTHROPIC_API_KEY'];
          break;
        case 'custom':
          if (profile.baseUrl) env['OPENAI_BASE_URL'] = profile.baseUrl;
          if (profile.authToken) env['OPENAI_API_KEY'] = profile.authToken;
          delete env['ANTHROPIC_API_KEY'];
          break;
      }
    }

    this.logger.info({ personaId: persona.id, toolCount: tools.length, credCount: creds.length }, 'Assembled prompt from DB persona');

    return { prompt, env, permPolicy };
  }
}

// ---------------------------------------------------------------------------
// ExecutionLifecycle — owns the execution state machine and in-memory maps.
// State transitions: queued → running → completed/failed/cancelled
// Encapsulates active/completed maps, tail buffer, progress, and review tracking.
// Each transition method is independently testable without mocking WebSocket events.
// ---------------------------------------------------------------------------

/**
 * Manages the in-memory execution state machine.
 *
 * The four core transitions (stdout, stderr, complete, disconnect) are expressed
 * as methods rather than event handler callbacks, making each independently
 * testable and revealing the underlying state machine structure.
 */
class ExecutionLifecycle {
  private active = new Map<string, ActiveExecution>();
  private completed = new LRUCache<string, CompletedExecution>(COMPLETED_CACHE_MAX_SIZE);

  /** Start tracking an execution in the active map. */
  start(executionId: string, workerId: string, personaId: string, projectId?: string): void {
    this.active.set(executionId, {
      workerId,
      personaId,
      projectId,
      startedAt: Date.now(),
      output: [],
      outputBytes: 0,
      totalOutputLines: 0,
      status: 'running',
    });
  }

  /** Get an active execution, or undefined if not found / already completed. */
  getActive(executionId: string): ActiveExecution | undefined {
    return this.active.get(executionId);
  }

  /** Get a completed execution from the cache, or undefined. */
  getCompleted(executionId: string): CompletedExecution | undefined {
    return this.completed.get(executionId);
  }

  /** Remove an execution from the active map (e.g. failed assignment). */
  removeActive(executionId: string): void {
    this.active.delete(executionId);
  }

  /** Process a stdout chunk — append to tail buffer. */
  stdout(executionId: string, chunk: string): ActiveExecution | undefined {
    const exec = this.active.get(executionId);
    if (exec) this.appendToTailBuffer(exec, chunk);
    return exec;
  }

  /** Process a stderr chunk — append to tail buffer with [STDERR] prefix. */
  stderr(executionId: string, chunk: string): { exec: ActiveExecution | undefined; line: string } {
    const exec = this.active.get(executionId);
    const line = `[STDERR] ${chunk}`;
    if (exec) this.appendToTailBuffer(exec, line);
    return { exec, line };
  }

  /**
   * Transition: running → completed/failed/cancelled.
   * Moves the execution from active → completed cache (freeing the output buffer).
   * Returns the former active execution (if any) and the canonical DB status.
   */
  complete(executionId: string, msg: ExecComplete): { exec: ActiveExecution | undefined; status: DbExecutionStatus } {
    const exec = this.active.get(executionId);
    const status = toDbStatus(msg.status);

    if (exec) {
      const now = Date.now();
      this.active.delete(executionId);
      this.completed.set(executionId, {
        workerId: exec.workerId,
        projectId: exec.projectId,
        startedAt: exec.startedAt,
        status,
        completedAt: now,
        exitCode: msg.exitCode,
        durationMs: msg.durationMs,
        sessionId: msg.sessionId,
        totalCostUsd: msg.totalCostUsd,
      });
    }

    return { exec, status };
  }

  /** Transition: running → failed (worker disconnect). Returns the former active execution. */
  disconnect(executionId: string): ActiveExecution | undefined {
    const exec = this.active.get(executionId);
    if (exec) {
      const now = Date.now();
      this.active.delete(executionId);
      this.completed.set(executionId, {
        workerId: exec.workerId,
        projectId: exec.projectId,
        startedAt: exec.startedAt,
        status: 'failed',
        completedAt: now,
      });
    }
    return exec;
  }

  /**
   * Transition: failed (completed cache) → running (active).
   * Used when a reconnected worker re-reports executions that were prematurely
   * marked as failed during disconnect.
   * Returns true if the execution was successfully reclaimed.
   */
  reclaim(executionId: string, workerId: string): boolean {
    const cached = this.completed.get(executionId);
    if (!cached || cached.status !== 'failed') return false;

    this.completed.delete(executionId);
    this.active.set(executionId, {
      workerId,
      personaId: '',  // Not available from cached entry; execution will complete normally
      projectId: cached.projectId,
      startedAt: cached.startedAt,
      output: [],
      outputBytes: 0,
      totalOutputLines: 0,
      status: 'running',
    });
    return true;
  }

  /** Update structured progress for an active execution. */
  updateProgress(executionId: string, msg: ExecProgress): void {
    const exec = this.active.get(executionId);
    if (exec) {
      exec.progress = {
        stage: msg.stage,
        percentEstimate: msg.percentEstimate,
        activeTool: msg.activeTool,
        message: msg.message,
        toolCallsCompleted: msg.toolCallsCompleted,
        stageStartedAt: msg.timestamp,
      };
    }
  }

  /** Register a review request — pauses the execution until human responds. */
  addReview(executionId: string, msg: WorkerReviewRequest): ReviewRequest | null {
    const exec = this.active.get(executionId);
    if (!exec) return null;

    const review: ReviewRequest = {
      reviewId: msg.reviewId,
      executionId: msg.executionId,
      personaId: exec.personaId,
      projectId: exec.projectId,
      payload: msg.payload,
      status: 'pending',
      createdAt: msg.timestamp,
      resolvedAt: null,
      responseMessage: null,
    };

    if (!exec.pendingReviews) exec.pendingReviews = [];
    exec.pendingReviews.push(review);

    exec.progress = {
      stage: 'paused_for_review',
      percentEstimate: null,
      activeTool: null,
      message: 'Execution paused — awaiting human review',
      toolCallsCompleted: exec.progress?.toolCallsCompleted ?? 0,
      stageStartedAt: msg.timestamp,
    };

    return review;
  }

  /** Iterate over all active executions. */
  activeEntries(): IterableIterator<[string, ActiveExecution]> {
    return this.active.entries();
  }

  /** Iterate over all active execution values. */
  activeValues(): IterableIterator<ActiveExecution> {
    return this.active.values();
  }

  /** Number of running executions in the active map. */
  runningCount(): number {
    let count = 0;
    for (const e of this.active.values()) {
      if (e.status === 'running') count++;
    }
    return count;
  }

  /** Clear all in-memory state (used during shutdown). */
  clear(): void {
    this.active.clear();
    this.completed.clear();
  }

  /**
   * Append a line to the rolling tail buffer. Evicts oldest lines when the
   * buffer exceeds TAIL_BUFFER_BYTES, keeping only the most recent output.
   */
  private appendToTailBuffer(exec: ActiveExecution, line: string): void {
    const lineBytes = Buffer.byteLength(line, 'utf8');
    exec.output.push(line);
    exec.outputBytes += lineBytes;
    exec.totalOutputLines++;

    while (exec.outputBytes > TAIL_BUFFER_BYTES && exec.output.length > 1) {
      const evicted = exec.output.shift()!;
      exec.outputBytes -= Buffer.byteLength(evicted, 'utf8');
    }
  }
}

export class Dispatcher {
  private queue = new ExecutionQueue();
  /** Execution state machine — owns active/completed maps, tail buffer, progress, reviews. */
  private readonly lifecycle = new ExecutionLifecycle();
  /**
   * Soft affinity map: personaId → workerId that last ran this persona.
   * Used for best-effort routing to reuse warm Claude sessions and cached
   * project context. Falls through to least-loaded when preferred worker
   * is unavailable or at capacity.
   */
  private personaAffinity = new LRUCache<string, string>(500);
  private database: Database.Database | null = null;
  private masterKeyHex: string | null = null;
  /** Guard against concurrent processQueue() runs which can trigger parallel OAuth refreshes. */
  private processing = false;
  /** Optional callback invoked when any execution completes (for event processor re-trigger). */
  private executionCompleteCallback: (() => void) | null = null;
  /** When true, new submissions are rejected and the queue is draining for shutdown. */
  private draining = false;
  /** Orchestrator-level metrics collector. */
  readonly metrics = new OrchestratorMetrics();
  /** Periodic timer for queue/worker snapshot updates. */
  private metricsTimer: ReturnType<typeof setInterval>;
  /** Composite sink for DB + Kafka + Metrics fan-out with isolated error handling. */
  private readonly sink: CompositeSink;
  /** Resolves persona config (credentials, prompt, env) independently from dispatch routing. */
  private readonly resolver: PersonaResolver;

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

    // Build the composite sink — each sink handles its own errors independently.
    this.sink = new CompositeSink(
      [
        new DbSink(() => this.database, this.logger),
        new KafkaSink(this.kafka, this.logger),
        new MetricsSink(this.metrics),
      ],
      this.metrics,
    );

    // --- Wire pool events to lifecycle transitions + sink fan-out ---

    // When a worker becomes ready, try to dispatch queued work
    this.pool.on('worker-ready', () => this.processQueue());
    this.pool.on('worker-connected', () => this.processQueue());

    // Track stdout for execution output
    this.pool.on('stdout', (_workerId: string, msg: ExecStdout) => {
      this.handleOutputChunk(msg.executionId, msg.chunk, msg.timestamp);
    });

    // Track stderr for debugging failed executions
    this.pool.on('stderr', (_workerId: string, msg: ExecStderr) => {
      const exec = this.active.get(msg.executionId);
      const line = '[STDERR] ' + msg.chunk;
      this.logger.warn({ executionId: msg.executionId, stderr: msg.chunk }, 'Execution stderr');
      this.sink.onOutput(msg.executionId, line + '\n', exec?.projectId, {
        executionId: msg.executionId, chunk: msg.chunk, timestamp: msg.timestamp,
      });
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

    // Forward persona events to Kafka (project-scoped)
    this.pool.on('persona-event', (_workerId: string, msg: WorkerEvent) => {
      const exec = this.active.get(msg.executionId);
      const eventTopic = projectTopic(TOPICS.EVENTS, exec?.projectId);
      this.kafka.produce(eventTopic, JSON.stringify({
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
        this.queue.unshift({ request, receivedAt: Date.now(), retryCount: 0, nextRetryAt: 0 });
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
            retryCount: 0,
            nextRetryAt: 0,
          });
          this.processQueue();
        }
      }
    });

    // Handle review requests from workers
    this.pool.on('review-request', (_workerId: string, msg: WorkerReviewRequest) => {
      const exec = this.active.get(msg.executionId);
      if (exec) {
        if (!exec.pendingReviews) exec.pendingReviews = [];
        exec.pendingReviews.push(msg as unknown as ReviewRequest);
        this.logger.info({ executionId: msg.executionId, reviewId: msg.reviewId }, 'Execution paused for human review');
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

          this.queue.unshift({ request, receivedAt: Date.now(), retryCount: 0, nextRetryAt: 0 });
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

    this.pool.on('worker-disconnected', (_workerId, executionId) => {
      if (executionId) {
        this.logger.error({ executionId }, 'Worker disconnected during execution');
        const exec = this.lifecycle.disconnect(executionId);
        this.sink.onFailed(executionId, 'Worker disconnected');
        this.sink.onLifecycle(exec?.projectId, {
          executionId, status: 'failed', error: 'Worker disconnected', durationMs: 0,
        });
      }
    });

    this.pool.on('executions-reclaimed', (workerId, msg) => {
      for (const executionId of msg.executionIds) {
        if (this.lifecycle.reclaim(executionId, workerId)) {
          this.sink.onReclaimed(executionId);
          this.logger.info({ executionId, workerId }, 'Execution reclaimed after worker reconnect — revived from failed state');
        } else {
          const cached = this.lifecycle.getCompleted(executionId);
          this.logger.warn({ executionId, workerId, hasCached: !!cached }, 'Worker reclaimed execution that was not in failed-completed state — ignoring');
        }
      }
    });

    // Periodically update queue/worker snapshots for metrics (every 5s)
    this.metricsTimer = setInterval(() => {
      const counts = this.pool.getWorkerCount();
      this.metrics.updateSnapshots(this.queue.length, counts.totalSlots, counts.availableSlots);
    }, 5_000);
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

  /**
   * Recover queued executions from the database after a restart/crash.
   * Loads executions in 'queued' status and populates the in-memory queue.
   */
  recoverQueue(): void {
    // Recovery is handled per-tenant through tenantDbManager
    if (!this.tenantDbManager) return;
    const tenantIds = this.tenantDbManager.listTenantIds();
    let totalRecovered = 0;
    for (const tenantId of tenantIds) {
      try {
        const tenantDb = this.tenantDbManager.getTenantDb(tenantId);
        const queued = db.loadQueuedExecutions(tenantDb);
        for (const item of queued) {
          try {
            const request: ExecRequest = JSON.parse(item.queueData);
            this.queue.push({ request, receivedAt: Date.now(), retryCount: item.retryCount, nextRetryAt: 0 });
            this.executionTenant.set(request.executionId, tenantId);
            totalRecovered++;
          } catch {
            this.logger.error({ executionId: item.id, tenantId }, 'Failed to parse queue_data during recovery');
            try { db.updateExecution(tenantDb, item.id, { status: 'failed', errorMessage: 'Recovery failed — corrupt queue data', completedAt: new Date().toISOString() }); } catch { /* best effort */ }
          }
        }
      } catch (err) {
        this.logger.error({ err, tenantId }, 'Failed to recover queued executions for tenant');
      }
    }
    if (totalRecovered > 0) {
      this.logger.info({ recovered: totalRecovered }, 'Recovered queued executions from database');
      this.processQueue();
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
        // Store the full ExecRequest so we can recover on restart
        db.storeQueueData(submitDb, request.executionId, JSON.stringify(request));
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

    this.queue.push({ request, receivedAt: Date.now(), retryCount: 0, nextRetryAt: 0 });
    this.incrementTenantCount(projectId);
    this.checkThresholds();
    this.processQueue();
    return { accepted: true };
  }

  /**
   * Drain the queue by dispatching as many items as possible in a single
   * invocation.  The OAuth token is resolved once at the top of each batch;
   * all dispatches within the same batch share that token.  This avoids the
   * N-microtask-bounce overhead of the previous one-item-per-tick design
   * while still serialising against concurrent OAuth refreshes via the
   * `processing` flag.
   */
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
      this.queue.unshift({ request, receivedAt: Date.now(), retryCount: 0, nextRetryAt: 0 });
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
      this.queue.unshift({ request, receivedAt: Date.now(), retryCount: 0, nextRetryAt: 0 });
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

  /**
   * Re-queue an execution with exponential backoff, or permanently fail it
   * after MAX_REQUEUE_RETRIES attempts.  Prevents infinite re-queue loops
   * when a transient resource (e.g. Claude token) is persistently unavailable.
   */
  private requeueOrFail(item: QueuedExecution, reason: string): void {
    item.retryCount += 1;
    this.metrics.recordExecutionRetry();

    if (item.retryCount > MAX_REQUEUE_RETRIES) {
      // Permanently fail
      const { request } = item;
      const errorMessage = `Dispatch failed after ${item.retryCount - 1} retries: ${reason}`;
      this.logger.error(
        { executionId: request.executionId, retryCount: item.retryCount, reason },
        'Execution exceeded max re-queue retries — marking as failed',
      );

      this.sink.onFailed(request.executionId, errorMessage);
      // Also clear queue_data since we're permanently failing
      if (this.database) {
        try { db.clearQueueData(this.database, request.executionId); } catch { /* best effort */ }
      }
      this.sink.onLifecycle(request.projectId, {
        executionId: request.executionId,
        status: 'failed',
        error: errorMessage,
        durationMs: 0,
      });
      return;
    }

    // Exponential backoff: 5s, 10s, 20s, 40s, 80s
    const delayMs = REQUEUE_BASE_DELAY_MS * Math.pow(2, item.retryCount - 1);
    item.nextRetryAt = Date.now() + delayMs;

    this.logger.warn(
      { executionId: item.request.executionId, retryCount: item.retryCount, delayMs, reason },
      'Re-queuing execution with backoff',
    );

    // Persist retry count to DB so recovery respects it
    if (this.database) {
      try {
        db.updateExecution(this.database, item.request.executionId, { retryCount: item.retryCount });
      } catch { /* best effort */ }
    }

    this.queue.pushBackoff(item);
    this.scheduleBackoffRetry(delayMs);
  }

  /** Schedule processQueue() to fire after a backoff delay so items are retried on time. */
  private scheduleBackoffRetry(delayMs: number): void {
    setTimeout(() => {
      this.processQueue();
    }, delayMs);
  }

  cancelExecution(executionId: string): boolean {
    const exec = this.lifecycle.getActive(executionId);
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
    return Array.from(this.lifecycle.activeEntries())
      .filter(([, e]) => e.status === 'running')
      .map(([executionId, e]) => ({
        executionId,
        workerId: e.workerId,
        startedAt: e.startedAt,
      }));
  }

  /**
   * Get execution state as a normalized `ExecutionSnapshot`.
   * Tries in-memory first (for real-time output), then falls back to DB
   * for completed/historical executions.  The `source` field tells callers
   * whether data is real-time, from the short-lived completion cache, or
   * reconstructed from the database.
   */
  getExecution(executionId: string): ExecutionSnapshot | null {
    // Check in-memory active executions first
    const active = this.lifecycle.getActive(executionId);
    if (active) {
      return {
        source: 'live',
        outputEvicted: false,
        workerId: active.workerId,
        projectId: active.projectId,
        startedAt: active.startedAt,
        output: active.output,
        outputBytes: active.outputBytes,
        totalOutputLines: active.totalOutputLines,
        status: active.status,
        completedAt: active.completedAt,
        exitCode: active.exitCode,
        durationMs: active.durationMs,
        sessionId: active.sessionId,
        totalCostUsd: active.totalCostUsd,
        progress: active.progress,
        pendingReviews: active.pendingReviews,
      };
    }

    // Check lightweight completed cache (avoids DB round-trip for recent completions).
    // Output is intentionally empty — the cache trades output fidelity for low memory.
    // Callers that need output should check `outputEvicted` and fall through to DB.
    const done = this.lifecycle.getCompleted(executionId);
    if (done) {
      return {
        source: 'cached',
        outputEvicted: true,
        workerId: done.workerId,
        projectId: done.projectId,
        startedAt: done.startedAt,
        output: [],
        outputBytes: 0,
        totalOutputLines: 0,
        status: done.status,
        completedAt: done.completedAt,
        exitCode: done.exitCode,
        durationMs: done.durationMs,
        sessionId: done.sessionId,
        totalCostUsd: done.totalCostUsd,
      };
    }

    const execDb = this.resolveDbForExecution(executionId);
    if (execDb) {
      const dbExec = db.getExecution(execDb, executionId);
      if (dbExec) {
        const output = dbExec.outputData ? dbExec.outputData.split('\n').filter(Boolean) : [];
        // Validate DB status against the known set instead of an unsafe cast.
        const validStatuses = new Set(['queued', 'running', 'completed', 'failed', 'cancelled']);
        const status: HistoricalExecutionSnapshot['status'] = validStatuses.has(dbExec.status)
          ? dbExec.status as HistoricalExecutionSnapshot['status']
          : 'failed';
        return {
          source: 'historical',
          outputEvicted: false,
          workerId: null,
          startedAt: dbExec.startedAt ? new Date(dbExec.startedAt).getTime() : null,
          output,
          outputBytes: Buffer.byteLength(dbExec.outputData ?? '', 'utf8'),
          totalOutputLines: output.length,
          status,
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
    if (!exec) return null;
    // If output was evicted (cached source), fall through to DB for real data.
    if (exec.outputEvicted && this.database) {
      const dbExec = db.getExecution(this.database, executionId);
      if (dbExec?.outputData) {
        return dbExec.outputData.split('\n').filter(Boolean);
      }
      return [];
    }
    return exec.output;
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

  /** Get pending review requests for a specific execution. */
  getReviewsForExecution(executionId: string): ReviewRequest[] {
    const exec = this.active.get(executionId);
    return exec?.pendingReviews ?? [];
  }

  /** Get a specific review request by ID. */
  getReview(executionId: string, reviewId: string): ReviewRequest | null {
    const exec = this.active.get(executionId);
    if (!exec?.pendingReviews) return null;
    return exec.pendingReviews.find(r => r.reviewId === reviewId) ?? null;
  }

  /**
   * Respond to a pending human review. Sends the decision to the worker
   * to resume the paused execution.
   */
  respondToReview(executionId: string, reviewId: string, decision: 'approved' | 'rejected', message: string, projectId?: string): boolean {
    const exec = this.active.get(executionId);
    if (!exec?.pendingReviews) return false;
    if (projectId && exec.projectId !== projectId) return false;

    const review = exec.pendingReviews.find(r => r.reviewId === reviewId);
    if (!review || review.status !== 'pending') return false;

    // Update review status
    review.status = decision;
    review.resolvedAt = Date.now();
    review.responseMessage = message;

    // Send review response to worker
    const sent = this.pool.send(exec.workerId, {
      type: 'review_response',
      executionId,
      reviewId,
      decision,
      message,
    });

    if (sent) {
      this.logger.info({ executionId, reviewId, decision }, 'Review response sent to worker');
    } else {
      this.logger.error({ executionId, reviewId }, 'Failed to send review response to worker');
      return false;
    }

    return true;
  }

  /** Get all pending reviews, optionally scoped to a project. */
  getAllPendingReviews(projectId?: string): ReviewRequest[] {
    const reviews: ReviewRequest[] = [];
    for (const [, exec] of this.active) {
      if (projectId && exec.projectId !== projectId) continue;
      if (exec.pendingReviews) {
        for (const r of exec.pendingReviews) {
          if (r.status === 'pending') reviews.push(r);
        }
      }
    }
    return reviews;
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
