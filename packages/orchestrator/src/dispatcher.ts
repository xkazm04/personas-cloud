import { type ExecAssign, type ExecRequest, type ExecStdout, type ExecStderr, type ExecComplete, type ExecProgress, type WorkerEvent, type WorkerReviewRequest, type PermissionPolicy, type ReviewRequest, type WorkerInfo, TOPICS, projectTopic, assemblePrompt, parseModelProfile, parsePermissionPolicy, decrypt, deriveMasterKey, type EncryptedPayload } from '@dac-cloud/shared';
import { OrchestratorMetrics } from './metrics.js';
import type Database from 'better-sqlite3';
import type { WorkerPool } from './workerPool.js';
import type { TokenManager } from './tokenManager.js';
import type { OAuthManager } from './oauth.js';
import type { KafkaClient } from './kafka.js';
import type { Logger } from 'pino';
import * as db from './db.js';

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
  completedAt?: number;
  exitCode?: number;
  durationMs?: number;
  sessionId?: string;
  totalCostUsd?: number;
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
  ) {
    // Build the persona resolver — separates "what to run" from "where to run".
    this.resolver = new PersonaResolver(
      () => this.database,
      () => this.masterKeyHex,
      this.logger,
    );

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

    this.pool.on('worker-ready', () => this.processQueue());
    this.pool.on('worker-connected', () => this.processQueue());

    this.pool.on('stdout', (_workerId, msg) => {
      const exec = this.lifecycle.stdout(msg.executionId, msg.chunk);
      this.sink.onOutput(msg.executionId, msg.chunk + '\n', exec?.projectId, {
        executionId: msg.executionId, chunk: msg.chunk, timestamp: msg.timestamp,
      });
    });

    this.pool.on('stderr', (_workerId, msg) => {
      const { exec, line } = this.lifecycle.stderr(msg.executionId, msg.chunk);
      this.logger.warn({ executionId: msg.executionId, stderr: msg.chunk }, 'Execution stderr');
      this.sink.onOutput(msg.executionId, line + '\n', exec?.projectId, {
        executionId: msg.executionId, chunk: msg.chunk, timestamp: msg.timestamp,
      });
    });

    this.pool.on('complete', (_workerId, msg) => {
      const { exec, status } = this.lifecycle.complete(msg.executionId, msg);
      this.sink.onLifecycle(exec?.projectId, {
        executionId: msg.executionId, status: msg.status, exitCode: msg.exitCode,
        durationMs: msg.durationMs, sessionId: msg.sessionId, totalCostUsd: msg.totalCostUsd,
      });
      this.sink.onComplete(msg.executionId, status, {
        durationMs: msg.durationMs, sessionId: msg.sessionId, totalCostUsd: msg.totalCostUsd,
      });
      this.sink.onMetrics(msg.executionId, status, msg.durationMs, msg.totalCostUsd ?? 0, exec?.personaId);
      if (this.executionCompleteCallback) {
        try { this.executionCompleteCallback(); } catch { /* best effort */ }
      }
      this.processQueue();
    });

    this.pool.on('persona-event', (_workerId, msg) => {
      const exec = this.lifecycle.getActive(msg.executionId);
      const eventTopic = projectTopic(TOPICS.EVENTS, exec?.projectId);
      this.kafka.produce(eventTopic, JSON.stringify({
        executionId: msg.executionId, eventType: msg.eventType,
        payload: msg.payload, timestamp: Date.now(),
      }), msg.executionId).catch(err => {
        this.logger.error({ err, executionId: msg.executionId }, 'Failed to produce event to Kafka');
      });
    });

    this.pool.on('progress', (_workerId, msg) => this.lifecycle.updateProgress(msg.executionId, msg));

    this.pool.on('review-request', (_workerId, msg) => {
      const review = this.lifecycle.addReview(msg.executionId, msg);
      if (review) {
        this.logger.info({ executionId: msg.executionId, reviewId: msg.reviewId }, 'Execution paused for human review');
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

  setDatabase(database: Database.Database): void {
    this.database = database;
  }

  setMasterKey(masterKeyHex: string): void {
    this.masterKeyHex = masterKeyHex;
  }

  getMasterKey(): string | null {
    return this.masterKeyHex;
  }

  /**
   * Register a callback to be invoked whenever an execution completes.
   * Used by the event processor to re-trigger processing of concurrency-blocked events.
   */
  onExecutionComplete(callback: () => void): void {
    this.executionCompleteCallback = callback;
  }

  /**
   * Recover queued executions from the database after a restart/crash.
   * Loads executions in 'queued' status that have queue_data (serialized ExecRequest)
   * and populates the in-memory queue. Also recovers stale 'running' executions.
   *
   * Must be called after setDatabase().
   */
  recoverQueue(): void {
    if (!this.database) return;

    // First, recover stale running executions (crashed mid-flight)
    const staleResult = db.recoverStaleRunningExecutions(this.database);
    if (staleResult.requeued > 0 || staleResult.failed > 0) {
      this.logger.warn({
        requeued: staleResult.requeued,
        failed: staleResult.failed,
      }, 'Recovered stale running executions from prior crash');
    }

    // Load queued executions from DB into in-memory queue
    const queued = db.loadQueuedExecutions(this.database);
    if (queued.length === 0) return;

    let recovered = 0;
    for (const item of queued) {
      try {
        const request: ExecRequest = JSON.parse(item.queueData);
        this.queue.pushReady({
          request,
          receivedAt: Date.now(),
          retryCount: item.retryCount,
          nextRetryAt: 0,
        });
        recovered++;
      } catch {
        // Invalid queue_data — fail the execution
        this.logger.error({ executionId: item.id }, 'Failed to parse queue_data during recovery — marking as failed');
        try {
          db.updateExecution(this.database!, item.id, {
            status: 'failed',
            errorMessage: 'Recovery failed — corrupt queue data',
            completedAt: new Date().toISOString(),
          });
          db.clearQueueData(this.database!, item.id);
        } catch { /* best effort */ }
      }
    }

    if (recovered > 0) {
      this.logger.info({ recovered }, 'Recovered queued executions from database');
      this.processQueue();
    }
  }

  /**
   * Submit an execution request to the queue.
   * Returns `true` if the request was accepted, `false` if rejected
   * (queue full or shutting down). Callers should return 429 when false.
   */
  submit(request: ExecRequest): boolean {
    if (this.draining) {
      this.logger.warn({ executionId: request.executionId }, 'Submission rejected — dispatcher is draining for shutdown');
      this.metrics.recordRejection();
      return false;
    }
    if (this.queue.length >= MAX_QUEUE_DEPTH) {
      this.logger.warn({
        executionId: request.executionId,
        queueLength: this.queue.length,
        maxQueueDepth: MAX_QUEUE_DEPTH,
      }, 'Submission rejected — queue is full');
      this.metrics.recordRejection();
      return false;
    }

    this.logger.info({
      executionId: request.executionId,
      personaId: request.personaId,
    }, 'Execution submitted');

    // Create DB record and persist queue_data for crash recovery
    if (this.database) {
      try {
        db.createExecution(this.database, {
          id: request.executionId,
          personaId: request.personaId,
          inputData: request.prompt,
          projectId: request.projectId,
        });
        // Store the full ExecRequest so we can recover on restart
        db.storeQueueData(this.database, request.executionId, JSON.stringify(request));
      } catch (err) {
        this.logger.error({ err, executionId: request.executionId }, 'Failed to create execution record');
      }
    }

    this.queue.pushReady({ request, receivedAt: Date.now(), retryCount: 0, nextRetryAt: 0 });
    this.metrics.recordSubmission(request.executionId);
    this.processQueue();
    return true;
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
    // Serialize: if a dispatch is already in-flight (which may be awaiting an
    // OAuth token refresh), skip this invocation.  The in-flight dispatch will
    // re-trigger processQueue() upon completion so no items are stranded.
    if (this.processing) return;

    // Acquire the processing lock BEFORE inspecting or mutating the queue.
    this.processing = true;
    try {
      if (this.queue.length === 0) return;

      // Resolve the Claude token once per batch — the only async step.
      const claudeToken = await this.resolveClaudeToken();

      // Promote any backoff items whose timer has elapsed into the ready queue.
      this.queue.promoteReady();

      // Dispatch in a loop: each iteration dequeues one item and assigns it
      // to an idle worker.  The loop exits when the queue is empty, no idle
      // workers remain, or the token is missing.
      while (this.queue.hasReady()) {
        const item = this.queue.shift();
        if (!item) break;

        if (!claudeToken) {
          this.requeueOrFail(item, 'No Claude token available (OAuth expired or not configured)');
          break;
        }

        // Affinity routing: prefer the worker that last ran this persona
        // for warm session/cache reuse. Fall through to least-loaded on miss.
        const preferredWorkerId = this.personaAffinity.get(item.request.personaId);
        const workerId = (preferredWorkerId && this.pool.getWorkerIfIdle(preferredWorkerId))
          ?? this.pool.getIdleWorker();

        if (!workerId) {
          // No workers available — put the item back and stop the batch
          this.queue.pushReady(item);
          this.logger.debug({ queueLength: this.queue.length }, 'No idle worker, execution queued');
          break;
        }

        if (preferredWorkerId && workerId === preferredWorkerId) {
          this.logger.debug({
            personaId: item.request.personaId,
            workerId,
          }, 'Affinity hit — routing to preferred worker');
        }

        this.dispatchToWorker(workerId, item, claudeToken);
      }
    } finally {
      this.processing = false;
      // If backoff items remain that weren't promotable, a scheduled timer
      // (from pushBackoff) or the next external trigger will re-enter.
    }
  }

  /**
   * Resolve a Claude API token, trying OAuth first (with auto-refresh)
   * then falling back to the stored token. Called once per processQueue
   * batch so all dispatches in the same tick share the same token.
   */
  private async resolveClaudeToken(): Promise<string | null> {
    if (this.oauth?.hasTokens()) {
      const token = await this.oauth.getValidAccessToken();
      if (token) {
        this.tokenManager.storeClaudeToken(token);
        return token;
      }
    }
    return this.tokenManager.getClaudeToken();
  }

  private dispatchToWorker(workerId: string, item: QueuedExecution, claudeToken: string): void {
    const { request } = item;

    // Resolve persona configuration (prompt, env, permissions) via PersonaResolver.
    // This separates "what to run" from "where to run it".
    const resolved = this.resolver.resolve(request, claudeToken);

    if ('error' in resolved) {
      this.logger.error({ executionId: request.executionId, personaId: request.personaId }, resolved.error);
      this.sink.onFailed(request.executionId, resolved.error);
      if (this.database) {
        try { db.clearQueueData(this.database, request.executionId); } catch { /* best effort */ }
      }
      this.sink.onLifecycle(request.projectId, {
        executionId: request.executionId,
        status: 'failed',
        error: resolved.error,
        durationMs: 0,
      });
      return;
    }

    const { prompt, env, permPolicy } = resolved;

    const assignment: ExecAssign = {
      type: 'assign',
      executionId: request.executionId,
      personaId: request.personaId,
      prompt,
      env,
      config: {
        timeoutMs: request.config.timeoutMs || 300_000,
        maxOutputBytes: MAX_OUTPUT_BYTES,
      },
      permissionPolicy: permPolicy ?? undefined,
    };

    // Track in-memory for real-time streaming (rolling tail buffer)
    this.lifecycle.start(request.executionId, workerId, request.personaId, request.projectId);

    // Mark running in DB (keep queue_data until assign is confirmed)
    if (this.database) {
      try {
        db.updateExecution(this.database, request.executionId, {
          status: 'running',
          startedAt: new Date().toISOString(),
        });
      } catch { /* best effort */ }
    }

    const sent = this.pool.assign(workerId, assignment);
    if (!sent) {
      this.logger.error({ workerId, executionId: request.executionId }, 'Failed to assign to worker');
      this.lifecycle.removeActive(request.executionId);
      if (this.database) {
        try {
          db.updateExecution(this.database, request.executionId, { status: 'queued' });
        } catch { /* best effort */ }
      }
      // Re-queue with retry tracking
      this.requeueOrFail(item, 'Failed to assign execution to worker');
    } else {
      // Assignment confirmed — now safe to clear queue_data
      if (this.database) {
        try {
          db.clearQueueData(this.database, request.executionId);
        } catch { /* best effort */ }
      }
      this.metrics.recordDispatch(request.executionId);
      // Record affinity so future executions of the same persona
      // prefer this worker (warm session / cached project context).
      this.personaAffinity.set(request.personaId, workerId);
      this.logger.info({
        workerId,
        executionId: request.executionId,
        personaId: request.personaId,
      }, 'Execution dispatched to worker');
    }
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

    // Fall back to DB
    if (this.database) {
      const dbExec = db.getExecution(this.database, executionId);
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

  /** Get pending review requests for a specific execution. */
  getReviewsForExecution(executionId: string): ReviewRequest[] {
    const exec = this.lifecycle.getActive(executionId);
    return exec?.pendingReviews ?? [];
  }

  /** Get a specific review request by ID. */
  getReview(executionId: string, reviewId: string): ReviewRequest | null {
    const exec = this.lifecycle.getActive(executionId);
    if (!exec?.pendingReviews) return null;
    return exec.pendingReviews.find(r => r.reviewId === reviewId) ?? null;
  }

  /**
   * Respond to a pending human review. Sends the decision to the worker
   * to resume the paused execution.
   */
  respondToReview(executionId: string, reviewId: string, decision: 'approved' | 'rejected', message: string, projectId?: string): boolean {
    const exec = this.lifecycle.getActive(executionId);
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
    for (const exec of this.lifecycle.activeValues()) {
      if (projectId && exec.projectId !== projectId) continue;
      if (exec.pendingReviews) {
        for (const r of exec.pendingReviews) {
          if (r.status === 'pending') reviews.push(r);
        }
      }
    }
    return reviews;
  }

  /**
   * Gracefully shut down the dispatcher:
   * 1. Reject new submissions (draining = true)
   * 2. Leave queued items in DB with queue_data intact (they'll be recovered on restart)
   * 3. Wait up to DRAIN_TIMEOUT_MS for in-flight executions to complete
   * 4. Force-fail any remaining in-flight executions
   */
  async shutdown(): Promise<void> {
    this.draining = true;
    clearInterval(this.metricsTimer);

    // Clear the in-memory queue — items remain in DB with status='queued'
    // and queue_data intact, so they will be recovered on next startup.
    const queuedCount = this.queue.length;
    this.queue.clear();
    if (queuedCount > 0) {
      this.logger.info({ queuedCount }, 'Queued executions preserved in DB for recovery on restart');
    }

    // Wait for in-flight executions to complete (poll every 500ms)
    if (this.lifecycle.runningCount() > 0) {
      this.logger.info({ activeCount: this.lifecycle.runningCount() }, 'Waiting for in-flight executions to drain...');
      const deadline = Date.now() + DRAIN_TIMEOUT_MS;
      await new Promise<void>((resolve) => {
        const check = () => {
          if (this.lifecycle.runningCount() === 0 || Date.now() >= deadline) {
            resolve();
            return;
          }
          setTimeout(check, 500);
        };
        check();
      });
    }

    // Force-fail any remaining in-flight executions that didn't complete in time
    for (const [executionId, exec] of this.lifecycle.activeEntries()) {
      if (exec.status === 'running') {
        this.logger.error({ executionId }, 'Force-failing in-flight execution after drain timeout');
        exec.status = 'failed';
        exec.completedAt = Date.now();
        this.sink.onFailed(executionId, 'Orchestrator shutdown — drain timeout exceeded');
      }
    }

    this.lifecycle.clear();
    this.personaAffinity.clear();
    this.logger.info('Dispatcher shutdown complete');
  }
}
