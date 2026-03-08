// ============================================================
// dac-wire: WebSocket protocol between orchestrator and workers
// ============================================================

// --- Worker → Orchestrator ---

export interface WorkerHello {
  type: 'hello';
  workerId: string;
  /** Worker auth token, sent in the hello message instead of URL query string to avoid proxy/log exposure. */
  token: string;
  version: string;
  capabilities: string[];
}

export interface WorkerReady {
  type: 'ready';
  /** Number of execution slots currently available on this worker. */
  availableSlots: number;
  /** Maximum concurrent execution slots configured for this worker. */
  maxSlots: number;
}

export interface ExecStdout {
  type: 'stdout';
  executionId: string;
  chunk: string;
  timestamp: number;
}

export interface ExecStderr {
  type: 'stderr';
  executionId: string;
  chunk: string;
  timestamp: number;
}

export interface ExecComplete {
  type: 'complete';
  executionId: string;
  status: 'completed' | 'failed' | 'cancelled';
  exitCode: number;
  durationMs: number;
  sessionId?: string;
  totalCostUsd?: number;
}

export interface WorkerEvent {
  type: 'event';
  executionId: string;
  eventType: 'manual_review' | 'user_message' | 'persona_action' | 'emit_event';
  payload: unknown;
}

export interface ExecProgress {
  type: 'progress';
  executionId: string;
  stage: import('./types.js').ExecutionStage;
  percentEstimate: number | null;
  activeTool: string | null;
  message: string;
  toolCallsCompleted: number;
  timestamp: number;
}

/** Worker → Orchestrator: execution is paused pending human review. */
export interface WorkerReviewRequest {
  type: 'review_request';
  executionId: string;
  reviewId: string;
  payload: unknown;
  timestamp: number;
}

/** System health metrics reported by the worker alongside each heartbeat. */
export interface WorkerHealthMetrics {
  /** CPU usage as a percentage (0–100), averaged across all cores. */
  cpuUsagePercent: number;
  /** Available (free) memory in bytes. */
  freeMemoryBytes: number;
  /** Total system memory in bytes. */
  totalMemoryBytes: number;
  /** Available disk space in bytes (in the temp directory). */
  freeDiskBytes: number | null;
  /** Number of executions completed since this worker started. */
  executionsCompleted: number;
  /** Average execution duration in milliseconds (null if no executions yet). */
  avgExecutionDurationMs: number | null;
  /** Worker process uptime in milliseconds. */
  uptimeMs: number;
}

export interface WorkerHeartbeat {
  type: 'heartbeat';
  timestamp: number;
  /** System health metrics — present on heartbeats from workers that support health reporting. */
  metrics?: WorkerHealthMetrics;
}

/**
 * Worker → Orchestrator: report executions that are still running locally
 * after a WebSocket reconnect. Sent immediately after receiving `ack` and
 * before the `ready` message so the orchestrator can reconcile its view of
 * the worker's active work before slot accounting resumes.
 */
export interface WorkerActiveExecutions {
  type: 'active_executions';
  /** Execution IDs still running on this worker. */
  executionIds: string[];
}

export type WorkerMessage =
  | WorkerHello
  | WorkerReady
  | ExecStdout
  | ExecStderr
  | ExecComplete
  | ExecProgress
  | WorkerEvent
  | WorkerReviewRequest
  | WorkerHeartbeat
  | WorkerActiveExecutions;

// --- Orchestrator → Worker ---

export interface OrchestratorAck {
  type: 'ack';
  workerId: string;
  sessionToken: string;
  protocolVersion: string;
}

export interface ExecAssign {
  type: 'assign';
  executionId: string;
  personaId: string;
  prompt: string;
  env: Record<string, string>;
  config: {
    timeoutMs: number;
    maxOutputBytes: number;
  };
  /** Per-persona permission policy. When absent, falls back to --dangerously-skip-permissions. */
  permissionPolicy?: {
    allowedTools?: string[];
    allowNetwork?: boolean;
    skipAllPermissions?: boolean;
  };
}

export interface ExecCancel {
  type: 'cancel';
  executionId: string;
}

export interface OrchestratorShutdown {
  type: 'shutdown';
  reason: string;
  gracePeriodMs: number;
}

export interface OrchestratorHeartbeat {
  type: 'heartbeat';
  timestamp: number;
}

/** Orchestrator → Worker: human review decision for a paused execution. */
export interface OrchestratorReviewResponse {
  type: 'review_response';
  executionId: string;
  reviewId: string;
  decision: 'approved' | 'rejected';
  message: string;
}

export type OrchestratorMessage =
  | OrchestratorAck
  | ExecAssign
  | ExecCancel
  | OrchestratorShutdown
  | OrchestratorHeartbeat
  | OrchestratorReviewResponse;

// --- Signed envelope for worker → orchestrator messages ---

/**
 * Wire format for signed worker messages. The `payload` field contains the
 * JSON-serialised WorkerMessage. The orchestrator verifies the HMAC before
 * deserialising the inner payload.
 */
export interface SignedWorkerEnvelope {
  _signed: true;
  seq: number;
  sig: string;
  payload: string;
}

// --- Helpers ---

export function parseMessage<T>(raw: string): T | null {
  try {
    return JSON.parse(raw) as T;
  } catch {
    return null;
  }
}

/**
 * Parse a raw WebSocket frame that may be a signed envelope or a plain message.
 * Returns the envelope if signed, or null if it's unsigned / unparseable.
 */
export function parseSignedEnvelope(raw: string): SignedWorkerEnvelope | null {
  try {
    const parsed = JSON.parse(raw);
    if (parsed && parsed._signed === true && typeof parsed.seq === 'number' && typeof parsed.sig === 'string' && typeof parsed.payload === 'string') {
      return parsed as SignedWorkerEnvelope;
    }
    return null;
  } catch {
    return null;
  }
}

export function serializeMessage(msg: WorkerMessage | OrchestratorMessage): string {
  return JSON.stringify(msg);
}
