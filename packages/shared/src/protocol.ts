// ============================================================
// dac-wire: WebSocket protocol between orchestrator and workers
// ============================================================

import type { ProtocolEventType } from './protocolRegistry.js';

// --- Worker → Orchestrator ---

export interface WorkerHello {
  type: 'hello';
  workerId: string;
  /** Worker auth token, sent in the hello message instead of URL query string to avoid proxy/log exposure. */
  token: string;
  version: string;
  capabilities: string[];
  imageDigest?: string;
  claudeCliVersion?: string;
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

export interface PhaseTimings {
  sandboxSetupMs: number;
  credentialInjectionMs: number;
  proxyStartupMs: number | null;
  cliRuntimeMs: number;
  cleanupMs: number;
}

export interface ExecComplete {
  type: 'complete';
  executionId: string;
  status: 'completed' | 'failed' | 'cancelled';
  exitCode: number;
  durationMs: number;
  sessionId?: string;
  totalCostUsd?: number;
  phaseTimings?: PhaseTimings;
}

export interface WorkerEvent {
  type: 'event';
  executionId: string;
  eventType: ProtocolEventType;
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
  /** CPU usage as a percentage (0-100), averaged across all cores. */
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

export interface WorkerBusy {
  type: 'busy';
  executionId: string;
  reason: string;
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
  | WorkerActiveExecutions
  | WorkerBusy;

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
    networkPolicy?: import('./networkPolicy.js').NetworkPolicy;
    maxBudgetUsd?: number;
    maxTurns?: number;
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

// --- Runtime validators ---

function isObj(v: unknown): v is Record<string, unknown> {
  return typeof v === 'object' && v !== null && !Array.isArray(v);
}

function hasStr(o: Record<string, unknown>, k: string): boolean {
  return typeof o[k] === 'string';
}

function hasNum(o: Record<string, unknown>, k: string): boolean {
  return typeof o[k] === 'number';
}

function hasStrArr(o: Record<string, unknown>, k: string): boolean {
  return Array.isArray(o[k]) && (o[k] as unknown[]).every(v => typeof v === 'string');
}

export function validateWorkerMessage(msg: unknown): WorkerMessage | null {
  if (!isObj(msg) || !hasStr(msg, 'type')) return null;
  const m = msg as Record<string, unknown> & { type: string };
  switch (m.type) {
    case 'hello':
      return hasStr(m, 'workerId') && hasStr(m, 'version') && hasStrArr(m, 'capabilities')
        ? (m as unknown as WorkerHello) : null;
    case 'ready':
      return m as unknown as WorkerReady;
    case 'stdout':
    case 'stderr':
      return hasStr(m, 'executionId') && hasStr(m, 'chunk') && hasNum(m, 'timestamp')
        ? (m as unknown as ExecStdout & ExecStderr) : null;
    case 'complete':
      return hasStr(m, 'executionId') && hasStr(m, 'status') && hasNum(m, 'exitCode') && hasNum(m, 'durationMs')
        ? (m as unknown as ExecComplete) : null;
    case 'event':
      return hasStr(m, 'executionId') && hasStr(m, 'eventType') && 'payload' in m
        ? (m as unknown as WorkerEvent) : null;
    case 'progress':
      return hasStr(m, 'executionId') && hasStr(m, 'stage') && hasStr(m, 'message') && hasNum(m, 'timestamp')
        ? (m as unknown as ExecProgress) : null;
    case 'review_request':
      return hasStr(m, 'executionId') && hasStr(m, 'reviewId') && hasNum(m, 'timestamp')
        ? (m as unknown as WorkerReviewRequest) : null;
    case 'heartbeat':
      return hasNum(m, 'timestamp') ? (m as unknown as WorkerHeartbeat) : null;
    case 'active_executions':
      return hasStrArr(m, 'executionIds')
        ? (m as unknown as WorkerActiveExecutions) : null;
    case 'busy':
      return hasStr(m, 'executionId') && hasStr(m, 'reason')
        ? (m as unknown as WorkerBusy) : null;
    default:
      return null;
  }
}

export function validateOrchestratorMessage(msg: unknown): OrchestratorMessage | null {
  if (!isObj(msg) || !hasStr(msg, 'type')) return null;
  const m = msg as Record<string, unknown> & { type: string };
  switch (m.type) {
    case 'ack':
      return hasStr(m, 'workerId') && hasStr(m, 'sessionToken')
        ? (m as unknown as OrchestratorAck) : null;
    case 'assign':
      return hasStr(m, 'executionId') && hasStr(m, 'personaId') && hasStr(m, 'prompt') && isObj(m.env) && isObj(m.config)
        ? (m as unknown as ExecAssign) : null;
    case 'cancel':
      return hasStr(m, 'executionId') ? (m as unknown as ExecCancel) : null;
    case 'shutdown':
      return hasStr(m, 'reason') && hasNum(m, 'gracePeriodMs')
        ? (m as unknown as OrchestratorShutdown) : null;
    case 'heartbeat':
      return hasNum(m, 'timestamp') ? (m as unknown as OrchestratorHeartbeat) : null;
    case 'review_response':
      return hasStr(m, 'executionId') && hasStr(m, 'reviewId') && hasStr(m, 'decision') && hasStr(m, 'message')
        ? (m as unknown as OrchestratorReviewResponse) : null;
    default:
      return null;
  }
}

// --- Helpers ---

export function parseWorkerMessage(raw: string): WorkerMessage | null {
  try {
    return validateWorkerMessage(JSON.parse(raw));
  } catch {
    return null;
  }
}

export function parseOrchestratorMessage(raw: string): OrchestratorMessage | null {
  try {
    return validateOrchestratorMessage(JSON.parse(raw));
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
