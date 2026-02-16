// ============================================================
// dac-wire: WebSocket protocol between orchestrator and workers
// ============================================================

// --- Worker → Orchestrator ---

export interface WorkerHello {
  type: 'hello';
  workerId: string;
  version: string;
  capabilities: string[];
}

export interface WorkerReady {
  type: 'ready';
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

export interface WorkerHeartbeat {
  type: 'heartbeat';
  timestamp: number;
}

export type WorkerMessage =
  | WorkerHello
  | WorkerReady
  | ExecStdout
  | ExecStderr
  | ExecComplete
  | WorkerEvent
  | WorkerHeartbeat;

// --- Orchestrator → Worker ---

export interface OrchestratorAck {
  type: 'ack';
  workerId: string;
  sessionToken: string;
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

export type OrchestratorMessage =
  | OrchestratorAck
  | ExecAssign
  | ExecCancel
  | OrchestratorShutdown
  | OrchestratorHeartbeat;

// --- Helpers ---

export function parseMessage<T>(raw: string): T | null {
  try {
    return JSON.parse(raw) as T;
  } catch {
    return null;
  }
}

export function serializeMessage(msg: WorkerMessage | OrchestratorMessage): string {
  return JSON.stringify(msg);
}
