// Execution request — produced to Kafka by Vibeman, consumed by orchestrator
export interface ExecRequest {
  executionId: string;
  personaId: string;
  prompt: string;
  credentialIds?: string[];
  inputData?: unknown;
  config: {
    timeoutMs: number;
    maxConcurrent?: number;
  };
  triggerId?: string;
  triggerType?: string;
}

// Execution result — produced to Kafka by orchestrator
export interface ExecResult {
  executionId: string;
  personaId: string;
  status: 'completed' | 'failed' | 'cancelled';
  exitCode: number;
  durationMs: number;
  sessionId?: string;
  totalCostUsd?: number;
  error?: string;
}

// Worker info as tracked by orchestrator
export interface WorkerInfo {
  workerId: string;
  status: 'connecting' | 'idle' | 'executing' | 'disconnected';
  version: string;
  capabilities: string[];
  currentExecutionId?: string;
  connectedAt: number;
  lastHeartbeat: number;
}

// Execution output chunk — produced to Kafka for Vibeman SSE consumption
export interface OutputChunk {
  executionId: string;
  chunk: string;
  timestamp: number;
}

// Persona event — forwarded from worker to Kafka event bus
export interface PersonaCloudEvent {
  executionId: string;
  personaId: string;
  eventType: 'manual_review' | 'user_message' | 'persona_action' | 'emit_event';
  payload: unknown;
  timestamp: number;
}

// Kafka topic names
export const TOPICS = {
  EXEC_REQUESTS: 'persona.exec.v1',
  EXEC_OUTPUT: 'persona.output.v1',
  EXEC_LIFECYCLE: 'persona.lifecycle.v1',
  EVENTS: 'persona.events.v1',
  DLQ: 'persona.dlq.v1',
} as const;

// Protocol version
export const PROTOCOL_VERSION = '0.1.0';
