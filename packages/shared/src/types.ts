// ---------------------------------------------------------------------------
// Persona domain types (mirroring desktop Tauri models)
// ---------------------------------------------------------------------------

export interface Persona {
  id: string;
  projectId: string;
  name: string;
  description: string | null;
  systemPrompt: string;
  structuredPrompt: string | null;
  icon: string | null;
  color: string | null;
  enabled: boolean;
  maxConcurrent: number;
  timeoutMs: number;
  modelProfile: string | null;
  maxBudgetUsd: number | null;
  maxTurns: number | null;
  designContext: string | null;
  groupId: string | null;
  createdAt: string;
  updatedAt: string;
}

export interface PersonaToolDefinition {
  id: string;
  name: string;
  category: string;
  description: string;
  scriptPath: string;
  inputSchema: string | null;
  outputSchema: string | null;
  requiresCredentialType: string | null;
  implementationGuide: string | null;
  isBuiltin: boolean;
  createdAt: string;
  updatedAt: string;
}

export interface StructuredPrompt {
  identity?: string;
  instructions?: string;
  toolGuidance?: string;
  examples?: string;
  errorHandling?: string;
  customSections?: Array<{ title?: string; label?: string; name?: string; key?: string; content?: string }>;
  webSearch?: string;
}

export interface ModelProfile {
  model?: string;
  provider?: string;
  baseUrl?: string;
  authToken?: string;
}

export interface PersonaCredential {
  id: string;
  projectId?: string;
  name: string;
  serviceType: string;
  encryptedData: string;
  iv: string;
  tag: string;
  metadata: string | null;
  lastUsedAt: string | null;
  createdAt: string;
  updatedAt: string;
}

export interface PersonaEvent {
  id: string;
  projectId: string;
  eventType: string;
  sourceType: string;
  sourceId: string | null;
  targetPersonaId: string | null;
  payload: string | null;
  status: string;
  errorMessage: string | null;
  processedAt: string | null;
  useCaseId: string | null;
  createdAt: string;
}

export interface PersonaEventSubscription {
  id: string;
  personaId: string;
  eventType: string;
  sourceFilter: string | null;
  enabled: boolean;
  useCaseId: string | null;
  createdAt: string;
  updatedAt: string;
}

export interface PersonaTrigger {
  id: string;
  personaId: string;
  triggerType: string;
  config: string | null;
  enabled: boolean;
  lastTriggeredAt: string | null;
  nextTriggerAt: string | null;
  useCaseId: string | null;
  createdAt: string;
  updatedAt: string;
}

export interface PersonaExecution {
  id: string;
  projectId?: string;
  personaId: string;
  triggerId: string | null;
  useCaseId: string | null;
  status: string;
  inputData: string | null;
  outputData: string | null;
  claudeSessionId: string | null;
  modelUsed: string | null;
  inputTokens: number;
  outputTokens: number;
  costUsd: number;
  errorMessage: string | null;
  durationMs: number | null;
  retryOfExecutionId: string | null;
  retryCount: number;
  startedAt: string | null;
  completedAt: string | null;
  createdAt: string;
}

// ---------------------------------------------------------------------------
// Execution request — produced to Kafka by Vibeman, consumed by orchestrator
// ---------------------------------------------------------------------------

export interface ExecRequest {
  executionId: string;
  personaId: string;
  prompt: string;
  projectId?: string;
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

// Protocol version — bump major on breaking changes, minor on additive changes
export const PROTOCOL_VERSION = '1.0.0';

/** Parse a semver string into [major, minor, patch]. Returns null on invalid input. */
export function parseSemver(version: string): [number, number, number] | null {
  const m = /^(\d+)\.(\d+)\.(\d+)/.exec(version);
  if (!m) return null;
  return [Number(m[1]), Number(m[2]), Number(m[3])];
}

/**
 * Check protocol version compatibility.
 * - 'compatible': exact match or same major with higher/equal minor
 * - 'warn': same major but different minor (near-compatible)
 * - 'incompatible': different major version
 * - 'invalid': could not parse version string
 */
export function checkProtocolCompatibility(
  remote: string,
  local: string = PROTOCOL_VERSION,
): 'compatible' | 'warn' | 'incompatible' | 'invalid' {
  const r = parseSemver(remote);
  const l = parseSemver(local);
  if (!r || !l) return 'invalid';

  if (r[0] !== l[0]) return 'incompatible';
  if (r[1] !== l[1]) return 'warn';
  return 'compatible';
}
