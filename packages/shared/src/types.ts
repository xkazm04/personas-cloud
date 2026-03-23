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
  inferenceProfileId: string | null;
  networkPolicy: string | null;
  maxBudgetUsd: number | null;
  maxTurns: number | null;
  designContext: string | null;
  groupId: string | null;
  createdAt: string;
  updatedAt: string;
}

/** Lightweight projection for list views — omits large text fields. */
export interface PersonaSummary {
  id: string;
  projectId: string;
  name: string;
  description: string | null;
  icon: string | null;
  color: string | null;
  enabled: boolean;
  groupId: string | null;
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

// ---------------------------------------------------------------------------
// Inference profiles
// ---------------------------------------------------------------------------

export type InferenceProvider = 'claude' | 'ollama' | 'litellm' | 'openai-compatible' | 'custom';

export interface InferenceProviderEnvMapping {
  envVar: string;
  source: 'baseUrl' | 'authToken' | 'model';
}

export interface InferenceProfile {
  id: string;
  projectId: string;
  name: string;
  provider: InferenceProvider;
  model?: string;
  baseUrl?: string;
  authTokenEncrypted?: string;
  authTokenIv?: string;
  authTokenTag?: string;
  envMappings: InferenceProviderEnvMapping[];
  removeEnvKeys?: string[];
  isPreset: boolean;
  createdAt: string;
  updatedAt: string;
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

// ---------------------------------------------------------------------------
// Event lifecycle state machine
// ---------------------------------------------------------------------------

/** All valid event statuses. */
export type EventStatus = 'pending' | 'processing' | 'delivered' | 'skipped' | 'failed' | 'partial' | 'partial-retry';

/** All valid EventStatus values as a runtime array (for CHECK constraints, etc.). */
export const EVENT_STATUSES: readonly EventStatus[] = [
  'pending', 'processing', 'delivered', 'skipped', 'failed', 'partial', 'partial-retry',
] as const;

/**
 * Legal state transitions for persona events.
 * Key = current status, Value = set of statuses it may transition to.
 */
export const EVENT_TRANSITIONS: Readonly<Record<EventStatus, ReadonlySet<EventStatus>>> = {
  'pending':       new Set<EventStatus>(['processing']),
  'processing':    new Set<EventStatus>(['delivered', 'skipped', 'failed', 'partial', 'partial-retry', 'pending']),
  'delivered':     new Set<EventStatus>([]),                       // terminal
  'skipped':       new Set<EventStatus>([]),                       // terminal
  'failed':        new Set<EventStatus>([]),                       // terminal
  'partial':       new Set<EventStatus>(['partial-retry']),        // can retry partial failures
  'partial-retry': new Set<EventStatus>(['processing']),           // retried partial goes back through processing
};

/**
 * Validate that a transition from `from` → `to` is legal.
 * Throws if the transition is invalid.
 */
export function validateEventTransition(from: EventStatus, to: EventStatus): void {
  const allowed = EVENT_TRANSITIONS[from];
  if (!allowed || !allowed.has(to)) {
    throw new Error(`Invalid event status transition: '${from}' → '${to}'`);
  }
}

export interface PersonaEvent {
  id: string;
  projectId: string;
  eventType: string;
  sourceType: string;
  sourceId: string | null;
  targetPersonaId: string | null;
  payload: string | null;
  status: EventStatus;
  errorMessage: string | null;
  processedAt: string | null;
  useCaseId: string | null;
  retryAfter: string | null;
  retryCount: number;
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

// ---------------------------------------------------------------------------
// Trigger config discriminated union
// ---------------------------------------------------------------------------

export type TriggerType = 'manual' | 'schedule' | 'polling' | 'webhook' | 'chain';

interface TriggerConfigBase {
  event_type?: string;
  payload?: unknown;
}

export interface ScheduleTriggerConfig extends TriggerConfigBase {
  kind: 'schedule';
  cron?: string;
  interval_seconds?: number;
}

export interface ManualTriggerConfig extends TriggerConfigBase {
  kind: 'manual';
}

export interface PollingTriggerConfig extends TriggerConfigBase {
  kind: 'polling';
  poll_url?: string;
  poll_interval_seconds?: number;
}

export interface WebhookTriggerConfig extends TriggerConfigBase {
  kind: 'webhook';
  url?: string;
  secret?: string;
}

export interface ChainTriggerConfig extends TriggerConfigBase {
  kind: 'chain';
  source_persona_id?: string;
  source_event_type?: string;
}

export type TriggerConfig =
  | ScheduleTriggerConfig
  | ManualTriggerConfig
  | PollingTriggerConfig
  | WebhookTriggerConfig
  | ChainTriggerConfig;

export interface PersonaTrigger {
  id: string;
  personaId: string;
  triggerType: TriggerType;
  config: string | null;
  enabled: boolean;
  lastTriggeredAt: string | null;
  nextTriggerAt: string | null;
  useCaseId: string | null;
  createdAt: string;
  updatedAt: string;
}

export type TriggerFireStatus = 'fired' | 'completed' | 'failed';

export interface TriggerFire {
  id: string;
  triggerId: string;
  eventId: string | null;
  executionId: string | null;
  status: TriggerFireStatus;
  durationMs: number | null;
  errorMessage: string | null;
  firedAt: string;
}

export interface TriggerStats {
  totalFires: number;
  successCount: number;
  failureCount: number;
  successRate: number;
  avgDurationMs: number | null;
  lastFireStatus: TriggerFireStatus | null;
  lastFiredAt: string | null;
}

/** Fully hydrated persona with all related entities. */
export interface HydratedPersona extends Persona {
  tools: PersonaToolDefinition[];
  credentials: Omit<PersonaCredential, 'encryptedData' | 'iv' | 'tag'>[];
  subscriptions: PersonaEventSubscription[];
  triggers: PersonaTrigger[];
}

export interface PersonaExecution {
  id: string;
  projectId?: string;
  personaId: string;
  triggerId: string | null;
  useCaseId: string | null;
  /** Originating event ID — enables bidirectional event↔execution tracing. */
  eventId: string | null;
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
  /** Originating event ID — enables bidirectional event↔execution tracing. */
  eventId?: string;
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
  phaseTimings?: import('./protocol.js').PhaseTimings;
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
  imageDigest?: string;
  claudeCliVersion?: string;
  verified: boolean;
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
  eventType: import('./protocolRegistry.js').ProtocolEventType;
  payload: unknown;
  timestamp: number;
}

// Execution progress (ephemeral — tracked in-memory + Kafka only)
export interface ProgressInfo {
  phase: string;
  percent: number;
  detail?: string;
  updatedAt: number;
}

// Kafka topic names
export const TOPICS = {
  EXEC_REQUESTS: 'persona.exec.v1',
  EXEC_OUTPUT: 'persona.output.v1',
  EXEC_LIFECYCLE: 'persona.lifecycle.v1',
  EXEC_PROGRESS: 'persona.progress.v1',
  EVENTS: 'persona.events.v1',
  DLQ: 'persona.dlq.v1',
  QUEUE_METRICS: 'persona.queue_metrics.v1',
} as const;

// Protocol version
export const PROTOCOL_VERSION = '0.2.0';
