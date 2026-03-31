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
  /** JSON-encoded PermissionPolicy. When null, defaults to skip-all (legacy). */
  permissionPolicy: string | null;
  /** HMAC secret for webhook signature verification. Null means webhooks require API key auth. */
  webhookSecret: string | null;
  /** JSON-encoded ModelProfile for model/provider overrides. */
  modelProfile?: string | null;
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

export type InferenceProvider = 'anthropic' | 'claude' | 'ollama' | 'litellm' | 'openai-compatible' | 'custom';

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

/**
 * Permission policy for constraining Claude CLI tool access per persona.
 * When absent, the executor falls back to `--dangerously-skip-permissions`
 * for backward compatibility. When present, only the listed tools are allowed
 * via `--allowedTools`.
 */
export interface ModelProfile {
  /** Claude model identifier override (e.g. "claude-sonnet-4-5-20250514"). */
  model?: string;
  /** Optional max tokens for the model. */
  maxTokens?: number;
  /** Inference provider (e.g. "ollama", "litellm", "custom"). */
  provider?: InferenceProvider;
  /** Base URL for the inference provider endpoint. */
  baseUrl?: string;
  /** Authentication token for the inference provider. */
  authToken?: string;
}

export interface PermissionPolicy {
  /** Explicit list of Claude CLI tool names to allow (e.g. "Bash", "Read", "Write", "Edit", "Glob", "Grep"). */
  allowedTools?: string[];
  /** If true, the Bash tool can make outbound network calls. Only relevant when Bash is in allowedTools. */
  allowNetwork?: boolean;
  /** If true, falls back to --dangerously-skip-permissions (legacy / admin override). */
  skipAllPermissions?: boolean;
}

export interface PersonaCredential {
  id: string;
  projectId?: string;
  name: string;
  serviceType: string;
  encryptedData: string;
  iv: string;
  tag: string;
  salt?: string;     // hex — per-credential PBKDF2 salt (absent in legacy data)
  iter?: number;     // PBKDF2 iteration count used to derive the key (absent in legacy data)
  metadata: string | null;
  lastUsedAt: string | null;
  createdAt: string;
  updatedAt: string;
}

// ---------------------------------------------------------------------------
// Event lifecycle state machine
// ---------------------------------------------------------------------------

/** All valid event statuses. */
export type EventStatus = 'pending' | 'processing' | 'delivered' | 'skipped' | 'failed' | 'partial' | 'partial-retry' | 'dead_letter';

/** All valid EventStatus values as a runtime array (for CHECK constraints, etc.). */
export const EVENT_STATUSES: readonly EventStatus[] = [
  'pending', 'processing', 'delivered', 'skipped', 'failed', 'partial', 'partial-retry', 'dead_letter',
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
  'dead_letter':   new Set<EventStatus>([]),                       // terminal
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
  /** Number of retry attempts so far. 0 on first processing. */
  retryCount: number;
  /** ISO timestamp — when the next retry should be attempted. Null if not in retry state. */
  nextRetryAt: string | null;
  createdAt: string;
}

export interface PersonaEventSubscription {
  id: string;
  personaId: string;
  eventType: string;
  sourceFilter: string | null;
  /** JSON-encoded payload filter conditions (EventBridge-style pattern). */
  payloadFilter: string | null;
  enabled: boolean;
  /** Maximum number of retry attempts before moving to dead_letter. Default 3. */
  maxRetries: number;
  /** Base delay in milliseconds for exponential backoff between retries. Default 5000. */
  retryBackoffMs: number;
  useCaseId: string | null;
  createdAt: string;
  updatedAt: string;
}

// ---------------------------------------------------------------------------
// Trigger config discriminated union
// ---------------------------------------------------------------------------

export type TriggerType = 'manual' | 'schedule' | 'polling' | 'webhook' | 'chain' | 'cron';

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
  /** Health status: 'healthy' (default), 'degraded' (e.g. broken cron expression). */
  healthStatus: 'healthy' | 'degraded';
  /** Human-readable reason for degraded health status. */
  healthMessage: string | null;
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

/**
 * Audit log entry for each trigger firing, recording outcome metrics.
 * Used by the trigger analytics system to correlate firing times with
 * execution success/failure, cost, and duration.
 */
export interface TriggerFiring {
  id: string;
  triggerId: string;
  personaId: string;
  projectId: string;
  eventId: string | null;
  executionId: string | null;
  /** Status of the firing: 'fired' (event published), 'dispatched' (execution started), 'completed', 'failed', 'skipped' */
  status: string;
  costUsd: number | null;
  durationMs: number | null;
  errorMessage: string | null;
  firedAt: string;
  resolvedAt: string | null;
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
// Execution progress — structured stage tracking for live executions
// ---------------------------------------------------------------------------

/** Semantic execution stages derived from Claude CLI stream-json output. */
export type ExecutionStage =
  | 'initializing'
  | 'thinking'
  | 'tool_calling'
  | 'tool_result'
  | 'generating'
  | 'completed'
  | 'failed';

/** Structured progress snapshot for a running execution. */
export interface ExecutionProgress {
  /** Current semantic stage of the execution. */
  stage: ExecutionStage;
  /** Estimated progress percentage (0–100). Null if indeterminate. */
  percentEstimate: number | null;
  /** Current tool being invoked (only set during tool_calling stage). */
  activeTool: string | null;
  /** Human-readable description of what's happening. */
  message: string;
  /** Number of tool calls completed so far. */
  toolCallsCompleted: number;
  /** Timestamp when this stage was entered. */
  stageStartedAt: number;
}

// ---------------------------------------------------------------------------
// Human-in-the-loop review — pause/resume semantics for manual_review events
// ---------------------------------------------------------------------------

/** A review request emitted when a persona triggers a manual_review event. */
export interface ReviewRequest {
  /** Unique ID for this review request. */
  reviewId: string;
  /** The execution that is paused awaiting review. */
  executionId: string;
  /** Persona that triggered the review. */
  personaId: string;
  /** Project scope. */
  projectId?: string;
  /** The payload from the manual_review persona protocol event. */
  payload: unknown;
  /** Current status of the review. */
  status: 'pending' | 'approved' | 'rejected' | 'timed_out';
  /** When the review was requested. */
  createdAt: number;
  /** When the review was resolved (approved/rejected/timed_out). */
  resolvedAt: number | null;
  /** The human reviewer's decision message (injected back into CLI stdin). */
  responseMessage: string | null;
}

// ---------------------------------------------------------------------------
// Execution request — dispatched to orchestrator for worker execution
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

// Worker info as tracked by orchestrator
export interface WorkerInfo {
  workerId: string;
  status: 'connecting' | 'idle' | 'executing' | 'disconnected';
  version: string;
  capabilities: string[];
  /** Active execution IDs on this worker (supports concurrent execution slots). */
  currentExecutionIds: string[];
  /** Maximum concurrent execution slots for this worker. */
  maxConcurrentSlots: number;
  /** Number of available slots (maxConcurrentSlots - currentExecutionIds.length). */
  availableSlots: number;
  connectedAt: number;
  lastHeartbeat: number;
  imageDigest?: string;
  claudeCliVersion?: string;
  verified: boolean;
  /** Latest system health metrics reported by this worker. Null until first heartbeat with metrics. */
  healthMetrics?: import('./protocol.js').WorkerHealthMetrics | null;
}


// ---------------------------------------------------------------------------
// Compilation API — programmatic persona creation via the design pipeline
// ---------------------------------------------------------------------------

export type CompilationStage =
  | 'prompt_assembly'
  | 'llm_generation'
  | 'result_parsing'
  | 'feasibility_check'
  | 'persist';

export interface CompileRequest {
  /** High-level intent describing what the persona should do. */
  intent: string;
  /** Optional tool names to include in the compiled persona. */
  tools?: string[];
  /** Optional connector/service definitions the persona should use. */
  connectors?: Array<{ name: string; description?: string }>;
  /** Optional model profile override for the compilation LLM call. */
  modelProfile?: string;
  /** Optional max budget for the compilation execution (default: $1.00). */
  maxBudgetUsd?: number;
}

export interface BatchCompileRequest {
  /** High-level description of the team/group of personas to create. */
  description: string;
  /** Number of personas to create (1-10). */
  count?: number;
  /** Optional tool names available to all compiled personas. */
  tools?: string[];
  /** Optional connector definitions available to all personas. */
  connectors?: Array<{ name: string; description?: string }>;
  /** Optional model profile override. */
  modelProfile?: string;
}

export interface CompileResult {
  /** Unique compilation job ID (same as executionId). */
  compileId: string;
  /** Status of the compilation. */
  status: 'queued' | 'running' | 'completed' | 'failed';
  /** Current compilation stage (null if queued). */
  stage: CompilationStage | null;
  /** The compiled persona (null until completed). */
  persona: Persona | null;
  /** Error message if failed. */
  error: string | null;
}

/** Per-item result within a batch compilation. */
export interface BatchCompileItemResult {
  /** Zero-based index within the batch. */
  index: number;
  /** Status of this individual item. */
  status: 'completed' | 'failed';
  /** The compiled persona (null if failed). */
  persona: Persona | null;
  /** Error message if this item failed. */
  error: string | null;
}

export interface BatchCompileResult {
  /** Unique batch compilation ID. */
  batchId: string;
  /** Individual compilation jobs. */
  compileIds: string[];
  /**
   * Overall status:
   * - `queued`: execution not yet started
   * - `running`: LLM generation in progress
   * - `completed`: all items parsed successfully
   * - `partial`: some items succeeded, some failed (HTTP 200 — check `results` for per-item status)
   * - `failed`: execution failed or no items could be parsed
   */
  status: 'queued' | 'running' | 'completed' | 'partial' | 'failed';
  /** Per-item results. Present when status is 'completed', 'partial', or 'failed' (post-execution). */
  results?: BatchCompileItemResult[];
  /** Top-level error message when the entire batch fails. */
  error?: string | null;
}

// ---------------------------------------------------------------------------
// Cloud Deployments — one-click local->cloud API deployment
// ---------------------------------------------------------------------------

export type DeploymentStatus = 'active' | 'paused' | 'failed';

export interface CloudDeployment {
  id: string;
  projectId: string;
  personaId: string;
  /** Slug-based route path: /api/deployed/{slug} */
  slug: string;
  /** Human-readable label (defaults to persona name) */
  label: string;
  status: DeploymentStatus;
  /** Whether the webhook endpoint is enabled */
  webhookEnabled: boolean;
  /** HMAC secret for verifying incoming webhook requests */
  webhookSecret: string | null;
  /** Total invocation count since deployment */
  invocationCount: number;
  /** Last invocation timestamp */
  lastInvokedAt: string | null;
  /** Monthly budget cap in USD — null means unlimited */
  maxMonthlyBudgetUsd: number | null;
  /** Accumulated cost for the current budget month */
  currentMonthCostUsd: number;
  /** Budget tracking month (YYYY-MM format) */
  budgetMonth: string | null;
  createdAt: string;
  updatedAt: string;
}

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
