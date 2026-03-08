import { z } from 'zod';
import { validatePayloadFilter } from '@dac-cloud/shared';

// ---------------------------------------------------------------------------
// Shared field constraints
// ---------------------------------------------------------------------------

const MAX_PROMPT_BYTES = 50 * 1024; // 50 KB
const MAX_SHORT_STRING = 200;       // name, icon, color, groupId
const MAX_MEDIUM_STRING = 2000;     // description
const MAX_LONG_STRING = 100 * 1024; // designContext, modelProfile, permissionPolicy

/** Regex matching null bytes and C0 control chars (U+0001-U+001F) except tab, newline, CR. */
const DANGEROUS_CHARS = /[\x00-\x08\x0B\x0C\x0E-\x1F]/;

// ---------------------------------------------------------------------------
// Helper: Lazy Singleton Caching
// ---------------------------------------------------------------------------

/**
 * Ensures a Zod schema is only constructed once (on first use) and then cached.
 * This avoids repeated prototype chain construction and reduces allocation pressure.
 */
function lazySingleton<T extends z.ZodTypeAny>(factory: () => T): z.ZodLazy<T> {
  let cached: T | null = null;
  return z.lazy(() => {
    if (!cached) {
      cached = factory();
    }
    return cached;
  });
}

// ---------------------------------------------------------------------------
// Reusable field schemas & refinements
// ---------------------------------------------------------------------------

const controlCharRefinement = [
  (s: string) => !DANGEROUS_CHARS.test(s),
  { message: 'contains invalid control characters' },
] as const;

const jsonRefinement = [
  (val: string | null | undefined) => {
    if (val === undefined || val === null) return true;
    try { JSON.parse(val); return true; } catch { return false; }
  },
  { message: 'must be valid JSON' },
] as const;

const payloadFilterRefinement = [
  (val: string | null | undefined) => {
    if (!val) return true;
    const err = validatePayloadFilter(val);
    return err === null;
  },
  { message: 'invalid payload filter pattern' },
] as const;

/** A string field that rejects dangerous control characters. */
function safeString(maxLength: number) {
  return z.string().max(maxLength).refine(...controlCharRefinement);
}

const shortStr = safeString(MAX_SHORT_STRING);
const mediumStr = safeString(MAX_MEDIUM_STRING);
const longStr = safeString(MAX_LONG_STRING);
const promptStr = safeString(MAX_PROMPT_BYTES);

const jsonStr = promptStr.nullable().optional().refine(...jsonRefinement);
const filterStr = longStr.nullable().optional().refine(...payloadFilterRefinement);

// ---------------------------------------------------------------------------
// OAuth callback
// ---------------------------------------------------------------------------

export const OAuthCallbackSchema = lazySingleton(() => z.object({
  code: z.string().min(1, 'code is required'),
  state: z.string().min(1, 'state is required'),
}).strict());

// ---------------------------------------------------------------------------
// Token injection
// ---------------------------------------------------------------------------

export const SetTokenSchema = lazySingleton(() => z.object({
  token: z.string().min(1, 'token is required'),
}).strict());

// ---------------------------------------------------------------------------
// Execute
// ---------------------------------------------------------------------------

export const ExecuteSchema = lazySingleton(() => z.object({
  prompt: z.string().min(1, 'prompt is required'),
  personaId: z.string().optional(),
  timeoutMs: z.number().int().min(1000).max(600_000).optional(),
}).strict());

// ---------------------------------------------------------------------------
// Persona CRUD
// ---------------------------------------------------------------------------

export const PersonaCreateSchema = lazySingleton(() => z.object({
  id: shortStr.optional(),
  projectId: shortStr.optional(),
  name: shortStr.optional(),
  description: mediumStr.nullable().optional(),
  systemPrompt: promptStr.optional(),
  structuredPrompt: jsonStr,
  icon: shortStr.nullable().optional(),
  color: shortStr.nullable().optional(),
  enabled: z.boolean().optional(),
  maxConcurrent: z.number().int().min(1).max(100).optional(),
  timeoutMs: z.number().int().min(1000).max(600_000).optional(),
  modelProfile: longStr.nullable().optional(),
  maxBudgetUsd: z.number().min(0).max(1000).optional(),
  maxTurns: z.number().int().min(1).max(1000).optional(),
  designContext: longStr.nullable().optional(),
  groupId: shortStr.nullable().optional(),
  permissionPolicy: longStr.nullable().optional(),
}).strict());

export const PersonaUpdateSchema = lazySingleton(() => z.object({
  name: shortStr.optional(),
  description: mediumStr.nullable().optional(),
  systemPrompt: promptStr.optional(),
  structuredPrompt: jsonStr,
  icon: shortStr.nullable().optional(),
  color: shortStr.nullable().optional(),
  enabled: z.boolean().optional(),
  maxConcurrent: z.number().int().min(1).max(100).optional(),
  timeoutMs: z.number().int().min(1000).max(600_000).optional(),
  modelProfile: longStr.nullable().optional(),
  maxBudgetUsd: z.number().min(0).max(1000).nullable().optional(),
  maxTurns: z.number().int().min(1).max(1000).nullable().optional(),
  designContext: longStr.nullable().optional(),
  groupId: shortStr.nullable().optional(),
  permissionPolicy: longStr.nullable().optional(),
}).strict());

// ---------------------------------------------------------------------------
// Links
// ---------------------------------------------------------------------------

export const LinkToolSchema = lazySingleton(() => z.object({
  toolId: shortStr.min(1, 'toolId is required'),
}).strict());

export const LinkCredentialSchema = lazySingleton(() => z.object({
  credentialId: shortStr.min(1, 'credentialId is required'),
}).strict());

// ---------------------------------------------------------------------------
// Tool Definition
// ---------------------------------------------------------------------------

export const ToolDefinitionCreateSchema = lazySingleton(() => z.object({
  id: shortStr.optional(),
  name: shortStr.optional(),
  category: shortStr.optional(),
  description: mediumStr.optional(),
  scriptPath: mediumStr.optional(),
  inputSchema: longStr.nullable().optional(),
  outputSchema: longStr.nullable().optional(),
  requiresCredentialType: shortStr.nullable().optional(),
  implementationGuide: longStr.nullable().optional(),
  isBuiltin: z.boolean().optional(),
}).strict());

// ---------------------------------------------------------------------------
// Credential
// ---------------------------------------------------------------------------

export const CredentialCreateSchema = lazySingleton(() => z.object({
  name: shortStr.min(1, 'name is required'),
  serviceType: shortStr.min(1, 'serviceType is required'),
  encryptedData: z.string().min(1, 'encryptedData is required'),
  iv: z.string().min(1, 'iv is required'),
  tag: z.string().min(1, 'tag is required'),
  salt: z.string().nullable().optional(),
  iter: z.number().int().positive().nullable().optional(),
  metadata: longStr.nullable().optional(),
}).strict());

// ---------------------------------------------------------------------------
// Subscription
// ---------------------------------------------------------------------------

export const SubscriptionCreateSchema = lazySingleton(() => z.object({
  personaId: shortStr.min(1, 'personaId is required'),
  eventType: shortStr.min(1, 'eventType is required'),
  sourceFilter: mediumStr.nullable().optional(),
  payloadFilter: filterStr,
  enabled: z.boolean().optional(),
  maxRetries: z.number().int().min(0).max(10).optional(),
  retryBackoffMs: z.number().int().min(1000).max(300_000).optional(),
  useCaseId: shortStr.nullable().optional(),
}).strict());

export const SubscriptionUpdateSchema = lazySingleton(() => z.object({
  eventType: shortStr.optional(),
  sourceFilter: mediumStr.nullable().optional(),
  payloadFilter: filterStr,
  enabled: z.boolean().nullable().optional(),
  maxRetries: z.number().int().min(0).max(10).nullable().optional(),
  retryBackoffMs: z.number().int().min(1000).max(300_000).nullable().optional(),
}).strict());

// ---------------------------------------------------------------------------
// Trigger
// ---------------------------------------------------------------------------

export const TriggerCreateSchema = lazySingleton(() => z.object({
  personaId: shortStr.min(1, 'personaId is required'),
  triggerType: shortStr.min(1, 'triggerType is required'),
  config: longStr.nullable().optional(),
  enabled: z.boolean().optional(),
  useCaseId: shortStr.nullable().optional(),
}).strict());

export const TriggerUpdateSchema = lazySingleton(() => z.object({
  triggerType: shortStr.optional(),
  config: longStr.nullable().optional(),
  enabled: z.boolean().nullable().optional(),
}).strict());

// ---------------------------------------------------------------------------
// Event
// ---------------------------------------------------------------------------

export const EventCreateSchema = lazySingleton(() => z.object({
  eventType: shortStr.min(1, 'eventType is required'),
  sourceType: shortStr.min(1, 'sourceType is required'),
  sourceId: shortStr.nullable().optional(),
  targetPersonaId: shortStr.nullable().optional(),
  payload: longStr.nullable().optional(),
  useCaseId: shortStr.nullable().optional(),
}).strict());

export const EventUpdateSchema = lazySingleton(() => z.object({
  status: shortStr.min(1, 'status is required'),
  metadata: longStr.optional(),
}).strict());

// ---------------------------------------------------------------------------
// Compilation
// ---------------------------------------------------------------------------

const connectorSchema = lazySingleton(() => z.object({
  name: z.string().min(1, 'each connector must have a name'),
  description: z.string().optional(),
}).passthrough());

export const CompileSchema = lazySingleton(() => z.object({
  intent: promptStr.min(1, 'intent is required'),
  tools: z.array(z.string()).max(50).optional(),
  connectors: z.array(connectorSchema).max(20).optional(),
  modelProfile: longStr.optional(),
  maxBudgetUsd: z.number().min(0).max(100).optional(),
}).strict());

export const BatchCompileSchema = lazySingleton(() => z.object({
  description: promptStr.min(1, 'description is required'),
  count: z.number().int().min(1).max(10).optional(),
  tools: z.array(z.string()).max(50).optional(),
  connectors: z.array(connectorSchema).max(20).optional(),
  modelProfile: longStr.optional(),
}).strict());

// ---------------------------------------------------------------------------
// Deployment
// ---------------------------------------------------------------------------

export const DeploymentCreateSchema = lazySingleton(() => z.object({
  personaId: shortStr.min(1, 'personaId is required'),
  label: shortStr.optional(),
  maxMonthlyBudgetUsd: z.number().min(0).max(10_000).optional(),
}).strict());

// ---------------------------------------------------------------------------
// Helper: format Zod errors
// ---------------------------------------------------------------------------

/**
 * Formats a ZodError into a single human-readable error string with
 * field-level detail. Returns the first error for each field.
 */
export function formatZodError(error: z.ZodError): string {
  const fieldErrors = error.issues.map((issue) => {
    const path = issue.path.length > 0 ? issue.path.join('.') : '';
    return path ? `${path}: ${issue.message}` : issue.message;
  });
  return fieldErrors.join('; ');
}
