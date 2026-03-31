// Barrel re-export — all consumers can still `import * as db from './db/index.js'`
import Database from 'better-sqlite3';
import { nanoid } from 'nanoid';
import type { Persona, PersonaToolDefinition, PersonaCredential, PersonaEventSubscription, PersonaTrigger, CloudDeployment, TriggerFire, TriggerFireStatus } from '@dac-cloud/shared';
import { stmt } from './_helpers.js';

export { initDb, initSystemDb, initTenantDb } from './schema.js';
export { getPersona, getPersonasByIds, listPersonas, listPersonasSummary, upsertPersona, deletePersona, getHydratedPersona, listDistinctProjectIds } from './personas.js';
export { getToolsForPersona, getToolsForPersonaIds, upsertToolDefinition, linkTool, unlinkTool } from './tools.js';
export { createCredential, getCredential, listCredentialsForPersona, linkCredential, deleteCredential } from './credentials.js';
export { publishEvent, getEvent, getPendingEvents, countPendingEvents, getOldestPendingEventCreatedAt, listEvents, updateEventStatus, resetStaleProcessingEvents, resetAllProcessingEvents, updateEventWithMetadata, deferEventForRetry, createSubscription, getSubscription, listSubscriptionsForPersona, getSubscriptionsByEventType, updateSubscription, deleteSubscription } from './events.js';
export { parseTriggerConfig, validateTriggerConfig, computeNextTriggerAtFromParsed, createTrigger, getTrigger, listTriggersForPersona, getDueTriggersWithPersona, getTriggersByIdsWithPersona, loadEnabledTriggerTimes, getEarliestNextTriggerTime, updateTriggerTimings, updateTrigger, deleteTrigger } from './triggers.js';
export type { DueTriggerRow } from './triggers.js';
export { createExecution, getExecution, listExecutions, updateExecution, appendOutput, countRunningExecutions, countRunningExecutionsByPersonaIds } from './executions.js';
export { recordTriggerFire, updateTriggerFire, listTriggerFires, getTriggerStats } from './triggerFires.js';
export { getInferenceProfile, listInferenceProfiles, upsertInferenceProfile, deleteInferenceProfile } from './inferenceProfiles.js';
export { queryDailyMetrics, queryDailyMetricsByPersona } from './dailyMetrics.js';
export type { DailyMetricsRow, DailyMetricsByPersonaRow, DailyMetricsQueryOpts } from './dailyMetrics.js';

// --- Cloud Deployments ---
export { listDeployments, getDeployment, getDeploymentByPersona, getDeploymentBySlug, createDeployment, updateDeploymentStatus, deleteDeployment, incrementDeploymentInvocation } from './deployments.js';

// ---------------------------------------------------------------------------
// Re-imported references for alias/wrapper functions below
// ---------------------------------------------------------------------------
import { getPersona, upsertPersona } from './personas.js';
import { listTriggerFires, getTriggerStats } from './triggerFires.js';
import { getTrigger } from './triggers.js';
import { recordTriggerFire as _recordTriggerFire, updateTriggerFire } from './triggerFires.js';
import { getDueTriggersWithPersona } from './triggers.js';
import { deferEventForRetry, updateEventStatus } from './events.js';
import { getToolsForPersona } from './tools.js';
import { listCredentialsForPersona } from './credentials.js';
import { listSubscriptionsForPersona } from './events.js';
import { listTriggersForPersona } from './triggers.js';
import { getDeploymentByPersona } from './deployments.js';

// ---------------------------------------------------------------------------
// Alias: listTriggerFirings — wraps listTriggerFires with an options object
// ---------------------------------------------------------------------------
export function listTriggerFirings(db: Database.Database, opts: {
  triggerId: string;
  status?: string;
  limit?: number;
  offset?: number;
}): TriggerFire[] {
  // The existing listTriggerFires only supports (triggerId, limit).
  // Add status filtering and offset here.
  const limit = opts.limit ?? 50;
  const offset = opts.offset ?? 0;
  if (opts.status) {
    const rows = stmt(db, `
      SELECT * FROM trigger_fires
      WHERE trigger_id = ? AND status = ?
      ORDER BY fired_at DESC
      LIMIT ? OFFSET ?
    `).all(opts.triggerId, opts.status, limit, offset) as Record<string, unknown>[];
    return rows.map(r => ({
      id: r.id as string,
      triggerId: r.trigger_id as string,
      eventId: (r.event_id as string) ?? null,
      executionId: (r.execution_id as string) ?? null,
      status: r.status as TriggerFireStatus,
      durationMs: (r.duration_ms as number) ?? null,
      errorMessage: (r.error_message as string) ?? null,
      firedAt: r.fired_at as string,
    }));
  }
  if (offset > 0) {
    const rows = stmt(db, `
      SELECT * FROM trigger_fires
      WHERE trigger_id = ?
      ORDER BY fired_at DESC
      LIMIT ? OFFSET ?
    `).all(opts.triggerId, limit, offset) as Record<string, unknown>[];
    return rows.map(r => ({
      id: r.id as string,
      triggerId: r.trigger_id as string,
      eventId: (r.event_id as string) ?? null,
      executionId: (r.execution_id as string) ?? null,
      status: r.status as TriggerFireStatus,
      durationMs: (r.duration_ms as number) ?? null,
      errorMessage: (r.error_message as string) ?? null,
      firedAt: r.fired_at as string,
    }));
  }
  return listTriggerFires(db, opts.triggerId, limit);
}

// ---------------------------------------------------------------------------
// Alias: getTriggerFiringStats → getTriggerStats
// ---------------------------------------------------------------------------
export { getTriggerStats as getTriggerFiringStats };

// ---------------------------------------------------------------------------
// Alias: recordTriggerFiring → recordTriggerFire (extra fields are ignored)
// ---------------------------------------------------------------------------
export function recordTriggerFiring(db: Database.Database, input: {
  triggerId: string;
  personaId?: string;
  projectId?: string;
  eventId?: string | null;
  status?: TriggerFireStatus;
}): TriggerFire {
  return _recordTriggerFire(db, {
    triggerId: input.triggerId,
    eventId: input.eventId,
    status: input.status,
  });
}

// ---------------------------------------------------------------------------
// Alias: getDueTriggers → getDueTriggersWithPersona
// ---------------------------------------------------------------------------
export function getDueTriggers(db: Database.Database, batchSize?: number) {
  return getDueTriggersWithPersona(db, batchSize);
}

// ---------------------------------------------------------------------------
// updateTriggerHealth — update health_status and health_message columns
// ---------------------------------------------------------------------------
export function updateTriggerHealth(db: Database.Database, id: string, status: string, message: string | null): void {
  const now = new Date().toISOString();
  stmt(db, 'UPDATE persona_triggers SET health_status = ?, health_message = ?, updated_at = ? WHERE id = ?')
    .run(status, message, now, id);
}

// ---------------------------------------------------------------------------
// getTriggerProjectId — look up a trigger's project_id
// ---------------------------------------------------------------------------
export function getTriggerProjectId(db: Database.Database, id: string): string | undefined {
  const row = stmt(db, 'SELECT project_id FROM persona_triggers WHERE id = ?').get(id) as { project_id: string } | undefined;
  return row?.project_id;
}

// ---------------------------------------------------------------------------
// updatePersona — partial update of a persona by ID
// ---------------------------------------------------------------------------
export function updatePersona(
  db: Database.Database,
  id: string,
  updates: Record<string, unknown>,
  scopedProjectId?: string,
): Persona | undefined {
  const persona = getPersona(db, id);
  if (!persona) return undefined;
  if (scopedProjectId && persona.projectId !== scopedProjectId) return undefined;

  const now = new Date().toISOString();
  const merged: Persona = {
    ...persona,
    name: (updates.name as string) ?? persona.name,
    description: updates.description !== undefined ? (updates.description as string | null) : persona.description,
    systemPrompt: (updates.systemPrompt as string) ?? persona.systemPrompt,
    structuredPrompt: updates.structuredPrompt !== undefined ? (updates.structuredPrompt as string | null) : persona.structuredPrompt,
    icon: updates.icon !== undefined ? (updates.icon as string | null) : persona.icon,
    color: updates.color !== undefined ? (updates.color as string | null) : persona.color,
    enabled: updates.enabled !== undefined ? (updates.enabled as boolean) : persona.enabled,
    maxConcurrent: (updates.maxConcurrent as number) ?? persona.maxConcurrent,
    timeoutMs: (updates.timeoutMs as number) ?? persona.timeoutMs,
    inferenceProfileId: updates.inferenceProfileId !== undefined ? (updates.inferenceProfileId as string | null) : persona.inferenceProfileId,
    networkPolicy: updates.networkPolicy !== undefined ? (updates.networkPolicy as string | null) : persona.networkPolicy,
    maxBudgetUsd: updates.maxBudgetUsd !== undefined ? (updates.maxBudgetUsd as number | null) : persona.maxBudgetUsd,
    maxTurns: updates.maxTurns !== undefined ? (updates.maxTurns as number | null) : persona.maxTurns,
    designContext: updates.designContext !== undefined ? (updates.designContext as string | null) : persona.designContext,
    groupId: updates.groupId !== undefined ? (updates.groupId as string | null) : persona.groupId,
    permissionPolicy: updates.permissionPolicy !== undefined ? (updates.permissionPolicy as string | null) : persona.permissionPolicy,
    webhookSecret: updates.webhookSecret !== undefined ? (updates.webhookSecret as string | null) : persona.webhookSecret,
    modelProfile: updates.modelProfile !== undefined ? (updates.modelProfile as string | null) : persona.modelProfile,
    updatedAt: now,
  };
  upsertPersona(db, merged);
  return merged;
}

// ---------------------------------------------------------------------------
// Event audit log — batch and single append, plus read
// ---------------------------------------------------------------------------

export interface EventAuditEntry {
  eventId: string;
  action: string;
  detail?: string | null;
}

export function batchAppendEventAudit(db: Database.Database, entries: EventAuditEntry[]): void {
  if (entries.length === 0) return;
  const now = new Date().toISOString();
  const insert = stmt(db, 'INSERT INTO event_audit (event_id, action, detail, created_at) VALUES (?, ?, ?, ?)');
  for (const entry of entries) {
    insert.run(entry.eventId, entry.action, entry.detail ?? null, now);
  }
}

export function appendEventAudit(db: Database.Database, entry: EventAuditEntry): void {
  batchAppendEventAudit(db, [entry]);
}

export function getEventAuditLog(db: Database.Database, eventId: string): Array<{ action: string; detail: string | null; createdAt: string }> {
  const rows = stmt(db, 'SELECT action, detail, created_at FROM event_audit WHERE event_id = ? ORDER BY created_at ASC').all(eventId) as Array<{
    action: string;
    detail: string | null;
    created_at: string;
  }>;
  return rows.map(r => ({ action: r.action, detail: r.detail, createdAt: r.created_at }));
}

// ---------------------------------------------------------------------------
// scheduleEventRetry — wraps deferEventForRetry with explicit delay and error message
// ---------------------------------------------------------------------------
export function scheduleEventRetry(db: Database.Database, eventId: string, retryCount: number, delayMs: number, errorMessage?: string): void {
  const retryAfter = new Date(Date.now() + delayMs).toISOString();
  stmt(db,
    "UPDATE persona_events SET status = 'pending', retry_after = ?, retry_count = ?, error_message = ?, processed_at = NULL WHERE id = ?",
  ).run(retryAfter, retryCount + 1, errorMessage ?? null, eventId);
}

// ---------------------------------------------------------------------------
// moveEventToDeadLetter — marks an event as permanently failed (dead letter)
// ---------------------------------------------------------------------------
export function moveEventToDeadLetter(db: Database.Database, eventId: string, errorMessage?: string): void {
  const now = new Date().toISOString();
  stmt(db,
    "UPDATE persona_events SET status = 'dead_letter', error_message = ?, processed_at = ? WHERE id = ?",
  ).run(errorMessage ?? null, now, eventId);
}

// ---------------------------------------------------------------------------
// Event dispatch idempotency tracking
// ---------------------------------------------------------------------------

export function hasEventBeenDispatched(db: Database.Database, eventId: string, personaId: string): boolean {
  const row = stmt(db, 'SELECT 1 FROM event_dispatches WHERE event_id = ? AND persona_id = ?').get(eventId, personaId);
  return row !== undefined;
}

/**
 * Record that an event has been dispatched to a persona. Returns true if the
 * row was newly inserted (i.e., not a duplicate). Returns false if a row
 * already existed (race condition / duplicate).
 */
export function recordEventDispatch(db: Database.Database, eventId: string, personaId: string, executionId: string): boolean {
  const now = new Date().toISOString();
  try {
    stmt(db, 'INSERT INTO event_dispatches (event_id, persona_id, execution_id, created_at) VALUES (?, ?, ?, ?)').run(eventId, personaId, executionId, now);
    return true;
  } catch {
    // UNIQUE constraint violation means it was already dispatched
    return false;
  }
}

export function clearEventDispatches(db: Database.Database, eventId: string): void {
  stmt(db, 'DELETE FROM event_dispatches WHERE event_id = ?').run(eventId);
}

// ---------------------------------------------------------------------------
// getTriggerFiringByEventId — find a trigger_fire row by its event_id
// ---------------------------------------------------------------------------
export function getTriggerFiringByEventId(db: Database.Database, eventId: string): TriggerFire | undefined {
  const row = stmt(db, 'SELECT * FROM trigger_fires WHERE event_id = ? LIMIT 1').get(eventId) as Record<string, unknown> | undefined;
  if (!row) return undefined;
  return {
    id: row.id as string,
    triggerId: row.trigger_id as string,
    eventId: (row.event_id as string) ?? null,
    executionId: (row.execution_id as string) ?? null,
    status: row.status as TriggerFireStatus,
    durationMs: (row.duration_ms as number) ?? null,
    errorMessage: (row.error_message as string) ?? null,
    firedAt: row.fired_at as string,
  };
}

// ---------------------------------------------------------------------------
// updateTriggerFiringDispatched — link a trigger firing to its execution
// ---------------------------------------------------------------------------
export function updateTriggerFiringDispatched(db: Database.Database, firingId: string, executionId: string): void {
  updateTriggerFire(db, firingId, { executionId });
}

// ---------------------------------------------------------------------------
// getPersonaValidationData — fetch all related data for a persona validation check
// ---------------------------------------------------------------------------
export function getPersonaValidationData(db: Database.Database, personaId: string): {
  tools: PersonaToolDefinition[];
  credentials: PersonaCredential[];
  subscriptions: PersonaEventSubscription[];
  triggers: PersonaTrigger[];
  deployment: CloudDeployment | undefined;
} {
  return {
    tools: getToolsForPersona(db, personaId),
    credentials: listCredentialsForPersona(db, personaId),
    subscriptions: listSubscriptionsForPersona(db, personaId),
    triggers: listTriggersForPersona(db, personaId),
    deployment: getDeploymentByPersona(db, personaId),
  };
}

// ---------------------------------------------------------------------------
// getExecutionStats — aggregate execution statistics
// ---------------------------------------------------------------------------
export function getExecutionStats(db: Database.Database, opts?: {
  personaId?: string;
  projectId?: string;
  periodDays?: number;
}): {
  total: number;
  queued: number;
  running: number;
  completed: number;
  failed: number;
  cancelled: number;
  avgDurationMs: number | null;
  totalCostUsd: number;
} {
  const clauses: string[] = [];
  const params: unknown[] = [];

  if (opts?.personaId) {
    clauses.push('persona_id = ?');
    params.push(opts.personaId);
  }
  if (opts?.projectId) {
    clauses.push('project_id = ?');
    params.push(opts.projectId);
  }
  if (opts?.periodDays) {
    const cutoff = new Date(Date.now() - opts.periodDays * 86400000).toISOString();
    clauses.push('created_at >= ?');
    params.push(cutoff);
  }

  const where = clauses.length > 0 ? `WHERE ${clauses.join(' AND ')}` : '';
  const row = stmt(db, `
    SELECT
      COUNT(*) AS total,
      SUM(CASE WHEN status = 'queued' THEN 1 ELSE 0 END) AS queued,
      SUM(CASE WHEN status = 'running' THEN 1 ELSE 0 END) AS running,
      SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS completed,
      SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed,
      SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled,
      AVG(CASE WHEN status = 'completed' THEN duration_ms ELSE NULL END) AS avg_duration_ms,
      COALESCE(SUM(cost_usd), 0) AS total_cost_usd
    FROM persona_executions ${where}
  `).get(...params) as Record<string, unknown>;

  return {
    total: (row.total as number) ?? 0,
    queued: (row.queued as number) ?? 0,
    running: (row.running as number) ?? 0,
    completed: (row.completed as number) ?? 0,
    failed: (row.failed as number) ?? 0,
    cancelled: (row.cancelled as number) ?? 0,
    avgDurationMs: row.avg_duration_ms != null ? Math.round(row.avg_duration_ms as number) : null,
    totalCostUsd: (row.total_cost_usd as number) ?? 0,
  };
}

// ---------------------------------------------------------------------------
// listToolDefinitions — list all tool definitions in the database
// ---------------------------------------------------------------------------
export function listToolDefinitions(db: Database.Database): PersonaToolDefinition[] {
  const rows = stmt(db, 'SELECT * FROM persona_tool_definitions ORDER BY name').all() as Record<string, unknown>[];
  return rows.map(r => ({
    id: r.id as string,
    name: r.name as string,
    category: r.category as string,
    description: r.description as string,
    scriptPath: (r.script_path as string) ?? '',
    inputSchema: (r.input_schema as string) ?? null,
    outputSchema: (r.output_schema as string) ?? null,
    requiresCredentialType: (r.requires_credential_type as string) ?? null,
    implementationGuide: (r.implementation_guide as string) ?? null,
    isBuiltin: r.is_builtin === 1,
    createdAt: r.created_at as string,
    updatedAt: r.updated_at as string,
  }));
}
