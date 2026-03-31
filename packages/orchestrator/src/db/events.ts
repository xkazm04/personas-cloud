import Database from 'better-sqlite3';
import { nanoid } from 'nanoid';
import type { PersonaEvent, PersonaEventSubscription, EventStatus } from '@dac-cloud/shared';
import { validateEventTransition } from '@dac-cloud/shared';
import { stmt, createRowMapper, buildWhereClause, mergeEnabled, mergeUpdate } from './_helpers.js';

// ---------------------------------------------------------------------------
// Row mappers
// ---------------------------------------------------------------------------

const rowToEvent = createRowMapper<PersonaEvent>({
  id: { col: 'id' },
  projectId: { col: 'project_id' },
  eventType: { col: 'event_type' },
  sourceType: { col: 'source_type' },
  sourceId: { col: 'source_id', nullable: true },
  targetPersonaId: { col: 'target_persona_id', nullable: true },
  payload: { col: 'payload', nullable: true },
  status: { col: 'status' },
  errorMessage: { col: 'error_message', nullable: true },
  processedAt: { col: 'processed_at', nullable: true },
  useCaseId: { col: 'use_case_id', nullable: true },
  retryCount: { col: 'retry_count', default: 0 },
  nextRetryAt: { col: 'retry_after', nullable: true },
  createdAt: { col: 'created_at' },
});

export const rowToSubscription = createRowMapper<PersonaEventSubscription>({
  id: { col: 'id' },
  personaId: { col: 'persona_id' },
  eventType: { col: 'event_type' },
  sourceFilter: { col: 'source_filter', nullable: true },
  payloadFilter: { col: 'payload_filter', nullable: true },
  enabled: { col: 'enabled', bool: true },
  maxRetries: { col: 'max_retries', default: 3 },
  retryBackoffMs: { col: 'retry_backoff_ms', default: 5000 },
  useCaseId: { col: 'use_case_id', nullable: true },
  createdAt: { col: 'created_at' },
  updatedAt: { col: 'updated_at' },
});

// ---------------------------------------------------------------------------
// Events CRUD
// ---------------------------------------------------------------------------

export function publishEvent(db: Database.Database, input: {
  eventType: string; sourceType: string; sourceId?: string | null; targetPersonaId?: string | null;
  projectId?: string; payload?: string | null; useCaseId?: string | null;
}): PersonaEvent {
  const now = new Date().toISOString();
  const id = nanoid();
  const projectId = input.projectId ?? 'default';
  const sourceId = input.sourceId ?? null;
  const targetPersonaId = input.targetPersonaId ?? null;
  const payload = input.payload ?? null;
  const useCaseId = input.useCaseId ?? null;
  stmt(db,`
    INSERT INTO persona_events (id, project_id, event_type, source_type, source_id, target_persona_id, payload, use_case_id, status, created_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?)
  `).run(id, projectId, input.eventType, input.sourceType, sourceId, targetPersonaId, payload, useCaseId, now);
  return {
    id,
    projectId,
    eventType: input.eventType,
    sourceType: input.sourceType,
    sourceId,
    targetPersonaId,
    payload,
    status: 'pending',
    errorMessage: null,
    processedAt: null,
    useCaseId,
    retryCount: 0,
    nextRetryAt: null,
    createdAt: now,
  };
}

export function getEvent(db: Database.Database, id: string): PersonaEvent | undefined {
  const row = stmt(db,'SELECT * FROM persona_events WHERE id = ?').get(id) as Record<string, unknown> | undefined;
  return row ? rowToEvent(row) : undefined;
}

export function getPendingEvents(db: Database.Database, limit: number = 50): PersonaEvent[] {
  const rows = stmt(db,
    "SELECT * FROM persona_events WHERE status IN ('pending', 'partial-retry') AND (retry_after IS NULL OR retry_after <= datetime('now')) ORDER BY created_at LIMIT ?",
  ).all(limit) as Record<string, unknown>[];
  return rows.map(rowToEvent);
}

export function listEvents(db: Database.Database, opts?: {
  eventType?: string; status?: EventStatus; projectId?: string; limit?: number; offset?: number;
}): PersonaEvent[] {
  const { sql: where, params } = buildWhereClause(
    { eventType: opts?.eventType, status: opts?.status, projectId: opts?.projectId },
    { eventType: 'event_type', status: 'status', projectId: 'project_id' },
  );
  const limit = opts?.limit ?? 50;
  const offset = opts?.offset ?? 0;
  const rows = stmt(db,`SELECT * FROM persona_events ${where} ORDER BY created_at DESC LIMIT ? OFFSET ?`).all(...params, limit, offset) as Record<string, unknown>[];
  return rows.map(rowToEvent);
}

export function updateEventStatus(db: Database.Database, id: string, status: EventStatus, errorMessage?: string | null): void {
  // Look up current status to validate the transition
  const row = stmt(db, 'SELECT status FROM persona_events WHERE id = ?').get(id) as { status: string } | undefined;
  if (row) {
    validateEventTransition(row.status as EventStatus, status);
  }
  const now = new Date().toISOString();
  stmt(db,'UPDATE persona_events SET status = ?, error_message = ?, processed_at = ? WHERE id = ?').run(status, errorMessage ?? null, now, id);
}

/**
 * Reset events stuck in 'processing' state back to 'pending'.
 * Events with processed_at older than maxAgeSeconds are considered stale (orphaned by a crash).
 * Returns the number of events reset.
 */
export function resetStaleProcessingEvents(db: Database.Database, maxAgeSeconds: number): number {
  const cutoff = new Date(Date.now() - maxAgeSeconds * 1000).toISOString();
  const result = stmt(db,
    "UPDATE persona_events SET status = 'pending', processed_at = NULL WHERE status = 'processing' AND processed_at < ?",
  ).run(cutoff);
  return result.changes;
}

/**
 * Reset ALL events in 'processing' state back to 'pending'.
 * Safe to call at startup when no event processor is running.
 * Returns the number of events reset.
 */
export function resetAllProcessingEvents(db: Database.Database): number {
  const result = stmt(db,
    "UPDATE persona_events SET status = 'pending', processed_at = NULL WHERE status = 'processing'",
  ).run();
  return result.changes;
}

export function updateEventWithMetadata(db: Database.Database, id: string, status: EventStatus, metadata?: string | null): PersonaEvent | undefined {
  // Look up current status to validate the transition
  const row = stmt(db, 'SELECT status FROM persona_events WHERE id = ?').get(id) as { status: string } | undefined;
  if (row) {
    validateEventTransition(row.status as EventStatus, status);
  }
  const now = new Date().toISOString();
  stmt(db,'UPDATE persona_events SET status = ?, processed_at = ?, payload = CASE WHEN ? IS NOT NULL THEN ? ELSE payload END WHERE id = ?')
    .run(status, now, metadata, metadata, id);
  return getEvent(db, id);
}

/**
 * Defer an event for retry with exponential backoff.
 * Sets retry_after = now + backoff, increments retry_count, and resets status to 'pending'.
 * Backoff: min(5 * 2^retryCount, 60) seconds.
 */
export function deferEventForRetry(db: Database.Database, id: string, currentRetryCount: number): void {
  const backoffSeconds = Math.min(5 * Math.pow(2, currentRetryCount), 60);
  const retryAfter = new Date(Date.now() + backoffSeconds * 1000).toISOString();
  stmt(db,
    "UPDATE persona_events SET status = 'pending', retry_after = ?, retry_count = ?, processed_at = NULL WHERE id = ?",
  ).run(retryAfter, currentRetryCount + 1, id);
}

export function countPendingEvents(db: Database.Database): number {
  const row = stmt(db,
    "SELECT COUNT(*) as cnt FROM persona_events WHERE status IN ('pending', 'partial-retry') AND (retry_after IS NULL OR retry_after <= datetime('now'))",
  ).get() as { cnt: number };
  return row.cnt;
}

export function getOldestPendingEventCreatedAt(db: Database.Database): string | null {
  const row = stmt(db,
    "SELECT created_at FROM persona_events WHERE status IN ('pending', 'partial-retry') AND (retry_after IS NULL OR retry_after <= datetime('now')) ORDER BY created_at ASC LIMIT 1",
  ).get() as { created_at: string } | undefined;
  return row?.created_at ?? null;
}

// ---------------------------------------------------------------------------
// Event Subscriptions CRUD
// ---------------------------------------------------------------------------

export function createSubscription(db: Database.Database, input: {
  personaId: string; eventType: string; sourceFilter?: string | null; enabled?: boolean; useCaseId?: string | null; projectId?: string;
}): PersonaEventSubscription {
  const now = new Date().toISOString();
  const id = nanoid();
  const sourceFilter = input.sourceFilter ?? null;
  const enabled = input.enabled !== false;
  const useCaseId = input.useCaseId ?? null;
  stmt(db,`
    INSERT INTO persona_event_subscriptions (id, project_id, persona_id, event_type, source_filter, enabled, use_case_id, created_at, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
  `).run(id, input.projectId ?? 'default', input.personaId, input.eventType, sourceFilter, enabled ? 1 : 0, useCaseId, now, now);
  return {
    id,
    personaId: input.personaId,
    eventType: input.eventType,
    sourceFilter,
    payloadFilter: null,
    enabled,
    maxRetries: 3,
    retryBackoffMs: 5000,
    useCaseId,
    createdAt: now,
    updatedAt: now,
  };
}

export function getSubscription(db: Database.Database, id: string): PersonaEventSubscription | undefined {
  const row = stmt(db,'SELECT * FROM persona_event_subscriptions WHERE id = ?').get(id) as Record<string, unknown> | undefined;
  return row ? rowToSubscription(row) : undefined;
}

export function listSubscriptionsForPersona(db: Database.Database, personaId: string): PersonaEventSubscription[] {
  const rows = stmt(db,'SELECT * FROM persona_event_subscriptions WHERE persona_id = ?').all(personaId) as Record<string, unknown>[];
  return rows.map(rowToSubscription);
}

export function getSubscriptionsByEventType(db: Database.Database, eventType: string, projectId?: string): PersonaEventSubscription[] {
  const { sql: where, params } = buildWhereClause(
    { eventType, projectId, enabled: 1 },
    { eventType: 'event_type', projectId: 'project_id', enabled: 'enabled' },
  );
  const rows = stmt(db, `SELECT * FROM persona_event_subscriptions ${where}`).all(...params) as Record<string, unknown>[];
  return rows.map(rowToSubscription);
}

export function updateSubscription(db: Database.Database, id: string, updates: {
  eventType?: string | null; sourceFilter?: string | null; enabled?: boolean | null;
}): void {
  const sub = getSubscription(db, id);
  if (!sub) return;
  mergeUpdate(db, 'persona_event_subscriptions', id, {
    event_type: updates.eventType ?? sub.eventType,
    source_filter: updates.sourceFilter !== undefined ? updates.sourceFilter : sub.sourceFilter,
    enabled: mergeEnabled(updates.enabled, sub.enabled),
  });
}

export function deleteSubscription(db: Database.Database, id: string): void {
  stmt(db,'DELETE FROM persona_event_subscriptions WHERE id = ?').run(id);
}
