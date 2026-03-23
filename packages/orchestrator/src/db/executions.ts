import Database from 'better-sqlite3';
import { nanoid } from 'nanoid';
import type { PersonaExecution } from '@dac-cloud/shared';
import { stmt, createRowMapper, buildWhereClause } from './_helpers.js';

// ---------------------------------------------------------------------------
// Row mapper
// ---------------------------------------------------------------------------

const rowToExecution = createRowMapper<PersonaExecution>({
  id: { col: 'id' },
  projectId: { col: 'project_id', default: 'default' },
  personaId: { col: 'persona_id' },
  triggerId: { col: 'trigger_id', nullable: true },
  useCaseId: { col: 'use_case_id', nullable: true },
  eventId: { col: 'event_id', nullable: true },
  status: { col: 'status' },
  inputData: { col: 'input_data', nullable: true },
  outputData: { col: 'output_data', nullable: true },
  claudeSessionId: { col: 'claude_session_id', nullable: true },
  modelUsed: { col: 'model_used', nullable: true },
  inputTokens: { col: 'input_tokens', default: 0 },
  outputTokens: { col: 'output_tokens', default: 0 },
  costUsd: { col: 'cost_usd', default: 0 },
  errorMessage: { col: 'error_message', nullable: true },
  durationMs: { col: 'duration_ms', nullable: true },
  retryOfExecutionId: { col: 'retry_of_execution_id', nullable: true },
  retryCount: { col: 'retry_count', default: 0 },
  startedAt: { col: 'started_at', nullable: true },
  completedAt: { col: 'completed_at', nullable: true },
  createdAt: { col: 'created_at' },
});

// ---------------------------------------------------------------------------
// CRUD
// ---------------------------------------------------------------------------

export function createExecution(db: Database.Database, input: {
  id?: string; personaId: string; triggerId?: string | null; useCaseId?: string | null; eventId?: string | null; inputData?: string | null; projectId?: string;
}): PersonaExecution {
  const now = new Date().toISOString();
  const id = input.id ?? nanoid();
  const projectId = input.projectId ?? 'default';
  const triggerId = input.triggerId ?? null;
  const useCaseId = input.useCaseId ?? null;
  const eventId = input.eventId ?? null;
  const inputData = input.inputData ?? null;
  stmt(db,`
    INSERT INTO persona_executions (id, project_id, persona_id, trigger_id, use_case_id, event_id, status, input_data, created_at)
    VALUES (?, ?, ?, ?, ?, ?, 'queued', ?, ?)
  `).run(id, projectId, input.personaId, triggerId, useCaseId, eventId, inputData, now);
  return {
    id,
    projectId,
    personaId: input.personaId,
    triggerId,
    useCaseId,
    eventId,
    status: 'queued',
    inputData,
    outputData: null,
    claudeSessionId: null,
    modelUsed: null,
    inputTokens: 0,
    outputTokens: 0,
    costUsd: 0,
    errorMessage: null,
    durationMs: null,
    retryOfExecutionId: null,
    retryCount: 0,
    startedAt: null,
    completedAt: null,
    createdAt: now,
  };
}

export function getExecution(db: Database.Database, id: string): PersonaExecution | undefined {
  const row = stmt(db,'SELECT * FROM persona_executions WHERE id = ?').get(id) as Record<string, unknown> | undefined;
  return row ? rowToExecution(row) : undefined;
}

export function listExecutions(db: Database.Database, opts?: {
  personaId?: string; status?: string; limit?: number; offset?: number; projectId?: string;
}): PersonaExecution[] {
  const { sql: where, params } = buildWhereClause(
    { projectId: opts?.projectId, personaId: opts?.personaId, status: opts?.status },
    { projectId: 'project_id', personaId: 'persona_id', status: 'status' },
  );
  const limit = opts?.limit ?? 50;
  const offset = opts?.offset ?? 0;
  const rows = stmt(db,`SELECT * FROM persona_executions ${where} ORDER BY created_at DESC LIMIT ? OFFSET ?`).all(...params, limit, offset) as Record<string, unknown>[];
  return rows.map(rowToExecution);
}

const TERMINAL_STATUSES = new Set(['completed', 'failed', 'cancelled']);

export function updateExecution(db: Database.Database, id: string, updates: Record<string, unknown>): void {
  const fields: string[] = [];
  const values: unknown[] = [];

  const columnMap: Record<string, string> = {
    status: 'status',
    outputData: 'output_data',
    claudeSessionId: 'claude_session_id',
    modelUsed: 'model_used',
    inputTokens: 'input_tokens',
    outputTokens: 'output_tokens',
    costUsd: 'cost_usd',
    errorMessage: 'error_message',
    durationMs: 'duration_ms',
    startedAt: 'started_at',
    completedAt: 'completed_at',
  };

  for (const [key, col] of Object.entries(columnMap)) {
    if (key in updates) {
      fields.push(`${col} = ?`);
      values.push(updates[key]);
    }
  }

  if (fields.length === 0) return;

  const newStatus = updates.status as string | undefined;
  let shouldRecordMetrics = false;
  let createdAt: string | undefined;

  // Check if transitioning to a terminal status to avoid double-counting
  if (newStatus && TERMINAL_STATUSES.has(newStatus)) {
    const current = stmt(db, 'SELECT status, created_at FROM persona_executions WHERE id = ?').get(id) as
      { status: string; created_at: string } | undefined;
    if (current && !TERMINAL_STATUSES.has(current.status)) {
      shouldRecordMetrics = true;
      createdAt = current.created_at;
    }
  }

  values.push(id);
  stmt(db,`UPDATE persona_executions SET ${fields.join(', ')} WHERE id = ?`).run(...values);

  // Incrementally update daily_metrics on terminal status transition
  if (shouldRecordMetrics && createdAt) {
    const date = createdAt.slice(0, 10); // YYYY-MM-DD
    const costUsd = (updates.costUsd as number) ?? 0;
    const durationMs = (updates.durationMs as number) ?? 0;
    const inputTokens = (updates.inputTokens as number) ?? 0;
    const outputTokens = (updates.outputTokens as number) ?? 0;
    const isCompleted = newStatus === 'completed' ? 1 : 0;
    const isFailed = newStatus === 'failed' ? 1 : 0;
    const isCancelled = newStatus === 'cancelled' ? 1 : 0;

    // Aggregate daily totals
    stmt(db, `
      INSERT INTO daily_metrics (date, total_executions, completed, failed, cancelled, total_cost_usd, total_duration_ms, total_input_tokens, total_output_tokens)
      VALUES (?, 1, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(date) DO UPDATE SET
        total_executions = total_executions + 1,
        completed = completed + excluded.completed,
        failed = failed + excluded.failed,
        cancelled = cancelled + excluded.cancelled,
        total_cost_usd = total_cost_usd + excluded.total_cost_usd,
        total_duration_ms = total_duration_ms + excluded.total_duration_ms,
        total_input_tokens = total_input_tokens + excluded.total_input_tokens,
        total_output_tokens = total_output_tokens + excluded.total_output_tokens
    `).run(date, isCompleted, isFailed, isCancelled, costUsd, durationMs, inputTokens, outputTokens);

    // Per-persona breakdown
    const personaId = (stmt(db, 'SELECT persona_id FROM persona_executions WHERE id = ?').get(id) as { persona_id: string } | undefined)?.persona_id;
    if (personaId) {
      stmt(db, `
        INSERT INTO daily_metrics_by_persona (date, persona_id, total_executions, completed, failed, cancelled, total_cost_usd, total_duration_ms, total_input_tokens, total_output_tokens)
        VALUES (?, ?, 1, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(date, persona_id) DO UPDATE SET
          total_executions = total_executions + 1,
          completed = completed + excluded.completed,
          failed = failed + excluded.failed,
          cancelled = cancelled + excluded.cancelled,
          total_cost_usd = total_cost_usd + excluded.total_cost_usd,
          total_duration_ms = total_duration_ms + excluded.total_duration_ms,
          total_input_tokens = total_input_tokens + excluded.total_input_tokens,
          total_output_tokens = total_output_tokens + excluded.total_output_tokens
      `).run(date, personaId, isCompleted, isFailed, isCancelled, costUsd, durationMs, inputTokens, outputTokens);
    }
  }
}

export function appendOutput(db: Database.Database, id: string, chunk: string): void {
  stmt(db,`
    UPDATE persona_executions SET output_data = COALESCE(output_data, '') || ? WHERE id = ?
  `).run(chunk, id);
}

export function countRunningExecutions(db: Database.Database, personaId: string): number {
  const row = stmt(db,"SELECT COUNT(*) as cnt FROM persona_executions WHERE persona_id = ? AND status = 'running'").get(personaId) as { cnt: number };
  return row.cnt;
}

/** Batch-count running executions for multiple personas. Returns a Map keyed by persona ID. */
export function countRunningExecutionsByPersonaIds(db: Database.Database, personaIds: string[]): Map<string, number> {
  const map = new Map<string, number>();
  if (personaIds.length === 0) return map;
  const placeholders = personaIds.map(() => '?').join(',');
  const rows = db.prepare(
    `SELECT persona_id, COUNT(*) as cnt FROM persona_executions WHERE persona_id IN (${placeholders}) AND status = 'running' GROUP BY persona_id`
  ).all(...personaIds) as Array<{ persona_id: string; cnt: number }>;
  for (const row of rows) {
    map.set(row.persona_id, row.cnt);
  }
  return map;
}
