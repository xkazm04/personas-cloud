import Database from 'better-sqlite3';
import { nanoid } from 'nanoid';
import type { TriggerFire, TriggerFireStatus, TriggerStats } from '@dac-cloud/shared';
import { stmt, createRowMapper } from './_helpers.js';

// ---------------------------------------------------------------------------
// Row mapper
// ---------------------------------------------------------------------------

const rowToTriggerFire = createRowMapper<TriggerFire>({
  id: { col: 'id' },
  triggerId: { col: 'trigger_id' },
  eventId: { col: 'event_id', nullable: true },
  executionId: { col: 'execution_id', nullable: true },
  status: { col: 'status' },
  durationMs: { col: 'duration_ms', nullable: true },
  errorMessage: { col: 'error_message', nullable: true },
  firedAt: { col: 'fired_at' },
});

// ---------------------------------------------------------------------------
// Write
// ---------------------------------------------------------------------------

export function recordTriggerFire(db: Database.Database, input: {
  triggerId: string;
  eventId?: string | null;
  status?: TriggerFireStatus;
}): TriggerFire {
  const id = nanoid();
  const now = new Date().toISOString();
  const status = input.status ?? 'fired';
  const eventId = input.eventId ?? null;
  stmt(db, `
    INSERT INTO trigger_fires (id, trigger_id, event_id, status, fired_at)
    VALUES (?, ?, ?, ?, ?)
  `).run(id, input.triggerId, eventId, status, now);
  return {
    id,
    triggerId: input.triggerId,
    eventId,
    executionId: null,
    status,
    durationMs: null,
    errorMessage: null,
    firedAt: now,
  };
}

export function updateTriggerFire(db: Database.Database, id: string, updates: {
  executionId?: string;
  status?: TriggerFireStatus;
  durationMs?: number;
  errorMessage?: string;
}): void {
  const setClauses: string[] = [];
  const values: unknown[] = [];
  if (updates.executionId !== undefined) { setClauses.push('execution_id = ?'); values.push(updates.executionId); }
  if (updates.status !== undefined) { setClauses.push('status = ?'); values.push(updates.status); }
  if (updates.durationMs !== undefined) { setClauses.push('duration_ms = ?'); values.push(updates.durationMs); }
  if (updates.errorMessage !== undefined) { setClauses.push('error_message = ?'); values.push(updates.errorMessage); }
  if (setClauses.length === 0) return;
  stmt(db, `UPDATE trigger_fires SET ${setClauses.join(', ')} WHERE id = ?`).run(...values, id);
}

// ---------------------------------------------------------------------------
// Read — history
// ---------------------------------------------------------------------------

export function listTriggerFires(db: Database.Database, triggerId: string, limit: number = 50): TriggerFire[] {
  const rows = stmt(db, `
    SELECT * FROM trigger_fires
    WHERE trigger_id = ?
    ORDER BY fired_at DESC
    LIMIT ?
  `).all(triggerId, limit) as Record<string, unknown>[];
  return rows.map(rowToTriggerFire);
}

// ---------------------------------------------------------------------------
// Read — aggregate stats
// ---------------------------------------------------------------------------

export function getTriggerStats(db: Database.Database, triggerId: string): TriggerStats {
  const row = stmt(db, `
    SELECT
      COUNT(*) AS total_fires,
      SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS success_count,
      SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failure_count,
      AVG(CASE WHEN status = 'completed' THEN duration_ms ELSE NULL END) AS avg_duration_ms
    FROM trigger_fires
    WHERE trigger_id = ?
  `).get(triggerId) as Record<string, unknown> | undefined;

  const lastRow = stmt(db, `
    SELECT status, fired_at FROM trigger_fires
    WHERE trigger_id = ?
    ORDER BY fired_at DESC
    LIMIT 1
  `).get(triggerId) as Record<string, unknown> | undefined;

  const totalFires = (row?.total_fires as number) ?? 0;
  const successCount = (row?.success_count as number) ?? 0;
  const failureCount = (row?.failure_count as number) ?? 0;
  const avgDurationMs = row?.avg_duration_ms != null ? Math.round(row.avg_duration_ms as number) : null;

  return {
    totalFires,
    successCount,
    failureCount,
    successRate: totalFires > 0 ? Math.round((successCount / totalFires) * 10000) / 10000 : 0,
    avgDurationMs,
    lastFireStatus: (lastRow?.status as TriggerFireStatus) ?? null,
    lastFiredAt: (lastRow?.fired_at as string) ?? null,
  };
}
