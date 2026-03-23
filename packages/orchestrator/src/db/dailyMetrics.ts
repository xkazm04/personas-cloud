import Database from 'better-sqlite3';
import { stmt } from './_helpers.js';

// ---------------------------------------------------------------------------
// Daily metrics query types
// ---------------------------------------------------------------------------

export interface DailyMetricsRow {
  date: string;
  totalExecutions: number;
  completed: number;
  failed: number;
  cancelled: number;
  totalCostUsd: number;
  totalDurationMs: number;
  avgDurationMs: number | null;
  totalInputTokens: number;
  totalOutputTokens: number;
}

export interface DailyMetricsByPersonaRow extends DailyMetricsRow {
  personaId: string;
}

export interface DailyMetricsQueryOpts {
  from?: string;  // YYYY-MM-DD inclusive
  to?: string;    // YYYY-MM-DD inclusive
}

// ---------------------------------------------------------------------------
// Queries
// ---------------------------------------------------------------------------

export function queryDailyMetrics(
  db: Database.Database,
  opts?: DailyMetricsQueryOpts,
): DailyMetricsRow[] {
  const clauses: string[] = [];
  const params: unknown[] = [];

  if (opts?.from) {
    clauses.push('date >= ?');
    params.push(opts.from);
  }
  if (opts?.to) {
    clauses.push('date <= ?');
    params.push(opts.to);
  }

  const where = clauses.length > 0 ? `WHERE ${clauses.join(' AND ')}` : '';
  const rows = stmt(db, `
    SELECT date, total_executions, completed, failed, cancelled,
           total_cost_usd, total_duration_ms, total_input_tokens, total_output_tokens
    FROM daily_metrics ${where} ORDER BY date DESC
  `).all(...params) as Array<{
    date: string;
    total_executions: number;
    completed: number;
    failed: number;
    cancelled: number;
    total_cost_usd: number;
    total_duration_ms: number;
    total_input_tokens: number;
    total_output_tokens: number;
  }>;

  return rows.map(r => ({
    date: r.date,
    totalExecutions: r.total_executions,
    completed: r.completed,
    failed: r.failed,
    cancelled: r.cancelled,
    totalCostUsd: r.total_cost_usd,
    totalDurationMs: r.total_duration_ms,
    avgDurationMs: r.total_executions > 0 ? Math.round(r.total_duration_ms / r.total_executions) : null,
    totalInputTokens: r.total_input_tokens,
    totalOutputTokens: r.total_output_tokens,
  }));
}

export function queryDailyMetricsByPersona(
  db: Database.Database,
  opts?: DailyMetricsQueryOpts,
): DailyMetricsByPersonaRow[] {
  const clauses: string[] = [];
  const params: unknown[] = [];

  if (opts?.from) {
    clauses.push('date >= ?');
    params.push(opts.from);
  }
  if (opts?.to) {
    clauses.push('date <= ?');
    params.push(opts.to);
  }

  const where = clauses.length > 0 ? `WHERE ${clauses.join(' AND ')}` : '';
  const rows = stmt(db, `
    SELECT date, persona_id, total_executions, completed, failed, cancelled,
           total_cost_usd, total_duration_ms, total_input_tokens, total_output_tokens
    FROM daily_metrics_by_persona ${where} ORDER BY date DESC, persona_id
  `).all(...params) as Array<{
    date: string;
    persona_id: string;
    total_executions: number;
    completed: number;
    failed: number;
    cancelled: number;
    total_cost_usd: number;
    total_duration_ms: number;
    total_input_tokens: number;
    total_output_tokens: number;
  }>;

  return rows.map(r => ({
    date: r.date,
    personaId: r.persona_id,
    totalExecutions: r.total_executions,
    completed: r.completed,
    failed: r.failed,
    cancelled: r.cancelled,
    totalCostUsd: r.total_cost_usd,
    totalDurationMs: r.total_duration_ms,
    avgDurationMs: r.total_executions > 0 ? Math.round(r.total_duration_ms / r.total_executions) : null,
    totalInputTokens: r.total_input_tokens,
    totalOutputTokens: r.total_output_tokens,
  }));
}
