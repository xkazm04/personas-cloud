import type Database from 'better-sqlite3';
import type { Logger } from 'pino';
import type { TenantDbManager } from './tenantDbManager.js';
import { RetentionPolicy } from './retentionPolicy.js';

// ── Declarative retention policies for tenant tables ────────────────────
// Each policy is parameterised by its TABLE_REGISTRY spec; the shared
// RetentionPolicy class handles safety-fraction checks and chunked deletes.

const execPolicy = new RetentionPolicy({
  table: 'persona_executions',
  deleteFilter: "status IN ('completed', 'failed', 'cancelled')",
});

const eventsPolicy = new RetentionPolicy({
  table: 'persona_events',
  deleteFilter: "status NOT IN ('pending', 'processing', 'partial-retry')",
});

const stuckEventsPolicy = new RetentionPolicy({
  table: 'persona_events',
  deleteFilter: "status IN ('pending', 'processing', 'partial-retry')",
});

const dailyMetricsPolicy = new RetentionPolicy({ table: 'daily_metrics' });
const dailyMetricsByPersonaPolicy = new RetentionPolicy({ table: 'daily_metrics_by_persona' });
const systemDailyMetricsPolicy = new RetentionPolicy({ table: 'system_daily_metrics' });

/** Yield the event loop so concurrent reads/writes can proceed between tenants. */
function yieldTick(): Promise<void> {
  return new Promise((resolve) => setImmediate(resolve));
}

/** How often we check whether it's time for the daily run (60 seconds). */
const SCHEDULE_CHECK_MS = 60_000;

/** system_kv keys used to persist retention job state. */
const LAST_RUN_KEY = 'retention_last_run_date';
const LAST_RUN_AT_KEY = 'retention_last_run_at';

/** Return today's UTC date as YYYY-MM-DD. */
function utcToday(): string {
  return new Date().toISOString().slice(0, 10);
}

/** Read the last-run UTC date from system_kv, or null if never run. */
function getLastRunDate(tenantDbManager: TenantDbManager): string | null {
  const row = tenantDbManager.getSystemDb().prepare(
    'SELECT value FROM system_kv WHERE key = ?',
  ).get(LAST_RUN_KEY) as { value: string } | undefined;
  return row?.value ?? null;
}

/** Persist the current UTC date as the last-run date. */
function setLastRunDate(tenantDbManager: TenantDbManager, date: string): void {
  tenantDbManager.getSystemDb().prepare(
    'INSERT INTO system_kv (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value',
  ).run(LAST_RUN_KEY, date);
}

/** Persist the ISO timestamp of the last successful run. */
function setLastRunAt(tenantDbManager: TenantDbManager, isoTimestamp: string): void {
  tenantDbManager.getSystemDb().prepare(
    'INSERT INTO system_kv (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value',
  ).run(LAST_RUN_AT_KEY, isoTimestamp);
}

/** Read the ISO timestamp of the last successful run, or null. */
function getLastRunAt(tenantDbManager: TenantDbManager): string | null {
  const row = tenantDbManager.getSystemDb().prepare(
    'SELECT value FROM system_kv WHERE key = ?',
  ).get(LAST_RUN_AT_KEY) as { value: string } | undefined;
  return row?.value ?? null;
}

// ── Structured metrics ──────────────────────────────────────────────────

/** Health snapshot returned by getHealth(). */
export interface RetentionHealth {
  lastRunAt: string | null;
  lastRunDurationMs: number | null;
  retentionDays: number;
  metricsRetentionDays: number;
  totalRuns: number;
  totalErrors: number;
  running: boolean;
  lastRun: RetentionRunMetrics | null;
}

/** Per-run structured metrics emitted as a log entry and cached in memory. */
export interface RetentionRunMetrics {
  tenantsProcessed: number;
  tenantErrors: number;
  rowsDeleted: {
    persona_executions: number;
    persona_events: number;
    persona_events_stuck: number;
    daily_metrics: number;
    daily_metrics_by_persona: number;
    system_daily_metrics: number;
  };
  retentionDays: number;
  metricsRetentionDays: number;
  cutoff: string;
  stuckCutoff: string;
  metricsCutoff: string;
  durationMs: number;
  errors: string[];
}

/** Handle returned by startRetentionJob. */
export interface RetentionJobHandle {
  timer: NodeJS.Timeout;
  getHealth: () => RetentionHealth;
}

/**
 * Daily retention job: purge old executions/events and aggregate daily metrics.
 *
 * Uses a 60-second wall-clock poll instead of a drifting 24-hour setInterval.
 * The last successful run date is persisted in system_kv so the job runs
 * exactly once per UTC calendar day regardless of restarts or event-loop delays.
 */
export function startRetentionJob(
  tenantDbManager: TenantDbManager,
  retentionDays: number,
  metricsRetentionDays: number,
  logger: Logger,
): RetentionJobHandle {
  logger.info({ retentionDays, metricsRetentionDays }, 'Retention job scheduled (wall-clock daily)');

  let running = false;
  let totalRuns = 0;
  let totalErrors = 0;
  let lastRunMetrics: RetentionRunMetrics | null = null;
  let lastRunDurationMs: number | null = null;

  // Seed lastRunAt from persisted value so the health endpoint is informative
  // even before the first run of this process.
  let lastRunAt: string | null = getLastRunAt(tenantDbManager);

  async function tick(): Promise<void> {
    if (running) return;

    const today = utcToday();
    const lastRun = getLastRunDate(tenantDbManager);

    if (lastRun === today) return; // already ran today

    running = true;
    logger.info({ today, lastRun }, 'Retention job triggered');
    const t0 = Date.now();
    try {
      const metrics = await runRetention(tenantDbManager, retentionDays, metricsRetentionDays, logger);
      const durationMs = Date.now() - t0;
      metrics.durationMs = durationMs;

      lastRunMetrics = metrics;
      lastRunDurationMs = durationMs;
      totalRuns++;
      if (metrics.tenantErrors > 0) totalErrors += metrics.tenantErrors;

      const now = new Date().toISOString();
      lastRunAt = now;
      setLastRunDate(tenantDbManager, today);
      setLastRunAt(tenantDbManager, now);

      logger.info({
        component: 'retention',
        event: 'retention_run_complete',
        durationMs,
        retentionDays,
        metricsRetentionDays,
        tenantsProcessed: metrics.tenantsProcessed,
        tenantErrors: metrics.tenantErrors,
        rowsDeleted: metrics.rowsDeleted,
        cutoff: metrics.cutoff,
        stuckCutoff: metrics.stuckCutoff,
        metricsCutoff: metrics.metricsCutoff,
        errors: metrics.errors.length > 0 ? metrics.errors : undefined,
      }, 'Retention job completed');
    } catch (err) {
      const durationMs = Date.now() - t0;
      totalErrors++;

      logger.error({
        component: 'retention',
        event: 'retention_run_failed',
        err,
        durationMs,
        retentionDays,
        metricsRetentionDays,
      }, 'Retention job failed — will retry next tick');
    } finally {
      running = false;
    }
  }

  // First check shortly after startup, then every 60 seconds
  setTimeout(() => tick(), 5000);

  const timer = setInterval(() => {
    tick();
  }, SCHEDULE_CHECK_MS);

  function getHealth(): RetentionHealth {
    return {
      lastRunAt,
      lastRunDurationMs,
      retentionDays,
      metricsRetentionDays,
      totalRuns,
      totalErrors,
      running,
      lastRun: lastRunMetrics,
    };
  }

  return { timer, getHealth };
}

async function runRetention(
  tenantDbManager: TenantDbManager,
  retentionDays: number,
  metricsRetentionDays: number,
  logger: Logger,
): Promise<RetentionRunMetrics> {
  const cutoff = new Date(Date.now() - retentionDays * 86_400_000).toISOString();
  const stuckCutoff = new Date(Date.now() - retentionDays * 7 * 86_400_000).toISOString();
  const metricsCutoff = new Date(Date.now() - metricsRetentionDays * 86_400_000).toISOString().slice(0, 10);

  const metrics: RetentionRunMetrics = {
    tenantsProcessed: 0,
    tenantErrors: 0,
    rowsDeleted: {
      persona_executions: 0,
      persona_events: 0,
      persona_events_stuck: 0,
      daily_metrics: 0,
      daily_metrics_by_persona: 0,
      system_daily_metrics: 0,
    },
    retentionDays,
    metricsRetentionDays,
    cutoff,
    stuckCutoff,
    metricsCutoff,
    durationMs: 0, // filled in by caller
    errors: [],
  };

  const tenantIds = tenantDbManager.listTenantIds();
  for (const projectId of tenantIds) {
    try {
      const db = tenantDbManager.getTenantDb(projectId);
      const ctx = { projectId };

      const purgedExecutions = await execPolicy.purge(db, cutoff, logger, ctx);
      const purgedEvents = await eventsPolicy.purge(db, cutoff, logger, ctx);
      const purgedStuckEvents = await stuckEventsPolicy.purge(db, stuckCutoff, logger, ctx);
      const purgedMetrics = await dailyMetricsPolicy.purge(db, metricsCutoff, logger, ctx);
      const purgedPersonaMetrics = await dailyMetricsByPersonaPolicy.purge(db, metricsCutoff, logger, ctx);

      metrics.rowsDeleted.persona_executions += purgedExecutions;
      metrics.rowsDeleted.persona_events += purgedEvents;
      metrics.rowsDeleted.persona_events_stuck += purgedStuckEvents;
      metrics.rowsDeleted.daily_metrics += purgedMetrics;
      metrics.rowsDeleted.daily_metrics_by_persona += purgedPersonaMetrics;
      metrics.tenantsProcessed++;

      if (purgedExecutions > 0 || purgedEvents > 0 || purgedStuckEvents > 0 || purgedMetrics > 0 || purgedPersonaMetrics > 0) {
        logger.info({ projectId, purgedExecutions, purgedEvents, purgedStuckEvents, purgedMetrics, purgedPersonaMetrics }, 'Retention cleanup complete');
      }
    } catch (err) {
      metrics.tenantErrors++;
      const msg = err instanceof Error ? err.message : String(err);
      metrics.errors.push(`${projectId}: ${msg}`);
      logger.error({ err, projectId }, 'Retention cleanup failed for tenant');
    }
    // Yield between tenants so the event loop stays responsive
    await yieldTick();
  }

  // ── Reduce phase: aggregate all tenant metrics into system.db ──
  try {
    aggregateSystemMetrics(tenantDbManager, logger);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    metrics.errors.push(`aggregation: ${msg}`);
    logger.error({ err }, 'System-wide metrics aggregation failed');
  }

  // ── Purge old system-wide metrics ──
  try {
    const purged = await systemDailyMetricsPolicy.purge(
      tenantDbManager.getSystemDb(), metricsCutoff, logger,
    );
    metrics.rowsDeleted.system_daily_metrics += purged;
    if (purged > 0) {
      logger.info({ purged }, 'Purged old system_daily_metrics rows');
    }
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    metrics.errors.push(`system_daily_metrics: ${msg}`);
    logger.error({ err }, 'system_daily_metrics purge failed');
  }

  return metrics;
}

/**
 * Map-reduce step: read daily_metrics from every tenant DB and write
 * a system-wide summary to the system_daily_metrics table in system.db.
 * Replaces rows wholesale each run so the system view is always consistent.
 */
function aggregateSystemMetrics(
  tenantDbManager: TenantDbManager,
  logger: Logger,
): void {
  const systemDb = tenantDbManager.getSystemDb();
  const tenantIds = tenantDbManager.listTenantIds();

  // Accumulate per-day totals across all tenants
  const dayMap = new Map<string, {
    total_executions: number;
    completed: number;
    failed: number;
    cancelled: number;
    total_cost_usd: number;
    total_duration_ms: number;
    total_input_tokens: number;
    total_output_tokens: number;
    tenant_count: number;
  }>();

  for (const projectId of tenantIds) {
    try {
      const db = tenantDbManager.getTenantDb(projectId);
      const rows = db.prepare(
        'SELECT date, total_executions, completed, failed, COALESCE(cancelled,0) as cancelled, total_cost_usd, total_duration_ms, COALESCE(total_input_tokens,0) as total_input_tokens, COALESCE(total_output_tokens,0) as total_output_tokens FROM daily_metrics',
      ).all() as Array<{
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

      for (const row of rows) {
        const existing = dayMap.get(row.date);
        if (existing) {
          existing.total_executions += row.total_executions;
          existing.completed += row.completed;
          existing.failed += row.failed;
          existing.cancelled += row.cancelled;
          existing.total_cost_usd += row.total_cost_usd;
          existing.total_duration_ms += row.total_duration_ms;
          existing.total_input_tokens += row.total_input_tokens;
          existing.total_output_tokens += row.total_output_tokens;
          existing.tenant_count += 1;
        } else {
          dayMap.set(row.date, {
            total_executions: row.total_executions,
            completed: row.completed,
            failed: row.failed,
            cancelled: row.cancelled,
            total_cost_usd: row.total_cost_usd,
            total_duration_ms: row.total_duration_ms,
            total_input_tokens: row.total_input_tokens,
            total_output_tokens: row.total_output_tokens,
            tenant_count: 1,
          });
        }
      }
    } catch (err) {
      logger.warn({ err, projectId }, 'Failed to read metrics for tenant during aggregation');
    }
  }

  if (dayMap.size === 0) return;

  // Write aggregated metrics in a single transaction
  const now = new Date().toISOString();
  systemDb.transaction(() => {
    const upsert = systemDb.prepare(`
      INSERT INTO system_daily_metrics
        (date, total_executions, completed, failed, cancelled, total_cost_usd, total_duration_ms, total_input_tokens, total_output_tokens, tenant_count, aggregated_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(date) DO UPDATE SET
        total_executions = excluded.total_executions,
        completed = excluded.completed,
        failed = excluded.failed,
        cancelled = excluded.cancelled,
        total_cost_usd = excluded.total_cost_usd,
        total_duration_ms = excluded.total_duration_ms,
        total_input_tokens = excluded.total_input_tokens,
        total_output_tokens = excluded.total_output_tokens,
        tenant_count = excluded.tenant_count,
        aggregated_at = excluded.aggregated_at
    `);
    for (const [date, m] of dayMap) {
      upsert.run(date, m.total_executions, m.completed, m.failed, m.cancelled, m.total_cost_usd, m.total_duration_ms, m.total_input_tokens, m.total_output_tokens, m.tenant_count, now);
    }
  })();

  logger.info({ days: dayMap.size, tenants: tenantIds.length }, 'System-wide metrics aggregation complete');
}
