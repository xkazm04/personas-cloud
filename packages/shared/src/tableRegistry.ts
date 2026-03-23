// ---------------------------------------------------------------------------
// Table lifecycle classification registry
// ---------------------------------------------------------------------------
//
// Every time-series table (one with a temporal column subject to retention)
// is classified as either *metrizable* or *ephemeral*:
//
//   metrizable  – contains numeric columns worth aggregating into summary
//                 metrics before rows are purged (e.g. cost_usd, duration_ms).
//   ephemeral   – rows are simply deleted after the retention window; no
//                 pre-purge aggregation is performed.
//
// This registry is the single source of truth for that classification.
// The retention engine and any future table additions should consult it
// rather than hard-coding lifecycle decisions in procedural code.
// ---------------------------------------------------------------------------

/** Lifecycle category for a time-series table. */
export type TableLifecycle = 'metrizable' | 'ephemeral';

/** Describes a numeric column that should be aggregated before purge. */
export interface MetrizableColumn {
  /** SQLite column name. */
  column: string;
  /** SQL aggregation function applied during rollup (SUM, AVG, COUNT, etc.). */
  aggregation: 'SUM' | 'COUNT' | 'AVG' | 'MAX' | 'MIN';
}

/** Retention spec for a metrizable table. */
export interface MetrizableTableSpec {
  lifecycle: 'metrizable';
  /** The SQLite table name. */
  table: string;
  /** Temporal column used to determine row age. */
  timestampColumn: string;
  /** Numeric columns aggregated into a summary table before purge. */
  metrizableColumns: MetrizableColumn[];
  /** Target table where aggregated metrics are written. */
  aggregationTarget: string;
  /** Maximum fraction of rows deletable in a single purge cycle (safety cap). */
  maxPurgeFraction?: number;
}

/** Retention spec for an ephemeral table. */
export interface EphemeralTableSpec {
  lifecycle: 'ephemeral';
  /** The SQLite table name. */
  table: string;
  /** Temporal column used to determine row age. */
  timestampColumn: string;
  /** Minimum retention days (e.g. compliance floor for audit data). */
  minRetentionDays?: number;
  /** Maximum fraction of rows deletable in a single purge cycle (safety cap). */
  maxPurgeFraction?: number;
}

export type TableRetentionSpec = MetrizableTableSpec | EphemeralTableSpec;

// ---------------------------------------------------------------------------
// Registry of all time-series tables and their lifecycle classification
// ---------------------------------------------------------------------------

export const TABLE_REGISTRY: readonly TableRetentionSpec[] = [
  // ── Metrizable: numeric data aggregated into daily_metrics before purge ──
  {
    lifecycle: 'metrizable',
    table: 'persona_executions',
    timestampColumn: 'created_at',
    metrizableColumns: [
      { column: 'cost_usd', aggregation: 'SUM' },
      { column: 'duration_ms', aggregation: 'SUM' },
      { column: 'input_tokens', aggregation: 'SUM' },
      { column: 'output_tokens', aggregation: 'SUM' },
    ],
    aggregationTarget: 'daily_metrics',
    maxPurgeFraction: 0.8,
  },

  // ── Ephemeral: purged without aggregation ────────────────────────────────
  {
    lifecycle: 'ephemeral',
    table: 'persona_events',
    timestampColumn: 'created_at',
    maxPurgeFraction: 0.8,
  },
  {
    lifecycle: 'ephemeral',
    table: 'audit_events',
    timestampColumn: 'timestamp',
    minRetentionDays: 90,
    maxPurgeFraction: 0.8,
  },
  {
    lifecycle: 'ephemeral',
    table: 'trigger_fires',
    timestampColumn: 'fired_at',
    maxPurgeFraction: 0.8,
  },
  {
    lifecycle: 'ephemeral',
    table: 'daily_metrics',
    timestampColumn: 'date',
    maxPurgeFraction: 0.8,
  },
  {
    lifecycle: 'ephemeral',
    table: 'daily_metrics_by_persona',
    timestampColumn: 'date',
    maxPurgeFraction: 0.8,
  },
  {
    lifecycle: 'ephemeral',
    table: 'system_daily_metrics',
    timestampColumn: 'date',
    maxPurgeFraction: 0.8,
  },
] as const;

// ---------------------------------------------------------------------------
// Lookup helpers
// ---------------------------------------------------------------------------

/** Look up the retention spec for a table by name. */
export function getTableSpec(tableName: string): TableRetentionSpec | undefined {
  return TABLE_REGISTRY.find((s) => s.table === tableName);
}

/** Return all tables classified with the given lifecycle. */
export function getTablesByLifecycle(lifecycle: TableLifecycle): readonly TableRetentionSpec[] {
  return TABLE_REGISTRY.filter((s) => s.lifecycle === lifecycle);
}

/** Type guard: is this spec metrizable? */
export function isMetrizable(spec: TableRetentionSpec): spec is MetrizableTableSpec {
  return spec.lifecycle === 'metrizable';
}

/** Type guard: is this spec ephemeral? */
export function isEphemeral(spec: TableRetentionSpec): spec is EphemeralTableSpec {
  return spec.lifecycle === 'ephemeral';
}
