import type Database from 'better-sqlite3';
import type { Logger } from 'pino';
import { getTableSpec } from '@dac-cloud/shared';
import type { TableRetentionSpec } from '@dac-cloud/shared';

/** Default rows deleted per chunk to keep individual lock durations short (~50ms). */
const DEFAULT_CHUNK_SIZE = 5000;

/** Yield the event loop so concurrent reads/writes can proceed between batches. */
function yieldTick(): Promise<void> {
  return new Promise((resolve) => setImmediate(resolve));
}

/** Configuration for a single table's retention policy. */
export interface RetentionPolicyConfig {
  /** Table name — must exist in TABLE_REGISTRY. */
  table: string;
  /** Override chunk size (default 5000). */
  chunkSize?: number;
  /** Additional SQL WHERE fragment appended after the cutoff condition (e.g. "AND status IN ('completed')"). */
  deleteFilter?: string;
}

/**
 * Declarative retention policy for a single table.
 *
 * Encapsulates the common purge algorithm: compute cutoff → safety-fraction
 * check → chunked delete with event-loop yields.  Each table registers its
 * lifecycle in TABLE_REGISTRY; this class reads the spec at construction time
 * so bug fixes and safety improvements apply everywhere at once.
 */
export class RetentionPolicy {
  readonly table: string;
  readonly timestampColumn: string;
  readonly maxPurgeFraction: number;
  private readonly chunkSize: number;
  private readonly deleteFilter: string;

  constructor(config: RetentionPolicyConfig) {
    const spec: TableRetentionSpec | undefined = getTableSpec(config.table);
    if (!spec) throw new Error(`Table "${config.table}" not found in TABLE_REGISTRY`);

    this.table = spec.table;
    this.timestampColumn = spec.timestampColumn;
    this.maxPurgeFraction = spec.maxPurgeFraction ?? 0.8;
    this.chunkSize = config.chunkSize ?? DEFAULT_CHUNK_SIZE;
    this.deleteFilter = config.deleteFilter ?? '';
  }

  /**
   * Purge rows older than `cutoff` from the given database.
   *
   * 1. Count total rows and purgeable candidates.
   * 2. Abort if candidates/total exceeds the safety-fraction cap.
   * 3. Delete in chunks of `chunkSize`, yielding the event loop between batches.
   *
   * Returns the number of rows deleted.
   */
  async purge(
    db: Database.Database,
    cutoff: string,
    logger: Logger,
    context?: Record<string, unknown>,
  ): Promise<number> {
    const filter = this.deleteFilter ? ` AND ${this.deleteFilter}` : '';

    // Pre-purge sanity check
    const total = (db.prepare(
      `SELECT COUNT(*) as cnt FROM ${this.table}`,
    ).get() as { cnt: number }).cnt;

    const candidates = (db.prepare(
      `SELECT COUNT(*) as cnt FROM ${this.table} WHERE ${this.timestampColumn} < ?${filter}`,
    ).get(cutoff) as { cnt: number }).cnt;

    if (total > 0 && candidates / total > this.maxPurgeFraction) {
      logger.error(
        { table: this.table, total, candidates, cutoff, maxFraction: this.maxPurgeFraction, ...context },
        'Purge aborted: deletion would exceed safety cap',
      );
      return 0;
    }

    // Chunked delete with event-loop yields
    const deleteStmt = db.prepare(
      `DELETE FROM ${this.table} WHERE rowid IN (SELECT rowid FROM ${this.table} WHERE ${this.timestampColumn} < ?${filter} LIMIT ?)`,
    );
    let totalDeleted = 0;
    while (true) {
      const result = deleteStmt.run(cutoff, this.chunkSize);
      totalDeleted += result.changes;
      if (result.changes < this.chunkSize) break;
      await yieldTick();
    }
    return totalDeleted;
  }
}
