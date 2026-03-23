import Database from 'better-sqlite3';
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import type { Logger } from 'pino';
import { initTenantDb, initSystemDb } from './db/index.js';

interface CachedDb {
  db: Database.Database;
  lastAccess: number;
}

export interface DbHealthStats {
  totalSizeBytes: number;
  tenantCount: number;
  totalExecutionRows: number;
  totalEventRows: number;
  oldestUnpurgedAgeMs: number | null;
  perTenant: Array<{
    projectId: string;
    sizeBytes: number;
    executionRows: number;
    eventRows: number;
    oldestRecordAge: string | null;
  }>;
}

const IDLE_EVICT_MS = 10 * 60 * 1000; // 10 minutes
const EVICT_CHECK_MS = 60 * 1000;     // check every 1 minute

/**
 * Manages per-tenant SQLite databases with connection pooling.
 *
 * Layout:
 *   {dataDir}/system.db           — tenant registry
 *   {dataDir}/tenants/{hash}.db   — per-tenant data
 *
 * The hash is derived from the projectId to produce safe filenames.
 */
export type TenantCreatedCallback = (tenantId: string) => void;

export class TenantDbManager {
  private systemDb: Database.Database;
  private tenantDbs = new Map<string, CachedDb>();
  private tenantsDir: string;
  private evictTimer: NodeJS.Timeout;
  private onTenantCreated: TenantCreatedCallback | null = null;

  constructor(
    private dataDir: string,
    private logger: Logger,
  ) {
    fs.mkdirSync(dataDir, { recursive: true });
    this.tenantsDir = path.join(dataDir, 'tenants');
    fs.mkdirSync(this.tenantsDir, { recursive: true });

    // Initialize system DB
    const systemDbPath = path.join(dataDir, 'system.db');
    this.systemDb = initSystemDb(systemDbPath, logger);

    // Idle eviction loop
    this.evictTimer = setInterval(() => this.evictIdle(), EVICT_CHECK_MS);
  }

  /**
   * Register a callback invoked when a new tenant is first registered.
   */
  setTenantCreatedCallback(cb: TenantCreatedCallback): void {
    this.onTenantCreated = cb;
  }

  getSystemDb(): Database.Database {
    return this.systemDb;
  }

  /**
   * Get (or create) a tenant-specific database.
   * Automatically registers the tenant in system.db on first access.
   */
  getTenantDb(projectId: string): Database.Database {
    const cached = this.tenantDbs.get(projectId);
    if (cached) {
      cached.lastAccess = Date.now();
      return cached.db;
    }

    // Create/open tenant DB
    const hash = this.hashProjectId(projectId);
    const dbPath = path.join(this.tenantsDir, `${hash}.db`);
    const db = initTenantDb(dbPath, this.logger);

    this.tenantDbs.set(projectId, { db, lastAccess: Date.now() });

    // Register in system DB if not already present
    this.registerTenant(projectId);

    this.logger.info({ projectId, dbPath }, 'Tenant database opened');
    return db;
  }

  /**
   * List all registered tenant IDs from the system DB.
   */
  listTenantIds(): string[] {
    const rows = this.systemDb
      .prepare('SELECT id FROM tenants ORDER BY id')
      .all() as Array<{ id: string }>;
    return rows.map(r => r.id);
  }

  /**
   * Execute a function against every tenant's database.
   * Opens DBs as needed (and caches them).
   */
  forEachTenant(fn: (db: Database.Database, projectId: string) => void): void {
    const ids = this.listTenantIds();
    for (const id of ids) {
      try {
        const db = this.getTenantDb(id);
        fn(db, id);
      } catch (err) {
        this.logger.error({ err, projectId: id }, 'Error processing tenant');
      }
    }
  }

  /**
   * Collect health stats across all tenant databases:
   * file size, row counts, and oldest un-purged record age.
   */
  getHealthStats(): DbHealthStats {
    const ids = this.listTenantIds();
    const now = Date.now();
    let totalSizeBytes = 0;
    let totalExecutionRows = 0;
    let totalEventRows = 0;
    let oldestUnpurgedAgeMs: number | null = null;
    const perTenant: DbHealthStats['perTenant'] = [];

    // Include system DB size
    const sysSizeRow = this.systemDb.prepare('SELECT page_count * page_size AS size FROM pragma_page_count(), pragma_page_size()').get() as { size: number } | undefined;
    if (sysSizeRow) totalSizeBytes += sysSizeRow.size;

    for (const id of ids) {
      try {
        const db = this.getTenantDb(id);

        const sizeRow = db.prepare('SELECT page_count * page_size AS size FROM pragma_page_count(), pragma_page_size()').get() as { size: number } | undefined;
        const sizeBytes = sizeRow?.size ?? 0;

        const execRow = db.prepare('SELECT COUNT(*) AS cnt FROM persona_executions').get() as { cnt: number };
        const eventRow = db.prepare('SELECT COUNT(*) AS cnt FROM persona_events').get() as { cnt: number };

        const oldestExec = db.prepare('SELECT MIN(created_at) AS oldest FROM persona_executions').get() as { oldest: string | null };
        const oldestEvent = db.prepare('SELECT MIN(created_at) AS oldest FROM persona_events').get() as { oldest: string | null };

        // Pick the oldest of the two tables
        let oldestRecord: string | null = null;
        if (oldestExec.oldest && oldestEvent.oldest) {
          oldestRecord = oldestExec.oldest < oldestEvent.oldest ? oldestExec.oldest : oldestEvent.oldest;
        } else {
          oldestRecord = oldestExec.oldest ?? oldestEvent.oldest;
        }

        if (oldestRecord) {
          const ageMs = now - new Date(oldestRecord).getTime();
          if (oldestUnpurgedAgeMs === null || ageMs > oldestUnpurgedAgeMs) {
            oldestUnpurgedAgeMs = ageMs;
          }
        }

        totalSizeBytes += sizeBytes;
        totalExecutionRows += execRow.cnt;
        totalEventRows += eventRow.cnt;

        perTenant.push({
          projectId: id,
          sizeBytes,
          executionRows: execRow.cnt,
          eventRows: eventRow.cnt,
          oldestRecordAge: oldestRecord,
        });
      } catch (err) {
        this.logger.warn({ err, projectId: id }, 'Failed to collect health stats for tenant');
      }
    }

    return {
      totalSizeBytes,
      tenantCount: ids.length,
      totalExecutionRows,
      totalEventRows,
      oldestUnpurgedAgeMs,
      perTenant,
    };
  }

  /**
   * Close all database connections and stop eviction timer.
   */
  close(): void {
    clearInterval(this.evictTimer);

    for (const [projectId, cached] of this.tenantDbs) {
      try {
        cached.db.close();
        this.logger.debug({ projectId }, 'Tenant database closed');
      } catch { /* best effort */ }
    }
    this.tenantDbs.clear();

    try {
      this.systemDb.close();
    } catch { /* best effort */ }
  }

  private registerTenant(projectId: string): void {
    const existing = this.systemDb
      .prepare('SELECT id FROM tenants WHERE id = ?')
      .get(projectId);

    if (!existing) {
      this.systemDb
        .prepare('INSERT INTO tenants (id, created_at) VALUES (?, ?)')
        .run(projectId, new Date().toISOString());
      this.logger.info({ projectId }, 'Tenant registered in system DB');
      this.onTenantCreated?.(projectId);
    }
  }

  private hashProjectId(projectId: string): string {
    return crypto.createHash('sha256').update(projectId).digest('hex').slice(0, 16);
  }

  private evictIdle(): void {
    const now = Date.now();
    for (const [projectId, cached] of this.tenantDbs) {
      if (now - cached.lastAccess > IDLE_EVICT_MS) {
        try {
          cached.db.close();
          this.tenantDbs.delete(projectId);
          this.logger.debug({ projectId }, 'Evicted idle tenant database');
        } catch (err) {
          this.logger.warn({ err, projectId }, 'Failed to evict tenant database');
        }
      }
    }
  }
}
