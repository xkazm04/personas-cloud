import crypto from 'node:crypto';
import fs from 'node:fs';
import Database from 'better-sqlite3';
import type { Logger } from 'pino';

/** SHA-256 hash chain genesis value (64 hex zeros). */
const GENESIS_HASH = '0'.repeat(64);

/**
 * Compute a chain hash: SHA-256(previousHash + deterministic JSON of entry fields).
 * Each entry's hash commits to the entire chain history before it.
 */
function computeChainHash(prevHash: string, entry: BufferedEntry): string {
  const payload = prevHash + JSON.stringify([
    entry.timestamp,
    entry.action,
    entry.tenantId,
    entry.actor,
    entry.resourceType,
    entry.resourceId,
    entry.detail,
    entry.ipAddress,
  ]);
  return crypto.createHash('sha256').update(payload).digest('hex');
}

const AUDIT_SCHEMA = `
CREATE TABLE IF NOT EXISTS audit_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  timestamp TEXT NOT NULL,
  action TEXT NOT NULL,
  tenant_id TEXT,
  actor TEXT,
  resource_type TEXT,
  resource_id TEXT,
  detail TEXT,
  ip_address TEXT,
  prev_hash TEXT
);
CREATE INDEX IF NOT EXISTS idx_audit_ts ON audit_events(timestamp);
CREATE INDEX IF NOT EXISTS idx_audit_tenant_action_ts ON audit_events(tenant_id, action, timestamp);
CREATE INDEX IF NOT EXISTS idx_audit_actor ON audit_events(actor);
CREATE INDEX IF NOT EXISTS idx_audit_resource_type ON audit_events(resource_type);
DROP INDEX IF EXISTS idx_audit_tenant_ts;
DROP INDEX IF EXISTS idx_audit_action;
`;

/** Thrown when an audit event fails to persist — signals an audit trail gap. */
export class AuditWriteError extends Error {
  constructor(
    public readonly entry: AuditEntry,
    cause: unknown,
  ) {
    super(`Audit write failed for action "${entry.action}"`);
    this.name = 'AuditWriteError';
    this.cause = cause;
  }
}

// ---------------------------------------------------------------------------
// Discriminated-union audit entries — each action carries its own typed payload
// ---------------------------------------------------------------------------

/** Fields common to every audit entry. */
interface AuditEntryBase {
  tenantId?: string;
  actor?: string;
  ipAddress?: string;
}

// --- Key management ---
interface KeyEncryptEntry extends AuditEntryBase {
  action: 'key:encrypt';
  resourceType: 'credential';
  resourceId: string;
}

interface KeyDecryptEntry extends AuditEntryBase {
  action: 'key:decrypt';
  resourceType: 'credential';
  resourceId: string;
}

interface KeyDecryptFailedEntry extends AuditEntryBase {
  action: 'key:decrypt_failed';
  resourceType: 'credential';
  resourceId: string;
  detail: { reason: string };
}

interface KeyDeleteEntry extends AuditEntryBase {
  action: 'key:delete';
  resourceType: 'credential';
  resourceId: string;
}

interface KeyAccessEntry extends AuditEntryBase {
  action: 'key:access';
  resourceType: 'credential';
  resourceId: string;
}

interface KeyDeriveEntry extends AuditEntryBase {
  action: 'key:derive';
}

// --- Persona management ---
interface PersonaCreateEntry extends AuditEntryBase {
  action: 'persona:create';
  resourceType: 'persona';
  resourceId: string;
}

interface PersonaDeleteEntry extends AuditEntryBase {
  action: 'persona:delete';
  resourceType: 'persona';
  resourceId: string;
}

// --- Tool management ---
interface ToolLinkEntry extends AuditEntryBase {
  action: 'tool:link';
  resourceType: 'persona';
  resourceId: string;
  detail: { toolId: string };
}

interface ToolDefinitionCreateEntry extends AuditEntryBase {
  action: 'tool-definition:create';
  resourceType: 'toolDefinition';
  resourceId: string;
}

// --- Credential linking ---
interface CredentialLinkEntry extends AuditEntryBase {
  action: 'credential:link';
  resourceType: 'persona';
  resourceId: string;
  detail: { credentialId: string };
}

// --- Subscriptions ---
interface SubscriptionCreateEntry extends AuditEntryBase {
  action: 'subscription:create';
  resourceType: 'subscription';
  resourceId: string;
  detail: { personaId: string; eventType: string };
}

interface SubscriptionUpdateEntry extends AuditEntryBase {
  action: 'subscription:update';
  resourceType: 'subscription';
  resourceId: string;
}

interface SubscriptionDeleteEntry extends AuditEntryBase {
  action: 'subscription:delete';
  resourceType: 'subscription';
  resourceId: string;
}

// --- Worker pool ---
interface WorkerConnectedEntry extends AuditEntryBase {
  action: 'worker:connected';
  detail: { workerId: string };
}

interface WorkerDisconnectedEntry extends AuditEntryBase {
  action: 'worker:disconnected';
  detail: { workerId: string };
  resourceId?: string;
}

// --- Triggers ---
interface TriggerCreateEntry extends AuditEntryBase {
  action: 'trigger:create';
  resourceType: 'trigger';
  resourceId?: string;
}

interface TriggerUpdateEntry extends AuditEntryBase {
  action: 'trigger:update';
  resourceType: 'trigger';
  resourceId: string;
}

interface TriggerDeleteEntry extends AuditEntryBase {
  action: 'trigger:delete';
  resourceType: 'trigger';
  resourceId: string;
}

// --- Events ---
interface EventPublishEntry extends AuditEntryBase {
  action: 'event:publish';
  resourceType: 'event';
  resourceId?: string;
}

interface EventUpdateEntry extends AuditEntryBase {
  action: 'event:update';
  resourceType: 'event';
  resourceId: string;
}

// --- Executions ---
interface ExecutionSubmitEntry extends AuditEntryBase {
  action: 'execution:submit';
  resourceType: 'execution';
  resourceId?: string;
}

// --- Webhooks ---
interface WebhookReceiveEntry extends AuditEntryBase {
  action: 'webhook:receive';
  resourceType: 'webhook';
  resourceId: string;
}

// --- OAuth lifecycle ---
interface OAuthAuthorizeEntry extends AuditEntryBase {
  action: 'oauth:authorize';
  resourceType: 'oauth';
  resourceId: string; // state parameter
}

interface OAuthExchangeEntry extends AuditEntryBase {
  action: 'oauth:exchange';
  resourceType: 'oauth';
  detail: { scopes: string[]; expiresAt: string };
}

interface OAuthRefreshEntry extends AuditEntryBase {
  action: 'oauth:refresh';
  resourceType: 'oauth';
  detail: { expiresAt: string };
}

interface OAuthRefreshFailedEntry extends AuditEntryBase {
  action: 'oauth:refresh_failed';
  resourceType: 'oauth';
  detail: { reason: string; errorType?: string };
}

interface OAuthTokenErrorEntry extends AuditEntryBase {
  action: 'oauth:token_error';
  resourceType: 'oauth';
  detail: { errorType: string; status?: number; transient: boolean; context: string };
}

interface OAuthDisconnectEntry extends AuditEntryBase {
  action: 'oauth:disconnect';
  resourceType: 'oauth';
  detail: { serverRevoked: boolean };
}

// --- Token lifecycle ---
interface TokenInjectEntry extends AuditEntryBase {
  action: 'token:inject';
  resourceType: 'token';
}

interface TokenExpiringSoonEntry extends AuditEntryBase {
  action: 'token:expiring_soon';
  resourceType: 'oauth';
  detail: { expiresAt: string; remainingMinutes: number };
}

/** Discriminated union of all audit event types. */
export type AuditEntry =
  | KeyEncryptEntry
  | KeyDecryptEntry
  | KeyDecryptFailedEntry
  | KeyDeleteEntry
  | KeyAccessEntry
  | KeyDeriveEntry
  | PersonaCreateEntry
  | PersonaDeleteEntry
  | ToolLinkEntry
  | ToolDefinitionCreateEntry
  | CredentialLinkEntry
  | SubscriptionCreateEntry
  | SubscriptionUpdateEntry
  | SubscriptionDeleteEntry
  | WorkerConnectedEntry
  | WorkerDisconnectedEntry
  | TriggerCreateEntry
  | TriggerUpdateEntry
  | TriggerDeleteEntry
  | EventPublishEntry
  | EventUpdateEntry
  | ExecutionSubmitEntry
  | WebhookReceiveEntry
  | OAuthAuthorizeEntry
  | OAuthExchangeEntry
  | OAuthRefreshEntry
  | OAuthRefreshFailedEntry
  | OAuthTokenErrorEntry
  | OAuthDisconnectEntry
  | TokenInjectEntry
  | TokenExpiringSoonEntry;

/** String-literal union of all valid audit actions. */
export type AuditAction = AuditEntry['action'];

/** Row shape returned by audit_events queries. */
export interface AuditEvent {
  id: number;
  timestamp: string;
  action: string;
  tenant_id: string | null;
  actor: string | null;
  resource_type: string | null;
  resource_id: string | null;
  detail: string | null;
  ip_address: string | null;
  prev_hash: string | null;
}

/** Result of verifying the audit hash chain. */
export interface ChainVerifyResult {
  valid: boolean;
  verifiedCount: number;
  brokenAtId?: number;
}

/** Max entries buffered before an automatic flush. */
const BUFFER_FLUSH_SIZE = 50;
/** Interval (ms) between timer-based flushes. */
const BUFFER_FLUSH_INTERVAL_MS = 100;
/** Flush latency (ms) above which a warning is emitted — indicates SQLite contention. */
const FLUSH_WARN_THRESHOLD_MS = 50;

interface BufferedEntry {
  timestamp: string;
  action: string;
  tenantId: string | null;
  actor: string | null;
  resourceType: string | null;
  resourceId: string | null;
  detail: string | null;
  ipAddress: string | null;
}

/** Health snapshot returned by AuditLog.getHealth(). */
export interface AuditLogHealth {
  /** Total number of audit events successfully written to disk. */
  totalEventsWritten: number;
  /** Total number of audit events dropped due to flush failures. */
  droppedEvents: number;
  /** Number of events currently waiting in the in-memory buffer. */
  bufferDepth: number;
  /** Highest buffer depth observed since startup. */
  bufferHighWaterMark: number;
  /** Duration of the most recent flush in milliseconds, or null if no flush has occurred. */
  lastFlushDurationMs: number | null;
  /** Result of the most recent chain verification, or null if verify() has not been called. */
  lastChainVerifyResult: ChainVerifyResult | null;
  /** Size of the audit SQLite database file in bytes, or null if unavailable. */
  dbSizeBytes: number | null;
}

export class AuditLog {
  private db: Database.Database;
  private insertStmt: Database.Statement;
  private queryStmtCache = new Map<string, Database.Statement>();
  private buffer: BufferedEntry[] = [];
  private flushTimer: NodeJS.Timeout | null = null;
  private batchInsert: (entries: BufferedEntry[], hashes: string[]) => void;
  private lastHash: string;
  private droppedEvents = 0;
  private totalEventsWritten = 0;
  private lastFlushDurationMs: number | null = null;
  private bufferHighWaterMark = 0;
  private lastChainVerifyResult: ChainVerifyResult | null = null;
  private dbPath: string;

  constructor(
    dbPath: string,
    private logger: Logger,
  ) {
    this.dbPath = dbPath;
    this.db = new Database(dbPath);
    this.db.pragma('journal_mode = WAL');
    this.db.exec(AUDIT_SCHEMA);

    // Migration: add prev_hash column to existing databases that lack it
    const cols = this.db.pragma('table_info(audit_events)') as { name: string }[];
    if (!cols.some((c) => c.name === 'prev_hash')) {
      this.db.exec('ALTER TABLE audit_events ADD COLUMN prev_hash TEXT');
    }

    this.insertStmt = this.db.prepare(
      'INSERT INTO audit_events (timestamp, action, tenant_id, actor, resource_type, resource_id, detail, ip_address, prev_hash) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
    );

    // Initialize chain from last stored hash (or genesis for empty / pre-chain DBs)
    const lastRow = this.db.prepare(
      'SELECT prev_hash FROM audit_events WHERE prev_hash IS NOT NULL ORDER BY id DESC LIMIT 1',
    ).get() as { prev_hash: string } | undefined;
    this.lastHash = lastRow?.prev_hash ?? GENESIS_HASH;

    // Wrap N inserts in a single transaction — one fsync instead of N
    const stmt = this.insertStmt;
    this.batchInsert = this.db.transaction((entries: BufferedEntry[], hashes: string[]) => {
      for (let i = 0; i < entries.length; i++) {
        const e = entries[i];
        stmt.run(e.timestamp, e.action, e.tenantId, e.actor, e.resourceType, e.resourceId, e.detail, e.ipAddress, hashes[i]);
      }
    });

    this.flushTimer = setInterval(() => this.flush(), BUFFER_FLUSH_INTERVAL_MS);

    logger.info({ dbPath }, 'Audit log initialized (buffered writes enabled, hash-chain active)');
  }

  /** Expose the underlying database for use by RetentionPolicy. */
  getDb(): Database.Database {
    return this.db;
  }

  /** Health snapshot — a nonzero droppedEvents value means the audit trail has gaps. */
  getHealth(): AuditLogHealth {
    let dbSizeBytes: number | null = null;
    try {
      dbSizeBytes = fs.statSync(this.dbPath).size;
    } catch { /* file may not exist yet or be inaccessible */ }

    return {
      totalEventsWritten: this.totalEventsWritten,
      droppedEvents: this.droppedEvents,
      bufferDepth: this.buffer.length,
      bufferHighWaterMark: this.bufferHighWaterMark,
      lastFlushDurationMs: this.lastFlushDurationMs,
      lastChainVerifyResult: this.lastChainVerifyResult,
      dbSizeBytes,
    };
  }

  /** Count audit events, optionally only those with timestamp before `cutoff`. */
  countEvents(before?: string): number {
    const stmt = before
      ? this.db.prepare('SELECT COUNT(*) as cnt FROM audit_events WHERE timestamp < ?')
      : this.db.prepare('SELECT COUNT(*) as cnt FROM audit_events');
    const row = (before ? stmt.get(before) : stmt.get()) as { cnt: number };
    return row.cnt;
  }

  /**
   * Delete audit events older than `cutoff` in chunks of `chunkSize`,
   * yielding the event loop between batches so concurrent DB operations
   * are not starved during large purges.
   */
  async purgeOlderThan(cutoff: string, chunkSize: number): Promise<number> {
    const deleteChunk = this.db.prepare(
      'DELETE FROM audit_events WHERE rowid IN (SELECT rowid FROM audit_events WHERE timestamp < ? LIMIT ?)',
    );
    let totalDeleted = 0;
    while (true) {
      const result = deleteChunk.run(cutoff, chunkSize);
      totalDeleted += result.changes;
      if (result.changes < chunkSize) break;
      await new Promise<void>((resolve) => setImmediate(resolve));
    }
    return totalDeleted;
  }

  /** Append an audit event to the in-memory buffer.  Flushed to disk every 100ms or at 50 entries. */
  record(entry: AuditEntry): void {
    this.buffer.push({
      timestamp: new Date().toISOString(),
      action: entry.action,
      tenantId: entry.tenantId ?? null,
      actor: entry.actor ?? null,
      resourceType: 'resourceType' in entry ? entry.resourceType : null,
      resourceId: 'resourceId' in entry ? (entry.resourceId ?? null) : null,
      detail: 'detail' in entry ? JSON.stringify(entry.detail) : null,
      ipAddress: entry.ipAddress ?? null,
    });
    if (this.buffer.length > this.bufferHighWaterMark) {
      this.bufferHighWaterMark = this.buffer.length;
    }
    if (this.buffer.length >= BUFFER_FLUSH_SIZE) {
      this.flush();
    }
  }

  /** Flush buffered entries to SQLite in a single transaction, computing chain hashes. */
  flush(): void {
    if (this.buffer.length === 0) return;
    const batchSize = this.buffer.length;
    const batch = this.buffer;
    this.buffer = [];

    // Compute chain hashes for the batch
    const hashes: string[] = [];
    let prevHash = this.lastHash;
    for (const entry of batch) {
      prevHash = computeChainHash(prevHash, entry);
      hashes.push(prevHash);
    }

    const flushStart = performance.now();
    try {
      this.batchInsert(batch, hashes);
      this.lastHash = prevHash;
      this.totalEventsWritten += batchSize;
    } catch (err) {
      this.droppedEvents += batchSize;
      this.logger.error({ err, batchSize, totalDropped: this.droppedEvents }, 'Audit batch write failed — audit trail gap');
    }
    const flushDurationMs = Math.round((performance.now() - flushStart) * 100) / 100;
    this.lastFlushDurationMs = flushDurationMs;

    if (flushDurationMs > FLUSH_WARN_THRESHOLD_MS) {
      this.logger.warn({ flushDurationMs, batchSize, bufferHighWaterMark: this.bufferHighWaterMark }, 'Audit flush exceeded latency threshold — possible SQLite contention');
    } else {
      this.logger.debug({ flushDurationMs, batchSize, bufferHighWaterMark: this.bufferHighWaterMark }, 'Audit flush completed');
    }
  }

  /** Aggregated audit event statistics for dashboard consumption. */
  stats(opts?: {
    tenantId?: string;
    since?: string;
    until?: string;
    bucket?: 'hour' | 'day';
  }): {
    byAction: { action: string; count: number }[];
    byTenant: { tenant_id: string; count: number }[];
    topActors: { actor: string; count: number }[];
    trend: { bucket: string; count: number }[];
  } {
    this.flush(); // ensure buffered entries are visible

    const params: unknown[] = [];
    const clauses: string[] = [];
    if (opts?.tenantId) { clauses.push('tenant_id = ?'); params.push(opts.tenantId); }
    if (opts?.since) { clauses.push('timestamp >= ?'); params.push(opts.since); }
    if (opts?.until) { clauses.push('timestamp < ?'); params.push(opts.until); }
    const where = clauses.length > 0 ? `WHERE ${clauses.join(' AND ')}` : '';

    const bucketExpr = opts?.bucket === 'hour'
      ? "substr(timestamp, 1, 13)"  // YYYY-MM-DDTHH
      : "substr(timestamp, 1, 10)"; // YYYY-MM-DD

    const byAction = this.db.prepare(
      `SELECT action, COUNT(*) as count FROM audit_events ${where} GROUP BY action ORDER BY count DESC`,
    ).all(...params) as { action: string; count: number }[];

    const byTenant = this.db.prepare(
      `SELECT COALESCE(tenant_id, '__none__') as tenant_id, COUNT(*) as count FROM audit_events ${where} GROUP BY tenant_id ORDER BY count DESC LIMIT 50`,
    ).all(...params) as { tenant_id: string; count: number }[];

    const topActors = this.db.prepare(
      `SELECT COALESCE(actor, '__none__') as actor, COUNT(*) as count FROM audit_events ${where} GROUP BY actor ORDER BY count DESC LIMIT 20`,
    ).all(...params) as { actor: string; count: number }[];

    const trend = this.db.prepare(
      `SELECT ${bucketExpr} as bucket, COUNT(*) as count FROM audit_events ${where} GROUP BY bucket ORDER BY bucket ASC`,
    ).all(...params) as { bucket: string; count: number }[];

    return { byAction, byTenant, topActors, trend };
  }

  /** Query audit events with optional filters. */
  query(opts?: {
    tenantId?: string;
    action?: AuditAction;
    since?: string;
    until?: string;
    actor?: string;
    resourceType?: string;
    resourceId?: string;
    limit?: number;
    offset?: number;
  }): AuditEvent[] {
    const params: unknown[] = [];
    const clauses: string[] = [];
    const keyParts: string[] = [];

    if (opts?.tenantId) { clauses.push('tenant_id = ?'); keyParts.push('tenant'); params.push(opts.tenantId); }
    if (opts?.action) { clauses.push('action = ?'); keyParts.push('action'); params.push(opts.action); }
    if (opts?.since) { clauses.push('timestamp >= ?'); keyParts.push('since'); params.push(opts.since); }
    if (opts?.until) { clauses.push('timestamp <= ?'); keyParts.push('until'); params.push(opts.until); }
    if (opts?.actor) { clauses.push('actor = ?'); keyParts.push('actor'); params.push(opts.actor); }
    if (opts?.resourceType) { clauses.push('resource_type = ?'); keyParts.push('rtype'); params.push(opts.resourceType); }
    if (opts?.resourceId) { clauses.push('resource_id = ?'); keyParts.push('rid'); params.push(opts.resourceId); }

    const key = keyParts.join('-') || 'none';
    let stmt = this.queryStmtCache.get(key);
    if (!stmt) {
      const where = clauses.length > 0 ? `WHERE ${clauses.join(' AND ')}` : '';
      stmt = this.db.prepare(
        `SELECT * FROM audit_events ${where} ORDER BY id DESC LIMIT ? OFFSET ?`,
      );
      this.queryStmtCache.set(key, stmt);
    }

    const limit = opts?.limit ?? 100;
    const offset = opts?.offset ?? 0;

    return stmt.all(...params, limit, offset) as AuditEvent[];
  }

  /**
   * Verify the integrity of the audit hash chain.
   * Iterates all chained rows and checks that each hash is consistent with
   * its predecessor. Pre-chain rows (migrated without prev_hash) are skipped.
   * If rows were purged, the chain will break at the first remaining row
   * — this is by design (deletions are detectable).
   *
   * When `since`/`until` are provided, only rows within that timestamp range
   * are checked. The initial previous-hash is seeded from the last chained
   * row before the range (or genesis if none exists).
   */
  verify(opts?: { since?: string; until?: string }): ChainVerifyResult {
    this.flush(); // ensure all buffered entries are persisted

    const hasRange = opts?.since || opts?.until;
    const clauses: string[] = [];
    const params: unknown[] = [];
    if (opts?.since) { clauses.push('timestamp >= ?'); params.push(opts.since); }
    if (opts?.until) { clauses.push('timestamp < ?'); params.push(opts.until); }
    const where = clauses.length > 0 ? `WHERE ${clauses.join(' AND ')}` : '';

    const iter = this.db.prepare(
      `SELECT * FROM audit_events ${where} ORDER BY id ASC`,
    ).iterate(...params);

    // Seed the previous hash: for ranged queries, look up the last chained
    // row before the first matching row; for full scans use genesis.
    let prevHash = GENESIS_HASH;
    let seeded = !hasRange; // full scan is pre-seeded with genesis

    let verifiedCount = 0;

    for (const raw of iter) {
      const row = raw as AuditEvent;
      if (row.prev_hash === null) continue; // pre-chain row

      // Lazily seed prevHash on the first chained row of a ranged query
      if (!seeded) {
        const prev = this.db.prepare(
          'SELECT prev_hash FROM audit_events WHERE id < ? AND prev_hash IS NOT NULL ORDER BY id DESC LIMIT 1',
        ).get(row.id) as { prev_hash: string } | undefined;
        prevHash = prev?.prev_hash ?? GENESIS_HASH;
        seeded = true;
      }

      const expected = computeChainHash(prevHash, {
        timestamp: row.timestamp,
        action: row.action,
        tenantId: row.tenant_id,
        actor: row.actor,
        resourceType: row.resource_type,
        resourceId: row.resource_id,
        detail: row.detail,
        ipAddress: row.ip_address,
      });

      if (row.prev_hash !== expected) {
        const result: ChainVerifyResult = { valid: false, verifiedCount, brokenAtId: row.id };
        this.lastChainVerifyResult = result;
        return result;
      }

      prevHash = row.prev_hash;
      verifiedCount++;
    }

    const result: ChainVerifyResult = { valid: true, verifiedCount };
    this.lastChainVerifyResult = result;
    return result;
  }

  close(): void {
    if (this.flushTimer) {
      clearInterval(this.flushTimer);
      this.flushTimer = null;
    }
    this.flush();
    try { this.db.close(); } catch { /* best effort */ }
  }
}
