# Phase 4: Scale to PostgreSQL for Multi-Orchestrator HA

> **Goal:** Replace SQLite with PostgreSQL to enable multiple orchestrator instances, regional deployment, and horizontal scaling beyond 5,000 agents.
> **Estimated effort:** 5-8 days
> **Risk:** Medium-High — storage layer change affects all DB operations
> **Cost impact:** Adds $0-25/month (Neon free tier or Supabase) but enables 10x+ scale
> **Dependency:** Phases 1-3 complete and stable in production
> **Trigger:** Execute this phase when single-orchestrator SQLite becomes a bottleneck (>5,000 agents, latency spikes during event processing, or need for regional presence)

---

## Why PostgreSQL (Not Another SQLite Instance)

| Requirement | SQLite | PostgreSQL |
|---|---|---|
| Single-writer concurrency | WAL mode handles reads, but writes serialize | Full MVCC, concurrent writers |
| Multi-instance access | Single process only (file lock) | Network-accessible, multiple clients |
| Cross-region replication | Not supported | Native (Neon branching, Supabase read replicas) |
| LISTEN/NOTIFY for event fan-out | Not available | Built-in pub/sub for cross-instance notification |
| Connection pooling | N/A | PgBouncer / Neon serverless driver |
| Operational maturity at scale | Excellent for embedded, fragile for multi-tenant | Industry standard for this workload |

---

## Architecture

```
                        PostgreSQL (Neon / Supabase)
                              │
                    ┌─────────┼─────────┐
                    │  LISTEN/NOTIFY     │
                    ▼         ▼         ▼
              Orch-US     Orch-EU     Orch-AP
              (iad)       (ams)       (nrt)
                │           │           │
                ▼           ▼           ▼
           Fly Machines  Fly Machines  Fly Machines
           (regional)   (regional)   (regional)
```

### Event Flow with LISTEN/NOTIFY

```
1. Webhook arrives at Orch-US
2. Orch-US inserts event into PostgreSQL
3. Orch-US executes: NOTIFY persona_events, '{"eventId":"...","projectId":"proj_A"}'
4. All orchestrators receive the notification
5. Each orchestrator checks: "Am I responsible for proj_A?"
   - Orch-US: yes → claims and processes the event
   - Orch-EU: no → ignores
   - Orch-AP: no → ignores
6. For global events (projectId=NULL):
   NOTIFY persona_events, '{"eventId":"...","projectId":null}'
   - ALL orchestrators check their local subscriptions
   - Each processes matches for their assigned projects
```

---

## Implementation Plan

### Step 1: Abstract the database layer

**Current problem:** All 50+ DB functions in `db.ts` call `better-sqlite3` directly with synchronous `.run()`, `.get()`, `.all()`. PostgreSQL is async. Changing every call site is error-prone.

**Solution:** Create a `DatabaseAdapter` interface that both SQLite and PostgreSQL implement:

**File:** `packages/orchestrator/src/db-adapter.ts` (new)

```typescript
export interface DatabaseAdapter {
  // Event operations
  publishEvent(params: PublishEventParams): Promise<PersonaEvent>;
  claimEvents(batchSize: number): Promise<ClaimedEvent[]>;
  updateEventStatus(eventId: string, status: string): Promise<void>;
  scheduleEventRetry(eventId: string, retryCount: number, delayMs: number, error: string): Promise<void>;
  moveEventToDeadLetter(eventId: string, error: string): Promise<void>;
  recoverStaleProcessingEvents(): Promise<number>;

  // Subscription operations
  getSubscriptionsByEventType(eventType: string, projectId?: string): Promise<PersonaEventSubscription[]>;
  createSubscription(params: CreateSubscriptionParams): Promise<PersonaEventSubscription>;

  // Persona operations
  getPersona(id: string): Promise<Persona | null>;
  getToolsForPersona(personaId: string): Promise<PersonaToolDefinition[]>;
  listCredentialsForPersona(personaId: string): Promise<PersonaCredential[]>;

  // Execution operations
  createExecution(params: CreateExecutionParams): Promise<PersonaExecution>;
  updateExecution(id: string, updates: Partial<PersonaExecution>): Promise<void>;
  countRunningExecutions(personaId: string): Promise<number>;

  // Trigger operations
  getDueTriggers(): Promise<DueTrigger[]>;
  recordTriggerFiring(params: TriggerFiringParams): Promise<void>;
  updateTriggerTimings(triggerId: string, lastFiredAt: string, nextTriggerAt: string | null): Promise<void>;

  // Audit
  batchAppendEventAudit(entries: AuditEntry[]): Promise<void>;

  // Lifecycle
  close(): Promise<void>;

  // Transaction support
  transaction<T>(fn: (adapter: DatabaseAdapter) => Promise<T>): Promise<T>;
}
```

### Step 2: Wrap existing SQLite code as SqliteAdapter

**File:** `packages/orchestrator/src/db-sqlite.ts` (new)

Wrap the existing `db.ts` functions as an implementation of `DatabaseAdapter`. Since `better-sqlite3` is synchronous, wrap calls in `Promise.resolve()`:

```typescript
import Database from 'better-sqlite3';
import * as db from './db.js';
import type { DatabaseAdapter } from './db-adapter.js';

export class SqliteAdapter implements DatabaseAdapter {
  constructor(private database: Database.Database) {}

  async publishEvent(params: PublishEventParams): Promise<PersonaEvent> {
    return db.publishEvent(this.database, params);
  }

  async claimEvents(batchSize: number): Promise<ClaimedEvent[]> {
    return db.claimEvents(this.database, batchSize);
  }

  async transaction<T>(fn: (adapter: DatabaseAdapter) => Promise<T>): Promise<T> {
    // SQLite transactions are synchronous — wrap with BEGIN/COMMIT
    return this.database.transaction(async () => {
      return fn(this);
    }).immediate();
  }

  // ... wrap all other db.* functions
}
```

This preserves all existing SQLite behavior with zero risk. The adapter is a thin wrapper.

### Step 3: Create PostgresAdapter

**File:** `packages/orchestrator/src/db-postgres.ts` (new)

```typescript
import { Pool, type PoolClient } from 'pg';
import type { DatabaseAdapter } from './db-adapter.js';

export class PostgresAdapter implements DatabaseAdapter {
  private pool: Pool;
  private listenClient: PoolClient | null = null;

  constructor(connectionString: string) {
    this.pool = new Pool({
      connectionString,
      max: 10,
      idleTimeoutMillis: 30_000,
    });
  }

  async publishEvent(params: PublishEventParams): Promise<PersonaEvent> {
    const result = await this.pool.query(
      `INSERT INTO persona_events (id, event_type, source_type, source_id, target_persona_id, project_id, use_case_id, payload, status, created_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'pending', NOW())
       RETURNING *`,
      [nanoid(), params.eventType, params.sourceType, params.sourceId, params.targetPersonaId, params.projectId, params.useCaseId, params.payload],
    );

    // Notify other orchestrators
    await this.pool.query(
      `SELECT pg_notify('persona_events', $1)`,
      [JSON.stringify({ eventId: result.rows[0].id, projectId: params.projectId })],
    );

    return result.rows[0];
  }

  async claimEvents(batchSize: number): Promise<ClaimedEvent[]> {
    // Use SELECT ... FOR UPDATE SKIP LOCKED for safe multi-instance claiming
    const result = await this.pool.query(
      `UPDATE persona_events
       SET status = 'processing'
       WHERE id IN (
         SELECT id FROM persona_events
         WHERE (status = 'pending' OR (status = 'retry' AND next_retry_at <= NOW()))
         ORDER BY created_at ASC
         LIMIT $1
         FOR UPDATE SKIP LOCKED
       )
       RETURNING *`,
      [batchSize],
    );
    return result.rows;
  }

  /**
   * Start listening for cross-instance event notifications.
   * When another orchestrator publishes an event, this fires the callback.
   */
  async startListening(callback: (payload: { eventId: string; projectId: string | null }) => void): Promise<void> {
    this.listenClient = await this.pool.connect();
    await this.listenClient.query('LISTEN persona_events');
    await this.listenClient.query('LISTEN persona_triggers');

    this.listenClient.on('notification', (msg) => {
      if (msg.channel === 'persona_events' && msg.payload) {
        try {
          callback(JSON.parse(msg.payload));
        } catch { /* malformed notification */ }
      }
    });
  }

  async transaction<T>(fn: (adapter: DatabaseAdapter) => Promise<T>): Promise<T> {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      const txAdapter = new PostgresTransactionAdapter(client);
      const result = await fn(txAdapter);
      await client.query('COMMIT');
      return result;
    } catch (err) {
      await client.query('ROLLBACK');
      throw err;
    } finally {
      client.release();
    }
  }

  // ... implement remaining methods
}
```

### Step 4: Update eventProcessor to be async-compatible

**File:** `packages/orchestrator/src/eventProcessor.ts`

The event processor currently uses synchronous SQLite calls inside `database.transaction()`. With the adapter pattern, all calls become async:

```typescript
// Before (sync):
const allEvents = db.claimEvents(database, CLAIM_BATCH_SIZE);

// After (async):
const allEvents = await adapter.claimEvents(CLAIM_BATCH_SIZE);
```

The `eventBusTick` function and `processEvent` function must become async. The notification system (`notify()` via `queueMicrotask`) already supports this — just await the tick:

```typescript
async function eventBusTick(adapter: DatabaseAdapter, dispatcher: Dispatcher, logger: Logger): Promise<boolean> {
  const allEvents = await adapter.claimEvents(CLAIM_BATCH_SIZE);
  // ... rest of logic, with await on each db call
}
```

### Step 5: Multi-orchestrator project assignment

Each orchestrator instance is responsible for a subset of projects. This prevents duplicate processing of scoped events:

```typescript
// orchestrator/src/config.ts — new config fields
assignedProjects: string[];         // e.g., ['proj_A', 'proj_B'] or ['*'] for all
instanceId: string;                 // Unique instance identifier

// In eventProcessor — filter claimed events by assignment
const allEvents = await adapter.claimEvents(CLAIM_BATCH_SIZE);
const myEvents = allEvents.filter(e =>
  e.projectId === null ||           // Global events processed by all
  config.assignedProjects.includes('*') ||
  config.assignedProjects.includes(e.projectId)
);
```

For global events, all orchestrators process the event but only dispatch to their assigned projects' subscriptions. The `FOR UPDATE SKIP LOCKED` in PostgreSQL prevents duplicate claiming.

### Step 6: PostgreSQL schema

**File:** `packages/orchestrator/src/schema.sql` (new)

Port the SQLite schema to PostgreSQL with appropriate types:

```sql
-- Key differences from SQLite:
-- 1. TEXT → TEXT (same)
-- 2. INTEGER → INTEGER or BIGINT
-- 3. REAL → DOUBLE PRECISION
-- 4. AUTOINCREMENT → GENERATED ALWAYS AS IDENTITY
-- 5. datetime('now') → NOW()
-- 6. Add proper indexes for FOR UPDATE SKIP LOCKED performance

CREATE TABLE IF NOT EXISTS persona_events (
  id TEXT PRIMARY KEY,
  event_type TEXT NOT NULL,
  source_type TEXT NOT NULL,
  source_id TEXT,
  target_persona_id TEXT,
  project_id TEXT,                    -- NULL = global event
  use_case_id TEXT,
  payload TEXT,
  status TEXT NOT NULL DEFAULT 'pending',
  retry_count INTEGER NOT NULL DEFAULT 0,
  next_retry_at TIMESTAMPTZ,
  error_message TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  processed_at TIMESTAMPTZ
);

-- Index for claim query (most critical for performance)
CREATE INDEX idx_pe_claim ON persona_events(status, created_at)
  WHERE status IN ('pending', 'retry');

-- Index for global event queries
CREATE INDEX idx_pe_global ON persona_events(status, event_type)
  WHERE project_id IS NULL;
```

### Step 7: Migration tooling

**File:** `packages/orchestrator/src/migrate-sqlite-to-pg.ts` (new)

One-time migration script to copy data from SQLite to PostgreSQL:

```typescript
// Read all tables from SQLite
// Insert into PostgreSQL in batches of 1000
// Verify row counts match
// This runs once during cutover
```

---

## Deployment Topology

### Single-region (first deployment)

```
Neon PostgreSQL (free tier, us-east-1)
         │
    Orch-US (Fly.io, iad)
    assignedProjects: ['*']
         │
    Fly Machines (iad, on-demand)
```

Cost: Neon free tier ($0) + Fly orchestrator ($10) + Fly Machines (~$36) = **$46/month**

### Multi-region (at scale)

```
Neon PostgreSQL (Pro, us-east-1, read replicas in EU/AP)
         │
    ┌────┼────┐
    │    │    │
 Orch-US  Orch-EU  Orch-AP
 iad      ams      nrt
 ['proj_1-500']  ['proj_501-800']  ['proj_801-1000']
    │        │         │
 Machines  Machines  Machines
 (regional) (regional) (regional)
```

Cost: Neon Pro ($20) + 3× Fly orchestrators ($30) + Fly Machines (~$36) = **$86/month**

---

## When NOT to Execute This Phase

- If you have < 3,000 agents — SQLite handles this easily
- If event processing latency is < 100ms — SQLite WAL is not the bottleneck
- If you don't need regional presence — single-region is fine
- If you don't need HA — a single orchestrator with systemd restart gives 99.9% uptime

**Measure first, migrate only when you hit real limits.** The SQLite adapter stays functional indefinitely as a fallback.

---

## Validation Checklist

- [ ] PostgresAdapter passes all existing unit tests (same interface as SqliteAdapter)
- [ ] `FOR UPDATE SKIP LOCKED` prevents duplicate event claims across instances
- [ ] `pg_notify` triggers event processing on remote instances within 100ms
- [ ] Global events are processed by all instances (each handles their projects)
- [ ] Scoped events are processed by only the assigned instance
- [ ] Transaction rollback works correctly (event status not corrupted)
- [ ] Migration script transfers all data without loss
- [ ] SQLite fallback still works (`DB_BACKEND=sqlite` env var)
- [ ] Connection pooling handles 10+ concurrent event batches
- [ ] Neon serverless driver handles cold starts gracefully
- [ ] Orchestrator instance crash → other instances pick up orphaned events via stale recovery
