# Phase 3: Global Event Subscriptions

> **Goal:** Enable cross-client event routing — global (shared) events that any user's agents can subscribe to, alongside project-scoped local events.
> **Estimated effort:** 2-3 days
> **Risk:** Low — extends existing subscription matching, no architectural changes
> **Cost impact:** None (infrastructure unchanged)
> **Dependency:** Phase 1 (Kafka removed), Phase 2 (Fly Machines operational)

---

## Current State

Events are scoped by `project_id`. The subscription matcher in `eventProcessor.ts` queries subscriptions for a specific event type AND project:

```typescript
// eventProcessor.ts:633-636
const projId = event.projectId !== 'default' ? event.projectId : undefined;
const subs = db.getSubscriptionsByEventType(database, event.eventType, projId);
```

```typescript
// db.ts — getSubscriptionsByEventType (simplified):
SELECT * FROM persona_event_subscriptions
WHERE event_type = ? AND project_id = ?
```

This means an event published to `project_A` only matches subscriptions in `project_A`. There is no way to:
- Publish a global event that all projects' agents can subscribe to
- Subscribe an agent to events from a different project
- Share event feeds (market data, system alerts) across users

---

## Design

### Event Scoping Model

```
┌─────────────────────────────────────────────────┐
│  Global Events (project_id = NULL)              │
│  e.g., market_data, system_alert, news_feed     │
│                                                  │
│  Visible to ALL subscriptions (global + scoped) │
└─────────────────────────────────────────────────┘

┌──────────────────────┐  ┌──────────────────────┐
│  Project A Events    │  │  Project B Events    │
│  (project_id = 'A')  │  │  (project_id = 'B')  │
│                      │  │                      │
│  Visible only to     │  │  Visible only to     │
│  Project A subs      │  │  Project B subs      │
└──────────────────────┘  └──────────────────────┘
```

### Subscription Matching Rules

| Event scope | Subscription scope | Match? |
|---|---|---|
| Global (NULL) | Global (NULL) | Yes |
| Global (NULL) | Project A | Yes — global events fan out to all projects |
| Project A | Project A | Yes — same project |
| Project A | Project B | **No** — cross-project isolation |
| Project A | Global (NULL) | **No** — scoped events don't leak upward |

---

## Implementation Plan

### Step 1: Allow NULL project_id in events

**File:** `packages/orchestrator/src/db.ts`

The `publishEvent` function currently requires `projectId` (defaults to `'default'`). Allow explicit `null` for global events:

```typescript
// db.ts — publishEvent
export function publishEvent(db: Database.Database, params: {
  eventType: string;
  sourceType: string;
  sourceId: string | null;
  targetPersonaId?: string | null;
  projectId?: string | null;       // null = global event
  useCaseId?: string | null;
  payload?: string | null;
}): PersonaEvent {
  const id = nanoid();
  const now = new Date().toISOString();
  const projectId = params.projectId ?? null;  // was: ?? 'default'

  db.prepare(`
    INSERT INTO persona_events (id, event_type, source_type, source_id, target_persona_id, project_id, use_case_id, payload, status, created_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?)
  `).run(id, params.eventType, params.sourceType, params.sourceId, params.targetPersonaId ?? null, projectId, params.useCaseId ?? null, params.payload ?? null, now);

  return { id, eventType: params.eventType, status: 'pending', projectId, createdAt: now } as PersonaEvent;
}
```

### Step 2: Update subscription query for global event fan-out

**File:** `packages/orchestrator/src/db.ts`

The key change — when an event has `project_id IS NULL` (global), match ALL subscriptions for that event type regardless of their project. When scoped, match only that project's subscriptions:

```typescript
export function getSubscriptionsByEventType(
  db: Database.Database,
  eventType: string,
  projectId: string | undefined,
): PersonaEventSubscription[] {
  if (!projectId) {
    // Global event → match all subscriptions for this event type (any project)
    return db.prepare(`
      SELECT * FROM persona_event_subscriptions
      WHERE event_type = ? AND enabled = 1
    `).all(eventType) as PersonaEventSubscription[];
  }

  // Scoped event → match only this project's subscriptions
  return db.prepare(`
    SELECT * FROM persona_event_subscriptions
    WHERE event_type = ? AND (project_id = ? OR project_id IS NULL) AND enabled = 1
  `).all(eventType, projectId) as PersonaEventSubscription[];
}
```

The `OR project_id IS NULL` clause allows global subscriptions (subscriptions not scoped to any project) to match scoped events within their own project context. This is optional — remove it if you want strict isolation.

### Step 3: Update eventProcessor for global event handling

**File:** `packages/orchestrator/src/eventProcessor.ts`

The event processor already handles the routing. The only change is in the cache key and subscription lookup:

```typescript
// eventProcessor.ts:633-636 — update project resolution
const projId = event.projectId; // was: event.projectId !== 'default' ? event.projectId : undefined
// null projectId now means "global" — getSubscriptionsByEventType handles the fan-out
const subs = db.getSubscriptionsByEventType(database, event.eventType, projId ?? undefined);
```

Update the batch cache key to handle null projects:

```typescript
function tickCacheKey(eventType: string, projectId: string | null | undefined): string {
  return `${eventType}:${projectId ?? '__global__'}`;
}
```

### Step 4: Add global event publishing HTTP endpoint

**File:** `packages/orchestrator/src/httpApi.ts`

Add an endpoint for publishing global events (admin-only or with specific scoping):

```typescript
// POST /api/events/global
// Requires admin auth — global events affect all projects
if (method === 'POST' && pathname === '/api/events/global') {
  requireAuth(req, auth, 'admin');
  const body = await parseBody(req, EventCreateSchema);

  const event = db.publishEvent(database, {
    eventType: body.eventType,
    sourceType: body.sourceType || 'api',
    sourceId: body.sourceId || null,
    projectId: null,  // null = global
    payload: body.payload ? JSON.stringify(body.payload) : null,
  });

  eventNotify(); // Trigger immediate processing
  return jsonResponse(res, event, 201);
}
```

### Step 5: Add global subscription creation

**File:** `packages/orchestrator/src/httpApi.ts`

Allow creating subscriptions without a `project_id` (admin creates shared subscriptions):

```typescript
// POST /api/subscriptions/global
// Creates a subscription that matches global events
if (method === 'POST' && pathname === '/api/subscriptions/global') {
  requireAuth(req, auth, 'admin');
  const body = await parseBody(req, SubscriptionCreateSchema);

  const sub = db.createSubscription(database, {
    ...body,
    projectId: null,  // Global scope
  });

  return jsonResponse(res, sub, 201);
}
```

### Step 6: Webhook ingestion with global scope

**File:** `packages/orchestrator/src/httpApi.ts`

For shared webhook endpoints (e.g., a market data feed), allow publishing as global:

```typescript
// POST /api/webhooks/:webhookId — existing webhook endpoint
// Add a `scope` query parameter:
const scope = url.searchParams.get('scope');
const projectId = scope === 'global' ? null : resolvedProjectId;
```

### Step 7: Update schema and add migration

**File:** `packages/orchestrator/src/db.ts`

Add migration to update the `project_id` column to be nullable in subscriptions:

```typescript
{
  version: 10,
  description: 'Allow NULL project_id for global events and subscriptions',
  up: (db) => {
    // SQLite columns are already nullable by default unless NOT NULL is specified.
    // The existing DEFAULT 'default' constraint needs to be updated.
    // Since SQLite doesn't support ALTER COLUMN, create index for global queries:
    db.exec(`
      CREATE INDEX IF NOT EXISTS idx_pes_global
        ON persona_event_subscriptions(event_type)
        WHERE project_id IS NULL;
      CREATE INDEX IF NOT EXISTS idx_pe_global
        ON persona_events(status, event_type)
        WHERE project_id IS NULL;
    `);
  },
}
```

---

## Concurrency & Rate Limiting Considerations

Global events can fan out to hundreds of subscriptions across many projects. This creates amplification:

| Scenario | Fan-out | Impact |
|---|---|---|
| Global event, 100 projects, 1 sub each | 100 executions | 100 Fly Machines created |
| Global event, 100 projects, 3 subs each | 300 executions | Dispatcher queue fills |

### Safeguards

1. **Batch claim limit** — `CLAIM_BATCH_SIZE = 50` in `eventProcessor.ts` already limits per-tick processing
2. **Queue depth** — `MAX_QUEUE_DEPTH = 500` in `dispatcher.ts` rejects excess
3. **Concurrency limits** — Per-persona `maxConcurrent` still applies
4. **Rate limiting on global event publish** — Add a separate rate category:

```typescript
// httpApi.ts — rate limiter
const CATEGORY_LIMITS: Record<RateCategory, number> = {
  auth: 10,
  execute: 30,
  webhook: 60,
  crud: 100,
  global_event: 5,   // Max 5 global events/minute per IP
};
```

---

## Example: Market Data Feed

```
1. External service calls POST /api/events/global
   { eventType: "market_data_updated", payload: { symbol: "AAPL", price: 185.42 } }

2. eventProcessor claims the event (project_id = NULL)

3. getSubscriptionsByEventType("market_data_updated", undefined)
   → Returns subscriptions from ALL projects:
     - Project A: "trading-bot" persona subscribes to market_data_updated
     - Project B: "portfolio-tracker" persona subscribes with payloadFilter: { symbol: "AAPL" }
     - Project C: no matching subscription

4. matchEvent evaluates payload filters:
   - Project A: match (no filter)
   - Project B: match (symbol === "AAPL")

5. dispatcher.submit() creates Fly Machines for both executions
```

---

## Validation Checklist

- [ ] Global event (project_id=NULL) matches subscriptions in all projects
- [ ] Scoped event (project_id='A') matches only project A subscriptions
- [ ] Cross-project isolation: project A events never reach project B agents
- [ ] Global event + payload filter works correctly
- [ ] Rate limiting prevents global event flood
- [ ] Concurrency limits still apply per-persona during fan-out
- [ ] Retry/dead-letter logic works for global events
- [ ] Audit trail records which projects received global events
- [ ] Desktop clients see global-triggered executions in their project view
