# Phase 1: Drop Kafka Dependency

> **Goal:** Remove Kafka as a runtime dependency without losing any functional capability.
> **Estimated effort:** 1-2 days
> **Risk:** Low — Kafka is already optional (`createNoopKafkaClient` exists and works)
> **Cost impact:** Saves $100-200/month (managed Kafka) + removes operational complexity

---

## Current State

Kafka serves three roles in the orchestrator:

| Role | Topic | Critical path? |
|---|---|---|
| Execution request ingestion | `persona.exec.v1` | No — `eventProcessor` dispatches directly via `dispatcher.submit()` |
| Execution output streaming | `persona.output.v1` | No — desktop clients receive output via WebSocket already |
| Lifecycle events | `persona.lifecycle.v1` | No — `DbSink` handles all state transitions |
| Dead letter routing | `persona.dlq.v1` | No — `db.moveEventToDeadLetter()` covers this |

The SQLite event bus is the **primary** event path. Kafka is a **secondary output sink** used by `KafkaSink` in the `CompositeSink` fan-out pattern.

### Files involved

| File | Kafka usage |
|---|---|
| `orchestrator/src/index.ts:55-101` | Creates Kafka client, subscribes to `EXEC_REQUESTS` topic |
| `orchestrator/src/kafka.ts` | Full Kafka client with `ProducerOutbox`, HMAC signing, nonce dedup |
| `orchestrator/src/dispatcher.ts:523-558` | `KafkaSink` — produces to output/lifecycle topics |
| `orchestrator/src/dispatcher.ts:1011-1070` | Dispatcher constructor accepts `KafkaClient`, event production |
| `orchestrator/src/config.ts` | 6 Kafka-related config fields |
| `shared/src/crypto.ts:139` | `signKafkaMessage` / `verifyKafkaMessage` |
| `shared/src/types.ts:312-337` | `TOPICS` constant, `projectTopic()`, `extractProjectId()` |

---

## Implementation Plan

### Step 1: Set `kafkaEnabled: false` as the default

**File:** `orchestrator/src/config.ts`

Change the default from reading `KAFKA_ENABLED` env var to defaulting to `false`:

```typescript
kafkaEnabled: process.env['KAFKA_ENABLED'] === 'true', // was: !== 'false'
```

This is a zero-risk change — `createNoopKafkaClient` already handles all method calls as no-ops. The entire system continues working via SQLite event bus + WebSocket output.

**Verify:** Start orchestrator without any `KAFKA_*` env vars. Confirm:
- Event processing works (`eventProcessor` claims + dispatches)
- Trigger scheduler fires events
- Worker output streams via WebSocket
- Execution lifecycle tracked in SQLite

### Step 2: Remove Kafka from the Dispatcher sink chain

**File:** `orchestrator/src/dispatcher.ts`

In the `Dispatcher` constructor (~line 1011), the `CompositeSink` is assembled with `DbSink`, `KafkaSink`, and `MetricsSink`. When Kafka is disabled, `KafkaSink` produces to the no-op client which silently drops messages — this works but is pointless. Clean it up:

```typescript
// Before:
const sinks: ExecutionSink[] = [
  new DbSink(this.getDatabase.bind(this), this.logger),
  new KafkaSink(this.kafka, this.logger),
  new MetricsSink(this.metrics),
];

// After:
const sinks: ExecutionSink[] = [
  new DbSink(this.getDatabase.bind(this), this.logger),
  new MetricsSink(this.metrics),
];
if (this.kafka !== noopKafka) {
  sinks.splice(1, 0, new KafkaSink(this.kafka, this.logger));
}
```

This makes the sink chain explicit about whether Kafka is active.

### Step 3: Remove Kafka subscription from startup

**File:** `orchestrator/src/index.ts:77-101`

The `kafka.subscribe()` block subscribes to `EXEC_REQUESTS` for cross-system execution injection. This is already guarded by `if (config.kafkaEnabled)`, so no change is needed. But document that this code path is inactive by default.

### Step 4: Remove Kafka config from required env vars

**File:** `orchestrator/src/config.ts`

Move Kafka config fields to optional-only (they already are, but clean up any documentation that suggests they're required):

```typescript
kafkaBrokers: process.env['KAFKA_BROKERS'] || '',
kafkaUsername: process.env['KAFKA_USERNAME'] || '',
kafkaPassword: process.env['KAFKA_PASSWORD'] || '',
kafkaSigningKey: process.env['KAFKA_SIGNING_KEY'] || '',
kafkaDeploymentId: process.env['KAFKA_DEPLOYMENT_ID'] || '',
```

### Step 5: Update Dockerfile / docker-compose

Remove Kafka broker from any docker-compose or deployment configuration. Remove Kafka-related env vars from deployment templates (`.env.example`, CI/CD configs).

### Step 6: Remove event production on execution complete

**File:** `orchestrator/src/dispatcher.ts:1069-1070`

The dispatcher produces events to Kafka when an execution completes (for external consumers). With Kafka disabled, this call goes to the no-op client. If Kafka is permanently removed, delete this code block:

```typescript
// This block can be removed:
const eventTopic = projectTopic(TOPICS.EVENTS, exec?.projectId);
this.kafka.produce(eventTopic, JSON.stringify({ ... }));
```

---

## What NOT to Delete Yet

Keep these files intact — they are clean, well-tested, and cost nothing to retain:

- `kafka.ts` — The `ProducerOutbox` class and `createNoopKafkaClient` are useful if you ever need Kafka again
- `shared/src/crypto.ts` — `signKafkaMessage`/`verifyKafkaMessage` are also used for other HMAC purposes
- `shared/src/types.ts` — `TOPICS` constant has no runtime cost

Deleting code that works and costs nothing is premature. Mark it as `@deprecated` if you want a signal.

---

## Validation Checklist

- [ ] Orchestrator starts with zero Kafka env vars
- [ ] Events flow: HTTP webhook → SQLite → eventProcessor → dispatcher → worker
- [ ] Trigger scheduler fires events and triggers immediate processing
- [ ] Execution output streams to desktop client via WebSocket
- [ ] Execution completion updates SQLite status correctly
- [ ] Dead letter events are recorded in SQLite (not Kafka DLQ)
- [ ] No Kafka-related errors in logs
- [ ] Managed Kafka service can be deprovisioned

---

## Rollback Plan

Set `KAFKA_ENABLED=true` and provide Kafka credentials. The system reconnects immediately — no code changes needed.
