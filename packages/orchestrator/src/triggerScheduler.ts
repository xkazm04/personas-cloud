import type Database from 'better-sqlite3';
import type { Logger } from 'pino';
import * as db from './db/index.js';
import { TriggerHeap, type HeapEntry } from './triggerHeap.js';
import type { TenantDbManager } from './tenantDbManager.js';

const MIN_DELAY_MS = 100;
const MAX_DELAY_MS = 60_000;
const HEARTBEAT_INTERVAL_MS = 60_000;

export type ErrorClass = 'config_error' | 'db_error' | 'orphan_trigger';

function zeroErrorCounts(): Record<ErrorClass, number> {
  return { config_error: 0, db_error: 0, orphan_trigger: 0 };
}

export interface SchedulerMetrics {
  heap_size: number;
  next_due_at: string | null;
  triggers_fired_total: number;
  triggers_missed_total: number;
  avg_tick_latency_ms: number;
  last_tick_at: string | null;
  errors_by_class: Record<ErrorClass, number>;
}

export interface TriggerSchedulerHandle {
  /** Stop the scheduler. */
  close(): void;
  /** Nudge the scheduler to re-evaluate immediately (e.g. after trigger create/update). */
  nudge(): void;
  /** Push or update a trigger in the heap after CRUD. */
  heapPush(triggerId: string, tenantId: string, nextTriggerAt: string | null): void;
  /** Remove a trigger from the heap (e.g. after delete or disable). */
  heapRemove(triggerId: string): void;
  /** Return current scheduler metrics for observability. */
  getMetrics(): SchedulerMetrics;
=======
import * as db from './db.js';
import { computeNextCron } from './cronParser.js';

/** Minimum allowed interval between trigger firings (60 seconds). */
const MIN_INTERVAL_MS = 60_000;

/** Max length for user-supplied event types from trigger configs. */
const MAX_EVENT_TYPE_LENGTH = 64;

/** Pattern for valid event types: alphanumeric and underscores only. */
const EVENT_TYPE_PATTERN = /^[a-zA-Z0-9_]+$/;

/**
 * Prefixes reserved for system-generated event types (e.g. GitLab webhooks,
 * generic webhook ingestion).  Trigger configs must not spoof these.
 */
const RESERVED_EVENT_TYPE_PREFIXES = ['gitlab_', 'webhook_'];

/**
 * Validate and sanitize a user-supplied event_type from trigger config.
 * Returns the validated event type, or null if the value is invalid.
 */
function validateTriggerEventType(raw: string): string | null {
  if (raw.length === 0 || raw.length > MAX_EVENT_TYPE_LENGTH) return null;
  if (!EVENT_TYPE_PATTERN.test(raw)) return null;
  const lower = raw.toLowerCase();
  for (const prefix of RESERVED_EVENT_TYPE_PREFIXES) {
    if (lower.startsWith(prefix)) return null;
  }
  return raw;
}

/** Shape of the JSON stored in trigger.config. */
interface TriggerConfig {
  event_type?: string;
  payload?: unknown;
  cron?: string;
  interval_seconds?: number;
}

// ---------------------------------------------------------------------------
// Parsed config cache — avoids JSON.parse on every 5-second tick for stable configs.
// Keyed by triggerId, stores the raw config string for invalidation detection.
// ---------------------------------------------------------------------------
const MAX_CONFIG_CACHE_SIZE = 500;

interface CachedTriggerConfig {
  raw: string;
  parsed: TriggerConfig;
}

const triggerConfigCache = new Map<string, CachedTriggerConfig>();

function getCachedTriggerConfig(triggerId: string, configJson: string | null): TriggerConfig | null {
  if (!configJson) return null;

  const cached = triggerConfigCache.get(triggerId);
  if (cached && cached.raw === configJson) {
    return cached.parsed;
  }

  try {
    const parsed = JSON.parse(configJson) as TriggerConfig;

    // Evict oldest entries if cache is full (simple FIFO eviction)
    if (triggerConfigCache.size >= MAX_CONFIG_CACHE_SIZE) {
      const firstKey = triggerConfigCache.keys().next().value!;
      triggerConfigCache.delete(firstKey);
    }

    triggerConfigCache.set(triggerId, { raw: configJson, parsed });
    return parsed;
  } catch {
    return null;
  }
}

/** Invalidate a specific trigger's cached config (e.g. after API update). */
export function invalidateTriggerConfigCache(triggerId: string): void {
  triggerConfigCache.delete(triggerId);
}

/**
 * Start the heap-driven trigger scheduler.
 *
 * On startup the scheduler loads all enabled triggers from the DB into an
 * in-memory min-heap keyed by next_trigger_at.  On each tick it pops entries
 * whose time has passed, fires them, and pushes the updated next-fire times
 * back into the heap.  The DB remains the source of truth — the heap is a
 * hot-path cache that eliminates full table scans.
=======
 * Runs every `interval` ms (default 5s), evaluates due triggers,
 * and publishes events to the event bus (DB-backed).
 *
 * @param onTrigger Optional callback invoked after one or more triggers fire.
 *                  Used to notify the event processor for immediate dispatch.
 */
export function startTriggerScheduler(
  database: Database.Database | null,
  logger: Logger,
  _interval?: number, // kept for signature compat; ignored
  tenantDbManager?: TenantDbManager,
  onEventPublished?: () => void,
): TriggerSchedulerHandle {
  let timer: ReturnType<typeof setTimeout> | null = null;
  let heartbeatTimer: ReturnType<typeof setInterval> | null = null;
  let stopped = false;
  const heap = new TriggerHeap();
=======
  interval: number = 5000,
  onTrigger?: () => void,
): NodeJS.Timeout {
  logger.info({ intervalMs: interval }, 'Trigger scheduler started');

  // ---- Metrics accumulators ----
  let triggersFiredTotal = 0;
  let triggersMissedTotal = 0;
  let tickCount = 0;
  let tickLatencySum = 0;
  let lastTickAt: number | null = null;
  /** Per-tick missed counts since last heartbeat, for correlation. */
  let tickMissedCounts: number[] = [];
  const errorsByClass = zeroErrorCounts();
  const errorsSinceHeartbeat = zeroErrorCounts();

  // ---- Heap bootstrap: load all enabled triggers from DB ----
  const bootstrap = buildHeap(heap, database, tenantDbManager, logger);

  logger.info(
    { heapSize: heap.size, minDelayMs: MIN_DELAY_MS, maxDelayMs: MAX_DELAY_MS, tenantIsolated: !!tenantDbManager, bootstrapDurationMs: bootstrap.totalDurationMs },
    'Heap-driven trigger scheduler started',
  );

  function scheduleTick(delayMs: number): void {
    if (stopped) return;
    timer = setTimeout(tick, delayMs);
  }

  function tick(): void {
    if (stopped) return;
    const tickStart = Date.now();
    try {
      const now = tickStart;
      const due = heap.popDue(now);

      if (due.length > 0) {
        const result = fireDueEntries(due, now, database, tenantDbManager, heap, logger, onEventPublished);
        triggersFiredTotal += result.fired;
        triggersMissedTotal += result.missed;
        if (result.missed > 0) {
          tickMissedCounts.push(result.missed);
        }
        for (const cls of Object.keys(result.errors) as ErrorClass[]) {
          errorsByClass[cls] += result.errors[cls];
          errorsSinceHeartbeat[cls] += result.errors[cls];
        }
      }
=======
      triggerSchedulerTick(database, logger, onTrigger);
    } catch (err) {
      logger.error({ err }, 'Trigger scheduler tick failed');
    }

    // Track tick latency
    const tickEnd = Date.now();
    tickCount++;
    tickLatencySum += tickEnd - tickStart;
    lastTickAt = tickEnd;

    // Schedule next tick based on heap peek
    const delayMs = computeNextDelay(heap);
    scheduleTick(delayMs);
  }

  // Kick off the first tick immediately
  scheduleTick(0);

  // ---- Heartbeat log every 60 seconds ----
  heartbeatTimer = setInterval(() => {
    if (stopped) return;
    const top = heap.peek();
    const missedSinceLastHeartbeat = tickMissedCounts.reduce((a, b) => a + b, 0);
    logger.info({
      heap_size: heap.size,
      next_due_at: top ? new Date(top.nextTriggerAt).toISOString() : null,
      triggers_fired_total: triggersFiredTotal,
      triggers_missed_total: triggersMissedTotal,
      triggers_missed_since_last_heartbeat: missedSinceLastHeartbeat,
      missed_per_tick: tickMissedCounts.length > 0 ? tickMissedCounts : undefined,
      avg_tick_latency_ms: tickCount > 0 ? Math.round(tickLatencySum / tickCount * 100) / 100 : 0,
      last_tick_at: lastTickAt ? new Date(lastTickAt).toISOString() : null,
      errors_by_class: errorsByClass,
      errors_since_heartbeat: errorsSinceHeartbeat,
    }, 'Scheduler heartbeat');
    tickMissedCounts = [];
    errorsSinceHeartbeat.config_error = 0;
    errorsSinceHeartbeat.db_error = 0;
    errorsSinceHeartbeat.orphan_trigger = 0;
  }, HEARTBEAT_INTERVAL_MS);

  return {
    close() {
      stopped = true;
      if (timer != null) {
        clearTimeout(timer);
        timer = null;
      }
      if (heartbeatTimer != null) {
        clearInterval(heartbeatTimer);
        heartbeatTimer = null;
      }
    },
    nudge() {
      if (stopped) return;
      if (timer != null) {
        clearTimeout(timer);
        timer = null;
      }
      scheduleTick(0);
    },
    heapPush(triggerId: string, tenantId: string, nextTriggerAt: string | null) {
      if (!nextTriggerAt) {
        heap.remove(triggerId);
        return;
      }
      heap.push({ triggerId, tenantId, nextTriggerAt: new Date(nextTriggerAt).getTime() });
    },
    heapRemove(triggerId: string) {
      heap.remove(triggerId);
    },
    getMetrics(): SchedulerMetrics {
      const top = heap.peek();
      return {
        heap_size: heap.size,
        next_due_at: top ? new Date(top.nextTriggerAt).toISOString() : null,
        triggers_fired_total: triggersFiredTotal,
        triggers_missed_total: triggersMissedTotal,
        avg_tick_latency_ms: tickCount > 0 ? Math.round(tickLatencySum / tickCount * 100) / 100 : 0,
        last_tick_at: lastTickAt ? new Date(lastTickAt).toISOString() : null,
        errors_by_class: { ...errorsByClass },
      };
    },
  };
}

// ---------------------------------------------------------------------------
// Heap bootstrap — load all enabled triggers from every tenant DB
// ---------------------------------------------------------------------------

interface BootstrapSummary {
  totalDurationMs: number;
  tenantCount: number;
  triggerCount: number;
  failedTenants: number;
}

function buildHeap(
  heap: TriggerHeap,
  database: Database.Database | null,
  tenantDbManager: TenantDbManager | undefined,
  logger: Logger,
): BootstrapSummary {
  const startMs = Date.now();
  heap.clear();

  let tenantCount = 0;
  let triggerCount = 0;
  let failedTenants = 0;

  if (tenantDbManager) {
    const tenantIds = tenantDbManager.listTenantIds();
    for (const tenantId of tenantIds) {
      tenantCount++;
      const heapSizeBefore = heap.size;
      try {
        const tenantDb = tenantDbManager.getTenantDb(tenantId);
        const loaded = loadDbIntoHeap(heap, tenantDb, tenantId);
        triggerCount += loaded;
        logger.debug({ tenantId, triggerCount: loaded }, 'Loaded tenant triggers into heap');
      } catch (err) {
        const loadedBeforeFailure = heap.size - heapSizeBefore;
        failedTenants++;
        logger.error({ err, tenantId, triggersLoadedBeforeFailure: loadedBeforeFailure }, 'Failed to load triggers for tenant into heap');
      }
    }
  } else if (database) {
    tenantCount = 1;
    const loaded = loadDbIntoHeap(heap, database, 'default');
    triggerCount += loaded;
  }

  const totalDurationMs = Date.now() - startMs;
  const summary: BootstrapSummary = { totalDurationMs, tenantCount, triggerCount, failedTenants };
  logger.info(summary, 'Heap bootstrap complete');
  return summary;
}

function loadDbIntoHeap(heap: TriggerHeap, dbConn: Database.Database, tenantId: string): number {
  const rows = db.loadEnabledTriggerTimes(dbConn);
  for (const row of rows) {
    heap.push({
      triggerId: row.id,
      tenantId,
      nextTriggerAt: new Date(row.nextTriggerAt).getTime(),
    });
  }
  return rows.length;
}

// ---------------------------------------------------------------------------
// Compute next delay from the heap (O(1) peek)
// ---------------------------------------------------------------------------

function computeNextDelay(heap: TriggerHeap): number {
  const top = heap.peek();
  if (!top) return MAX_DELAY_MS;
  const deltaMs = top.nextTriggerAt - Date.now();
  return Math.min(MAX_DELAY_MS, Math.max(MIN_DELAY_MS, deltaMs));
}

// ---------------------------------------------------------------------------
// Fire due triggers — batch-fetch from DB by ID, then process
// ---------------------------------------------------------------------------

function fireDueEntries(
  due: HeapEntry[],
  tickNow: number,
  database: Database.Database | null,
  tenantDbManager: TenantDbManager | undefined,
  heap: TriggerHeap,
  logger: Logger,
  onEventPublished?: () => void,
): { fired: number; missed: number; errors: Record<ErrorClass, number> } {
  let firedCount = 0;
  let missedCount = 0;
  const errors = zeroErrorCounts();

  // Build latency lookup from heap entries for missed-trigger detection
  const scheduledAtByTriggerId = new Map<string, number>();
  for (const entry of due) {
    scheduledAtByTriggerId.set(entry.triggerId, entry.nextTriggerAt);
  }

  // Group by tenantId so we hit each DB once with a batch query
  const byTenant = new Map<string, HeapEntry[]>();
  for (const entry of due) {
    let list = byTenant.get(entry.tenantId);
    if (!list) {
      list = [];
      byTenant.set(entry.tenantId, list);
    }
    list.push(entry);
  }

  for (const [tenantId, entries] of byTenant) {
    try {
      const tenantDb = tenantDbManager
        ? tenantDbManager.getTenantDb(tenantId)
        : database;
      if (!tenantDb) continue;

      const ids = entries.map(e => e.triggerId);
      const triggers = db.getTriggersByIdsWithPersona(tenantDb, ids);

      for (const trigger of triggers) {
        // Skip polling triggers
        if (trigger.triggerType === 'polling') continue;

        // Detect missed trigger (due more than 60 s ago) and log with full context
        const scheduledAt = scheduledAtByTriggerId.get(trigger.id);
        if (scheduledAt !== undefined) {
          const latencyMs = tickNow - scheduledAt;
          if (latencyMs > MAX_DELAY_MS) {
            missedCount++;
            logger.warn({
              triggerId: trigger.id,
              tenantId,
              personaId: trigger.personaId,
              scheduledAt: new Date(scheduledAt).toISOString(),
              latencyMs,
            }, 'Missed trigger: fired late');
          }
        }

        // Phase tracks which operation we're in for error classification
        let phase: ErrorClass = 'config_error';
        try {
          const parsedConfig = trigger.parsedConfig;
          let eventType = 'trigger_fired';
          let payload: string | null = null;
          if (parsedConfig) {
            if (parsedConfig.event_type) eventType = parsedConfig.event_type;
            if (parsedConfig.payload) payload = JSON.stringify(parsedConfig.payload);
          }

          // Skip triggers for deleted or disabled personas
          if (trigger.personaProjectId == null) {
            errors.orphan_trigger++;
            logger.warn({ triggerId: trigger.id, personaId: trigger.personaId, error_class: 'orphan_trigger' as ErrorClass }, 'Skipping trigger: persona not found (deleted?)');
            continue;
          }
          if (!trigger.personaEnabled) {
            errors.orphan_trigger++;
            logger.debug({ triggerId: trigger.id, personaId: trigger.personaId, error_class: 'orphan_trigger' as ErrorClass }, 'Skipping trigger: persona is disabled');
            continue;
          }

          const triggerProjectId = tenantId !== 'default' ? tenantId : trigger.personaProjectId;

          // DB operations phase
          phase = 'db_error';

          // Publish event
          const publishedEvent = db.publishEvent(tenantDb, {
            eventType,
            sourceType: 'trigger',
            sourceId: trigger.id,
            targetPersonaId: trigger.personaId,
            projectId: triggerProjectId,
            useCaseId: trigger.useCaseId,
            payload,
          });
          onEventPublished?.();

          // Record fire history
          db.recordTriggerFire(tenantDb, {
            triggerId: trigger.id,
            eventId: publishedEvent.id,
          });

          // Compute next trigger time (config operation) and update DB
          phase = 'config_error';
          const now = new Date().toISOString();
          const nextTriggerAt = db.computeNextTriggerAtFromParsed(trigger.triggerType, parsedConfig);

          phase = 'db_error';
          db.updateTriggerTimings(tenantDb, trigger.id, now, nextTriggerAt);

          // Push updated time back into heap
          if (nextTriggerAt) {
            heap.push({ triggerId: trigger.id, tenantId, nextTriggerAt: new Date(nextTriggerAt).getTime() });
          }

          logger.info({
            triggerId: trigger.id,
            personaId: trigger.personaId,
            eventType,
            nextTriggerAt,
          }, 'Trigger fired');
          firedCount++;
        } catch (err) {
          errors[phase]++;
          logger.error({ err, triggerId: trigger.id, error_class: phase }, 'Failed to fire trigger');
        }
      }
    } catch (err) {
      errors.db_error++;
      logger.error({ err, tenantId, error_class: 'db_error' as ErrorClass }, 'Trigger scheduler tick failed for tenant');
    }
  }
  return { fired: firedCount, missed: missedCount, errors };
=======
/** Pre-resolved trigger data computed during the read phase. */
interface ResolvedTrigger {
  trigger: ReturnType<typeof db.getDueTriggers>[number];
  config: TriggerConfig | null;
  eventType: string;
  payload: string | null;
  projectId: string | undefined;
  nextTriggerAt: string | null;
  nowIso: string;
  healthUpdate: { status: 'healthy' | 'degraded'; message: string | null } | null;
}

function triggerSchedulerTick(
  database: Database.Database,
  logger: Logger,
  onTrigger?: () => void,
): void {
  // --- Read phase (shared read lock) ---
  // Query due triggers and pre-resolve all data needed for the write phase.
  // This keeps the exclusive write lock window as short as possible.
  const triggers = db.getDueTriggers(database);
  if (triggers.length === 0) return;

  const resolved: ResolvedTrigger[] = [];
  for (const trigger of triggers) {
    try {
      const config = getCachedTriggerConfig(trigger.id, trigger.config);

      let eventType = 'trigger_fired';
      if (config?.event_type) {
        const validated = validateTriggerEventType(config.event_type);
        if (validated) {
          eventType = validated;
        } else {
          logger.warn(
            { triggerId: trigger.id, rawEventType: config.event_type },
            'Trigger config contains invalid or reserved event_type — falling back to "trigger_fired"',
          );
        }
      }
      const payload = config?.payload ? JSON.stringify(config.payload) : null;

      // Look up persona to get its projectId for event isolation (read-only)
      const persona = db.getPersona(database, trigger.personaId);
      const projectId = persona?.projectId;

      // Compute next trigger time
      const now = new Date();
      const nowIso = now.toISOString();
      let nextTriggerAt: string | null = null;
      let healthUpdate: ResolvedTrigger['healthUpdate'] = null;

      if (trigger.triggerType === 'schedule' && config) {
        if (config.cron) {
          nextTriggerAt = computeNextCron(config.cron, now);
          if (!nextTriggerAt) {
            logger.warn({ triggerId: trigger.id, cron: config.cron }, 'Could not parse cron expression — trigger will not reschedule');
            healthUpdate = { status: 'degraded', message: `Invalid cron expression "${config.cron}" — trigger will not reschedule` };
          } else if (trigger.healthStatus === 'degraded') {
            healthUpdate = { status: 'healthy', message: null };
          }
        } else if (config.interval_seconds) {
          const intervalMs = Math.max(config.interval_seconds * 1000, MIN_INTERVAL_MS);
          const next = new Date(now.getTime() + intervalMs);
          nextTriggerAt = next.toISOString();
        }
      }

      resolved.push({ trigger, config, eventType, payload, projectId, nextTriggerAt, nowIso, healthUpdate });
    } catch (err) {
      logger.error({ err, triggerId: trigger.id }, 'Failed to resolve trigger during read phase');
    }
  }

  if (resolved.length === 0) return;

  // --- Write phase (BEGIN IMMEDIATE — exclusive write lock) ---
  // Only INSERTs and UPDATEs happen here; all computation was done above.
  const txn = database.transaction(() => {
    for (const { trigger, eventType, payload, projectId, nextTriggerAt, nowIso, healthUpdate } of resolved) {
      try {
        const event = db.publishEvent(database, {
          eventType,
          sourceType: 'trigger',
          sourceId: trigger.id,
          targetPersonaId: trigger.personaId,
          projectId,
          useCaseId: trigger.useCaseId,
          payload,
        });

        db.recordTriggerFiring(database, {
          triggerId: trigger.id,
          personaId: trigger.personaId,
          projectId: projectId ?? 'default',
          eventId: event.id,
        });

        if (healthUpdate) {
          db.updateTriggerHealth(database, trigger.id, healthUpdate.status, healthUpdate.message);
        }

        db.updateTriggerTimings(database, trigger.id, nowIso, nextTriggerAt);

        logger.info({
          triggerId: trigger.id,
          personaId: trigger.personaId,
          eventType,
          nextTriggerAt,
        }, 'Trigger fired');
      } catch (err) {
        logger.error({ err, triggerId: trigger.id }, 'Failed to fire trigger');
      }
    }
  });

  try {
    txn.immediate();
    if (onTrigger) onTrigger();
  } catch (err) {
    logger.error({ err }, 'Trigger scheduler tick transaction failed');
  }
}
