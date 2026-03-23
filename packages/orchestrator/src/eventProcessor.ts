import type Database from 'better-sqlite3';
import type { Logger } from 'pino';
import { matchEvent, assemblePrompt } from '@dac-cloud/shared';
import type { ExecRequest, EventMatch, PersonaEvent, PersonaEventSubscription } from '@dac-cloud/shared';
import { nanoid } from 'nanoid';
import * as db from './db/index.js';
import type { Dispatcher } from './dispatcher.js';
import type { TenantDbManager } from './tenantDbManager.js';

/** Default threshold (seconds) for the periodic stale-event reaper. */
const STALE_EVENT_REAPER_SECONDS = 120;
/** Run the reaper every N ticks (at 2s interval, 30 ticks = ~60s). */
const REAPER_TICK_INTERVAL = 30;

export interface EventProcessorHealth {
  lastTickAt: number | null;
  pendingEvents: number;
  backlogAgeMs: number | null;
}

export interface EventProcessorCounters {
  eventsFetched: number;
  delivered: number;
  skipped: number;
  failed: number;
  ticks: number;
}

export interface EventProcessorHandle {
  timer: NodeJS.Timeout;
  getHealth: () => EventProcessorHealth;
  /** Signal that new events are available — triggers an immediate tick (coalesced). */
  nudge: () => void;
  /** Return cumulative counters since last call and reset them. */
  snapshotAndResetCounters: () => EventProcessorCounters;
}

/**
 * Recover events orphaned in 'processing' state from a prior crash.
 * Call once at startup before the tick loop begins.
 */
function recoverOrphanedEvents(
  database: Database.Database | null,
  logger: Logger,
  tenantDbManager?: TenantDbManager,
): void {
  if (tenantDbManager) {
    tenantDbManager.forEachTenant((tenantDb, projectId) => {
      const count = db.resetAllProcessingEvents(tenantDb);
      if (count > 0) {
        logger.warn({ projectId, count }, 'Reset orphaned processing events to pending at startup');
      }
    });
  } else if (database) {
    const count = db.resetAllProcessingEvents(database);
    if (count > 0) {
      logger.warn({ count }, 'Reset orphaned processing events to pending at startup');
    }
  }
}

/**
 * Periodic reaper: reset events stuck in 'processing' longer than the threshold.
 * Guards against edge cases during normal operation.
 */
function reapStaleEvents(
  database: Database.Database | null,
  logger: Logger,
  tenantDbManager?: TenantDbManager,
): void {
  if (tenantDbManager) {
    tenantDbManager.forEachTenant((tenantDb, projectId) => {
      const count = db.resetStaleProcessingEvents(tenantDb, STALE_EVENT_REAPER_SECONDS);
      if (count > 0) {
        logger.warn({ projectId, count, thresholdSec: STALE_EVENT_REAPER_SECONDS }, 'Reaped stale processing events');
      }
    });
  } else if (database) {
    const count = db.resetStaleProcessingEvents(database, STALE_EVENT_REAPER_SECONDS);
    if (count > 0) {
      logger.warn({ count, thresholdSec: STALE_EVENT_REAPER_SECONDS }, 'Reaped stale processing events');
    }
  }
}

/** Fallback poll interval (safety net only — primary dispatch is change-driven). */
const FALLBACK_POLL_MS = 30_000;
/** Periodic recovery interval for stale 'processing' events (60 seconds). */
const STALE_RECOVERY_MS = 60_000;
/**
 * Maximum consecutive microtask re-entries before yielding to the event loop
 * via setTimeout(0).  Prevents microtask starvation under sustained event
 * bursts — ensures setInterval callbacks (fallback poll, stale recovery,
 * trigger scheduler) and I/O handlers get a chance to run.
 */
const MAX_MICROTASK_REENTRIES = 10;

export interface EventProcessor {
  /** Signal that new events have been inserted and should be processed immediately. */
  notify(): void;
  /** Stop both the fallback poll and any pending notification tick. */
  stop(): void;
}

/**
 * Start the event processing loop.
 * Supports both tenant-isolated DBs (via TenantDbManager) and legacy single DB.
 *
 * Primary dispatch is change-driven: callers invoke `nudge()` after inserting
 * events, which triggers an immediate processing tick.  A fallback poll acts
 * as a safety net in case a notification is missed.
 *
 * On startup, recovers any events left in 'processing' state from a prior
 * crash by resetting them back to 'pending'.
 */
export function startEventProcessor(
  database: Database.Database | null,
  dispatcher: Dispatcher,
  logger: Logger,
  interval: number = 2000,
  tenantDbManager?: TenantDbManager,
): EventProcessorHandle {
  // At startup, reset any events orphaned in 'processing' from a prior crash
  recoverOrphanedEvents(database, logger, tenantDbManager);

  logger.info({ intervalMs: interval, tenantIsolated: !!tenantDbManager }, 'Event processor started (demand-driven with backstop)');

  let tickCount = 0;

  // Shared health state updated on every tick
  const health: EventProcessorHealth = {
    lastTickAt: null,
    pendingEvents: 0,
    backlogAgeMs: null,
  };

  // Interval counters — reset each time snapshotAndResetCounters() is called
  const counters = { eventsFetched: 0, delivered: 0, skipped: 0, failed: 0, ticks: 0 };

  /** Core tick body — runs both on-demand (nudge) and via backstop interval. */
  function runTick(): void {
    try {
      tickCount++;
      health.lastTickAt = Date.now();

      // Periodically reap stale processing events as a safety net
      if (tickCount % REAPER_TICK_INTERVAL === 0) {
        reapStaleEvents(database, logger, tenantDbManager);
      }

      // Collect pending event stats for health reporting
      let totalPending = 0;
      let oldestCreatedAt: string | null = null;

      counters.ticks++;

      if (tenantDbManager) {
        tenantDbManager.forEachTenant((tenantDb, projectId) => {
          totalPending += db.countPendingEvents(tenantDb);
          const oldest = db.getOldestPendingEventCreatedAt(tenantDb);
          if (oldest && (!oldestCreatedAt || oldest < oldestCreatedAt)) {
            oldestCreatedAt = oldest;
          }
          eventBusTick(tenantDb, dispatcher, logger, counters, projectId);
        });
      } else if (database) {
        totalPending = db.countPendingEvents(database);
        oldestCreatedAt = db.getOldestPendingEventCreatedAt(database);
        eventBusTick(database, dispatcher, logger, counters);
      }

      health.pendingEvents = totalPending;
      health.backlogAgeMs = oldestCreatedAt
        ? Math.max(0, Date.now() - new Date(oldestCreatedAt + 'Z').getTime())
        : null;
    } catch (err) {
      logger.error({ err }, 'Event processor fallback tick failed');
    }
  }

  // Backstop interval — catches any missed nudges or deferred/retried events
  const timer = setInterval(runTick, interval);

  // Demand-driven nudge: coalesce rapid-fire signals into a single immediate tick
  let nudgeScheduled = false;
  function nudge(): void {
    if (nudgeScheduled) return;
    nudgeScheduled = true;
    setImmediate(() => {
      nudgeScheduled = false;
      runTick();
    });
  }

  return {
    timer,
    getHealth: () => ({ ...health }),
    nudge,
    snapshotAndResetCounters: () => {
      const snap = { ...counters };
      counters.eventsFetched = 0;
      counters.delivered = 0;
      counters.skipped = 0;
      counters.failed = 0;
      counters.ticks = 0;
      return snap;
    },
  };
}

/** Default retry policy used when no subscription-level overrides exist. */
const DEFAULT_MAX_RETRIES = 3;
const DEFAULT_RETRY_BACKOFF_MS = 5000;
/** Short retry delay for concurrency-blocked events (transient condition). */
const CONCURRENCY_RETRY_DELAY_MS = 2000;
/** Max retries specifically for concurrency-blocked events before falling back to normal retry. */
const CONCURRENCY_MAX_RETRIES = 15;

const CLAIM_BATCH_SIZE = 50;

/** Pre-fetched retry policies keyed by "eventType:projectId". */
type RetryPolicyMap = Map<string, { maxRetries: number; retryBackoffMs: number }>;

/** Per-tick subscription cache keyed by "eventType:projectId". */
type SubscriptionCache = Map<string, PersonaEventSubscription[]>;

function tickCacheKey(eventType: string, projectId: string): string {
  return `${eventType}:${projectId}`;
}

// ---------------------------------------------------------------------------
// AuditRecorder — accumulates audit entries during event processing and
// flushes them as a batch, separating audit concerns from dispatch logic.
// ---------------------------------------------------------------------------

type AuditEntry = {
  eventId: string;
  action: string;
  subscriptionId?: string | null;
  personaId?: string | null;
  executionId?: string | null;
  detail?: string | null;
};

class AuditRecorder {
  private entries: AuditEntry[] = [];

  constructor(private readonly eventId: string) {}

  recordProcessingStarted(eventType: string, sourceType: string, sourceId: string | null): void {
    this.entries.push({
      eventId: this.eventId,
      action: 'processing_started',
      detail: JSON.stringify({ eventType, sourceType, sourceId }),
    });
  }

  recordSubscriptionsEvaluated(
    subsChecked: number,
    matchedCount: number,
    matchedSubscriptions: string[],
    matchedPersonas: string[],
  ): void {
    this.entries.push({
      eventId: this.eventId,
      action: 'subscriptions_evaluated',
      detail: JSON.stringify({ subscriptionsChecked: subsChecked, matchedCount, matchedSubscriptions, matchedPersonas }),
    });
  }

  recordTriggerDirectDispatchBlocked(personaId: string, eventProjectId: string, personaProjectId: string): void {
    this.entries.push({
      eventId: this.eventId,
      action: 'trigger_direct_dispatch_blocked',
      personaId,
      detail: `Cross-project dispatch blocked: event project ${eventProjectId} !== persona project ${personaProjectId}`,
    });
  }

  recordTriggerDirectDispatch(personaId: string): void {
    this.entries.push({
      eventId: this.eventId,
      action: 'trigger_direct_dispatch',
      personaId,
      detail: 'No subscriptions matched — dispatching directly to trigger target persona',
    });
  }

  recordSkipped(processingDurationMs: number): void {
    this.entries.push({
      eventId: this.eventId,
      action: 'status_changed',
      detail: JSON.stringify({ status: 'skipped', reason: 'No subscriptions matched', processingDurationMs }),
    });
  }

  recordDispatchSkippedDuplicate(subscriptionId: string, personaId: string, reason: string): void {
    this.entries.push({
      eventId: this.eventId,
      action: 'dispatch_skipped_duplicate',
      subscriptionId,
      personaId,
      detail: reason,
    });
  }

  recordDispatchFailed(subscriptionId: string, personaId: string, reason: string): void {
    this.entries.push({
      eventId: this.eventId,
      action: 'dispatch_failed',
      subscriptionId,
      personaId,
      detail: reason,
    });
  }

  recordConcurrencyBlocked(subscriptionId: string, personaId: string, running: number, maxConcurrent: number): void {
    this.entries.push({
      eventId: this.eventId,
      action: 'dispatch_concurrency_blocked',
      subscriptionId,
      personaId,
      detail: JSON.stringify({ reason: 'Concurrency limit reached — will retry', running, maxConcurrent }),
    });
  }

  recordDispatchRejected(subscriptionId: string, personaId: string, executionId: string): void {
    this.entries.push({
      eventId: this.eventId,
      action: 'dispatch_rejected',
      subscriptionId,
      personaId,
      executionId,
      detail: 'Execution queue full',
    });
  }

  recordDispatchSucceeded(subscriptionId: string, personaId: string, executionId: string): void {
    this.entries.push({
      eventId: this.eventId,
      action: 'dispatch_succeeded',
      subscriptionId,
      personaId,
      executionId,
    });
  }

  recordDispatchError(subscriptionId: string, personaId: string, err: unknown): void {
    this.entries.push({
      eventId: this.eventId,
      action: 'dispatch_error',
      subscriptionId,
      personaId,
      detail: err instanceof Error ? err.message : String(err),
    });
  }

  recordProcessingError(err: unknown): void {
    this.entries.push({
      eventId: this.eventId,
      action: 'processing_error',
      detail: err instanceof Error ? err.message : String(err),
    });
  }

  recordOutcome(outcome: EventOutcome, delivered: number, failed: number, concurrencyBlocked: number, processingDurationMs: number): void {
    // Outcome-specific audit entries
    switch (outcome.type) {
      case 'concurrency_retry':
        this.entries.push({
          eventId: this.eventId,
          action: 'concurrency_retry_scheduled',
          detail: JSON.stringify({
            delivered: outcome.delivered,
            concurrencyBlocked: outcome.concurrencyBlocked,
            retryCount: outcome.retryCount + 1,
            ...(outcome.maxConcurrencyRetries != null ? { maxConcurrencyRetries: outcome.maxConcurrencyRetries } : {}),
            delayMs: CONCURRENCY_RETRY_DELAY_MS,
          }),
        });
        break;
      case 'hard_retry':
        this.entries.push({
          eventId: this.eventId,
          action: 'retry_scheduled',
          detail: JSON.stringify({
            retryCount: outcome.retryCount + 1,
            maxRetries: outcome.maxRetries,
            ...(outcome.reason ? { reason: outcome.reason } : {}),
            backoffMs: outcome.backoffMs * Math.pow(2, outcome.retryCount),
            nextRetryAt: new Date(Date.now() + outcome.backoffMs * Math.pow(2, outcome.retryCount)).toISOString(),
          }),
        });
        break;
      case 'dead_letter':
        this.entries.push({
          eventId: this.eventId,
          action: 'moved_to_dead_letter',
          detail: JSON.stringify({
            retryCount: outcome.retryCount,
            maxRetries: outcome.maxRetries,
            reason: outcome.errorMessage,
          }),
        });
        break;
    }

    // Final status audit entry (always)
    this.entries.push({
      eventId: this.eventId,
      action: 'status_changed',
      detail: JSON.stringify({
        status: outcome.status,
        delivered,
        failed,
        concurrencyBlocked,
        errorMessage: outcome.errorMessage ?? null,
        processingDurationMs,
      }),
    });
  }

  /** Flush all accumulated entries as a batch insert. */
  flush(database: Database.Database): void {
    if (this.entries.length > 0) {
      db.batchAppendEventAudit(database, this.entries);
    }
  }
}

// ---------------------------------------------------------------------------
// EventOutcome — discriminated union replacing the if/else cascade.
// Pure function resolveOutcome() determines the outcome; a single switch
// in applyOutcome() handles DB writes and metrics.
// ---------------------------------------------------------------------------

type EventOutcome =
  | { type: 'delivered'; status: 'delivered'; errorMessage?: undefined }
  | { type: 'partial'; status: 'partial'; errorMessage?: undefined }
  | { type: 'concurrency_retry'; status: 'retry'; delivered: number; concurrencyBlocked: number; retryCount: number; maxConcurrencyRetries?: number; errorMessage: string }
  | { type: 'hard_retry'; status: 'retry'; retryCount: number; maxRetries: number; backoffMs: number; reason?: string; errorMessage: string }
  | { type: 'dead_letter'; status: 'dead_letter'; retryCount: number; maxRetries: number; errorMessage: string };

function resolveOutcome(
  delivered: number,
  failed: number,
  concurrencyBlocked: number,
  retryCount: number,
  retryPolicy: { maxRetries: number; retryBackoffMs: number },
): EventOutcome {
  const totalUndelivered = failed + concurrencyBlocked;

  if (totalUndelivered === 0) {
    return { type: 'delivered', status: 'delivered' };
  }

  if (delivered > 0 && concurrencyBlocked > 0 && failed === 0) {
    return {
      type: 'concurrency_retry', status: 'retry',
      delivered, concurrencyBlocked, retryCount,
      errorMessage: `${concurrencyBlocked} match(es) blocked by concurrency limit — re-queuing`,
    };
  }

  if (delivered > 0) {
    return { type: 'partial', status: 'partial' };
  }

  if (concurrencyBlocked > 0 && failed === 0) {
    if (retryCount < CONCURRENCY_MAX_RETRIES) {
      return {
        type: 'concurrency_retry', status: 'retry',
        delivered, concurrencyBlocked, retryCount,
        maxConcurrencyRetries: CONCURRENCY_MAX_RETRIES,
        errorMessage: `All ${concurrencyBlocked} match(es) blocked by concurrency limit — re-queuing`,
      };
    }
    // Exhausted concurrency retries — fall through to normal retry/dead_letter
    const maxRetries = CONCURRENCY_MAX_RETRIES + (retryPolicy.maxRetries ?? DEFAULT_MAX_RETRIES);
    const backoffMs = retryPolicy.retryBackoffMs ?? DEFAULT_RETRY_BACKOFF_MS;
    const baseError = `Concurrency limit blocked all matches after ${CONCURRENCY_MAX_RETRIES} attempts`;

    if (retryCount < maxRetries) {
      return {
        type: 'hard_retry', status: 'retry',
        retryCount, maxRetries, backoffMs,
        reason: 'Concurrency retries exhausted, falling back to normal retry policy',
        errorMessage: baseError,
      };
    }
    return {
      type: 'dead_letter', status: 'dead_letter',
      retryCount, maxRetries,
      errorMessage: `Max retries (${maxRetries}) exceeded after concurrency blocking — moved to dead letter queue`,
    };
  }

  // All dispatches hard-failed — normal retry/dead_letter
  const maxRetries = retryPolicy.maxRetries ?? DEFAULT_MAX_RETRIES;
  const backoffMs = retryPolicy.retryBackoffMs ?? DEFAULT_RETRY_BACKOFF_MS;

  if (retryCount < maxRetries) {
    return {
      type: 'hard_retry', status: 'retry',
      retryCount, maxRetries, backoffMs,
      errorMessage: 'All subscription matches failed',
    };
  }
  return {
    type: 'dead_letter', status: 'dead_letter',
    retryCount, maxRetries,
    errorMessage: `Max retries (${maxRetries}) exceeded — moved to dead letter queue`,
  };
}

function applyOutcome(
  database: Database.Database,
  dispatcher: Dispatcher,
  eventId: string,
  retryCount: number,
  outcome: EventOutcome,
): void {
  switch (outcome.type) {
    case 'delivered':
      db.updateEventStatus(database, eventId, 'delivered');
      break;
    case 'partial':
      db.updateEventStatus(database, eventId, 'partial');
      break;
    case 'concurrency_retry':
      db.scheduleEventRetry(database, eventId, retryCount, CONCURRENCY_RETRY_DELAY_MS, outcome.errorMessage);
      break;
    case 'hard_retry':
      db.scheduleEventRetry(database, eventId, retryCount, outcome.backoffMs, outcome.errorMessage);
      break;
    case 'dead_letter':
      db.moveEventToDeadLetter(database, eventId, outcome.errorMessage);
      break;
  }

  if (outcome.status === 'retry') dispatcher.metrics.recordEventRetry();
  if (outcome.status === 'dead_letter') dispatcher.metrics.recordDeadLetter();
}

// ---------------------------------------------------------------------------
// DispatchResult — discriminated union for the outcome of dispatching a single
// subscription match.  Returned by dispatchMatch() so the loop body becomes a
// one-liner dispatch + tally.
// ---------------------------------------------------------------------------

type DispatchResult =
  | { type: 'dispatched'; executionId: string }
  | { type: 'duplicate' }
  | { type: 'concurrency_blocked'; running: number; maxConcurrent: number }
  | { type: 'persona_not_found' }
  | { type: 'queue_full'; executionId: string }
  | { type: 'error'; err: unknown };

/**
 * Attempt to dispatch a single subscription match for a given event.
 * Pure in the sense that all side-effects (DB writes, queue submission) are
 * passed as explicit dependencies — making this independently testable.
 */
function dispatchMatch(
  database: Database.Database,
  dispatcher: Dispatcher,
  logger: Logger,
  event: ReturnType<typeof db.getEvent> & {},
  match: ReturnType<typeof matchEvent>[number],
): DispatchResult {
  try {
    // Idempotency check
    if (db.hasEventBeenDispatched(database, event.id, match.personaId)) {
      logger.debug({ eventId: event.id, personaId: match.personaId }, 'Skipping duplicate dispatch — already processed');
      return { type: 'duplicate' };
    }

    const persona = db.getPersona(database, match.personaId);
    if (!persona) {
      logger.warn({ personaId: match.personaId, eventId: event.id }, 'Persona not found for event match');
      return { type: 'persona_not_found' };
    }

    // Check concurrency
    const running = db.countRunningExecutions(database, persona.id);
    if (running >= persona.maxConcurrent) {
      logger.debug({ personaId: persona.id, running, max: persona.maxConcurrent }, 'Persona at concurrency limit, will re-queue');
      return { type: 'concurrency_blocked', running, maxConcurrent: persona.maxConcurrent };
    }

    // Build input data from event payload
    let inputData: Record<string, unknown> | undefined;
    if (match.payload) {
      try {
        inputData = JSON.parse(match.payload);
      } catch {
        inputData = { raw: match.payload };
      }
    }

    // Assemble prompt
    const tools = db.getToolsForPersona(database, persona.id);
    const prompt = assemblePrompt(persona, tools, inputData);

    // Submit execution
    const request: ExecRequest = {
      executionId: nanoid(),
      personaId: persona.id,
      prompt,
      inputData,
      config: { timeoutMs: persona.timeoutMs },
      triggerId: undefined,
      triggerType: undefined,
    };

    // Record dispatch before submitting — this is the idempotency key.
    const isNew = db.recordEventDispatch(database, event.id, persona.id, request.executionId);
    if (!isNew) {
      logger.debug({ eventId: event.id, personaId: persona.id }, 'Skipping duplicate dispatch — race condition caught');
      return { type: 'duplicate' };
    }

    if (!dispatcher.submit(request)) {
      logger.warn({ eventId: event.id, personaId: persona.id }, 'Execution queue full — skipping event dispatch');
      return { type: 'queue_full', executionId: request.executionId };
    }

    // Link trigger firing to the dispatched execution
    if (event.sourceType === 'trigger') {
      const firing = db.getTriggerFiringByEventId(database, event.id);
      if (firing) {
        db.updateTriggerFiringDispatched(database, firing.id, request.executionId);
      }
    }

    logger.info({
      eventId: event.id,
      personaId: persona.id,
      executionId: request.executionId,
      eventType: event.eventType,
      useCaseId: match.useCaseId,
    }, 'Event dispatched to persona');

    return { type: 'dispatched', executionId: request.executionId };
  } catch (err) {
    logger.error({ err, personaId: match.personaId, eventId: event.id }, 'Failed to dispatch event match');
    return { type: 'error', err };
  }
}

// ---------------------------------------------------------------------------

function eventBusTick(
  database: Database.Database,
  dispatcher: Dispatcher,
  logger: Logger,
  intervalCounters: { eventsFetched: number; delivered: number; skipped: number; failed: number },
  projectId?: string,
): void {
  const tickStartMs = performance.now();
  let eventsFetched = 0;
  let matchesFound = 0;
  let delivered = 0;
  let skipped = 0;
  let failed = 0;

  try {
    const events = db.getPendingEvents(database, 50);
    eventsFetched = events.length;
    if (eventsFetched === 0) return;

    // Collect dispatch requests to fire *after* the transaction commits so that
    // the dispatcher never sees stale DB state.
    const pendingDispatches: ExecRequest[] = [];

    // Wrap all DB reads/writes in a single transaction to avoid per-statement
    // fsync overhead (up to 100 individual commits per tick otherwise).
    database.transaction(() => {
      // Cache subscription lookups per unique (eventType, projectId) to avoid
      // redundant DB queries when the same event type fires multiple times in a batch.
      const subsCache = new Map<string, ReturnType<typeof db.getSubscriptionsByEventType>>();

      // Phase 1: Mark events as processing, compute matches, collect unique persona IDs.
      const eventMatches: Array<{ event: PersonaEvent; matches: EventMatch[] }> = [];
      const allPersonaIds = new Set<string>();

      for (const event of events) {
        db.updateEventStatus(database, event.id, 'processing');

        const cacheKey = `${event.eventType}\0${event.projectId}`;
        let subs = subsCache.get(cacheKey);
        if (subs === undefined) {
          subs = db.getSubscriptionsByEventType(database, event.eventType, event.projectId);
          subsCache.set(cacheKey, subs);
        }
        const matches = matchEvent(event, subs);

        if (matches.length === 0) {
          db.updateEventStatus(database, event.id, 'skipped');
          skipped++;
          continue;
        }

        matchesFound += matches.length;
        eventMatches.push({ event, matches });
        for (const match of matches) {
          allPersonaIds.add(match.personaId);
        }
      }

      if (allPersonaIds.size === 0) return;

      // Phase 2: Batch-fetch personas, tools, and running counts (2-3 queries total).
      const personaIds = [...allPersonaIds];
      const personaMap = db.getPersonasByIds(database, personaIds);
      const toolsMap = db.getToolsForPersonaIds(database, personaIds);
      const runningMap = db.countRunningExecutionsByPersonaIds(database, personaIds);

      // Phase 3: Dispatch using Map lookups instead of per-match queries.
      // Track persona slots claimed within this tick to prevent over-dispatch.
      const claimed = new Map<string, number>();

      for (const { event, matches } of eventMatches) {
        let eventDelivered = 0;
        let eventFailed = 0;
        let throttled = 0;

        for (const match of matches) {
          try {
            const persona = personaMap.get(match.personaId);
            if (!persona) {
              logger.warn({ personaId: match.personaId, eventId: event.id }, 'Persona not found for event match');
              eventFailed++;
              failed++;
              continue;
            }

            // Check concurrency (in-tick claims + batched DB count)
            const tickClaimed = claimed.get(persona.id) ?? 0;
            const running = runningMap.get(persona.id) ?? 0;
            if (running + tickClaimed >= persona.maxConcurrent) {
              logger.info({ personaId: persona.id, running, tickClaimed, max: persona.maxConcurrent }, 'Persona at concurrency limit, deferring');
              throttled++;
              continue;
            }

            // Build input data from event payload
            let inputData: Record<string, unknown> | undefined;
            if (match.payload) {
              try {
                inputData = JSON.parse(match.payload);
              } catch (parseError) {
                logger.warn({ eventId: event.id, personaId: match.personaId, parseError }, 'Event payload JSON parse failed, using raw');
                inputData = { raw: match.payload };
              }
            }

            // Assemble prompt using batched persona + tools
            const tools = toolsMap.get(persona.id) ?? [];
            const prompt = assemblePrompt(persona, tools, inputData);

            // Queue dispatch request (fired after transaction commits)
            const request: ExecRequest = {
              executionId: nanoid(),
              personaId: persona.id,
              prompt,
              projectId: projectId ?? persona.projectId,
              config: { timeoutMs: persona.timeoutMs },
              eventId: event.id,
            };

            pendingDispatches.push(request);
            claimed.set(persona.id, (claimed.get(persona.id) ?? 0) + 1);
            eventDelivered++;
            delivered++;

            logger.info({
              eventId: event.id,
              personaId: persona.id,
              executionId: request.executionId,
              eventType: event.eventType,
              useCaseId: match.useCaseId,
            }, 'Event dispatched to persona');
          } catch (err) {
            logger.error({ err, personaId: match.personaId, eventId: event.id }, 'Failed to dispatch event match');
            eventFailed++;
            failed++;
          }
        }

        // Update event status
        if (eventDelivered > 0 && eventFailed === 0 && throttled === 0) {
          db.updateEventStatus(database, event.id, 'delivered');
        } else if (eventDelivered > 0 && throttled > 0) {
          // Some matches delivered but others were concurrency-limited.
          // Defer with backoff so throttled matches get retried later.
          db.deferEventForRetry(database, event.id, event.retryCount);
          logger.info({ eventId: event.id, delivered: eventDelivered, throttled, failed: eventFailed, retryCount: event.retryCount + 1 }, 'Event partially delivered, deferring throttled matches for retry');
        } else if (eventDelivered > 0 && eventFailed > 0) {
          // Some matches delivered but others failed — mark as partial-retry
          // so the event can be re-processed for the failed matches.
          db.updateEventStatus(database, event.id, 'partial-retry');
          logger.info({ eventId: event.id, delivered: eventDelivered, failed: eventFailed }, 'Event partially delivered with failures, marked for retry');
        } else if (throttled > 0) {
          // All (or remaining) matches were concurrency-limited — defer with backoff.
          db.deferEventForRetry(database, event.id, event.retryCount);
          logger.info({ eventId: event.id, throttled, failed: eventFailed, retryCount: event.retryCount + 1 }, 'Event deferred due to concurrency limits');
        } else {
          db.updateEventStatus(database, event.id, 'failed', 'All subscription matches failed');
        }
      }
    })();

    // Fire dispatches after the transaction has committed successfully
    for (const request of pendingDispatches) {
      dispatcher.submit(request);
    }
  } finally {
    const tickDurationMs = performance.now() - tickStartMs;

    // Accumulate into interval counters
    intervalCounters.eventsFetched += eventsFetched;
    intervalCounters.delivered += delivered;
    intervalCounters.skipped += skipped;
    intervalCounters.failed += failed;

    logger.info({
      tickDurationMs,
      eventsFetched,
      matchesFound,
      delivered,
      skipped,
      failed
    }, 'Event processor tick complete');
  }
}
