import type Database from 'better-sqlite3';
import type { Logger } from 'pino';
import { matchEvent, assemblePrompt } from '@dac-cloud/shared';
import type { ExecRequest, EventMatch, PersonaEvent } from '@dac-cloud/shared';
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

/**
 * Start the event processing loop.
 * Supports both tenant-isolated DBs (via TenantDbManager) and legacy single DB.
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
      logger.error({ err }, 'Event processor tick failed');
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
