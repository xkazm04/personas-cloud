import type Database from 'better-sqlite3';
import type { Logger } from 'pino';
import { matchEvent, assemblePrompt } from '@dac-cloud/shared';
import type { ExecRequest, PersonaEventSubscription } from '@dac-cloud/shared';
import { nanoid } from 'nanoid';
import * as db from './db.js';
import type { Dispatcher } from './dispatcher.js';

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
 * Ported from desktop engine/background.rs::event_bus_tick().
 *
 * Primary dispatch is change-driven: callers invoke `notify()` after inserting
 * events, which triggers an immediate processing tick.  A fallback poll every
 * 30s acts as a safety net in case a notification is missed.
 *
 * On startup, recovers any events left in 'processing' state from a prior
 * crash by resetting them back to 'pending'.
 */
export function startEventProcessor(
  database: Database.Database,
  dispatcher: Dispatcher,
  logger: Logger,
  fallbackInterval: number = FALLBACK_POLL_MS,
): EventProcessor {
  // Recover events stuck in 'processing' from a prior crash.
  const recovered = db.recoverStaleProcessingEvents(database);
  if (recovered > 0) {
    logger.warn({ recovered }, 'Recovered stale processing events back to pending');
  }

  logger.info({ fallbackIntervalMs: fallbackInterval }, 'Event processor started (change-driven with fallback poll)');

  // --- Stale recovery sweep ---
  // Periodically recover events stuck in 'processing' state (e.g. due to
  // transaction failures or prior crashes).
  const recoveryTimer = setInterval(() => {
    try {
      const recoveredCount = db.recoverStaleProcessingEvents(database);
      if (recoveredCount > 0) {
        logger.warn({ recoveredCount }, 'Recovered stale processing events during runtime sweep');
        dispatcher.metrics.recordStaleRecovery(recoveredCount);
        notify(); // Trigger immediate processing of recovered events
      }
    } catch (err) {
      logger.error({ err }, 'Event processor recovery sweep failed');
    }
  }, STALE_RECOVERY_MS);

  // --- Fallback safety-net poll ---
  const fallbackTimer = setInterval(() => {
    try {
      eventBusTick(database, dispatcher, logger);
    } catch (err) {
      logger.error({ err }, 'Event processor fallback tick failed');
    }
  }, fallbackInterval);

  // --- Change-driven notification ---
  // Coalesce rapid-fire notifications into a single tick using a microtask
  // flag.  This avoids redundant DB queries when multiple events are
  // inserted in quick succession (e.g. a batch of webhooks).
  let notifyPending = false;
  /** Consecutive microtask re-entries without yielding to the event loop. */
  let microtaskDepth = 0;

  function notify(): void {
    if (notifyPending) return;
    notifyPending = true;

    const scheduleNext = () => {
      notifyPending = false;
      try {
        const batchFull = eventBusTick(database, dispatcher, logger);
        // If we hit the batch limit, schedule another tick immediately
        // so remaining events don't wait for the 30s fallback poll.
        if (batchFull) {
          microtaskDepth++;
          if (microtaskDepth >= MAX_MICROTASK_REENTRIES) {
            // Yield to the event loop so timers (stale recovery, trigger
            // scheduler, fallback poll) and I/O handlers can run.
            microtaskDepth = 0;
            setTimeout(() => notify(), 0);
          } else {
            notify();
          }
        } else {
          microtaskDepth = 0;
        }
      } catch (err) {
        microtaskDepth = 0;
        logger.error({ err }, 'Event processor notify tick failed');
      }
    };

    queueMicrotask(scheduleNext);
  }

  // Process any events that exist at startup (recovered or pre-existing)
  notify();

  return {
    notify,
    stop() {
      clearInterval(fallbackTimer);
      clearInterval(recoveryTimer);
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

/** Returns true if the batch was full (more events may be pending). */
function eventBusTick(
  database: Database.Database,
  dispatcher: Dispatcher,
  logger: Logger,
): boolean {
  // Atomically claim both pending and retry-due events in a single transaction.
  // This reduces lock contention and ensures idempotency for retry events.
  const allEvents = db.claimEvents(database, CLAIM_BATCH_SIZE);
  if (allEvents.length === 0) return false;

  const retryCount = allEvents.filter(e => e.status === 'retry').length;
  if (retryCount > 0) {
    logger.info({ count: retryCount }, 'Claimed retry events for re-processing');
  }

  // Pre-fetch retry policies and subscriptions for all unique eventType+project
  // combinations in this batch, eliminating per-event DB reads during processEvent.
  const retryPolicies: RetryPolicyMap = new Map();
  const subscriptionCache: SubscriptionCache = new Map();
  const seen = new Set<string>();
  for (const event of allEvents) {
    const projId = event.projectId !== 'default' ? event.projectId : undefined;
    const key = tickCacheKey(event.eventType, projId ?? '');
    if (!seen.has(key)) {
      seen.add(key);
      retryPolicies.set(key, db.getRetryPolicyForEventType(database, event.eventType, projId));
      subscriptionCache.set(key, db.getSubscriptionsByEventType(database, event.eventType, projId));
    }
  }

  for (const event of allEvents) {
    processEvent(database, dispatcher, logger, event, retryPolicies, subscriptionCache);
  }

  return allEvents.length >= CLAIM_BATCH_SIZE;
}

/**
 * Process a single event. Business-critical state transitions (event status,
 * dispatch records, trigger firings) run inside a transaction. Audit entries
 * are accumulated in-memory and flushed in a separate write after the
 * transaction commits, reducing lock hold time and preventing audit I/O
 * failures from rolling back successful dispatches.
 */
function processEvent(
  database: Database.Database,
  dispatcher: Dispatcher,
  logger: Logger,
  event: ReturnType<typeof db.getEvent> & {},
  retryPolicies: RetryPolicyMap,
  subscriptionCache: SubscriptionCache,
): void {
  const audit = new AuditRecorder(event.id);

  const txn = database.transaction(() => {
    const processingStartMs = Date.now();

    audit.recordProcessingStarted(event.eventType, event.sourceType, event.sourceId);

    // Get subscriptions by event type, scoped to the event's project (per-tick cache)
    const projId = event.projectId !== 'default' ? event.projectId : undefined;
    const subsKey = tickCacheKey(event.eventType, projId ?? '');
    const subs = subscriptionCache.get(subsKey)
      ?? db.getSubscriptionsByEventType(database, event.eventType, projId);

    // On retry, reuse cached match results from the prior processing to avoid
    // re-parsing payloadFilter + payload for every subscription. This also
    // ensures retry consistency if subscriptions were modified between attempts.
    let matches: ReturnType<typeof matchEvent>;
    if (event.retryCount > 0) {
      const cached = db.getCachedMatchResults(database, event.id);
      if (cached && cached.length > 0) {
        matches = cached.map(c => ({
          eventId: event.id,
          eventType: event.eventType,
          subscriptionId: c.subscriptionId,
          personaId: c.personaId,
          payload: event.payload,
          sourceId: event.sourceId,
          useCaseId: subs.find(s => s.id === c.subscriptionId)?.useCaseId ?? null,
        }));
        logger.debug({ eventId: event.id, cachedMatches: cached.length }, 'Using cached match results for retry');
      } else {
        matches = matchEvent(event, subs);
      }
    } else {
      matches = matchEvent(event, subs);
    }

    audit.recordSubscriptionsEvaluated(
      subs.length, matches.length,
      matches.map(m => m.subscriptionId),
      matches.map(m => m.personaId),
    );

    // Direct dispatch for targeted trigger events: when a trigger fires with a
    // specific targetPersonaId and no subscriptions match, bypass the subscription
    // system and dispatch directly to the target persona.
    if (matches.length === 0 && event.sourceType === 'trigger' && event.targetPersonaId) {
      const targetPersona = db.getPersona(database, event.targetPersonaId);
      if (targetPersona && event.projectId && targetPersona.projectId !== event.projectId) {
        logger.warn({
          eventId: event.id,
          targetPersonaId: event.targetPersonaId,
          eventProjectId: event.projectId,
          personaProjectId: targetPersona.projectId,
        }, 'Trigger direct dispatch blocked — target persona belongs to a different project');

        audit.recordTriggerDirectDispatchBlocked(event.targetPersonaId, event.projectId, targetPersona.projectId);
      } else if (targetPersona) {
        audit.recordTriggerDirectDispatch(event.targetPersonaId);

        matches.push({
          eventId: event.id,
          eventType: event.eventType,
          subscriptionId: '__trigger_direct__',
          personaId: event.targetPersonaId,
          payload: event.payload,
          sourceId: event.sourceId,
          useCaseId: event.useCaseId ?? null,
        });
      }
    }

    if (matches.length === 0) {
      db.updateEventStatus(database, event.id, 'skipped');
      if (event.sourceType === 'trigger') {
        db.skipTriggerFiring(database, event.id);
      }
      audit.recordSkipped(Date.now() - processingStartMs);
      return; // audit.flush() happens after txn commits
    }

    let delivered = 0;
    let failed = 0;
    let concurrencyBlocked = 0;

    for (const match of matches) {
      const result = dispatchMatch(database, dispatcher, logger, event, match);

      switch (result.type) {
        case 'dispatched':
          delivered++;
          audit.recordDispatchSucceeded(match.subscriptionId, match.personaId, result.executionId);
          break;
        case 'duplicate':
          delivered++;
          audit.recordDispatchSkippedDuplicate(match.subscriptionId, match.personaId, 'Event already dispatched to this persona');
          break;
        case 'concurrency_blocked':
          concurrencyBlocked++;
          dispatcher.metrics.recordConcurrencyBlock();
          audit.recordConcurrencyBlocked(match.subscriptionId, match.personaId, result.running, result.maxConcurrent);
          break;
        case 'persona_not_found':
          failed++;
          audit.recordDispatchFailed(match.subscriptionId, match.personaId, 'Persona not found');
          break;
        case 'queue_full':
          audit.recordDispatchRejected(match.subscriptionId, match.personaId, result.executionId);
          break;
        case 'error':
          failed++;
          audit.recordDispatchError(match.subscriptionId, match.personaId, result.err);
          break;
      }
    }

    // Resolve outcome via pure function
    const retryPolicy = retryPolicies.get(tickCacheKey(event.eventType, event.projectId !== 'default' ? event.projectId : ''))
      ?? { maxRetries: DEFAULT_MAX_RETRIES, retryBackoffMs: DEFAULT_RETRY_BACKOFF_MS };

    const outcome = resolveOutcome(delivered, failed, concurrencyBlocked, event.retryCount, retryPolicy);

    // Apply business-critical DB writes + metrics (inside txn)
    applyOutcome(database, dispatcher, event.id, event.retryCount, outcome);

    // Record outcome audit entries (buffered — flushed after txn commits)
    audit.recordOutcome(outcome, delivered, failed, concurrencyBlocked, Date.now() - processingStartMs);
  });

  try {
    txn();

    // Flush audit entries outside the main transaction. This decouples
    // observability writes from business-critical state transitions:
    // - Reduces transaction duration and SQLite lock hold time
    // - Prevents audit I/O failures from rolling back successful dispatches
    // - Audit writes are best-effort — lost entries don't affect correctness
    try {
      audit.flush(database);
    } catch (auditErr) {
      logger.warn({ err: auditErr, eventId: event.id }, 'Failed to flush audit log — event processing succeeded but audit entries were lost');
    }
  } catch (err) {
    logger.error({ err, eventId: event.id }, 'Event processing transaction failed — event will be recovered by the next stale sweep (within 60s)');

    // Flush whatever audit entries we collected before the failure (best effort).
    // Include the error itself as an additional entry.
    audit.recordProcessingError(err);
    try {
      audit.flush(database);
    } catch { /* best effort */ }
  }
}
