import type Database from 'better-sqlite3';
import type { Logger } from 'pino';
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
 * Start the trigger scheduler loop.
 * Ported from desktop engine/background.rs::trigger_scheduler_tick().
 *
 * Runs every `interval` ms (default 5s), evaluates due triggers,
 * and publishes events to the event bus (DB-backed).
 *
 * @param onTrigger Optional callback invoked after one or more triggers fire.
 *                  Used to notify the event processor for immediate dispatch.
 */
export function startTriggerScheduler(
  database: Database.Database,
  logger: Logger,
  interval: number = 5000,
  onTrigger?: () => void,
): NodeJS.Timeout {
  logger.info({ intervalMs: interval }, 'Trigger scheduler started');

  return setInterval(() => {
    try {
      triggerSchedulerTick(database, logger, onTrigger);
    } catch (err) {
      logger.error({ err }, 'Trigger scheduler tick failed');
    }
  }, interval);
}

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
