import type Database from 'better-sqlite3';
import type { Logger } from 'pino';
import * as db from './db.js';

/**
 * Start the trigger scheduler loop.
 * Ported from desktop engine/background.rs::trigger_scheduler_tick().
 *
 * Runs every `interval` ms (default 5s), evaluates due triggers,
 * and publishes events to the event bus (DB-backed).
 */
export function startTriggerScheduler(
  database: Database.Database,
  logger: Logger,
  interval: number = 5000,
): NodeJS.Timeout {
  logger.info({ intervalMs: interval }, 'Trigger scheduler started');

  return setInterval(() => {
    try {
      triggerSchedulerTick(database, logger);
    } catch (err) {
      logger.error({ err }, 'Trigger scheduler tick failed');
    }
  }, interval);
}

function triggerSchedulerTick(
  database: Database.Database,
  logger: Logger,
): void {
  const triggers = db.getDueTriggers(database);
  if (triggers.length === 0) return;

  for (const trigger of triggers) {
    // Skip polling triggers (handled separately if needed)
    if (trigger.triggerType === 'polling') continue;

    try {
      // Parse config
      let eventType = 'trigger_fired';
      let payload: string | null = null;

      if (trigger.config) {
        try {
          const config = JSON.parse(trigger.config) as {
            event_type?: string;
            payload?: unknown;
            cron?: string;
          };
          if (config.event_type) eventType = config.event_type;
          if (config.payload) payload = JSON.stringify(config.payload);
        } catch {
          // Invalid config JSON, use defaults
        }
      }

      // Look up persona to get its projectId for event isolation
      const persona = db.getPersona(database, trigger.personaId);
      const triggerProjectId = persona?.projectId;

      // Publish event to bus
      db.publishEvent(database, {
        eventType,
        sourceType: 'trigger',
        sourceId: trigger.id,
        targetPersonaId: trigger.personaId,
        projectId: triggerProjectId,
        useCaseId: trigger.useCaseId,
        payload,
      });

      // Compute next trigger time
      const now = new Date().toISOString();
      let nextTriggerAt: string | null = null;

      if (trigger.triggerType === 'schedule' && trigger.config) {
        try {
          const config = JSON.parse(trigger.config) as { cron?: string; interval_seconds?: number };
          if (config.cron) {
            nextTriggerAt = computeNextCron(config.cron);
          } else if (config.interval_seconds) {
            const next = new Date(Date.now() + config.interval_seconds * 1000);
            nextTriggerAt = next.toISOString();
          }
        } catch {
          // Cannot compute next, trigger won't fire again until updated
        }
      }

      db.updateTriggerTimings(database, trigger.id, now, nextTriggerAt);

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
}

/**
 * Simple cron next-time computation.
 * Supports basic interval-based patterns. For full cron support,
 * add cron-parser dependency later.
 */
function computeNextCron(cron: string): string | null {
  // Basic patterns: "every Xm", "every Xh", "every Xs"
  const match = cron.match(/^every\s+(\d+)([smhd])$/i);
  if (match) {
    const value = parseInt(match[1]!, 10);
    const unit = match[2]!.toLowerCase();
    const multipliers: Record<string, number> = { s: 1000, m: 60000, h: 3600000, d: 86400000 };
    const ms = value * (multipliers[unit] ?? 60000);
    return new Date(Date.now() + ms).toISOString();
  }

  // Standard cron — attempt basic parsing for common patterns
  // For now, default to 1 hour if we can't parse
  // TODO: Add cron-parser for full cron support
  return new Date(Date.now() + 3600000).toISOString();
}
