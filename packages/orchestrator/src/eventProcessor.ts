import type Database from 'better-sqlite3';
import type { Logger } from 'pino';
import { matchEvent, assemblePrompt } from '@dac-cloud/shared';
import type { ExecRequest } from '@dac-cloud/shared';
import { nanoid } from 'nanoid';
import * as db from './db.js';
import type { Dispatcher } from './dispatcher.js';

/**
 * Start the event processing loop.
 * Ported from desktop engine/background.rs::event_bus_tick().
 *
 * Runs every `interval` ms (default 2s), picks up pending events,
 * matches them to subscriptions, and dispatches executions.
 */
export function startEventProcessor(
  database: Database.Database,
  dispatcher: Dispatcher,
  logger: Logger,
  interval: number = 2000,
): NodeJS.Timeout {
  logger.info({ intervalMs: interval }, 'Event processor started');

  return setInterval(() => {
    try {
      eventBusTick(database, dispatcher, logger);
    } catch (err) {
      logger.error({ err }, 'Event processor tick failed');
    }
  }, interval);
}

function eventBusTick(
  database: Database.Database,
  dispatcher: Dispatcher,
  logger: Logger,
): void {
  const events = db.getPendingEvents(database, 50);
  if (events.length === 0) return;

  for (const event of events) {
    // Mark as processing
    db.updateEventStatus(database, event.id, 'processing');

    // Get subscriptions by event type, scoped to the event's project
    const subs = db.getSubscriptionsByEventType(database, event.eventType, event.projectId !== 'default' ? event.projectId : undefined);
    const matches = matchEvent(event, subs);

    if (matches.length === 0) {
      db.updateEventStatus(database, event.id, 'skipped');
      continue;
    }

    let delivered = 0;
    let failed = 0;

    for (const match of matches) {
      try {
        const persona = db.getPersona(database, match.personaId);
        if (!persona) {
          logger.warn({ personaId: match.personaId, eventId: event.id }, 'Persona not found for event match');
          failed++;
          continue;
        }

        // Check concurrency
        const running = db.countRunningExecutions(database, persona.id);
        if (running >= persona.maxConcurrent) {
          logger.debug({ personaId: persona.id, running, max: persona.maxConcurrent }, 'Persona at concurrency limit, skipping');
          failed++;
          continue;
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
          config: { timeoutMs: persona.timeoutMs },
          triggerId: undefined,
          triggerType: undefined,
        };

        dispatcher.submit(request);
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
        failed++;
      }
    }

    // Update event status
    if (failed === 0) {
      db.updateEventStatus(database, event.id, 'delivered');
    } else if (delivered > 0) {
      db.updateEventStatus(database, event.id, 'partial');
    } else {
      db.updateEventStatus(database, event.id, 'failed', 'All subscription matches failed');
    }
  }
}
