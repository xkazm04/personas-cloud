import type { PersonaEvent, PersonaEventSubscription } from './types.js';

/**
 * A matched subscription: an event matched to a persona that should execute.
 * Ported from engine/bus.rs.
 */
export interface EventMatch {
  eventId: string;
  eventType: string;
  subscriptionId: string;
  personaId: string;
  payload: string | null;
  sourceId: string | null;
  useCaseId: string | null;
}

/**
 * Match a single event against a list of subscriptions.
 *
 * Rules:
 * 1. subscription.eventType must equal event.eventType
 * 2. If subscription.sourceFilter is set, event.sourceId must match (exact or wildcard)
 * 3. If event.targetPersonaId is set, only that persona's subscriptions match
 * 4. subscription.enabled must be true
 */
export function matchEvent(
  event: PersonaEvent,
  subscriptions: PersonaEventSubscription[],
): EventMatch[] {
  return subscriptions
    .filter((sub) => {
      if (!sub.enabled) return false;
      if (sub.eventType !== event.eventType) return false;
      if (event.targetPersonaId && event.targetPersonaId !== sub.personaId) return false;
      if (sub.sourceFilter && !sourceFilterMatches(sub.sourceFilter, event.sourceId)) return false;
      return true;
    })
    .map((sub) => ({
      eventId: event.id,
      eventType: event.eventType,
      subscriptionId: sub.id,
      personaId: sub.personaId,
      payload: event.payload,
      sourceId: event.sourceId,
      useCaseId: sub.useCaseId,
    }));
}

/**
 * Simple matching: exact match or prefix wildcard (trailing `*`).
 * Ported from engine/bus.rs::source_filter_matches.
 */
function sourceFilterMatches(filter: string, sourceId: string | null): boolean {
  if (!sourceId) return false;

  if (filter.endsWith('*')) {
    const prefix = filter.slice(0, -1);
    return sourceId.startsWith(prefix);
  }
  return sourceId === filter;
}
