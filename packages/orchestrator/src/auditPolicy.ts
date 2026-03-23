import type { AuditLog, AuditEntry } from './auditLog.js';

/**
 * Converts an event-map value (either a tuple of args or a listener function)
 * to an audit-mapper that receives the same args and returns an AuditEntry.
 */
type AuditMapperFor<T> =
  T extends (...args: infer A) => any ? (...args: A) => AuditEntry
  : T extends any[] ? (...args: T) => AuditEntry
  : (...args: any[]) => AuditEntry;

/**
 * Minimal interface for anything with a typed `on` method
 * (Node EventEmitter, WorkerPool, etc.).
 *
 * @typeParam EventMap - maps event names to listener signatures (function-based
 *   like WorkerPoolEvents) or argument tuples (Node EventEmitter-style like
 *   TenantKeyEvents).
 */
export interface Auditable<EventMap extends Record<string, any> = Record<string, (...args: any[]) => void>> {
  on<K extends string & keyof EventMap>(event: K, listener: (...args: any[]) => void): any;
}

/**
 * Declarative audit policy: maps emitter events to AuditEntry projections.
 *
 * Instead of manually wiring `.on()` calls per emitter, declare a mapping
 * object and call `attach()`. New emitters plug in with one line.
 */
export class AuditPolicy {
  constructor(private auditLog: AuditLog) {}

  /**
   * Subscribe to all events in `mappings` on the given emitter.
   *
   * Supply the emitter's event-map as a type parameter to get compile-time
   * enforcement that mapping keys are valid event names and mapper args
   * match the event's listener signature.
   */
  attach<EventMap extends Record<string, any>>(
    emitter: Auditable<EventMap>,
    mappings: { [K in string & keyof EventMap]?: AuditMapperFor<EventMap[K]> },
  ): this {
    for (const [event, mapper] of Object.entries(mappings)) {
      emitter.on(event as string & keyof EventMap, (...args: any[]) => {
        try {
          this.auditLog.record((mapper as (...a: any[]) => AuditEntry)(...args));
        } catch {
          // AuditLog.record() already logs at error level with full payload.
          // Swallow here to prevent crashing the emitter's listener chain.
        }
      });
    }
    return this;
  }
}
