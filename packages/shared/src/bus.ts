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
 * Callers are expected to pre-filter subscriptions by eventType (e.g. via
 * getSubscriptionsByEventType) so no eventType check is performed here.
 *
 * Rules:
 * 1. If event.targetPersonaId is set, only that persona's subscriptions match
 * 2. If subscription.sourceFilter is set, event.sourceId must match (exact or wildcard)
 * 3. If subscription.payloadFilter is set, event.payload must match the filter conditions
 * 4. subscription.enabled must be true
 */
export function matchEvent(
  event: PersonaEvent,
  subscriptions: PersonaEventSubscription[],
): EventMatch[] {
  return subscriptions
    .filter((sub) => {
      if (event.targetPersonaId && event.targetPersonaId !== sub.personaId) return false;
      if (sub.sourceFilter && !sourceFilterMatches(sub.sourceFilter, event.sourceId)) return false;
      if (sub.payloadFilter && !payloadFilterMatches(sub.payloadFilter, event.payload)) return false;
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

// ---------------------------------------------------------------------------
// Payload filter evaluation — EventBridge-style pattern matching
// ---------------------------------------------------------------------------

/**
 * A single condition matcher within a payload filter value array.
 *
 * - string/number/boolean: exact match
 * - { prefix: "..." }: string starts with
 * - { suffix: "..." }: string ends with
 * - { anything-but: [...] }: value is NOT any of the listed values
 * - { numeric: [">=", 10, "<", 100] }: numeric range comparisons (pairs of op+value)
 * - { exists: true/false }: field exists / does not exist
 */
type ConditionMatcher =
  | string
  | number
  | boolean
  | { prefix: string }
  | { suffix: string }
  | { 'anything-but': (string | number)[] }
  | { numeric: (string | number)[] }
  | { exists: boolean };

/**
 * A payload filter is a JSON object where:
 * - Keys are dot-notation paths into the event payload (e.g. "ref", "action", "commit.author.name")
 * - Values are arrays of condition matchers (OR within array, AND across keys)
 *
 * Example:
 * {
 *   "ref": ["refs/heads/main", "refs/heads/develop"],
 *   "action": ["push"],
 *   "commits.count": [{ "numeric": [">=", 1] }]
 * }
 *
 * This matches events where ref is "refs/heads/main" OR "refs/heads/develop",
 * AND action is "push", AND commits.count >= 1.
 */
export type PayloadFilter = Record<string, ConditionMatcher[]>;

// ---------------------------------------------------------------------------
// Compiled payload filter — pre-resolves filter structure for reuse
// ---------------------------------------------------------------------------

/** A pre-compiled, reusable predicate for a payload filter. */
type CompiledPayloadFilter = (payloadJson: string | null) => boolean;

/** Cache of compiled filter predicates keyed by the filter JSON string. */
const compiledFilterCache = new Map<string, CompiledPayloadFilter>();
const MAX_COMPILED_FILTER_CACHE = 500;

/**
 * Compile a JSON-encoded payload filter into a reusable predicate function.
 * The compiled predicate parses the filter JSON once and pre-resolves all
 * path/condition structures so the hot path is a single function call.
 */
export function compilePayloadFilter(filterJson: string): CompiledPayloadFilter {
  const cached = compiledFilterCache.get(filterJson);
  if (cached) return cached;

  let filter: PayloadFilter;
  try {
    filter = JSON.parse(filterJson);
  } catch {
    const failPredicate: CompiledPayloadFilter = () => false;
    cacheCompiledFilter(filterJson, failPredicate);
    return failPredicate;
  }

  if (typeof filter !== 'object' || filter === null || Array.isArray(filter)) {
    const failPredicate: CompiledPayloadFilter = () => false;
    cacheCompiledFilter(filterJson, failPredicate);
    return failPredicate;
  }

  if (Object.keys(filter).length === 0) {
    const passPredicate: CompiledPayloadFilter = () => true;
    cacheCompiledFilter(filterJson, passPredicate);
    return passPredicate;
  }

  // Pre-resolve: build an array of { pathParts, matchers } for direct evaluation
  const rules: Array<{ pathParts: string[]; matchers: ConditionMatcher[] }> = [];
  for (const [path, matchers] of Object.entries(filter)) {
    if (!Array.isArray(matchers) || matchers.length === 0) continue;
    const pathParts = path.split('.');
    if (pathParts.length > MAX_PATH_DEPTH) continue;
    rules.push({ pathParts, matchers });
  }

  const predicate: CompiledPayloadFilter = (payloadJson) => {
    let payload: unknown;
    try {
      payload = payloadJson ? JSON.parse(payloadJson) : null;
    } catch {
      payload = null;
    }

    for (const { pathParts, matchers } of rules) {
      const value = resolvePathParts(payload, pathParts);
      const anyMatch = matchers.some((matcher) => matchCondition(matcher, value));
      if (!anyMatch) return false;
    }
    return true;
  };

  cacheCompiledFilter(filterJson, predicate);
  return predicate;
}

function cacheCompiledFilter(key: string, predicate: CompiledPayloadFilter): void {
  if (compiledFilterCache.size >= MAX_COMPILED_FILTER_CACHE) {
    const firstKey = compiledFilterCache.keys().next().value!;
    compiledFilterCache.delete(firstKey);
  }
  compiledFilterCache.set(key, predicate);
}

/** Invalidate all compiled payload filter caches (e.g. after subscription updates). */
export function invalidateCompiledFilterCache(): void {
  compiledFilterCache.clear();
}

/**
 * Evaluate a JSON-encoded payload filter against a JSON-encoded event payload.
 * Uses compiled predicate cache for performance.
 */
function payloadFilterMatches(filterJson: string, payloadJson: string | null): boolean {
  return compilePayloadFilter(filterJson)(payloadJson);
}

/** Maximum number of dot-separated segments allowed in a payload filter path. */
const MAX_PATH_DEPTH = 10;

/** Maximum number of elements allowed in an anything-but or numeric condition array. */
const MAX_CONDITION_ARRAY_LENGTH = 100;

/**
 * Resolve a dot-notation path against an object.
 * Returns undefined if the path doesn't exist or exceeds MAX_PATH_DEPTH segments.
 */
function resolvePath(obj: unknown, path: string): unknown {
  return resolvePathParts(obj, path.split('.'));
}

/**
 * Resolve a pre-split dot-notation path against an object.
 * Used by compiled filters to avoid repeated string splitting.
 */
function resolvePathParts(obj: unknown, parts: string[]): unknown {
  if (parts.length > MAX_PATH_DEPTH) return undefined;
  let current: unknown = obj;

  for (const part of parts) {
    if (current === null || current === undefined) return undefined;
    if (typeof current !== 'object') return undefined;
    current = (current as Record<string, unknown>)[part];
  }

  return current;
}

/**
 * Evaluate a single condition matcher against a resolved value.
 */
function matchCondition(matcher: ConditionMatcher, value: unknown): boolean {
  // Primitive exact match
  if (typeof matcher === 'string' || typeof matcher === 'number' || typeof matcher === 'boolean') {
    return value === matcher;
  }

  if (typeof matcher !== 'object' || matcher === null) return false;

  // { exists: true/false }
  if ('exists' in matcher) {
    return matcher.exists ? value !== undefined : value === undefined;
  }

  // { prefix: "..." }
  if ('prefix' in matcher) {
    return typeof value === 'string' && value.startsWith(matcher.prefix);
  }

  // { suffix: "..." }
  if ('suffix' in matcher) {
    return typeof value === 'string' && value.endsWith(matcher.suffix);
  }

  // { anything-but: [...] }
  if ('anything-but' in matcher) {
    if (value === undefined || value === null) return false;
    return !matcher['anything-but'].some((v) => v === value);
  }

  // { numeric: [">=", 10, "<", 100] }
  if ('numeric' in matcher) {
    if (typeof value !== 'number') return false;
    const ops = matcher.numeric;
    for (let i = 0; i < ops.length - 1; i += 2) {
      const op = ops[i];
      const threshold = ops[i + 1];
      if (typeof op !== 'string' || typeof threshold !== 'number') return false;
      switch (op) {
        case '=': if (!(value === threshold)) return false; break;
        case '>': if (!(value > threshold)) return false; break;
        case '>=': if (!(value >= threshold)) return false; break;
        case '<': if (!(value < threshold)) return false; break;
        case '<=': if (!(value <= threshold)) return false; break;
        default: return false;
      }
    }
    return true;
  }

  return false;
}

/**
 * Validate a payload filter JSON string. Returns null if valid, or an error message.
 */
export function validatePayloadFilter(filterJson: string): string | null {
  let filter: unknown;
  try {
    filter = JSON.parse(filterJson);
  } catch (e) {
    return 'Invalid JSON';
  }

  if (typeof filter !== 'object' || filter === null || Array.isArray(filter)) {
    return 'Filter must be a JSON object';
  }

  for (const [key, value] of Object.entries(filter as Record<string, unknown>)) {
    if (!key || key.length > 200) {
      return `Invalid field path: "${key}"`;
    }

    const segments = key.split('.');
    if (segments.length > MAX_PATH_DEPTH) {
      return `Field path "${key}" exceeds maximum depth of ${MAX_PATH_DEPTH} segments`;
    }

    if (!Array.isArray(value)) {
      return `Filter value for "${key}" must be an array of conditions`;
    }

    if (value.length === 0) {
      return `Filter value for "${key}" must not be empty`;
    }

    for (const condition of value) {
      const err = validateCondition(condition, key);
      if (err) return err;
    }
  }

  return null;
}

function validateCondition(condition: unknown, key: string): string | null {
  if (condition === null || condition === undefined) {
    return `Condition for "${key}" must not be null/undefined`;
  }

  const t = typeof condition;
  if (t === 'string' || t === 'number' || t === 'boolean') return null;

  if (t !== 'object' || Array.isArray(condition)) {
    return `Invalid condition type for "${key}"`;
  }

  const obj = condition as Record<string, unknown>;
  const keys = Object.keys(obj);
  if (keys.length !== 1) {
    return `Condition object for "${key}" must have exactly one operator`;
  }

  const op = keys[0];
  switch (op) {
    case 'prefix':
    case 'suffix':
      if (typeof obj[op] !== 'string') return `"${op}" value for "${key}" must be a string`;
      break;
    case 'exists':
      if (typeof obj[op] !== 'boolean') return `"exists" value for "${key}" must be a boolean`;
      break;
    case 'anything-but':
      if (!Array.isArray(obj[op])) return `"anything-but" value for "${key}" must be an array`;
      if ((obj[op] as unknown[]).length > MAX_CONDITION_ARRAY_LENGTH) {
        return `"anything-but" array for "${key}" exceeds maximum length of ${MAX_CONDITION_ARRAY_LENGTH}`;
      }
      for (const v of obj[op] as unknown[]) {
        if (typeof v !== 'string' && typeof v !== 'number') {
          return `"anything-but" array for "${key}" must contain only strings or numbers`;
        }
      }
      break;
    case 'numeric':
      if (!Array.isArray(obj[op])) return `"numeric" value for "${key}" must be an array`;
      {
        const arr = obj[op] as unknown[];
        if (arr.length > MAX_CONDITION_ARRAY_LENGTH) {
          return `"numeric" array for "${key}" exceeds maximum length of ${MAX_CONDITION_ARRAY_LENGTH}`;
        }
        if (arr.length === 0 || arr.length % 2 !== 0) {
          return `"numeric" for "${key}" must have pairs of [operator, value]`;
        }
        for (let i = 0; i < arr.length; i += 2) {
          if (typeof arr[i] !== 'string' || !['=', '>', '>=', '<', '<='].includes(arr[i] as string)) {
            return `Invalid numeric operator for "${key}" at position ${i}`;
          }
          if (typeof arr[i + 1] !== 'number') {
            return `Numeric threshold for "${key}" at position ${i + 1} must be a number`;
          }
        }
      }
      break;
    default:
      return `Unknown operator "${op}" for "${key}"`;
  }

  return null;
}
