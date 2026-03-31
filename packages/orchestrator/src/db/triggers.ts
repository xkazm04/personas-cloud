import Database from 'better-sqlite3';
import { nanoid } from 'nanoid';
import { CronExpressionParser } from 'cron-parser';
import { z } from 'zod';
import type { PersonaTrigger, TriggerType, TriggerConfig } from '@dac-cloud/shared';
import { stmt, createRowMapper, mergeEnabled, mergeUpdate } from './_helpers.js';

// ---------------------------------------------------------------------------
// Row mapper
// ---------------------------------------------------------------------------

export const rowToTrigger = createRowMapper<PersonaTrigger>({
  id: { col: 'id' },
  personaId: { col: 'persona_id' },
  triggerType: { col: 'trigger_type' },
  config: { col: 'config', nullable: true },
  enabled: { col: 'enabled', bool: true },
  lastTriggeredAt: { col: 'last_triggered_at', nullable: true },
  nextTriggerAt: { col: 'next_trigger_at', nullable: true },
  healthStatus: { col: 'health_status', default: 'healthy' },
  healthMessage: { col: 'health_message', nullable: true },
  useCaseId: { col: 'use_case_id', nullable: true },
  createdAt: { col: 'created_at' },
  updatedAt: { col: 'updated_at' },
});

// ---------------------------------------------------------------------------
// Trigger config — Zod schemas & discriminated union
// ---------------------------------------------------------------------------

const triggerConfigBase = z.object({
  event_type: z.string().optional(),
  payload: z.unknown().optional(),
});

const scheduleTriggerConfigSchema = triggerConfigBase.extend({
  kind: z.literal('schedule'),
  cron: z.string().optional(),
  interval_seconds: z.number().positive().optional(),
});

const manualTriggerConfigSchema = triggerConfigBase.extend({
  kind: z.literal('manual'),
});

const pollingTriggerConfigSchema = triggerConfigBase.extend({
  kind: z.literal('polling'),
  poll_url: z.string().optional(),
  poll_interval_seconds: z.number().positive().optional(),
});

const webhookTriggerConfigSchema = triggerConfigBase.extend({
  kind: z.literal('webhook'),
  url: z.string().optional(),
  secret: z.string().optional(),
});

const chainTriggerConfigSchema = triggerConfigBase.extend({
  kind: z.literal('chain'),
  source_persona_id: z.string().optional(),
  source_event_type: z.string().optional(),
});

const triggerConfigSchema = z.discriminatedUnion('kind', [
  scheduleTriggerConfigSchema,
  manualTriggerConfigSchema,
  pollingTriggerConfigSchema,
  webhookTriggerConfigSchema,
  chainTriggerConfigSchema,
]);

const VALID_TRIGGER_TYPES = new Set<TriggerType>(['manual', 'schedule', 'polling', 'webhook', 'chain']);

function coerceTriggerType(raw: string): TriggerType {
  if (!VALID_TRIGGER_TYPES.has(raw as TriggerType)) {
    throw new Error(`Invalid trigger type: ${raw}`);
  }
  return raw as TriggerType;
}

/**
 * Validate and parse a raw config JSON string into a typed TriggerConfig.
 * If `triggerType` is provided, the `kind` discriminant is injected automatically
 * so callers don't need to include it in the stored JSON.
 */
export function parseTriggerConfig(
  config: string | null,
  triggerType?: TriggerType,
): TriggerConfig | null {
  if (!config) return null;
  try {
    const raw = JSON.parse(config) as Record<string, unknown>;
    // Inject `kind` from triggerType when not present in the JSON
    if (triggerType && !raw.kind) {
      raw.kind = triggerType;
    }
    return triggerConfigSchema.parse(raw);
  } catch {
    return null;
  }
}

/**
 * Validate config at write time — throws on invalid config.
 * Returns the canonical JSON string to persist.
 */
export function validateTriggerConfig(
  triggerType: TriggerType,
  config: string | null,
): string | null {
  if (!config) return null;
  const parsed = parseTriggerConfig(config, triggerType);
  if (!parsed) throw new Error(`Invalid trigger config for type "${triggerType}"`);
  // Strip `kind` from persisted JSON — it's redundant with trigger_type column
  const { kind: _, ...rest } = parsed;
  return JSON.stringify(rest);
}

/**
 * Compute the next fire time from a typed trigger config.
 * Supports cron expressions, shorthand "every Xm/h/s/d", and interval_seconds.
 */
export function computeNextTriggerAtFromParsed(
  triggerType: TriggerType,
  parsed: TriggerConfig | null,
): string | null {
  if (triggerType !== 'schedule' || !parsed || parsed.kind !== 'schedule') return null;

  if (parsed.cron) {
    // Shorthand patterns: "every Xm", "every Xh", "every Xs", "every Xd"
    const match = parsed.cron.match(/^every\s+(\d+)([smhd])$/i);
    if (match) {
      const value = parseInt(match[1]!, 10);
      const unit = match[2]!.toLowerCase();
      const multipliers: Record<string, number> = { s: 1000, m: 60000, h: 3600000, d: 86400000 };
      const ms = value * (multipliers[unit] ?? 60000);
      return new Date(Date.now() + ms).toISOString();
    }

    // Standard cron expression
    try {
      const interval = CronExpressionParser.parse(parsed.cron);
      return interval.next().toISOString();
    } catch {
      return null;
    }
  }

  if (parsed.interval_seconds) {
    return new Date(Date.now() + parsed.interval_seconds * 1000).toISOString();
  }

  return null;
}

// ---------------------------------------------------------------------------
// CRUD
// ---------------------------------------------------------------------------

export function createTrigger(db: Database.Database, input: {
  personaId: string; triggerType: string; config?: string | null; enabled?: boolean; useCaseId?: string | null; projectId?: string;
}): PersonaTrigger {
  const now = new Date().toISOString();
  const id = nanoid();
  const triggerType = coerceTriggerType(input.triggerType);
  // Validate config at write time — throws on invalid config
  const config = input.config ? validateTriggerConfig(triggerType, input.config) : null;
  const enabled = input.enabled !== false;
  const useCaseId = input.useCaseId ?? null;
  const nextTriggerAt = enabled ? computeNextTriggerAtFromParsed(triggerType, parseTriggerConfig(config, triggerType)) : null;
  stmt(db,`
    INSERT INTO persona_triggers (id, project_id, persona_id, trigger_type, config, enabled, use_case_id, next_trigger_at, created_at, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `).run(id, input.projectId ?? 'default', input.personaId, triggerType, config, enabled ? 1 : 0, useCaseId, nextTriggerAt, now, now);
  return {
    id,
    personaId: input.personaId,
    triggerType,
    config,
    enabled,
    lastTriggeredAt: null,
    nextTriggerAt,
    healthStatus: 'healthy',
    healthMessage: null,
    useCaseId,
    createdAt: now,
    updatedAt: now,
  };
}

export function getTrigger(db: Database.Database, id: string): PersonaTrigger | undefined {
  const row = stmt(db,'SELECT * FROM persona_triggers WHERE id = ?').get(id) as Record<string, unknown> | undefined;
  return row ? rowToTrigger(row) : undefined;
}

export function listTriggersForPersona(db: Database.Database, personaId: string): PersonaTrigger[] {
  const rows = stmt(db,'SELECT * FROM persona_triggers WHERE persona_id = ?').all(personaId) as Record<string, unknown>[];
  return rows.map(rowToTrigger);
}

export interface DueTriggerRow extends PersonaTrigger {
  personaProjectId: string | null;
  personaEnabled: boolean | null;
  /** Pre-parsed typed config — parsed once on load, never in the hot loop. */
  parsedConfig: TriggerConfig | null;
}

export function getDueTriggersWithPersona(db: Database.Database, batchSize: number = 100): DueTriggerRow[] {
  const rows = stmt(db, `
    SELECT t.*, p.project_id AS persona_project_id, p.enabled AS persona_enabled
    FROM persona_triggers t
    LEFT JOIN personas p ON p.id = t.persona_id
    WHERE t.enabled = 1 AND t.next_trigger_at <= strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
    ORDER BY t.next_trigger_at ASC
    LIMIT ?
  `).all(batchSize) as Record<string, unknown>[];
  return rows.map(row => {
    const trigger = rowToTrigger(row);
    return {
      ...trigger,
      personaProjectId: (row.persona_project_id as string) ?? null,
      personaEnabled: row.persona_enabled != null ? row.persona_enabled === 1 : null,
      parsedConfig: parseTriggerConfig(trigger.config, trigger.triggerType),
    };
  });
}

export function updateTriggerTimings(db: Database.Database, id: string, lastTriggeredAt: string, nextTriggerAt: string | null): void {
  stmt(db,'UPDATE persona_triggers SET last_triggered_at = ?, next_trigger_at = ?, updated_at = ? WHERE id = ?')
    .run(lastTriggeredAt, nextTriggerAt, new Date().toISOString(), id);
}

export function updateTrigger(db: Database.Database, id: string, updates: {
  triggerType?: string | null; config?: string | null; enabled?: boolean | null;
}): void {
  const trigger = getTrigger(db, id);
  if (!trigger) return;
  const effectiveType = updates.triggerType ? coerceTriggerType(updates.triggerType) : trigger.triggerType;
  const effectiveConfig = updates.config !== undefined
    ? (updates.config ? validateTriggerConfig(effectiveType, updates.config) : updates.config)
    : trigger.config;
  const effectiveEnabled = mergeEnabled(updates.enabled, trigger.enabled);
  mergeUpdate(db, 'persona_triggers', id, {
    trigger_type: effectiveType,
    config: effectiveConfig,
    enabled: effectiveEnabled,
  });
  const nextTriggerAt = effectiveEnabled
    ? computeNextTriggerAtFromParsed(effectiveType, parseTriggerConfig(effectiveConfig, effectiveType))
    : null;
  stmt(db, 'UPDATE persona_triggers SET next_trigger_at = ? WHERE id = ?').run(nextTriggerAt, id);
}

export function deleteTrigger(db: Database.Database, id: string): void {
  stmt(db,'DELETE FROM persona_triggers WHERE id = ?').run(id);
}

/**
 * Return the earliest `next_trigger_at` for enabled triggers, or null if none exist.
 */
/**
 * Fetch specific triggers by ID with persona JOIN (for heap-driven firing).
 * Returns only enabled triggers that match the given IDs.
 */
export function getTriggersByIdsWithPersona(db: Database.Database, ids: string[]): DueTriggerRow[] {
  if (ids.length === 0) return [];
  const placeholders = ids.map(() => '?').join(',');
  const rows = stmt(db, `
    SELECT t.*, p.project_id AS persona_project_id, p.enabled AS persona_enabled
    FROM persona_triggers t
    LEFT JOIN personas p ON p.id = t.persona_id
    WHERE t.id IN (${placeholders})
  `).all(...ids) as Record<string, unknown>[];
  return rows.map(row => {
    const trigger = rowToTrigger(row);
    return {
      ...trigger,
      personaProjectId: (row.persona_project_id as string) ?? null,
      personaEnabled: row.persona_enabled != null ? row.persona_enabled === 1 : null,
      parsedConfig: parseTriggerConfig(trigger.config, trigger.triggerType),
    };
  });
}

/**
 * Load all enabled triggers with a non-null next_trigger_at.
 * Used to rebuild the in-memory heap on startup.
 * Returns { id, next_trigger_at } pairs only — lightweight for heap init.
 */
export function loadEnabledTriggerTimes(db: Database.Database): Array<{ id: string; nextTriggerAt: string }> {
  const rows = stmt(db, `
    SELECT id, next_trigger_at
    FROM persona_triggers
    WHERE enabled = 1 AND next_trigger_at IS NOT NULL
  `).all() as Array<{ id: string; next_trigger_at: string }>;
  return rows.map(r => ({ id: r.id, nextTriggerAt: r.next_trigger_at }));
}

export function getEarliestNextTriggerTime(db: Database.Database): string | null {
  const row = stmt(db, `
    SELECT MIN(next_trigger_at) AS earliest
    FROM persona_triggers
    WHERE enabled = 1 AND next_trigger_at IS NOT NULL
  `).get() as { earliest: string | null } | undefined;
  return row?.earliest ?? null;
}
