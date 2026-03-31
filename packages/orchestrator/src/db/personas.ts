import { performance } from 'node:perf_hooks';
import Database from 'better-sqlite3';
import type { Logger } from 'pino';
import type {
  Persona,
  PersonaSummary,
  HydratedPersona,
} from '@dac-cloud/shared';
import { stmt, createRowMapper, buildWhereClause } from './_helpers.js';
import { rowToTool } from './tools.js';
import { rowToRedactedCredential } from './credentials.js';
import { rowToSubscription } from './events.js';
import { rowToTrigger } from './triggers.js';

const HYDRATION_WARN_THRESHOLD_MS = 100;

// ---------------------------------------------------------------------------
// Row mappers
// ---------------------------------------------------------------------------

const rowToPersona = createRowMapper<Persona>({
  id: { col: 'id' },
  projectId: { col: 'project_id' },
  name: { col: 'name' },
  description: { col: 'description', nullable: true },
  systemPrompt: { col: 'system_prompt' },
  structuredPrompt: { col: 'structured_prompt', nullable: true },
  icon: { col: 'icon', nullable: true },
  color: { col: 'color', nullable: true },
  enabled: { col: 'enabled', bool: true },
  maxConcurrent: { col: 'max_concurrent' },
  timeoutMs: { col: 'timeout_ms' },
  inferenceProfileId: { col: 'inference_profile_id', nullable: true },
  networkPolicy: { col: 'network_policy', nullable: true },
  maxBudgetUsd: { col: 'max_budget_usd', nullable: true },
  maxTurns: { col: 'max_turns', nullable: true },
  designContext: { col: 'design_context', nullable: true },
  groupId: { col: 'group_id', nullable: true },
  permissionPolicy: { col: 'permission_policy', nullable: true },
  webhookSecret: { col: 'webhook_secret', nullable: true },
  modelProfile: { col: 'model_profile', nullable: true },
  createdAt: { col: 'created_at' },
  updatedAt: { col: 'updated_at' },
});

const rowToPersonaSummary = createRowMapper<PersonaSummary>({
  id: { col: 'id' },
  projectId: { col: 'project_id' },
  name: { col: 'name' },
  description: { col: 'description', nullable: true },
  icon: { col: 'icon', nullable: true },
  color: { col: 'color', nullable: true },
  enabled: { col: 'enabled', bool: true },
  groupId: { col: 'group_id', nullable: true },
});

// ---------------------------------------------------------------------------
// CRUD
// ---------------------------------------------------------------------------

export function getPersona(db: Database.Database, id: string): Persona | undefined {
  const row = stmt(db,'SELECT * FROM personas WHERE id = ?').get(id) as Record<string, unknown> | undefined;
  return row ? rowToPersona(row) : undefined;
}

/** Batch-fetch multiple personas by ID. Returns a Map keyed by persona ID. */
export function getPersonasByIds(db: Database.Database, ids: string[]): Map<string, Persona> {
  const map = new Map<string, Persona>();
  if (ids.length === 0) return map;
  const placeholders = ids.map(() => '?').join(',');
  const rows = db.prepare(`SELECT * FROM personas WHERE id IN (${placeholders})`).all(...ids) as Record<string, unknown>[];
  for (const row of rows) {
    const persona = rowToPersona(row);
    map.set(persona.id, persona);
  }
  return map;
}

export function listPersonas(db: Database.Database, projectId?: string): Persona[] {
  const { sql: where, params } = buildWhereClause({ projectId }, { projectId: 'project_id' });
  const rows = stmt(db, `SELECT * FROM personas ${where} ORDER BY name`).all(...params) as Record<string, unknown>[];
  return rows.map(rowToPersona);
}

const SUMMARY_COLS = 'id, project_id, name, description, icon, color, enabled, group_id';

export function listPersonasSummary(db: Database.Database, projectId?: string): PersonaSummary[] {
  const { sql: where, params } = buildWhereClause({ projectId }, { projectId: 'project_id' });
  const rows = stmt(db, `SELECT ${SUMMARY_COLS} FROM personas ${where} ORDER BY name`).all(...params) as Record<string, unknown>[];
  return rows.map(rowToPersonaSummary);
}

export function upsertPersona(db: Database.Database, p: Persona): void {
  stmt(db,`
    INSERT INTO personas (id, project_id, name, description, system_prompt, structured_prompt, icon, color, enabled, max_concurrent, timeout_ms, inference_profile_id, network_policy, max_budget_usd, max_turns, design_context, group_id, created_at, updated_at)
    VALUES (@id, @projectId, @name, @description, @systemPrompt, @structuredPrompt, @icon, @color, @enabled, @maxConcurrent, @timeoutMs, @inferenceProfileId, @networkPolicy, @maxBudgetUsd, @maxTurns, @designContext, @groupId, @createdAt, @updatedAt)
    ON CONFLICT(id) DO UPDATE SET
      name = excluded.name, description = excluded.description, system_prompt = excluded.system_prompt,
      structured_prompt = excluded.structured_prompt, icon = excluded.icon, color = excluded.color,
      enabled = excluded.enabled, max_concurrent = excluded.max_concurrent, timeout_ms = excluded.timeout_ms,
      inference_profile_id = excluded.inference_profile_id,
      network_policy = excluded.network_policy, max_budget_usd = excluded.max_budget_usd, max_turns = excluded.max_turns,
      design_context = excluded.design_context, group_id = excluded.group_id, updated_at = excluded.updated_at
  `).run({
    id: p.id,
    projectId: p.projectId,
    name: p.name,
    description: p.description,
    systemPrompt: p.systemPrompt,
    structuredPrompt: p.structuredPrompt,
    icon: p.icon,
    color: p.color,
    enabled: p.enabled ? 1 : 0,
    maxConcurrent: p.maxConcurrent,
    timeoutMs: p.timeoutMs,
    inferenceProfileId: p.inferenceProfileId,
    networkPolicy: p.networkPolicy,
    maxBudgetUsd: p.maxBudgetUsd,
    maxTurns: p.maxTurns,
    designContext: p.designContext,
    groupId: p.groupId,
    createdAt: p.createdAt,
    updatedAt: p.updatedAt,
  });
}

export function deletePersona(db: Database.Database, id: string, projectId?: string): void {
  if (projectId) {
    stmt(db,'DELETE FROM personas WHERE id = ? AND project_id = ?').run(id, projectId);
  } else {
    stmt(db,'DELETE FROM personas WHERE id = ?').run(id);
  }
}

/** Fetch persona + tools, credentials (redacted), subscriptions, triggers in one transaction. */
export function getHydratedPersona(database: Database.Database, id: string, logger?: Logger): HydratedPersona | undefined {
  const start = performance.now();

  const row = stmt(database,'SELECT * FROM personas WHERE id = ?').get(id) as Record<string, unknown> | undefined;
  if (!row) return undefined;

  const persona = rowToPersona(row);

  const toolRows = stmt(database,`
    SELECT t.* FROM persona_tool_definitions t
    JOIN persona_tools pt ON pt.tool_id = t.id
    WHERE pt.persona_id = ?
  `).all(id) as Record<string, unknown>[];

  const credRows = stmt(database,`
    SELECT c.id, c.project_id, c.name, c.service_type, c.metadata, c.last_used_at, c.created_at, c.updated_at
    FROM persona_credentials c
    JOIN persona_credential_links cl ON cl.credential_id = c.id
    WHERE cl.persona_id = ?
  `).all(id) as Record<string, unknown>[];

  const subRows = stmt(database,
    'SELECT * FROM persona_event_subscriptions WHERE persona_id = ?',
  ).all(id) as Record<string, unknown>[];

  const triggerRows = stmt(database,
    'SELECT * FROM persona_triggers WHERE persona_id = ?',
  ).all(id) as Record<string, unknown>[];

  const durationMs = performance.now() - start;

  if (logger) {
    logger.debug({ personaId: id, durationMs: Math.round(durationMs * 100) / 100 }, 'getHydratedPersona completed');
    if (durationMs > HYDRATION_WARN_THRESHOLD_MS) {
      logger.warn({ personaId: id, durationMs: Math.round(durationMs * 100) / 100, thresholdMs: HYDRATION_WARN_THRESHOLD_MS }, 'getHydratedPersona exceeded duration threshold');
    }
  }

  return {
    ...persona,
    tools: toolRows.map(rowToTool),
    credentials: credRows.map(rowToRedactedCredential),
    subscriptions: subRows.map(rowToSubscription),
    triggers: triggerRows.map(rowToTrigger),
  };
}

// ---------------------------------------------------------------------------
// Admin queries
// ---------------------------------------------------------------------------

export function listDistinctProjectIds(db: Database.Database): string[] {
  const rows = stmt(db,'SELECT DISTINCT project_id FROM personas ORDER BY project_id').all() as Array<{ project_id: string }>;
  return rows.map(r => r.project_id);
}
