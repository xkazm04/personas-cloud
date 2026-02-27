import Database from 'better-sqlite3';
import { nanoid } from 'nanoid';
import type { Logger } from 'pino';
import type {
  Persona,
  PersonaToolDefinition,
  PersonaCredential,
  PersonaEvent,
  PersonaEventSubscription,
  PersonaTrigger,
  PersonaExecution,
} from '@dac-cloud/shared';

// ---------------------------------------------------------------------------
// Initialization
// ---------------------------------------------------------------------------

export function initDb(dbPath: string, logger: Logger): Database.Database {
  const db = new Database(dbPath);
  db.pragma('journal_mode = WAL');
  db.pragma('foreign_keys = ON');

  db.exec(SCHEMA);
  runMigrations(db, logger);
  logger.info({ dbPath }, 'Database initialized');
  return db;
}

// ---------------------------------------------------------------------------
// Migrations
// ---------------------------------------------------------------------------

function runMigrations(db: Database.Database, logger: Logger): void {
  // Migration: add project_id column to tables that need per-user isolation
  const tablesToMigrate = [
    'persona_credentials',
    'persona_event_subscriptions',
    'persona_triggers',
    'persona_executions',
  ];

  for (const table of tablesToMigrate) {
    const columns = db.prepare(`PRAGMA table_info(${table})`).all() as Array<{ name: string }>;
    const hasProjectId = columns.some(c => c.name === 'project_id');
    if (!hasProjectId) {
      db.exec(`ALTER TABLE ${table} ADD COLUMN project_id TEXT NOT NULL DEFAULT 'default'`);
      logger.info({ table }, 'Migration: added project_id column');
    }
  }

  // Indexes for project_id
  db.exec(`
    CREATE INDEX IF NOT EXISTS idx_personas_project ON personas(project_id);
    CREATE INDEX IF NOT EXISTS idx_pe_project ON persona_events(project_id);
    CREATE INDEX IF NOT EXISTS idx_pcred_project ON persona_credentials(project_id);
    CREATE INDEX IF NOT EXISTS idx_pes_project ON persona_event_subscriptions(project_id);
    CREATE INDEX IF NOT EXISTS idx_pt_project ON persona_triggers(project_id);
    CREATE INDEX IF NOT EXISTS idx_pex_project ON persona_executions(project_id);
  `);
}

// ---------------------------------------------------------------------------
// Schema
// ---------------------------------------------------------------------------

const SCHEMA = `
CREATE TABLE IF NOT EXISTS personas (
  id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL DEFAULT 'default',
  name TEXT NOT NULL,
  description TEXT,
  system_prompt TEXT NOT NULL DEFAULT '',
  structured_prompt TEXT,
  icon TEXT,
  color TEXT,
  enabled INTEGER NOT NULL DEFAULT 1,
  max_concurrent INTEGER NOT NULL DEFAULT 1,
  timeout_ms INTEGER NOT NULL DEFAULT 300000,
  model_profile TEXT,
  max_budget_usd REAL,
  max_turns INTEGER,
  design_context TEXT,
  group_id TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS persona_tool_definitions (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL UNIQUE,
  category TEXT NOT NULL,
  description TEXT NOT NULL,
  script_path TEXT NOT NULL DEFAULT '',
  input_schema TEXT,
  output_schema TEXT,
  requires_credential_type TEXT,
  implementation_guide TEXT,
  is_builtin INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS persona_tools (
  persona_id TEXT NOT NULL REFERENCES personas(id) ON DELETE CASCADE,
  tool_id TEXT NOT NULL REFERENCES persona_tool_definitions(id) ON DELETE CASCADE,
  PRIMARY KEY (persona_id, tool_id)
);

CREATE TABLE IF NOT EXISTS persona_credentials (
  id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL DEFAULT 'default',
  name TEXT NOT NULL,
  service_type TEXT NOT NULL,
  encrypted_data TEXT NOT NULL,
  iv TEXT NOT NULL,
  tag TEXT NOT NULL,
  metadata TEXT,
  last_used_at TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS persona_credential_links (
  persona_id TEXT NOT NULL REFERENCES personas(id) ON DELETE CASCADE,
  credential_id TEXT NOT NULL REFERENCES persona_credentials(id) ON DELETE CASCADE,
  PRIMARY KEY (persona_id, credential_id)
);

CREATE TABLE IF NOT EXISTS persona_events (
  id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL DEFAULT 'default',
  event_type TEXT NOT NULL,
  source_type TEXT NOT NULL,
  source_id TEXT,
  target_persona_id TEXT,
  payload TEXT,
  status TEXT NOT NULL DEFAULT 'pending',
  error_message TEXT,
  processed_at TEXT,
  use_case_id TEXT,
  created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_pe_status ON persona_events(status);

CREATE TABLE IF NOT EXISTS persona_event_subscriptions (
  id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL DEFAULT 'default',
  persona_id TEXT NOT NULL REFERENCES personas(id) ON DELETE CASCADE,
  event_type TEXT NOT NULL,
  source_filter TEXT,
  enabled INTEGER NOT NULL DEFAULT 1,
  use_case_id TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS persona_triggers (
  id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL DEFAULT 'default',
  persona_id TEXT NOT NULL REFERENCES personas(id) ON DELETE CASCADE,
  trigger_type TEXT NOT NULL CHECK(trigger_type IN ('manual','schedule','polling','webhook','chain')),
  config TEXT,
  enabled INTEGER NOT NULL DEFAULT 1,
  last_triggered_at TEXT,
  next_trigger_at TEXT,
  use_case_id TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS persona_executions (
  id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL DEFAULT 'default',
  persona_id TEXT NOT NULL,
  trigger_id TEXT,
  use_case_id TEXT,
  status TEXT NOT NULL DEFAULT 'queued' CHECK(status IN ('queued','running','completed','failed','cancelled')),
  input_data TEXT,
  output_data TEXT,
  claude_session_id TEXT,
  model_used TEXT,
  input_tokens INTEGER DEFAULT 0,
  output_tokens INTEGER DEFAULT 0,
  cost_usd REAL DEFAULT 0,
  error_message TEXT,
  duration_ms INTEGER,
  retry_of_execution_id TEXT,
  retry_count INTEGER DEFAULT 0,
  started_at TEXT,
  completed_at TEXT,
  created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_pex_persona ON persona_executions(persona_id);
CREATE INDEX IF NOT EXISTS idx_pex_status ON persona_executions(status);
`;

// ---------------------------------------------------------------------------
// Row → domain type mappers
// ---------------------------------------------------------------------------

function rowToPersona(row: Record<string, unknown>): Persona {
  return {
    id: row['id'] as string,
    projectId: row['project_id'] as string,
    name: row['name'] as string,
    description: (row['description'] as string) ?? null,
    systemPrompt: row['system_prompt'] as string,
    structuredPrompt: (row['structured_prompt'] as string) ?? null,
    icon: (row['icon'] as string) ?? null,
    color: (row['color'] as string) ?? null,
    enabled: row['enabled'] === 1,
    maxConcurrent: row['max_concurrent'] as number,
    timeoutMs: row['timeout_ms'] as number,
    modelProfile: (row['model_profile'] as string) ?? null,
    maxBudgetUsd: (row['max_budget_usd'] as number) ?? null,
    maxTurns: (row['max_turns'] as number) ?? null,
    designContext: (row['design_context'] as string) ?? null,
    groupId: (row['group_id'] as string) ?? null,
    createdAt: row['created_at'] as string,
    updatedAt: row['updated_at'] as string,
  };
}

function rowToTool(row: Record<string, unknown>): PersonaToolDefinition {
  return {
    id: row['id'] as string,
    name: row['name'] as string,
    category: row['category'] as string,
    description: row['description'] as string,
    scriptPath: row['script_path'] as string,
    inputSchema: (row['input_schema'] as string) ?? null,
    outputSchema: (row['output_schema'] as string) ?? null,
    requiresCredentialType: (row['requires_credential_type'] as string) ?? null,
    implementationGuide: (row['implementation_guide'] as string) ?? null,
    isBuiltin: row['is_builtin'] === 1,
    createdAt: row['created_at'] as string,
    updatedAt: row['updated_at'] as string,
  };
}

function rowToCredential(row: Record<string, unknown>): PersonaCredential {
  return {
    id: row['id'] as string,
    projectId: (row['project_id'] as string) ?? 'default',
    name: row['name'] as string,
    serviceType: row['service_type'] as string,
    encryptedData: row['encrypted_data'] as string,
    iv: row['iv'] as string,
    tag: row['tag'] as string,
    metadata: (row['metadata'] as string) ?? null,
    lastUsedAt: (row['last_used_at'] as string) ?? null,
    createdAt: row['created_at'] as string,
    updatedAt: row['updated_at'] as string,
  };
}

function rowToEvent(row: Record<string, unknown>): PersonaEvent {
  return {
    id: row['id'] as string,
    projectId: row['project_id'] as string,
    eventType: row['event_type'] as string,
    sourceType: row['source_type'] as string,
    sourceId: (row['source_id'] as string) ?? null,
    targetPersonaId: (row['target_persona_id'] as string) ?? null,
    payload: (row['payload'] as string) ?? null,
    status: row['status'] as string,
    errorMessage: (row['error_message'] as string) ?? null,
    processedAt: (row['processed_at'] as string) ?? null,
    useCaseId: (row['use_case_id'] as string) ?? null,
    createdAt: row['created_at'] as string,
  };
}

function rowToSubscription(row: Record<string, unknown>): PersonaEventSubscription {
  return {
    id: row['id'] as string,
    personaId: row['persona_id'] as string,
    eventType: row['event_type'] as string,
    sourceFilter: (row['source_filter'] as string) ?? null,
    enabled: row['enabled'] === 1,
    useCaseId: (row['use_case_id'] as string) ?? null,
    createdAt: row['created_at'] as string,
    updatedAt: row['updated_at'] as string,
  };
}

function rowToTrigger(row: Record<string, unknown>): PersonaTrigger {
  return {
    id: row['id'] as string,
    personaId: row['persona_id'] as string,
    triggerType: row['trigger_type'] as string,
    config: (row['config'] as string) ?? null,
    enabled: row['enabled'] === 1,
    lastTriggeredAt: (row['last_triggered_at'] as string) ?? null,
    nextTriggerAt: (row['next_trigger_at'] as string) ?? null,
    useCaseId: (row['use_case_id'] as string) ?? null,
    createdAt: row['created_at'] as string,
    updatedAt: row['updated_at'] as string,
  };
}

function rowToExecution(row: Record<string, unknown>): PersonaExecution {
  return {
    id: row['id'] as string,
    projectId: (row['project_id'] as string) ?? 'default',
    personaId: row['persona_id'] as string,
    triggerId: (row['trigger_id'] as string) ?? null,
    useCaseId: (row['use_case_id'] as string) ?? null,
    status: row['status'] as string,
    inputData: (row['input_data'] as string) ?? null,
    outputData: (row['output_data'] as string) ?? null,
    claudeSessionId: (row['claude_session_id'] as string) ?? null,
    modelUsed: (row['model_used'] as string) ?? null,
    inputTokens: (row['input_tokens'] as number) ?? 0,
    outputTokens: (row['output_tokens'] as number) ?? 0,
    costUsd: (row['cost_usd'] as number) ?? 0,
    errorMessage: (row['error_message'] as string) ?? null,
    durationMs: (row['duration_ms'] as number) ?? null,
    retryOfExecutionId: (row['retry_of_execution_id'] as string) ?? null,
    retryCount: (row['retry_count'] as number) ?? 0,
    startedAt: (row['started_at'] as string) ?? null,
    completedAt: (row['completed_at'] as string) ?? null,
    createdAt: row['created_at'] as string,
  };
}

// ---------------------------------------------------------------------------
// Persona CRUD
// ---------------------------------------------------------------------------

export function getPersona(db: Database.Database, id: string): Persona | undefined {
  const row = db.prepare('SELECT * FROM personas WHERE id = ?').get(id) as Record<string, unknown> | undefined;
  return row ? rowToPersona(row) : undefined;
}

export function listPersonas(db: Database.Database, projectId?: string): Persona[] {
  if (projectId) {
    const rows = db.prepare('SELECT * FROM personas WHERE project_id = ? ORDER BY name').all(projectId) as Record<string, unknown>[];
    return rows.map(rowToPersona);
  }
  const rows = db.prepare('SELECT * FROM personas ORDER BY name').all() as Record<string, unknown>[];
  return rows.map(rowToPersona);
}

export function upsertPersona(db: Database.Database, p: Persona): void {
  db.prepare(`
    INSERT INTO personas (id, project_id, name, description, system_prompt, structured_prompt, icon, color, enabled, max_concurrent, timeout_ms, model_profile, max_budget_usd, max_turns, design_context, group_id, created_at, updated_at)
    VALUES (@id, @projectId, @name, @description, @systemPrompt, @structuredPrompt, @icon, @color, @enabled, @maxConcurrent, @timeoutMs, @modelProfile, @maxBudgetUsd, @maxTurns, @designContext, @groupId, @createdAt, @updatedAt)
    ON CONFLICT(id) DO UPDATE SET
      name = excluded.name, description = excluded.description, system_prompt = excluded.system_prompt,
      structured_prompt = excluded.structured_prompt, icon = excluded.icon, color = excluded.color,
      enabled = excluded.enabled, max_concurrent = excluded.max_concurrent, timeout_ms = excluded.timeout_ms,
      model_profile = excluded.model_profile, max_budget_usd = excluded.max_budget_usd, max_turns = excluded.max_turns,
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
    modelProfile: p.modelProfile,
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
    db.prepare('DELETE FROM personas WHERE id = ? AND project_id = ?').run(id, projectId);
  } else {
    db.prepare('DELETE FROM personas WHERE id = ?').run(id);
  }
}

// ---------------------------------------------------------------------------
// Tool Definitions
// ---------------------------------------------------------------------------

export function getToolsForPersona(db: Database.Database, personaId: string): PersonaToolDefinition[] {
  const rows = db.prepare(`
    SELECT t.* FROM persona_tool_definitions t
    JOIN persona_tools pt ON pt.tool_id = t.id
    WHERE pt.persona_id = ?
  `).all(personaId) as Record<string, unknown>[];
  return rows.map(rowToTool);
}

export function upsertToolDefinition(db: Database.Database, t: PersonaToolDefinition): void {
  db.prepare(`
    INSERT INTO persona_tool_definitions (id, name, category, description, script_path, input_schema, output_schema, requires_credential_type, implementation_guide, is_builtin, created_at, updated_at)
    VALUES (@id, @name, @category, @description, @scriptPath, @inputSchema, @outputSchema, @requiresCredentialType, @implementationGuide, @isBuiltin, @createdAt, @updatedAt)
    ON CONFLICT(id) DO UPDATE SET
      name = excluded.name, category = excluded.category, description = excluded.description,
      script_path = excluded.script_path, input_schema = excluded.input_schema, output_schema = excluded.output_schema,
      requires_credential_type = excluded.requires_credential_type, implementation_guide = excluded.implementation_guide,
      is_builtin = excluded.is_builtin, updated_at = excluded.updated_at
  `).run({
    id: t.id,
    name: t.name,
    category: t.category,
    description: t.description,
    scriptPath: t.scriptPath,
    inputSchema: t.inputSchema,
    outputSchema: t.outputSchema,
    requiresCredentialType: t.requiresCredentialType,
    implementationGuide: t.implementationGuide,
    isBuiltin: t.isBuiltin ? 1 : 0,
    createdAt: t.createdAt,
    updatedAt: t.updatedAt,
  });
}

export function linkTool(db: Database.Database, personaId: string, toolId: string): void {
  db.prepare('INSERT OR IGNORE INTO persona_tools (persona_id, tool_id) VALUES (?, ?)').run(personaId, toolId);
}

export function unlinkTool(db: Database.Database, personaId: string, toolId: string): void {
  db.prepare('DELETE FROM persona_tools WHERE persona_id = ? AND tool_id = ?').run(personaId, toolId);
}

// ---------------------------------------------------------------------------
// Credentials
// ---------------------------------------------------------------------------

export function createCredential(db: Database.Database, input: {
  name: string; serviceType: string; encryptedData: string; iv: string; tag: string; metadata?: string | null; projectId?: string;
}): PersonaCredential {
  const now = new Date().toISOString();
  const id = nanoid();
  db.prepare(`
    INSERT INTO persona_credentials (id, project_id, name, service_type, encrypted_data, iv, tag, metadata, created_at, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `).run(id, input.projectId ?? 'default', input.name, input.serviceType, input.encryptedData, input.iv, input.tag, input.metadata ?? null, now, now);
  return getCredential(db, id)!;
}

export function getCredential(db: Database.Database, id: string): PersonaCredential | undefined {
  const row = db.prepare('SELECT * FROM persona_credentials WHERE id = ?').get(id) as Record<string, unknown> | undefined;
  return row ? rowToCredential(row) : undefined;
}

export function listCredentialsForPersona(db: Database.Database, personaId: string): PersonaCredential[] {
  const rows = db.prepare(`
    SELECT c.* FROM persona_credentials c
    JOIN persona_credential_links cl ON cl.credential_id = c.id
    WHERE cl.persona_id = ?
  `).all(personaId) as Record<string, unknown>[];
  return rows.map(rowToCredential);
}

export function linkCredential(db: Database.Database, personaId: string, credentialId: string): void {
  db.prepare('INSERT OR IGNORE INTO persona_credential_links (persona_id, credential_id) VALUES (?, ?)').run(personaId, credentialId);
}

export function deleteCredential(db: Database.Database, id: string): void {
  db.prepare('DELETE FROM persona_credentials WHERE id = ?').run(id);
}

// ---------------------------------------------------------------------------
// Events
// ---------------------------------------------------------------------------

export function publishEvent(db: Database.Database, input: {
  eventType: string; sourceType: string; sourceId?: string | null; targetPersonaId?: string | null;
  projectId?: string; payload?: string | null; useCaseId?: string | null;
}): PersonaEvent {
  const now = new Date().toISOString();
  const id = nanoid();
  db.prepare(`
    INSERT INTO persona_events (id, project_id, event_type, source_type, source_id, target_persona_id, payload, use_case_id, status, created_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?)
  `).run(id, input.projectId ?? 'default', input.eventType, input.sourceType, input.sourceId ?? null, input.targetPersonaId ?? null, input.payload ?? null, input.useCaseId ?? null, now);
  return getEvent(db, id)!;
}

export function getEvent(db: Database.Database, id: string): PersonaEvent | undefined {
  const row = db.prepare('SELECT * FROM persona_events WHERE id = ?').get(id) as Record<string, unknown> | undefined;
  return row ? rowToEvent(row) : undefined;
}

export function getPendingEvents(db: Database.Database, limit: number = 50): PersonaEvent[] {
  const rows = db.prepare("SELECT * FROM persona_events WHERE status = 'pending' ORDER BY created_at LIMIT ?").all(limit) as Record<string, unknown>[];
  return rows.map(rowToEvent);
}

export function listEvents(db: Database.Database, opts?: {
  eventType?: string; status?: string; projectId?: string; limit?: number; offset?: number;
}): PersonaEvent[] {
  const clauses: string[] = [];
  const params: unknown[] = [];
  if (opts?.eventType) { clauses.push('event_type = ?'); params.push(opts.eventType); }
  if (opts?.status) { clauses.push('status = ?'); params.push(opts.status); }
  if (opts?.projectId) { clauses.push('project_id = ?'); params.push(opts.projectId); }
  const where = clauses.length > 0 ? `WHERE ${clauses.join(' AND ')}` : '';
  const limit = opts?.limit ?? 50;
  const offset = opts?.offset ?? 0;
  const rows = db.prepare(`SELECT * FROM persona_events ${where} ORDER BY created_at DESC LIMIT ? OFFSET ?`).all(...params, limit, offset) as Record<string, unknown>[];
  return rows.map(rowToEvent);
}

export function updateEventStatus(db: Database.Database, id: string, status: string, errorMessage?: string | null): void {
  const now = new Date().toISOString();
  db.prepare('UPDATE persona_events SET status = ?, error_message = ?, processed_at = ? WHERE id = ?').run(status, errorMessage ?? null, now, id);
}

export function updateEventWithMetadata(db: Database.Database, id: string, status: string, metadata?: string | null): PersonaEvent | undefined {
  const now = new Date().toISOString();
  db.prepare('UPDATE persona_events SET status = ?, processed_at = ?, payload = CASE WHEN ? IS NOT NULL THEN ? ELSE payload END WHERE id = ?')
    .run(status, now, metadata, metadata, id);
  return getEvent(db, id);
}

// ---------------------------------------------------------------------------
// Event Subscriptions
// ---------------------------------------------------------------------------

export function createSubscription(db: Database.Database, input: {
  personaId: string; eventType: string; sourceFilter?: string | null; enabled?: boolean; useCaseId?: string | null; projectId?: string;
}): PersonaEventSubscription {
  const now = new Date().toISOString();
  const id = nanoid();
  db.prepare(`
    INSERT INTO persona_event_subscriptions (id, project_id, persona_id, event_type, source_filter, enabled, use_case_id, created_at, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
  `).run(id, input.projectId ?? 'default', input.personaId, input.eventType, input.sourceFilter ?? null, input.enabled !== false ? 1 : 0, input.useCaseId ?? null, now, now);
  return getSubscription(db, id)!;
}

export function getSubscription(db: Database.Database, id: string): PersonaEventSubscription | undefined {
  const row = db.prepare('SELECT * FROM persona_event_subscriptions WHERE id = ?').get(id) as Record<string, unknown> | undefined;
  return row ? rowToSubscription(row) : undefined;
}

export function listSubscriptionsForPersona(db: Database.Database, personaId: string): PersonaEventSubscription[] {
  const rows = db.prepare('SELECT * FROM persona_event_subscriptions WHERE persona_id = ?').all(personaId) as Record<string, unknown>[];
  return rows.map(rowToSubscription);
}

export function getSubscriptionsByEventType(db: Database.Database, eventType: string, projectId?: string): PersonaEventSubscription[] {
  if (projectId) {
    const rows = db.prepare('SELECT * FROM persona_event_subscriptions WHERE event_type = ? AND project_id = ?').all(eventType, projectId) as Record<string, unknown>[];
    return rows.map(rowToSubscription);
  }
  const rows = db.prepare('SELECT * FROM persona_event_subscriptions WHERE event_type = ?').all(eventType) as Record<string, unknown>[];
  return rows.map(rowToSubscription);
}

export function updateSubscription(db: Database.Database, id: string, updates: {
  eventType?: string | null; sourceFilter?: string | null; enabled?: boolean | null;
}): void {
  const now = new Date().toISOString();
  const sub = getSubscription(db, id);
  if (!sub) return;
  db.prepare(`
    UPDATE persona_event_subscriptions SET event_type = ?, source_filter = ?, enabled = ?, updated_at = ? WHERE id = ?
  `).run(
    updates.eventType ?? sub.eventType,
    updates.sourceFilter !== undefined ? updates.sourceFilter : sub.sourceFilter,
    updates.enabled !== undefined && updates.enabled !== null ? (updates.enabled ? 1 : 0) : (sub.enabled ? 1 : 0),
    now,
    id,
  );
}

export function deleteSubscription(db: Database.Database, id: string): void {
  db.prepare('DELETE FROM persona_event_subscriptions WHERE id = ?').run(id);
}

// ---------------------------------------------------------------------------
// Triggers
// ---------------------------------------------------------------------------

export function createTrigger(db: Database.Database, input: {
  personaId: string; triggerType: string; config?: string | null; enabled?: boolean; useCaseId?: string | null; projectId?: string;
}): PersonaTrigger {
  const now = new Date().toISOString();
  const id = nanoid();
  db.prepare(`
    INSERT INTO persona_triggers (id, project_id, persona_id, trigger_type, config, enabled, use_case_id, created_at, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
  `).run(id, input.projectId ?? 'default', input.personaId, input.triggerType, input.config ?? null, input.enabled !== false ? 1 : 0, input.useCaseId ?? null, now, now);
  return getTrigger(db, id)!;
}

export function getTrigger(db: Database.Database, id: string): PersonaTrigger | undefined {
  const row = db.prepare('SELECT * FROM persona_triggers WHERE id = ?').get(id) as Record<string, unknown> | undefined;
  return row ? rowToTrigger(row) : undefined;
}

export function listTriggersForPersona(db: Database.Database, personaId: string): PersonaTrigger[] {
  const rows = db.prepare('SELECT * FROM persona_triggers WHERE persona_id = ?').all(personaId) as Record<string, unknown>[];
  return rows.map(rowToTrigger);
}

export function getDueTriggers(db: Database.Database): PersonaTrigger[] {
  const rows = db.prepare("SELECT * FROM persona_triggers WHERE enabled = 1 AND next_trigger_at <= datetime('now')").all() as Record<string, unknown>[];
  return rows.map(rowToTrigger);
}

export function updateTriggerTimings(db: Database.Database, id: string, lastTriggeredAt: string, nextTriggerAt: string | null): void {
  db.prepare('UPDATE persona_triggers SET last_triggered_at = ?, next_trigger_at = ?, updated_at = ? WHERE id = ?')
    .run(lastTriggeredAt, nextTriggerAt, new Date().toISOString(), id);
}

export function updateTrigger(db: Database.Database, id: string, updates: {
  triggerType?: string | null; config?: string | null; enabled?: boolean | null;
}): void {
  const now = new Date().toISOString();
  const trigger = getTrigger(db, id);
  if (!trigger) return;
  db.prepare(`
    UPDATE persona_triggers SET trigger_type = ?, config = ?, enabled = ?, updated_at = ? WHERE id = ?
  `).run(
    updates.triggerType ?? trigger.triggerType,
    updates.config !== undefined ? updates.config : trigger.config,
    updates.enabled !== undefined && updates.enabled !== null ? (updates.enabled ? 1 : 0) : (trigger.enabled ? 1 : 0),
    now,
    id,
  );
}

export function deleteTrigger(db: Database.Database, id: string): void {
  db.prepare('DELETE FROM persona_triggers WHERE id = ?').run(id);
}

// ---------------------------------------------------------------------------
// Executions
// ---------------------------------------------------------------------------

export function createExecution(db: Database.Database, input: {
  id?: string; personaId: string; triggerId?: string | null; useCaseId?: string | null; inputData?: string | null; projectId?: string;
}): PersonaExecution {
  const now = new Date().toISOString();
  const id = input.id ?? nanoid();
  db.prepare(`
    INSERT INTO persona_executions (id, project_id, persona_id, trigger_id, use_case_id, status, input_data, created_at)
    VALUES (?, ?, ?, ?, ?, 'queued', ?, ?)
  `).run(id, input.projectId ?? 'default', input.personaId, input.triggerId ?? null, input.useCaseId ?? null, input.inputData ?? null, now);
  return getExecution(db, id)!;
}

export function getExecution(db: Database.Database, id: string): PersonaExecution | undefined {
  const row = db.prepare('SELECT * FROM persona_executions WHERE id = ?').get(id) as Record<string, unknown> | undefined;
  return row ? rowToExecution(row) : undefined;
}

export function listExecutions(db: Database.Database, opts?: {
  personaId?: string; status?: string; limit?: number; offset?: number; projectId?: string;
}): PersonaExecution[] {
  let sql = 'SELECT * FROM persona_executions WHERE 1=1';
  const params: unknown[] = [];

  if (opts?.projectId) {
    sql += ' AND project_id = ?';
    params.push(opts.projectId);
  }
  if (opts?.personaId) {
    sql += ' AND persona_id = ?';
    params.push(opts.personaId);
  }
  if (opts?.status) {
    sql += ' AND status = ?';
    params.push(opts.status);
  }
  sql += ' ORDER BY created_at DESC';
  if (opts?.limit) {
    sql += ' LIMIT ?';
    params.push(opts.limit);
  }
  if (opts?.offset) {
    sql += ' OFFSET ?';
    params.push(opts.offset);
  }

  const rows = db.prepare(sql).all(...params) as Record<string, unknown>[];
  return rows.map(rowToExecution);
}

export function updateExecution(db: Database.Database, id: string, updates: Record<string, unknown>): void {
  const fields: string[] = [];
  const values: unknown[] = [];

  const columnMap: Record<string, string> = {
    status: 'status',
    outputData: 'output_data',
    claudeSessionId: 'claude_session_id',
    modelUsed: 'model_used',
    inputTokens: 'input_tokens',
    outputTokens: 'output_tokens',
    costUsd: 'cost_usd',
    errorMessage: 'error_message',
    durationMs: 'duration_ms',
    startedAt: 'started_at',
    completedAt: 'completed_at',
  };

  for (const [key, col] of Object.entries(columnMap)) {
    if (key in updates) {
      fields.push(`${col} = ?`);
      values.push(updates[key]);
    }
  }

  if (fields.length === 0) return;
  values.push(id);
  db.prepare(`UPDATE persona_executions SET ${fields.join(', ')} WHERE id = ?`).run(...values);
}

export function appendOutput(db: Database.Database, id: string, chunk: string): void {
  db.prepare(`
    UPDATE persona_executions SET output_data = COALESCE(output_data, '') || ? WHERE id = ?
  `).run(chunk, id);
}

export function countRunningExecutions(db: Database.Database, personaId: string): number {
  const row = db.prepare("SELECT COUNT(*) as cnt FROM persona_executions WHERE persona_id = ? AND status = 'running'").get(personaId) as { cnt: number };
  return row.cnt;
}

// ---------------------------------------------------------------------------
// Admin queries
// ---------------------------------------------------------------------------

export function listDistinctProjectIds(db: Database.Database): string[] {
  const rows = db.prepare('SELECT DISTINCT project_id FROM personas ORDER BY project_id').all() as Array<{ project_id: string }>;
  return rows.map(r => r.project_id);
}
