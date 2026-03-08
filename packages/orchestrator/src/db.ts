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
  CloudDeployment,
  TriggerFiring,
} from '@dac-cloud/shared';
import { validatePayloadFilter, compilePayloadFilter } from '@dac-cloud/shared';

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

/** Current migration version. Increment when adding new migrations. */
const LATEST_MIGRATION_VERSION = 9;

type Migration = { version: number; description: string; up: (db: Database.Database) => void };

const migrations: Migration[] = [
  {
    version: 1,
    description: 'Add project_id to credentials, subscriptions, triggers, executions',
    up: (db) => {
      for (const table of ['persona_credentials', 'persona_event_subscriptions', 'persona_triggers', 'persona_executions']) {
        db.exec(`ALTER TABLE ${table} ADD COLUMN project_id TEXT NOT NULL DEFAULT 'default'`);
      }
      db.exec(`
        CREATE INDEX IF NOT EXISTS idx_personas_project ON personas(project_id);
        CREATE INDEX IF NOT EXISTS idx_pe_project ON persona_events(project_id);
        CREATE INDEX IF NOT EXISTS idx_pcred_project ON persona_credentials(project_id);
        CREATE INDEX IF NOT EXISTS idx_pes_project ON persona_event_subscriptions(project_id);
        CREATE INDEX IF NOT EXISTS idx_pt_project ON persona_triggers(project_id);
        CREATE INDEX IF NOT EXISTS idx_pex_project ON persona_executions(project_id);
      `);
    },
  },
  {
    version: 2,
    description: 'Add permission_policy to personas',
    up: (db) => { db.exec("ALTER TABLE personas ADD COLUMN permission_policy TEXT"); },
  },
  {
    version: 3,
    description: 'Add webhook_secret to personas',
    up: (db) => { db.exec("ALTER TABLE personas ADD COLUMN webhook_secret TEXT"); },
  },
  {
    version: 4,
    description: 'Add salt and iter to persona_credentials',
    up: (db) => {
      db.exec("ALTER TABLE persona_credentials ADD COLUMN salt TEXT");
      db.exec("ALTER TABLE persona_credentials ADD COLUMN iter INTEGER");
    },
  },
  {
    version: 5,
    description: 'Add payload_filter to persona_event_subscriptions',
    up: (db) => { db.exec("ALTER TABLE persona_event_subscriptions ADD COLUMN payload_filter TEXT"); },
  },
  {
    version: 6,
    description: 'Add retry_count and next_retry_at to persona_events',
    up: (db) => {
      db.exec("ALTER TABLE persona_events ADD COLUMN retry_count INTEGER NOT NULL DEFAULT 0");
      db.exec("ALTER TABLE persona_events ADD COLUMN next_retry_at TEXT");
      db.exec("CREATE INDEX IF NOT EXISTS idx_pe_retry ON persona_events(status, next_retry_at)");
    },
  },
  {
    version: 7,
    description: 'Add max_retries and retry_backoff_ms to persona_event_subscriptions',
    up: (db) => {
      db.exec("ALTER TABLE persona_event_subscriptions ADD COLUMN max_retries INTEGER NOT NULL DEFAULT 3");
      db.exec("ALTER TABLE persona_event_subscriptions ADD COLUMN retry_backoff_ms INTEGER NOT NULL DEFAULT 5000");
    },
  },
  {
    version: 8,
    description: 'Add queue_data to persona_executions',
    up: (db) => { db.exec("ALTER TABLE persona_executions ADD COLUMN queue_data TEXT"); },
  },
  {
    version: 9,
    description: 'Add health_status and health_message to persona_triggers',
    up: (db) => {
      db.exec("ALTER TABLE persona_triggers ADD COLUMN health_status TEXT NOT NULL DEFAULT 'healthy'");
      db.exec("ALTER TABLE persona_triggers ADD COLUMN health_message TEXT");
    },
  },
  {
    version: 10,
    description: 'Add budget tracking columns to cloud_deployments',
    up: (db) => {
      db.exec("ALTER TABLE cloud_deployments ADD COLUMN max_monthly_budget_usd REAL");
      db.exec("ALTER TABLE cloud_deployments ADD COLUMN current_month_cost_usd REAL NOT NULL DEFAULT 0");
      db.exec("ALTER TABLE cloud_deployments ADD COLUMN budget_month TEXT");
    },
  },
];

function getMigrationVersion(db: Database.Database): number {
  // Check if migration_version table exists
  const tableExists = db.prepare(
    "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'migration_version'"
  ).get();

  if (!tableExists) {
    db.exec("CREATE TABLE migration_version (version INTEGER NOT NULL)");
    // Bootstrap: detect current state from existing schema to avoid re-running migrations
    // on databases that were created before the version table existed.
    const columns = db.prepare("PRAGMA table_info(persona_triggers)").all() as Array<{ name: string }>;
    const hasHealthStatus = columns.some(c => c.name === 'health_status');
    if (hasHealthStatus) {
      // All migrations already applied — stamp at latest
      db.prepare("INSERT INTO migration_version (version) VALUES (?)").run(LATEST_MIGRATION_VERSION);
      return LATEST_MIGRATION_VERSION;
    }
    // Check how far existing migrations got by inspecting a mid-range column
    const execCols = db.prepare("PRAGMA table_info(persona_executions)").all() as Array<{ name: string }>;
    const hasQueueData = execCols.some(c => c.name === 'queue_data');
    if (hasQueueData) {
      db.prepare("INSERT INTO migration_version (version) VALUES (?)").run(8);
      return 8;
    }
    const subCols = db.prepare("PRAGMA table_info(persona_event_subscriptions)").all() as Array<{ name: string }>;
    const hasMaxRetries = subCols.some(c => c.name === 'max_retries');
    if (hasMaxRetries) {
      db.prepare("INSERT INTO migration_version (version) VALUES (?)").run(7);
      return 7;
    }
    const evtCols = db.prepare("PRAGMA table_info(persona_events)").all() as Array<{ name: string }>;
    const hasRetryCount = evtCols.some(c => c.name === 'retry_count');
    if (hasRetryCount) {
      db.prepare("INSERT INTO migration_version (version) VALUES (?)").run(6);
      return 6;
    }
    const hasPayloadFilter = subCols.some(c => c.name === 'payload_filter');
    if (hasPayloadFilter) {
      db.prepare("INSERT INTO migration_version (version) VALUES (?)").run(5);
      return 5;
    }
    const credCols = db.prepare("PRAGMA table_info(persona_credentials)").all() as Array<{ name: string }>;
    const hasSalt = credCols.some(c => c.name === 'salt');
    if (hasSalt) {
      db.prepare("INSERT INTO migration_version (version) VALUES (?)").run(4);
      return 4;
    }
    const personaCols = db.prepare("PRAGMA table_info(personas)").all() as Array<{ name: string }>;
    const hasWebhook = personaCols.some(c => c.name === 'webhook_secret');
    if (hasWebhook) {
      db.prepare("INSERT INTO migration_version (version) VALUES (?)").run(3);
      return 3;
    }
    const hasPerm = personaCols.some(c => c.name === 'permission_policy');
    if (hasPerm) {
      db.prepare("INSERT INTO migration_version (version) VALUES (?)").run(2);
      return 2;
    }
    const hasProjectId = execCols.some(c => c.name === 'project_id');
    if (hasProjectId) {
      db.prepare("INSERT INTO migration_version (version) VALUES (?)").run(1);
      return 1;
    }
    // Brand new database — no migrations applied yet
    db.prepare("INSERT INTO migration_version (version) VALUES (?)").run(0);
    return 0;
  }

  const row = db.prepare("SELECT version FROM migration_version").get() as { version: number } | undefined;
  return row?.version ?? 0;
}

function setMigrationVersion(db: Database.Database, version: number): void {
  db.prepare("UPDATE migration_version SET version = ?").run(version);
}

function runMigrations(db: Database.Database, logger: Logger): void {
  const currentVersion = getMigrationVersion(db);

  if (currentVersion >= LATEST_MIGRATION_VERSION) {
    logger.debug({ currentVersion }, 'Database schema is up to date');
    return;
  }

  const pending = migrations.filter(m => m.version > currentVersion);
  logger.info({ currentVersion, targetVersion: LATEST_MIGRATION_VERSION, pendingCount: pending.length }, 'Running database migrations');

  // Run all pending migrations in a single transaction for atomicity.
  // If any migration fails, the entire batch is rolled back.
  const applyAll = db.transaction(() => {
    for (const migration of pending) {
      migration.up(db);
      setMigrationVersion(db, migration.version);
      logger.info({ version: migration.version, description: migration.description }, 'Migration applied');
    }
  });

  try {
    applyAll();
  } catch (err) {
    logger.error({ err }, 'Migration batch failed — all changes rolled back');
    throw err;
  }
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
  salt TEXT,
  iter INTEGER,
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
  health_status TEXT NOT NULL DEFAULT 'healthy',
  health_message TEXT,
  use_case_id TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS persona_event_dispatches (
  event_id TEXT NOT NULL,
  persona_id TEXT NOT NULL,
  execution_id TEXT NOT NULL,
  created_at TEXT NOT NULL,
  PRIMARY KEY (event_id, persona_id)
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

CREATE TABLE IF NOT EXISTS event_audit_log (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  event_id TEXT NOT NULL,
  action TEXT NOT NULL,
  subscription_id TEXT,
  persona_id TEXT,
  execution_id TEXT,
  detail TEXT,
  created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_eal_event ON event_audit_log(event_id);

CREATE TABLE IF NOT EXISTS cloud_deployments (
  id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL DEFAULT 'default',
  persona_id TEXT NOT NULL REFERENCES personas(id) ON DELETE CASCADE,
  slug TEXT NOT NULL UNIQUE,
  label TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'active' CHECK(status IN ('active','paused','failed')),
  webhook_enabled INTEGER NOT NULL DEFAULT 1,
  webhook_secret TEXT,
  invocation_count INTEGER NOT NULL DEFAULT 0,
  last_invoked_at TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_cd_project ON cloud_deployments(project_id);
CREATE INDEX IF NOT EXISTS idx_cd_persona ON cloud_deployments(persona_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_cd_slug ON cloud_deployments(slug);

CREATE TABLE IF NOT EXISTS trigger_firings (
  id TEXT PRIMARY KEY,
  trigger_id TEXT NOT NULL,
  persona_id TEXT NOT NULL,
  project_id TEXT NOT NULL DEFAULT 'default',
  event_id TEXT,
  execution_id TEXT,
  status TEXT NOT NULL DEFAULT 'fired' CHECK(status IN ('fired','dispatched','completed','failed','skipped')),
  cost_usd REAL,
  duration_ms INTEGER,
  error_message TEXT,
  fired_at TEXT NOT NULL,
  resolved_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_tf_trigger ON trigger_firings(trigger_id);
CREATE INDEX IF NOT EXISTS idx_tf_persona ON trigger_firings(persona_id);
CREATE INDEX IF NOT EXISTS idx_tf_project ON trigger_firings(project_id);
CREATE INDEX IF NOT EXISTS idx_tf_status ON trigger_firings(status);
CREATE INDEX IF NOT EXISTS idx_tf_fired_at ON trigger_firings(fired_at);
`;

// ---------------------------------------------------------------------------
// Row → domain type auto-mapper
// ---------------------------------------------------------------------------

/** Convert snake_case to camelCase. */
function snakeToCamel(s: string): string {
  return s.replace(/_([a-z])/g, (_, c) => c.toUpperCase());
}

/**
 * Field override: transform a raw column value before assignment.
 * - `'bool'`: SQLite integer → boolean (1 = true)
 * - `'undefined'`: null/undefined → undefined (for optional fields)
 * - any other value: used as fallback default when the column is null/undefined
 */
type FieldOverride = 'bool' | 'undefined' | string | number;

/**
 * Map a SQLite row (snake_case columns) to a domain type (camelCase fields).
 * Columns are auto-converted to camelCase. Override specific fields for
 * booleans, defaults, or undefined-vs-null semantics.
 */
function mapRow<T>(row: Record<string, unknown>, overrides: Record<string, FieldOverride> = {}): T {
  const result: Record<string, unknown> = {};
  for (const [col, val] of Object.entries(row)) {
    const key = snakeToCamel(col);
    const override = overrides[key];
    if (override === 'bool') {
      result[key] = val === 1;
    } else if (override === 'undefined') {
      result[key] = val ?? undefined;
    } else if (override !== undefined) {
      result[key] = val ?? override;
    } else {
      result[key] = val ?? null;
    }
  }
  return result as T;
}

// Column-map declarations: only fields needing special treatment
const personaOverrides: Record<string, FieldOverride> = { enabled: 'bool' };
const toolOverrides: Record<string, FieldOverride> = { isBuiltin: 'bool' };
const credentialOverrides: Record<string, FieldOverride> = { projectId: 'default', salt: 'undefined', iter: 'undefined' };
const eventOverrides: Record<string, FieldOverride> = { retryCount: 0 };
const subscriptionOverrides: Record<string, FieldOverride> = { enabled: 'bool', maxRetries: 3, retryBackoffMs: 5000 };
const triggerOverrides: Record<string, FieldOverride> = { enabled: 'bool', healthStatus: 'healthy' };
const executionOverrides: Record<string, FieldOverride> = { projectId: 'default', inputTokens: 0, outputTokens: 0, costUsd: 0, retryCount: 0 };
const deploymentOverrides: Record<string, FieldOverride> = { webhookEnabled: 'bool', invocationCount: 0, currentMonthCostUsd: 0 };
const triggerFiringOverrides: Record<string, FieldOverride> = {};

const rowToPersona = (row: Record<string, unknown>): Persona => mapRow<Persona>(row, personaOverrides);
const rowToTool = (row: Record<string, unknown>): PersonaToolDefinition => mapRow<PersonaToolDefinition>(row, toolOverrides);
const rowToCredential = (row: Record<string, unknown>): PersonaCredential => mapRow<PersonaCredential>(row, credentialOverrides);
const rowToEvent = (row: Record<string, unknown>): PersonaEvent => mapRow<PersonaEvent>(row, eventOverrides);
const rowToSubscription = (row: Record<string, unknown>): PersonaEventSubscription => mapRow<PersonaEventSubscription>(row, subscriptionOverrides);
const rowToTrigger = (row: Record<string, unknown>): PersonaTrigger => mapRow<PersonaTrigger>(row, triggerOverrides);
const rowToExecution = (row: Record<string, unknown>): PersonaExecution => mapRow<PersonaExecution>(row, executionOverrides);
const rowToDeployment = (row: Record<string, unknown>): CloudDeployment => mapRow<CloudDeployment>(row, deploymentOverrides);
const rowToTriggerFiring = (row: Record<string, unknown>): TriggerFiring => mapRow<TriggerFiring>(row, triggerFiringOverrides);

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
    INSERT INTO personas (id, project_id, name, description, system_prompt, structured_prompt, icon, color, enabled, max_concurrent, timeout_ms, model_profile, max_budget_usd, max_turns, design_context, group_id, permission_policy, webhook_secret, created_at, updated_at)
    VALUES (@id, @projectId, @name, @description, @systemPrompt, @structuredPrompt, @icon, @color, @enabled, @maxConcurrent, @timeoutMs, @modelProfile, @maxBudgetUsd, @maxTurns, @designContext, @groupId, @permissionPolicy, @webhookSecret, @createdAt, @updatedAt)
    ON CONFLICT(id) DO UPDATE SET
      name = excluded.name, description = excluded.description, system_prompt = excluded.system_prompt,
      structured_prompt = excluded.structured_prompt, icon = excluded.icon, color = excluded.color,
      enabled = excluded.enabled, max_concurrent = excluded.max_concurrent, timeout_ms = excluded.timeout_ms,
      model_profile = excluded.model_profile, max_budget_usd = excluded.max_budget_usd, max_turns = excluded.max_turns,
      design_context = excluded.design_context, group_id = excluded.group_id,
      permission_policy = excluded.permission_policy, webhook_secret = excluded.webhook_secret, updated_at = excluded.updated_at
    WHERE personas.project_id = excluded.project_id
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
    permissionPolicy: p.permissionPolicy,
    webhookSecret: p.webhookSecret,
    createdAt: p.createdAt,
    updatedAt: p.updatedAt,
  });
}

/**
 * Partially update a persona, setting only the provided fields.
 * Returns the updated persona or undefined if not found.
 */
export function updatePersona(db: Database.Database, id: string, updates: Record<string, unknown>, projectId?: string): Persona | undefined {
  const existing = getPersona(db, id);
  if (!existing) return undefined;
  if (projectId && existing.projectId !== projectId) return undefined;

  const columnMap: Record<string, string> = {
    name: 'name',
    description: 'description',
    systemPrompt: 'system_prompt',
    structuredPrompt: 'structured_prompt',
    icon: 'icon',
    color: 'color',
    enabled: 'enabled',
    maxConcurrent: 'max_concurrent',
    timeoutMs: 'timeout_ms',
    modelProfile: 'model_profile',
    maxBudgetUsd: 'max_budget_usd',
    maxTurns: 'max_turns',
    designContext: 'design_context',
    groupId: 'group_id',
    permissionPolicy: 'permission_policy',
    webhookSecret: 'webhook_secret',
  };

  const fields: string[] = [];
  const values: unknown[] = [];

  for (const [key, col] of Object.entries(columnMap)) {
    if (key in updates) {
      let val = updates[key];
      // Convert boolean to integer for SQLite
      if (key === 'enabled' && typeof val === 'boolean') val = val ? 1 : 0;
      fields.push(`${col} = ?`);
      values.push(val);
    }
  }

  if (fields.length === 0) return existing;

  fields.push('updated_at = ?');
  values.push(new Date().toISOString());
  values.push(id);

  db.prepare(`UPDATE personas SET ${fields.join(', ')} WHERE id = ?`).run(...values);
  return getPersona(db, id);
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

export function listToolDefinitions(db: Database.Database): PersonaToolDefinition[] {
  const rows = db.prepare('SELECT * FROM persona_tool_definitions ORDER BY name').all() as Record<string, unknown>[];
  return rows.map(rowToTool);
}

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
  name: string; serviceType: string; encryptedData: string; iv: string; tag: string; salt?: string | null; iter?: number | null; metadata?: string | null; projectId?: string;
}): PersonaCredential {
  const now = new Date().toISOString();
  const id = nanoid();
  db.prepare(`
    INSERT INTO persona_credentials (id, project_id, name, service_type, encrypted_data, iv, tag, salt, iter, metadata, created_at, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `).run(id, input.projectId ?? 'default', input.name, input.serviceType, input.encryptedData, input.iv, input.tag, input.salt ?? null, input.iter ?? null, input.metadata ?? null, now, now);
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

export function deleteCredential(db: Database.Database, id: string, projectId?: string): void {
  if (projectId) {
    db.prepare('DELETE FROM persona_credentials WHERE id = ? AND project_id = ?').run(id, projectId);
  } else {
    db.prepare('DELETE FROM persona_credentials WHERE id = ?').run(id);
  }
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

/**
 * Atomically claim both pending and retry-due events, marking them as 'processing'.
 * Returns the list of claimed events. Dispatch records for retry events are
 * cleared within the same transaction to ensure consistency and minimize lock contention.
 */
export function claimEvents(db: Database.Database, limit: number = 50): PersonaEvent[] {
  const now = new Date().toISOString();

  // BEGIN IMMEDIATE acquires a write lock before reading, so concurrent
  // callers block until this transaction commits.
  const claim = db.transaction(() => {
    // 1. Pending events
    const pendingRows = db.prepare(
      "SELECT * FROM persona_events WHERE status = 'pending' ORDER BY created_at LIMIT ?"
    ).all(limit) as Record<string, unknown>[];

    // 2. Retry-due events
    const retryRows = db.prepare(
      "SELECT * FROM persona_events WHERE status = 'retry' AND next_retry_at <= ? ORDER BY next_retry_at LIMIT ?"
    ).all(now, limit) as Record<string, unknown>[];

    const allRows = [...pendingRows, ...retryRows];
    if (allRows.length === 0) return [];

    const allIds = allRows.map(r => r['id'] as string);
    const placeholders = allIds.map(() => '?').join(',');
    db.prepare(
      `UPDATE persona_events SET status = 'processing', processed_at = ? WHERE id IN (${placeholders})`
    ).run(now, ...allIds);

    // Clear previous dispatch records for retry events so they can be re-dispatched
    if (retryRows.length > 0) {
      const retryIds = retryRows.map(r => r['id'] as string);
      const retryPlaceholders = retryIds.map(() => '?').join(',');
      db.prepare(
        `DELETE FROM persona_event_dispatches WHERE event_id IN (${retryPlaceholders})`
      ).run(...retryIds);
    }

    return allRows.map(rowToEvent);
  });

  return claim.immediate();
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

/**
 * Reset events stuck in 'processing' back to 'pending' so they can be
 * retried. An event is considered stuck if it has been in 'processing'
 * for longer than `staleMinutes` (default 5).
 *
 * Called once at startup to recover from prior crashes.
 */
export function recoverStaleProcessingEvents(db: Database.Database, staleMinutes: number = 5): number {
  const cutoff = new Date(Date.now() - staleMinutes * 60_000).toISOString();
  const result = db.prepare(
    "UPDATE persona_events SET status = 'pending', error_message = NULL, processed_at = NULL WHERE status = 'processing' AND processed_at <= ?"
  ).run(cutoff);
  return result.changes;
}

/**
 * Schedule an event for retry with exponential backoff.
 * Sets status to 'retry', increments retry_count, and computes next_retry_at.
 */
export function scheduleEventRetry(
  db: Database.Database,
  id: string,
  retryCount: number,
  backoffBaseMs: number,
  errorMessage?: string,
): void {
  const delayMs = backoffBaseMs * Math.pow(2, retryCount); // exponential: base * 2^n
  const nextRetryAt = new Date(Date.now() + delayMs).toISOString();
  const now = new Date().toISOString();
  db.prepare(
    'UPDATE persona_events SET status = ?, retry_count = ?, next_retry_at = ?, error_message = ?, processed_at = ? WHERE id = ?'
  ).run('retry', retryCount + 1, nextRetryAt, errorMessage ?? null, now, id);
}

/**
 * Move an event to the dead letter queue after exhausting all retries.
 */
export function moveEventToDeadLetter(db: Database.Database, id: string, errorMessage?: string): void {
  const now = new Date().toISOString();
  db.prepare(
    'UPDATE persona_events SET status = ?, error_message = ?, next_retry_at = NULL, processed_at = ? WHERE id = ?'
  ).run('dead_letter', errorMessage ?? 'Max retries exceeded', now, id);
}

/**
 * Get the maximum retry configuration across all subscriptions matching a given event type.
 */
export function getRetryPolicyForEventType(
  db: Database.Database,
  eventType: string,
  projectId?: string,
): { maxRetries: number; retryBackoffMs: number } {
  const subs = getSubscriptionsByEventType(db, eventType, projectId);
  if (subs.length === 0) return { maxRetries: 3, retryBackoffMs: 5000 };
  return {
    maxRetries: Math.max(...subs.map(s => s.maxRetries)),
    retryBackoffMs: Math.max(...subs.map(s => s.retryBackoffMs)),
  };
}

export function updateEventWithMetadata(db: Database.Database, id: string, status: string, metadata?: string | null): PersonaEvent | undefined {
  const now = new Date().toISOString();
  db.prepare('UPDATE persona_events SET status = ?, processed_at = ?, payload = CASE WHEN ? IS NOT NULL THEN ? ELSE payload END WHERE id = ?')
    .run(status, now, metadata, metadata, id);
  return getEvent(db, id);
}

/**
 * Record that an event has been dispatched to a persona. Uses INSERT OR IGNORE
 * so the same (event_id, persona_id) pair is silently skipped on retry.
 *
 * @returns true if the row was inserted (first dispatch), false if it already existed (duplicate).
 */
export function recordEventDispatch(db: Database.Database, eventId: string, personaId: string, executionId: string): boolean {
  const now = new Date().toISOString();
  const result = db.prepare(
    'INSERT OR IGNORE INTO persona_event_dispatches (event_id, persona_id, execution_id, created_at) VALUES (?, ?, ?, ?)'
  ).run(eventId, personaId, executionId, now);
  return result.changes > 0;
}

/**
 * Clear all dispatch records for an event so it can be re-dispatched on retry.
 */
export function clearEventDispatches(db: Database.Database, eventId: string): void {
  db.prepare('DELETE FROM persona_event_dispatches WHERE event_id = ?').run(eventId);
}

/**
 * Check whether an event has already been dispatched to a persona.
 */
export function hasEventBeenDispatched(db: Database.Database, eventId: string, personaId: string): boolean {
  const row = db.prepare(
    'SELECT 1 FROM persona_event_dispatches WHERE event_id = ? AND persona_id = ?'
  ).get(eventId, personaId);
  return row !== undefined;
}

// ---------------------------------------------------------------------------
// Subscription Cache
// ---------------------------------------------------------------------------

interface SubscriptionCacheEntry {
  subs: PersonaEventSubscription[];
  timestamp: number;
}

const SUBSCRIPTION_CACHE_TTL_MS = 60_000;
const subscriptionCache = new Map<string, SubscriptionCacheEntry>();

function getSubscriptionCacheKey(eventType: string, projectId?: string): string {
  return `${projectId ?? 'all'}:${eventType}`;
}

/**
 * Invalidate subscription cache entries for a specific event type and project.
 * Only clears the affected cache keys, preserving unrelated entries.
 */
export function invalidateSubscriptionCache(eventType?: string, projectId?: string): void {
  if (!eventType) {
    subscriptionCache.clear();
    return;
  }
  // Invalidate the specific key and the "all projects" key for this event type
  subscriptionCache.delete(getSubscriptionCacheKey(eventType, projectId));
  subscriptionCache.delete(getSubscriptionCacheKey(eventType, undefined));
}

// ---------------------------------------------------------------------------
// Event Subscriptions
// ---------------------------------------------------------------------------

export function createSubscription(db: Database.Database, input: {
  personaId: string; eventType: string; sourceFilter?: string | null; payloadFilter?: string | null; enabled?: boolean;
  maxRetries?: number; retryBackoffMs?: number; useCaseId?: string | null; projectId?: string;
}): PersonaEventSubscription {
  // Pre-validate and pre-compile payload filter at write time so invalid
  // filters surface as errors here rather than silently dropping events.
  if (input.payloadFilter) {
    const filterErr = validatePayloadFilter(input.payloadFilter);
    if (filterErr) throw new Error(`Invalid payloadFilter: ${filterErr}`);
    compilePayloadFilter(input.payloadFilter);
  }

  const now = new Date().toISOString();
  const id = nanoid();
  db.prepare(`
    INSERT INTO persona_event_subscriptions (id, project_id, persona_id, event_type, source_filter, payload_filter, enabled, max_retries, retry_backoff_ms, use_case_id, created_at, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `).run(id, input.projectId ?? 'default', input.personaId, input.eventType, input.sourceFilter ?? null, input.payloadFilter ?? null, input.enabled !== false ? 1 : 0, input.maxRetries ?? 3, input.retryBackoffMs ?? 5000, input.useCaseId ?? null, now, now);

  invalidateSubscriptionCache(input.eventType, input.projectId);
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
  const cacheKey = getSubscriptionCacheKey(eventType, projectId);
  const now = Date.now();
  const cached = subscriptionCache.get(cacheKey);

  if (cached && now - cached.timestamp < SUBSCRIPTION_CACHE_TTL_MS) {
    return cached.subs;
  }

  let subs: PersonaEventSubscription[];
  if (projectId) {
    const rows = db.prepare('SELECT * FROM persona_event_subscriptions WHERE event_type = ? AND project_id = ?').all(eventType, projectId) as Record<string, unknown>[];
    subs = rows.map(rowToSubscription);
  } else {
    const rows = db.prepare('SELECT * FROM persona_event_subscriptions WHERE event_type = ?').all(eventType) as Record<string, unknown>[];
    subs = rows.map(rowToSubscription);
  }

  subscriptionCache.set(cacheKey, { subs, timestamp: now });
  return subs;
}

export function updateSubscription(db: Database.Database, id: string, updates: {
  eventType?: string | null; sourceFilter?: string | null; payloadFilter?: string | null; enabled?: boolean | null;
  maxRetries?: number | null; retryBackoffMs?: number | null;
}, projectId?: string): void {
  // Pre-validate and pre-compile payload filter at write time.
  const newFilter = updates.payloadFilter;
  if (newFilter) {
    const filterErr = validatePayloadFilter(newFilter);
    if (filterErr) throw new Error(`Invalid payloadFilter: ${filterErr}`);
    compilePayloadFilter(newFilter);
  }

  const now = new Date().toISOString();
  const sub = getSubscription(db, id);
  if (!sub) return;
  const sql = projectId
    ? 'UPDATE persona_event_subscriptions SET event_type = ?, source_filter = ?, payload_filter = ?, enabled = ?, max_retries = ?, retry_backoff_ms = ?, updated_at = ? WHERE id = ? AND project_id = ?'
    : 'UPDATE persona_event_subscriptions SET event_type = ?, source_filter = ?, payload_filter = ?, enabled = ?, max_retries = ?, retry_backoff_ms = ?, updated_at = ? WHERE id = ?';
  const params: unknown[] = [
    updates.eventType ?? sub.eventType,
    updates.sourceFilter !== undefined ? updates.sourceFilter : sub.sourceFilter,
    updates.payloadFilter !== undefined ? updates.payloadFilter : sub.payloadFilter,
    updates.enabled !== undefined && updates.enabled !== null ? (updates.enabled ? 1 : 0) : (sub.enabled ? 1 : 0),
    updates.maxRetries !== undefined && updates.maxRetries !== null ? updates.maxRetries : sub.maxRetries,
    updates.retryBackoffMs !== undefined && updates.retryBackoffMs !== null ? updates.retryBackoffMs : sub.retryBackoffMs,
    now,
    id,
  ];
  if (projectId) params.push(projectId);
  db.prepare(sql).run(...params);
  // Invalidate both old and new event types if it changed
  invalidateSubscriptionCache(sub.eventType, projectId);
  const newEventType = updates.eventType ?? sub.eventType;
  if (newEventType !== sub.eventType) {
    invalidateSubscriptionCache(newEventType, projectId);
  }
}

export function deleteSubscription(db: Database.Database, id: string, projectId?: string): void {
  const sub = getSubscription(db, id);
  if (projectId) {
    db.prepare('DELETE FROM persona_event_subscriptions WHERE id = ? AND project_id = ?').run(id, projectId);
  } else {
    db.prepare('DELETE FROM persona_event_subscriptions WHERE id = ?').run(id);
  }
  if (sub) {
    invalidateSubscriptionCache(sub.eventType, projectId);
  }
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

export function updateTriggerHealth(db: Database.Database, id: string, healthStatus: 'healthy' | 'degraded', healthMessage: string | null): void {
  db.prepare('UPDATE persona_triggers SET health_status = ?, health_message = ?, updated_at = ? WHERE id = ?')
    .run(healthStatus, healthMessage, new Date().toISOString(), id);
}

export function updateTrigger(db: Database.Database, id: string, updates: {
  triggerType?: string | null; config?: string | null; enabled?: boolean | null;
}, projectId?: string): void {
  const now = new Date().toISOString();
  const trigger = getTrigger(db, id);
  if (!trigger) return;

  // Reset health status when config changes — the scheduler will re-evaluate
  const configChanged = updates.config !== undefined && updates.config !== trigger.config;
  const sql = projectId
    ? 'UPDATE persona_triggers SET trigger_type = ?, config = ?, enabled = ?, health_status = ?, health_message = ?, updated_at = ? WHERE id = ? AND project_id = ?'
    : 'UPDATE persona_triggers SET trigger_type = ?, config = ?, enabled = ?, health_status = ?, health_message = ?, updated_at = ? WHERE id = ?';
  const params: unknown[] = [
    updates.triggerType ?? trigger.triggerType,
    updates.config !== undefined ? updates.config : trigger.config,
    updates.enabled !== undefined && updates.enabled !== null ? (updates.enabled ? 1 : 0) : (trigger.enabled ? 1 : 0),
    configChanged ? 'healthy' : trigger.healthStatus,
    configChanged ? null : trigger.healthMessage,
    now,
    id,
  ];
  if (projectId) params.push(projectId);
  db.prepare(sql).run(...params);
}

export function deleteTrigger(db: Database.Database, id: string, projectId?: string): void {
  if (projectId) {
    db.prepare('DELETE FROM persona_triggers WHERE id = ? AND project_id = ?').run(id, projectId);
  } else {
    db.prepare('DELETE FROM persona_triggers WHERE id = ?').run(id);
  }
}

/** Get a trigger's project_id for ownership checks. */
export function getTriggerProjectId(db: Database.Database, id: string): string | undefined {
  const row = db.prepare('SELECT project_id FROM persona_triggers WHERE id = ?').get(id) as { project_id: string } | undefined;
  return row?.project_id;
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

/** Columns to select for list queries (excludes output_data which can be megabytes). */
const EXECUTION_LIST_COLUMNS = [
  'id', 'project_id', 'persona_id', 'trigger_id', 'use_case_id',
  'status', 'input_data', 'claude_session_id', 'model_used',
  'input_tokens', 'output_tokens', 'cost_usd', 'error_message',
  'duration_ms', 'retry_of_execution_id', 'retry_count',
  'started_at', 'completed_at', 'created_at',
].join(', ');

export function listExecutions(db: Database.Database, opts?: {
  personaId?: string; status?: string; limit?: number; offset?: number; projectId?: string;
}): PersonaExecution[] {
  let sql = `SELECT ${EXECUTION_LIST_COLUMNS} FROM persona_executions WHERE 1=1`;
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
    retryCount: 'retry_count',
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
// Durable Queue — persist ExecRequest data for crash recovery
// ---------------------------------------------------------------------------

/**
 * Store the serialized ExecRequest as queue_data on an execution record.
 * Called when an execution is first queued so it can be recovered on restart.
 */
export function storeQueueData(db: Database.Database, executionId: string, queueData: string): void {
  db.prepare('UPDATE persona_executions SET queue_data = ? WHERE id = ?').run(queueData, executionId);
}

/**
 * Clear queue_data from an execution record once it has been dispatched to a worker.
 * This prevents it from being re-queued on recovery.
 */
export function clearQueueData(db: Database.Database, executionId: string): void {
  db.prepare('UPDATE persona_executions SET queue_data = NULL WHERE id = ?').run(executionId);
}

/**
 * Recover queued executions that were never dispatched (crash recovery).
 * Returns executions in 'queued' status that have queue_data, ordered by creation time.
 */
export function loadQueuedExecutions(db: Database.Database): Array<{
  id: string;
  queueData: string;
  retryCount: number;
}> {
  const rows = db.prepare(`
    SELECT id, queue_data, retry_count
    FROM persona_executions
    WHERE status = 'queued' AND queue_data IS NOT NULL
    ORDER BY created_at ASC
  `).all() as Array<{ id: string; queue_data: string; retry_count: number }>;

  return rows.map(r => ({
    id: r.id,
    queueData: r.queue_data,
    retryCount: r.retry_count,
  }));
}

/**
 * Recover executions stuck in 'running' status from a prior crash.
 * If an execution has been in 'running' state for longer than staleMinutes,
 * it's assumed the process crashed — reset to 'queued' if queue_data exists,
 * otherwise mark as 'failed'.
 */
export function recoverStaleRunningExecutions(db: Database.Database, staleMinutes: number = 5): { requeued: number; failed: number } {
  const cutoff = new Date(Date.now() - staleMinutes * 60 * 1000).toISOString();

  // Re-queue those with queue_data (can be re-dispatched)
  const requeueResult = db.prepare(`
    UPDATE persona_executions
    SET status = 'queued', started_at = NULL
    WHERE status = 'running' AND started_at < ? AND queue_data IS NOT NULL
  `).run(cutoff);

  // Fail those without queue_data (no way to re-dispatch)
  const failResult = db.prepare(`
    UPDATE persona_executions
    SET status = 'failed', error_message = 'Orchestrator crashed — execution was in-flight', completed_at = ?
    WHERE status = 'running' AND started_at < ? AND queue_data IS NULL
  `).run(new Date().toISOString(), cutoff);

  return {
    requeued: requeueResult.changes,
    failed: failResult.changes,
  };
}

// ---------------------------------------------------------------------------
// Event Audit Log
// ---------------------------------------------------------------------------

export interface EventAuditEntry {
  id: number;
  eventId: string;
  action: string;
  subscriptionId: string | null;
  personaId: string | null;
  executionId: string | null;
  detail: string | null;
  createdAt: string;
}

/**
 * Append an entry to the event audit log.
 */
export function appendEventAudit(db: Database.Database, entry: {
  eventId: string;
  action: string;
  subscriptionId?: string | null;
  personaId?: string | null;
  executionId?: string | null;
  detail?: string | null;
}): void {
  db.prepare(`
    INSERT INTO event_audit_log (event_id, action, subscription_id, persona_id, execution_id, detail, created_at)
    VALUES (?, ?, ?, ?, ?, ?, ?)
  `).run(
    entry.eventId,
    entry.action,
    entry.subscriptionId ?? null,
    entry.personaId ?? null,
    entry.executionId ?? null,
    entry.detail ?? null,
    new Date().toISOString(),
  );
}

/**
 * Batch-insert multiple audit log entries in a single prepared statement.
 * More efficient than calling appendEventAudit N times per event.
 */
export function batchAppendEventAudit(db: Database.Database, entries: Array<{
  eventId: string;
  action: string;
  subscriptionId?: string | null;
  personaId?: string | null;
  executionId?: string | null;
  detail?: string | null;
}>): void {
  if (entries.length === 0) return;
  const now = new Date().toISOString();
  const stmt = db.prepare(`
    INSERT INTO event_audit_log (event_id, action, subscription_id, persona_id, execution_id, detail, created_at)
    VALUES (?, ?, ?, ?, ?, ?, ?)
  `);
  for (const entry of entries) {
    stmt.run(
      entry.eventId,
      entry.action,
      entry.subscriptionId ?? null,
      entry.personaId ?? null,
      entry.executionId ?? null,
      entry.detail ?? null,
      now,
    );
  }
}

/**
 * Get the full audit trail for an event, ordered chronologically.
 */
export function getEventAuditLog(db: Database.Database, eventId: string): EventAuditEntry[] {
  const rows = db.prepare(
    'SELECT * FROM event_audit_log WHERE event_id = ? ORDER BY id ASC'
  ).all(eventId) as Array<Record<string, unknown>>;

  return rows.map(row => mapRow<EventAuditEntry>(row));
}

/**
 * Retrieve the cached match results from a prior processing of this event.
 * Returns the matched subscription/persona pairs from the `subscriptions_evaluated`
 * audit entry, or null if no prior evaluation exists.
 */
export function getCachedMatchResults(db: Database.Database, eventId: string): Array<{ subscriptionId: string; personaId: string }> | null {
  const row = db.prepare(
    "SELECT detail FROM event_audit_log WHERE event_id = ? AND action = 'subscriptions_evaluated' ORDER BY id DESC LIMIT 1"
  ).get(eventId) as { detail: string } | undefined;

  if (!row?.detail) return null;

  try {
    const parsed = JSON.parse(row.detail) as {
      matchedSubscriptions?: string[];
      matchedPersonas?: string[];
    };
    if (!parsed.matchedSubscriptions || !parsed.matchedPersonas) return null;
    if (parsed.matchedSubscriptions.length !== parsed.matchedPersonas.length) return null;
    return parsed.matchedSubscriptions.map((subId, i) => ({
      subscriptionId: subId,
      personaId: parsed.matchedPersonas![i],
    }));
  } catch {
    return null;
  }
}

// ---------------------------------------------------------------------------
// Admin queries
// ---------------------------------------------------------------------------

export function listDistinctProjectIds(db: Database.Database): string[] {
  const rows = db.prepare('SELECT DISTINCT project_id FROM personas ORDER BY project_id').all() as Array<{ project_id: string }>;
  return rows.map(r => r.project_id);
}

// ---------------------------------------------------------------------------
// Cloud Deployments
// ---------------------------------------------------------------------------

export function createDeployment(database: Database.Database, input: {
  personaId: string; slug: string; label: string; webhookSecret?: string | null; projectId?: string;
  maxMonthlyBudgetUsd?: number | null;
}): CloudDeployment {
  const now = new Date().toISOString();
  const id = nanoid();
  const budgetMonth = now.slice(0, 7); // "YYYY-MM"
  database.prepare(`
    INSERT INTO cloud_deployments (id, project_id, persona_id, slug, label, status, webhook_enabled, webhook_secret, max_monthly_budget_usd, current_month_cost_usd, budget_month, created_at, updated_at)
    VALUES (?, ?, ?, ?, ?, 'active', 1, ?, ?, 0, ?, ?, ?)
  `).run(id, input.projectId ?? 'default', input.personaId, input.slug, input.label, input.webhookSecret ?? null, input.maxMonthlyBudgetUsd ?? null, budgetMonth, now, now);
  return getDeployment(database, id)!;
}

export function getDeployment(database: Database.Database, id: string): CloudDeployment | undefined {
  const row = database.prepare('SELECT * FROM cloud_deployments WHERE id = ?').get(id) as Record<string, unknown> | undefined;
  return row ? rowToDeployment(row) : undefined;
}

export function getDeploymentBySlug(database: Database.Database, slug: string): CloudDeployment | undefined {
  const row = database.prepare('SELECT * FROM cloud_deployments WHERE slug = ?').get(slug) as Record<string, unknown> | undefined;
  return row ? rowToDeployment(row) : undefined;
}

export function getDeploymentByPersona(database: Database.Database, personaId: string): CloudDeployment | undefined {
  const row = database.prepare('SELECT * FROM cloud_deployments WHERE persona_id = ?').get(personaId) as Record<string, unknown> | undefined;
  return row ? rowToDeployment(row) : undefined;
}

export function listDeployments(database: Database.Database, projectId?: string): CloudDeployment[] {
  if (projectId) {
    const rows = database.prepare('SELECT * FROM cloud_deployments WHERE project_id = ? ORDER BY created_at DESC').all(projectId) as Record<string, unknown>[];
    return rows.map(rowToDeployment);
  }
  const rows = database.prepare('SELECT * FROM cloud_deployments ORDER BY created_at DESC').all() as Record<string, unknown>[];
  return rows.map(rowToDeployment);
}

export function updateDeploymentStatus(database: Database.Database, id: string, status: CloudDeployment['status']): void {
  const now = new Date().toISOString();
  database.prepare('UPDATE cloud_deployments SET status = ?, updated_at = ? WHERE id = ?').run(status, now, id);
}

export function incrementDeploymentInvocation(database: Database.Database, id: string): void {
  const now = new Date().toISOString();
  database.prepare('UPDATE cloud_deployments SET invocation_count = invocation_count + 1, last_invoked_at = ?, updated_at = ? WHERE id = ?').run(now, now, id);
}

/** Add execution cost to a deployment's monthly budget tracker. Resets if month rolled over. */
export function addDeploymentCost(database: Database.Database, id: string, costUsd: number): void {
  const now = new Date().toISOString();
  const currentMonth = now.slice(0, 7);
  const deployment = getDeployment(database, id);
  if (!deployment) return;

  if (deployment.budgetMonth !== currentMonth) {
    // Month rolled over — reset counter
    database.prepare(
      'UPDATE cloud_deployments SET current_month_cost_usd = ?, budget_month = ?, updated_at = ? WHERE id = ?',
    ).run(costUsd, currentMonth, now, id);
  } else {
    database.prepare(
      'UPDATE cloud_deployments SET current_month_cost_usd = current_month_cost_usd + ?, updated_at = ? WHERE id = ?',
    ).run(costUsd, now, id);
  }
}

/** Update a deployment's monthly budget cap. Pass null to remove the cap. */
export function updateDeploymentBudget(database: Database.Database, id: string, maxMonthlyBudgetUsd: number | null): void {
  const now = new Date().toISOString();
  database.prepare('UPDATE cloud_deployments SET max_monthly_budget_usd = ?, updated_at = ? WHERE id = ?').run(maxMonthlyBudgetUsd, now, id);
}

export function deleteDeployment(database: Database.Database, id: string, projectId?: string): void {
  if (projectId) {
    database.prepare('DELETE FROM cloud_deployments WHERE id = ? AND project_id = ?').run(id, projectId);
  } else {
    database.prepare('DELETE FROM cloud_deployments WHERE id = ?').run(id);
  }
}

// ---------------------------------------------------------------------------
// Trigger Firings — audit log for trigger firing outcomes
// ---------------------------------------------------------------------------

/**
 * Record a trigger firing when the trigger scheduler fires a trigger.
 * Initially status='fired' with the event ID from the published event.
 */
export function recordTriggerFiring(db: Database.Database, input: {
  triggerId: string;
  personaId: string;
  projectId: string;
  eventId: string;
}): TriggerFiring {
  const id = nanoid();
  const now = new Date().toISOString();
  db.prepare(`
    INSERT INTO trigger_firings (id, trigger_id, persona_id, project_id, event_id, status, fired_at)
    VALUES (?, ?, ?, ?, ?, 'fired', ?)
  `).run(id, input.triggerId, input.personaId, input.projectId, input.eventId, now);
  return getTriggerFiring(db, id)!;
}

export function getTriggerFiring(db: Database.Database, id: string): TriggerFiring | undefined {
  const row = db.prepare('SELECT * FROM trigger_firings WHERE id = ?').get(id) as Record<string, unknown> | undefined;
  return row ? rowToTriggerFiring(row) : undefined;
}

/**
 * Find a trigger firing by the event ID it produced.
 * Used by the event processor to link dispatched executions back to trigger firings.
 */
export function getTriggerFiringByEventId(db: Database.Database, eventId: string): TriggerFiring | undefined {
  const row = db.prepare('SELECT * FROM trigger_firings WHERE event_id = ?').get(eventId) as Record<string, unknown> | undefined;
  return row ? rowToTriggerFiring(row) : undefined;
}

/**
 * Update a trigger firing when the execution is dispatched (links the execution ID).
 */
export function updateTriggerFiringDispatched(db: Database.Database, id: string, executionId: string): void {
  db.prepare(
    "UPDATE trigger_firings SET status = 'dispatched', execution_id = ? WHERE id = ?"
  ).run(executionId, id);
}

/**
 * Resolve a trigger firing with final outcome from the execution.
 */
export function resolveTriggerFiring(db: Database.Database, executionId: string, outcome: {
  status: 'completed' | 'failed';
  costUsd?: number | null;
  durationMs?: number | null;
  errorMessage?: string | null;
}): void {
  const now = new Date().toISOString();
  db.prepare(
    'UPDATE trigger_firings SET status = ?, cost_usd = ?, duration_ms = ?, error_message = ?, resolved_at = ? WHERE execution_id = ?'
  ).run(outcome.status, outcome.costUsd ?? null, outcome.durationMs ?? null, outcome.errorMessage ?? null, now, executionId);
}

/**
 * Mark a trigger firing as skipped (no subscription matched or event was dropped).
 */
export function skipTriggerFiring(db: Database.Database, eventId: string): void {
  const now = new Date().toISOString();
  db.prepare(
    "UPDATE trigger_firings SET status = 'skipped', resolved_at = ? WHERE event_id = ? AND status = 'fired'"
  ).run(now, eventId);
}

/**
 * List trigger firings for a trigger, ordered newest first.
 */
export function listTriggerFirings(db: Database.Database, opts?: {
  triggerId?: string;
  personaId?: string;
  projectId?: string;
  status?: string;
  limit?: number;
  offset?: number;
}): TriggerFiring[] {
  const clauses: string[] = [];
  const params: unknown[] = [];
  if (opts?.triggerId) { clauses.push('trigger_id = ?'); params.push(opts.triggerId); }
  if (opts?.personaId) { clauses.push('persona_id = ?'); params.push(opts.personaId); }
  if (opts?.projectId) { clauses.push('project_id = ?'); params.push(opts.projectId); }
  if (opts?.status) { clauses.push('status = ?'); params.push(opts.status); }
  const where = clauses.length > 0 ? `WHERE ${clauses.join(' AND ')}` : '';
  const limit = opts?.limit ?? 50;
  const offset = opts?.offset ?? 0;
  const rows = db.prepare(`SELECT * FROM trigger_firings ${where} ORDER BY fired_at DESC LIMIT ? OFFSET ?`).all(...params, limit, offset) as Record<string, unknown>[];
  return rows.map(rowToTriggerFiring);
}

/**
 * Get summary analytics for a trigger: total firings, success rate, avg duration, total cost.
 */
/** Aggregated execution statistics for a persona or all personas in a project. */
export function getExecutionStats(database: Database.Database, opts?: {
  personaId?: string;
  projectId?: string;
  periodDays?: number;
}): {
  totalExecutions: number;
  completed: number;
  failed: number;
  cancelled: number;
  successRate: number | null;
  totalCostUsd: number;
  avgCostUsd: number | null;
  avgDurationMs: number | null;
  dailyBreakdown: Array<{ date: string; count: number; cost: number; successRate: number | null }>;
  topErrors: Array<{ message: string; count: number }>;
} {
  const days = opts?.periodDays ?? 7;
  const cutoff = new Date(Date.now() - days * 86_400_000).toISOString();

  const conditions: string[] = ['created_at >= ?'];
  const params: unknown[] = [cutoff];
  if (opts?.projectId) { conditions.push('project_id = ?'); params.push(opts.projectId); }
  if (opts?.personaId) { conditions.push('persona_id = ?'); params.push(opts.personaId); }
  const where = conditions.join(' AND ');

  // Aggregate totals
  const row = database.prepare(`
    SELECT
      COUNT(*) as total,
      SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed,
      SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed,
      SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END) as cancelled,
      COALESCE(SUM(cost_usd), 0) as total_cost,
      AVG(CASE WHEN status IN ('completed','failed') THEN cost_usd ELSE NULL END) as avg_cost,
      AVG(CASE WHEN status = 'completed' THEN duration_ms ELSE NULL END) as avg_duration
    FROM persona_executions WHERE ${where}
  `).get(...params) as Record<string, unknown>;

  const total = (row['total'] as number) ?? 0;
  const completed = (row['completed'] as number) ?? 0;
  const failed = (row['failed'] as number) ?? 0;
  const resolved = completed + failed;

  // Daily breakdown
  const dailyRows = database.prepare(`
    SELECT
      DATE(created_at) as day,
      COUNT(*) as count,
      COALESCE(SUM(cost_usd), 0) as cost,
      SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as day_completed,
      SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as day_failed
    FROM persona_executions WHERE ${where}
    GROUP BY DATE(created_at) ORDER BY day ASC
  `).all(...params) as Array<Record<string, unknown>>;

  const dailyBreakdown = dailyRows.map((r) => {
    const dc = (r['day_completed'] as number) ?? 0;
    const df = (r['day_failed'] as number) ?? 0;
    const dr = dc + df;
    return {
      date: r['day'] as string,
      count: (r['count'] as number) ?? 0,
      cost: (r['cost'] as number) ?? 0,
      successRate: dr > 0 ? dc / dr : null,
    };
  });

  // Top errors
  const errorRows = database.prepare(`
    SELECT error_message as message, COUNT(*) as count
    FROM persona_executions WHERE ${where} AND error_message IS NOT NULL AND error_message != ''
    GROUP BY error_message ORDER BY count DESC LIMIT 5
  `).all(...params) as Array<Record<string, unknown>>;

  const topErrors = errorRows.map((r) => ({
    message: (r['message'] as string) ?? '',
    count: (r['count'] as number) ?? 0,
  }));

  return {
    totalExecutions: total,
    completed,
    failed,
    cancelled: (row['cancelled'] as number) ?? 0,
    successRate: resolved > 0 ? completed / resolved : null,
    totalCostUsd: (row['total_cost'] as number) ?? 0,
    avgCostUsd: row['avg_cost'] != null ? Math.round((row['avg_cost'] as number) * 100) / 100 : null,
    avgDurationMs: row['avg_duration'] != null ? Math.round(row['avg_duration'] as number) : null,
    dailyBreakdown,
    topErrors,
  };
}

export function getTriggerFiringStats(db: Database.Database, triggerId: string): {
  totalFirings: number;
  completed: number;
  failed: number;
  skipped: number;
  avgDurationMs: number | null;
  totalCostUsd: number;
  successRate: number | null;
} {
  const row = db.prepare(`
    SELECT
      COUNT(*) as total,
      SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed,
      SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed,
      SUM(CASE WHEN status = 'skipped' THEN 1 ELSE 0 END) as skipped,
      AVG(CASE WHEN status = 'completed' THEN duration_ms ELSE NULL END) as avg_duration,
      COALESCE(SUM(cost_usd), 0) as total_cost
    FROM trigger_firings WHERE trigger_id = ?
  `).get(triggerId) as Record<string, unknown>;

  const total = (row['total'] as number) ?? 0;
  const completed = (row['completed'] as number) ?? 0;
  const failed = (row['failed'] as number) ?? 0;
  const resolved = completed + failed;

  return {
    totalFirings: total,
    completed,
    failed,
    skipped: (row['skipped'] as number) ?? 0,
    avgDurationMs: row['avg_duration'] != null ? Math.round(row['avg_duration'] as number) : null,
    totalCostUsd: (row['total_cost'] as number) ?? 0,
    successRate: resolved > 0 ? completed / resolved : null,
  };
}

// ---------------------------------------------------------------------------
// Batch persona validation data — single roundtrip for all related entities
// ---------------------------------------------------------------------------

export interface PersonaValidationData {
  tools: PersonaToolDefinition[];
  credentials: PersonaCredential[];
  subscriptions: PersonaEventSubscription[];
  triggers: PersonaTrigger[];
  deployment: CloudDeployment | undefined;
}

export function getPersonaValidationData(database: Database.Database, personaId: string): PersonaValidationData {
  const tools = getToolsForPersona(database, personaId);
  const credentials = listCredentialsForPersona(database, personaId);
  const subscriptions = listSubscriptionsForPersona(database, personaId);
  const triggers = listTriggersForPersona(database, personaId);
  const deployment = getDeploymentByPersona(database, personaId);
  return { tools, credentials, subscriptions, triggers, deployment };
}
