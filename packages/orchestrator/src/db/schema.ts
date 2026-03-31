import Database from 'better-sqlite3';
import type { Logger } from 'pino';
import { stmt } from './_helpers.js';

// ---------------------------------------------------------------------------
// System schema (tenant registry)
// ---------------------------------------------------------------------------

const SYSTEM_SCHEMA = `
CREATE TABLE IF NOT EXISTS tenants (
  id TEXT PRIMARY KEY,
  name TEXT,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS system_daily_metrics (
  date TEXT NOT NULL,
  total_executions INTEGER DEFAULT 0,
  completed INTEGER DEFAULT 0,
  failed INTEGER DEFAULT 0,
  cancelled INTEGER DEFAULT 0,
  total_cost_usd REAL DEFAULT 0,
  total_duration_ms INTEGER DEFAULT 0,
  total_input_tokens INTEGER DEFAULT 0,
  total_output_tokens INTEGER DEFAULT 0,
  tenant_count INTEGER DEFAULT 0,
  aggregated_at TEXT NOT NULL,
  PRIMARY KEY (date)
);

CREATE TABLE IF NOT EXISTS system_kv (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
`;

const INFERENCE_PROFILES_TABLE = `
CREATE TABLE IF NOT EXISTS inference_profiles (
  id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL DEFAULT 'default',
  name TEXT NOT NULL,
  provider TEXT NOT NULL,
  model TEXT,
  base_url TEXT,
  auth_token_encrypted TEXT,
  auth_token_iv TEXT,
  auth_token_tag TEXT,
  env_mappings TEXT NOT NULL DEFAULT '[]',
  remove_env_keys TEXT NOT NULL DEFAULT '[]',
  is_preset INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
`;

const DAILY_METRICS_TABLE = `
CREATE TABLE IF NOT EXISTS daily_metrics (
  date TEXT NOT NULL,
  total_executions INTEGER DEFAULT 0,
  completed INTEGER DEFAULT 0,
  failed INTEGER DEFAULT 0,
  cancelled INTEGER DEFAULT 0,
  total_cost_usd REAL DEFAULT 0,
  total_duration_ms INTEGER DEFAULT 0,
  total_input_tokens INTEGER DEFAULT 0,
  total_output_tokens INTEGER DEFAULT 0,
  PRIMARY KEY (date)
);

CREATE TABLE IF NOT EXISTS daily_metrics_by_persona (
  date TEXT NOT NULL,
  persona_id TEXT NOT NULL,
  total_executions INTEGER DEFAULT 0,
  completed INTEGER DEFAULT 0,
  failed INTEGER DEFAULT 0,
  cancelled INTEGER DEFAULT 0,
  total_cost_usd REAL DEFAULT 0,
  total_duration_ms INTEGER DEFAULT 0,
  total_input_tokens INTEGER DEFAULT 0,
  total_output_tokens INTEGER DEFAULT 0,
  PRIMARY KEY (date, persona_id)
);

CREATE TABLE IF NOT EXISTS retention_watermark (
  key TEXT PRIMARY KEY DEFAULT 'executions',
  last_rowid INTEGER NOT NULL DEFAULT 0
);
`;

// ---------------------------------------------------------------------------
// Shared entity tables (used by both legacy single-db and per-tenant dbs)
// ---------------------------------------------------------------------------

const SHARED_TABLES = `
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
  inference_profile_id TEXT,
  network_policy TEXT,
  max_budget_usd REAL,
  max_turns INTEGER,
  design_context TEXT,
  group_id TEXT,
  permission_policy TEXT,
  webhook_secret TEXT,
  model_profile TEXT,
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
  status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','processing','delivered','skipped','failed','partial','partial-retry','dead_letter')),
  error_message TEXT,
  processed_at TEXT,
  use_case_id TEXT,
  retry_after TEXT,
  retry_count INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_pe_status ON persona_events(status);
CREATE INDEX IF NOT EXISTS idx_pe_created_status ON persona_events(created_at, status);

CREATE TABLE IF NOT EXISTS persona_event_subscriptions (
  id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL DEFAULT 'default',
  persona_id TEXT NOT NULL REFERENCES personas(id) ON DELETE CASCADE,
  event_type TEXT NOT NULL,
  source_filter TEXT,
  payload_filter TEXT,
  enabled INTEGER NOT NULL DEFAULT 1,
  max_retries INTEGER NOT NULL DEFAULT 3,
  retry_backoff_ms INTEGER NOT NULL DEFAULT 5000,
  use_case_id TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_pes_event_type ON persona_event_subscriptions(event_type, enabled);

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
CREATE INDEX IF NOT EXISTS idx_pt_due ON persona_triggers(enabled, next_trigger_at);

CREATE TABLE IF NOT EXISTS trigger_fires (
  id TEXT PRIMARY KEY,
  trigger_id TEXT NOT NULL,
  event_id TEXT,
  execution_id TEXT,
  status TEXT NOT NULL DEFAULT 'fired' CHECK(status IN ('fired','completed','failed')),
  duration_ms INTEGER,
  error_message TEXT,
  fired_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_tf_trigger_fired ON trigger_fires(trigger_id, fired_at DESC);
CREATE INDEX IF NOT EXISTS idx_tf_trigger_status ON trigger_fires(trigger_id, status);

CREATE TABLE IF NOT EXISTS persona_executions (
  id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL DEFAULT 'default',
  persona_id TEXT NOT NULL,
  trigger_id TEXT,
  use_case_id TEXT,
  event_id TEXT,
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
CREATE INDEX IF NOT EXISTS idx_pex_created_status ON persona_executions(created_at, status);

${DAILY_METRICS_TABLE}

${INFERENCE_PROFILES_TABLE}

CREATE TABLE IF NOT EXISTS event_audit (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  event_id TEXT NOT NULL,
  action TEXT NOT NULL,
  detail TEXT,
  created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_ea_event ON event_audit(event_id, created_at);

CREATE TABLE IF NOT EXISTS event_dispatches (
  event_id TEXT NOT NULL,
  persona_id TEXT NOT NULL,
  execution_id TEXT,
  created_at TEXT NOT NULL,
  PRIMARY KEY (event_id, persona_id)
);

CREATE TABLE IF NOT EXISTS cloud_deployments (
  id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL DEFAULT 'default',
  persona_id TEXT NOT NULL,
  slug TEXT NOT NULL UNIQUE,
  label TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'active' CHECK(status IN ('active','paused','failed')),
  webhook_enabled INTEGER NOT NULL DEFAULT 1,
  webhook_secret TEXT,
  invocation_count INTEGER NOT NULL DEFAULT 0,
  last_invoked_at TEXT,
  max_monthly_budget_usd REAL,
  current_month_cost_usd REAL NOT NULL DEFAULT 0,
  budget_month TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_cd_persona ON cloud_deployments(persona_id);
CREATE INDEX IF NOT EXISTS idx_cd_slug ON cloud_deployments(slug);
`;

// ---------------------------------------------------------------------------
// Migrations (legacy single-db)
// ---------------------------------------------------------------------------

function runLegacyMigrations(db: Database.Database, logger: Logger): void {
  const tablesToMigrate = [
    'persona_credentials',
    'persona_event_subscriptions',
    'persona_triggers',
    'persona_executions',
  ];

  for (const table of tablesToMigrate) {
    const columns = stmt(db,`PRAGMA table_info(${table})`).all() as Array<{ name: string }>;
    const hasProjectId = columns.some(c => c.name === 'project_id');
    if (!hasProjectId) {
      db.exec(`ALTER TABLE ${table} ADD COLUMN project_id TEXT NOT NULL DEFAULT 'default'`);
      logger.info({ table }, 'Migration: added project_id column');
    }
  }

  db.exec(`
    CREATE INDEX IF NOT EXISTS idx_personas_project ON personas(project_id);
    CREATE INDEX IF NOT EXISTS idx_pe_project ON persona_events(project_id);
    CREATE INDEX IF NOT EXISTS idx_pcred_project ON persona_credentials(project_id);
    CREATE INDEX IF NOT EXISTS idx_pes_project ON persona_event_subscriptions(project_id);
    CREATE INDEX IF NOT EXISTS idx_pes_event_type ON persona_event_subscriptions(event_type, enabled);
    CREATE INDEX IF NOT EXISTS idx_pt_project ON persona_triggers(project_id);
    DROP INDEX IF EXISTS idx_pt_next_trigger;
    CREATE INDEX IF NOT EXISTS idx_pt_due ON persona_triggers(enabled, next_trigger_at);
    CREATE INDEX IF NOT EXISTS idx_pex_project ON persona_executions(project_id);
  `);

  // Add inference_profile_id to personas if missing
  const personaCols = stmt(db,'PRAGMA table_info(personas)').all() as Array<{ name: string }>;
  if (!personaCols.some(c => c.name === 'inference_profile_id')) {
    db.exec('ALTER TABLE personas ADD COLUMN inference_profile_id TEXT');
    logger.info('Migration: added inference_profile_id to personas');
  }
  if (!personaCols.some(c => c.name === 'network_policy')) {
    db.exec('ALTER TABLE personas ADD COLUMN network_policy TEXT');
    logger.info('Migration: added network_policy to personas');
  }

  // Create inference_profiles table if missing
  db.exec(INFERENCE_PROFILES_TABLE);

  // Add retry_after and retry_count to persona_events if missing
  const eventCols = stmt(db,'PRAGMA table_info(persona_events)').all() as Array<{ name: string }>;
  if (!eventCols.some(c => c.name === 'retry_after')) {
    db.exec('ALTER TABLE persona_events ADD COLUMN retry_after TEXT');
    logger.info('Migration: added retry_after to persona_events');
  }
  if (!eventCols.some(c => c.name === 'retry_count')) {
    db.exec('ALTER TABLE persona_events ADD COLUMN retry_count INTEGER NOT NULL DEFAULT 0');
    logger.info('Migration: added retry_count to persona_events');
  }

  // Add event_id to persona_executions for event↔execution correlation tracing
  const execCols = stmt(db,'PRAGMA table_info(persona_executions)').all() as Array<{ name: string }>;
  if (!execCols.some(c => c.name === 'event_id')) {
    db.exec('ALTER TABLE persona_executions ADD COLUMN event_id TEXT');
    logger.info('Migration: added event_id to persona_executions');
  }

  // Add new metric columns to daily_metrics
  runDailyMetricsMigrations(db, logger, 'Migration');

  // Add health_status / health_message to persona_triggers if missing
  const triggerCols = stmt(db, 'PRAGMA table_info(persona_triggers)').all() as Array<{ name: string }>;
  if (!triggerCols.some(c => c.name === 'health_status')) {
    db.exec("ALTER TABLE persona_triggers ADD COLUMN health_status TEXT NOT NULL DEFAULT 'healthy'");
    logger.info('Migration: added health_status to persona_triggers');
  }
  if (!triggerCols.some(c => c.name === 'health_message')) {
    db.exec('ALTER TABLE persona_triggers ADD COLUMN health_message TEXT');
    logger.info('Migration: added health_message to persona_triggers');
  }

  // Add permission_policy, webhook_secret, model_profile to personas if missing
  if (!personaCols.some(c => c.name === 'permission_policy')) {
    db.exec('ALTER TABLE personas ADD COLUMN permission_policy TEXT');
    logger.info('Migration: added permission_policy to personas');
  }
  if (!personaCols.some(c => c.name === 'webhook_secret')) {
    db.exec('ALTER TABLE personas ADD COLUMN webhook_secret TEXT');
    logger.info('Migration: added webhook_secret to personas');
  }
  if (!personaCols.some(c => c.name === 'model_profile')) {
    db.exec('ALTER TABLE personas ADD COLUMN model_profile TEXT');
    logger.info('Migration: added model_profile to personas');
  }

  // Add payload_filter, max_retries, retry_backoff_ms to persona_event_subscriptions if missing
  const subCols = stmt(db, 'PRAGMA table_info(persona_event_subscriptions)').all() as Array<{ name: string }>;
  if (!subCols.some(c => c.name === 'payload_filter')) {
    db.exec('ALTER TABLE persona_event_subscriptions ADD COLUMN payload_filter TEXT');
    logger.info('Migration: added payload_filter to persona_event_subscriptions');
  }
  if (!subCols.some(c => c.name === 'max_retries')) {
    db.exec('ALTER TABLE persona_event_subscriptions ADD COLUMN max_retries INTEGER NOT NULL DEFAULT 3');
    logger.info('Migration: added max_retries to persona_event_subscriptions');
  }
  if (!subCols.some(c => c.name === 'retry_backoff_ms')) {
    db.exec('ALTER TABLE persona_event_subscriptions ADD COLUMN retry_backoff_ms INTEGER NOT NULL DEFAULT 5000');
    logger.info('Migration: added retry_backoff_ms to persona_event_subscriptions');
  }

  // Create new tables if they don't exist (event_audit, event_dispatches, cloud_deployments)
  db.exec(`
    CREATE TABLE IF NOT EXISTS event_audit (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      event_id TEXT NOT NULL,
      action TEXT NOT NULL,
      detail TEXT,
      created_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_ea_event ON event_audit(event_id, created_at);

    CREATE TABLE IF NOT EXISTS event_dispatches (
      event_id TEXT NOT NULL,
      persona_id TEXT NOT NULL,
      execution_id TEXT,
      created_at TEXT NOT NULL,
      PRIMARY KEY (event_id, persona_id)
    );

    CREATE TABLE IF NOT EXISTS cloud_deployments (
      id TEXT PRIMARY KEY,
      project_id TEXT NOT NULL DEFAULT 'default',
      persona_id TEXT NOT NULL,
      slug TEXT NOT NULL UNIQUE,
      label TEXT NOT NULL,
      status TEXT NOT NULL DEFAULT 'active' CHECK(status IN ('active','paused','failed')),
      webhook_enabled INTEGER NOT NULL DEFAULT 1,
      webhook_secret TEXT,
      invocation_count INTEGER NOT NULL DEFAULT 0,
      last_invoked_at TEXT,
      max_monthly_budget_usd REAL,
      current_month_cost_usd REAL NOT NULL DEFAULT 0,
      budget_month TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_cd_persona ON cloud_deployments(persona_id);
    CREATE INDEX IF NOT EXISTS idx_cd_slug ON cloud_deployments(slug);
  `);
}

// ---------------------------------------------------------------------------
// Initialization
// ---------------------------------------------------------------------------

/** Legacy: initialize a single monolithic database (used by migration script). */
export function initDb(dbPath: string, logger: Logger): Database.Database {
  const db = new Database(dbPath);
  db.pragma('journal_mode = WAL');
  db.pragma('foreign_keys = ON');

  db.exec(SHARED_TABLES);
  runLegacyMigrations(db, logger);
  logger.info({ dbPath }, 'Database initialized (legacy single-db)');
  return db;
}

/** Initialize the system-level database (tenant registry). */
export function initSystemDb(dbPath: string, logger: Logger): Database.Database {
  const db = new Database(dbPath);
  db.pragma('journal_mode = WAL');
  db.pragma('foreign_keys = ON');

  db.exec(SYSTEM_SCHEMA);
  runSystemMigrations(db, logger);
  logger.info({ dbPath }, 'System database initialized');
  return db;
}

/** Migrations applied to the system database. */
function runSystemMigrations(db: Database.Database, logger: Logger): void {
  const cols = stmt(db, 'PRAGMA table_info(system_daily_metrics)').all() as Array<{ name: string }>;
  if (!cols.some(c => c.name === 'cancelled')) {
    db.exec('ALTER TABLE system_daily_metrics ADD COLUMN cancelled INTEGER DEFAULT 0');
    logger.info('System migration: added cancelled to system_daily_metrics');
  }
  if (!cols.some(c => c.name === 'total_input_tokens')) {
    db.exec('ALTER TABLE system_daily_metrics ADD COLUMN total_input_tokens INTEGER DEFAULT 0');
    logger.info('System migration: added total_input_tokens to system_daily_metrics');
  }
  if (!cols.some(c => c.name === 'total_output_tokens')) {
    db.exec('ALTER TABLE system_daily_metrics ADD COLUMN total_output_tokens INTEGER DEFAULT 0');
    logger.info('System migration: added total_output_tokens to system_daily_metrics');
  }
}

/** Initialize a per-tenant database. */
export function initTenantDb(dbPath: string, logger: Logger): Database.Database {
  const db = new Database(dbPath);
  db.pragma('journal_mode = WAL');
  db.pragma('foreign_keys = ON');

  db.exec(SHARED_TABLES);
  runTenantMigrations(db, logger);
  logger.info({ dbPath }, 'Tenant database initialized');
  return db;
}

/** Add cancelled, token, and per-persona columns to daily_metrics tables. */
function runDailyMetricsMigrations(db: Database.Database, logger: Logger, prefix: string): void {
  const metricsCols = stmt(db, 'PRAGMA table_info(daily_metrics)').all() as Array<{ name: string }>;
  if (!metricsCols.some(c => c.name === 'cancelled')) {
    db.exec('ALTER TABLE daily_metrics ADD COLUMN cancelled INTEGER DEFAULT 0');
    logger.info(`${prefix}: added cancelled to daily_metrics`);
  }
  if (!metricsCols.some(c => c.name === 'total_input_tokens')) {
    db.exec('ALTER TABLE daily_metrics ADD COLUMN total_input_tokens INTEGER DEFAULT 0');
    logger.info(`${prefix}: added total_input_tokens to daily_metrics`);
  }
  if (!metricsCols.some(c => c.name === 'total_output_tokens')) {
    db.exec('ALTER TABLE daily_metrics ADD COLUMN total_output_tokens INTEGER DEFAULT 0');
    logger.info(`${prefix}: added total_output_tokens to daily_metrics`);
  }

  // Create daily_metrics_by_persona if missing
  db.exec(`
    CREATE TABLE IF NOT EXISTS daily_metrics_by_persona (
      date TEXT NOT NULL,
      persona_id TEXT NOT NULL,
      total_executions INTEGER DEFAULT 0,
      completed INTEGER DEFAULT 0,
      failed INTEGER DEFAULT 0,
      cancelled INTEGER DEFAULT 0,
      total_cost_usd REAL DEFAULT 0,
      total_duration_ms INTEGER DEFAULT 0,
      total_input_tokens INTEGER DEFAULT 0,
      total_output_tokens INTEGER DEFAULT 0,
      PRIMARY KEY (date, persona_id)
    )
  `);
}

/** Migrations applied to per-tenant databases. */
function runTenantMigrations(db: Database.Database, logger: Logger): void {
  const eventCols = stmt(db,'PRAGMA table_info(persona_events)').all() as Array<{ name: string }>;
  if (!eventCols.some(c => c.name === 'retry_after')) {
    db.exec('ALTER TABLE persona_events ADD COLUMN retry_after TEXT');
    logger.info('Tenant migration: added retry_after to persona_events');
  }
  if (!eventCols.some(c => c.name === 'retry_count')) {
    db.exec('ALTER TABLE persona_events ADD COLUMN retry_count INTEGER NOT NULL DEFAULT 0');
    logger.info('Tenant migration: added retry_count to persona_events');
  }

  // Add event_id to persona_executions for event↔execution correlation tracing
  const execCols = stmt(db,'PRAGMA table_info(persona_executions)').all() as Array<{ name: string }>;
  if (!execCols.some(c => c.name === 'event_id')) {
    db.exec('ALTER TABLE persona_executions ADD COLUMN event_id TEXT');
    logger.info('Tenant migration: added event_id to persona_executions');
  }

  // Add new metric columns to daily_metrics
  runDailyMetricsMigrations(db, logger, 'Tenant migration');

  // Add health_status / health_message to persona_triggers if missing
  const triggerCols = stmt(db, 'PRAGMA table_info(persona_triggers)').all() as Array<{ name: string }>;
  if (!triggerCols.some(c => c.name === 'health_status')) {
    db.exec("ALTER TABLE persona_triggers ADD COLUMN health_status TEXT NOT NULL DEFAULT 'healthy'");
    logger.info('Tenant migration: added health_status to persona_triggers');
  }
  if (!triggerCols.some(c => c.name === 'health_message')) {
    db.exec('ALTER TABLE persona_triggers ADD COLUMN health_message TEXT');
    logger.info('Tenant migration: added health_message to persona_triggers');
  }

  // Add permission_policy, webhook_secret, model_profile to personas if missing
  const personaCols = stmt(db, 'PRAGMA table_info(personas)').all() as Array<{ name: string }>;
  if (!personaCols.some(c => c.name === 'permission_policy')) {
    db.exec('ALTER TABLE personas ADD COLUMN permission_policy TEXT');
    logger.info('Tenant migration: added permission_policy to personas');
  }
  if (!personaCols.some(c => c.name === 'webhook_secret')) {
    db.exec('ALTER TABLE personas ADD COLUMN webhook_secret TEXT');
    logger.info('Tenant migration: added webhook_secret to personas');
  }
  if (!personaCols.some(c => c.name === 'model_profile')) {
    db.exec('ALTER TABLE personas ADD COLUMN model_profile TEXT');
    logger.info('Tenant migration: added model_profile to personas');
  }

  // Add payload_filter, max_retries, retry_backoff_ms to persona_event_subscriptions if missing
  const subCols = stmt(db, 'PRAGMA table_info(persona_event_subscriptions)').all() as Array<{ name: string }>;
  if (!subCols.some(c => c.name === 'payload_filter')) {
    db.exec('ALTER TABLE persona_event_subscriptions ADD COLUMN payload_filter TEXT');
    logger.info('Tenant migration: added payload_filter to persona_event_subscriptions');
  }
  if (!subCols.some(c => c.name === 'max_retries')) {
    db.exec('ALTER TABLE persona_event_subscriptions ADD COLUMN max_retries INTEGER NOT NULL DEFAULT 3');
    logger.info('Tenant migration: added max_retries to persona_event_subscriptions');
  }
  if (!subCols.some(c => c.name === 'retry_backoff_ms')) {
    db.exec('ALTER TABLE persona_event_subscriptions ADD COLUMN retry_backoff_ms INTEGER NOT NULL DEFAULT 5000');
    logger.info('Tenant migration: added retry_backoff_ms to persona_event_subscriptions');
  }

  // Create new tables if they don't exist
  db.exec(`
    CREATE TABLE IF NOT EXISTS event_audit (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      event_id TEXT NOT NULL,
      action TEXT NOT NULL,
      detail TEXT,
      created_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_ea_event ON event_audit(event_id, created_at);

    CREATE TABLE IF NOT EXISTS event_dispatches (
      event_id TEXT NOT NULL,
      persona_id TEXT NOT NULL,
      execution_id TEXT,
      created_at TEXT NOT NULL,
      PRIMARY KEY (event_id, persona_id)
    );

    CREATE TABLE IF NOT EXISTS cloud_deployments (
      id TEXT PRIMARY KEY,
      project_id TEXT NOT NULL DEFAULT 'default',
      persona_id TEXT NOT NULL,
      slug TEXT NOT NULL UNIQUE,
      label TEXT NOT NULL,
      status TEXT NOT NULL DEFAULT 'active' CHECK(status IN ('active','paused','failed')),
      webhook_enabled INTEGER NOT NULL DEFAULT 1,
      webhook_secret TEXT,
      invocation_count INTEGER NOT NULL DEFAULT 0,
      last_invoked_at TEXT,
      max_monthly_budget_usd REAL,
      current_month_cost_usd REAL NOT NULL DEFAULT 0,
      budget_month TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_cd_persona ON cloud_deployments(persona_id);
    CREATE INDEX IF NOT EXISTS idx_cd_slug ON cloud_deployments(slug);
  `);
}
