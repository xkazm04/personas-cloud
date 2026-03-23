#!/usr/bin/env node
/**
 * Migration script: single personas.db → per-tenant databases.
 *
 * Usage:
 *   npx tsx packages/orchestrator/src/cli/migrate-to-tenant-dbs.ts [oldDbPath] [dataDir]
 *
 * Defaults:
 *   oldDbPath = data/personas.db
 *   dataDir   = data/
 *
 * What it does:
 *   1. Opens the legacy single-file personas.db
 *   2. Discovers all distinct project_id values
 *   3. For each project_id, creates a tenant DB via TenantDbManager
 *   4. Copies all rows from the legacy DB into the correct tenant DB
 *   5. Prints a summary
 *
 * The legacy DB is NOT deleted — you can remove it manually after verifying.
 */
import path from 'node:path';
import pino from 'pino';
import { initDb } from '../db/index.js';
import { TenantDbManager } from '../tenantDbManager.js';
import * as db from '../db/index.js';

const logger = pino({ level: 'info' });

const oldDbPath = process.argv[2] || path.join(process.cwd(), 'data', 'personas.db');
const dataDir = process.argv[3] || path.join(process.cwd(), 'data');

async function main() {
  logger.info({ oldDbPath, dataDir }, 'Starting migration to tenant databases');

  // Open legacy DB
  const legacyDb = initDb(oldDbPath, logger);

  // Create tenant DB manager
  const mgr = new TenantDbManager(dataDir, logger);

  // Discover all project IDs
  const projectIds = db.listDistinctProjectIds(legacyDb);
  logger.info({ projectIds }, `Found ${projectIds.length} project(s) to migrate`);

  const stats: Record<string, Record<string, number>> = {};

  for (const projectId of projectIds) {
    logger.info({ projectId }, `Migrating project: ${projectId}`);
    const tenantDb = mgr.getTenantDb(projectId);
    const counts: Record<string, number> = {};

    // Migrate personas
    const personas = db.listPersonas(legacyDb, projectId);
    for (const p of personas) {
      db.upsertPersona(tenantDb, p);
    }
    counts['personas'] = personas.length;

    // Migrate tool definitions + links
    // Tool definitions are global — copy all of them into each tenant DB
    const allTools = legacyDb.prepare('SELECT * FROM persona_tool_definitions').all() as Record<string, unknown>[];
    for (const toolRow of allTools) {
      tenantDb.prepare(`
        INSERT OR IGNORE INTO persona_tool_definitions (id, name, category, description, script_path, input_schema, output_schema, requires_credential_type, implementation_guide, is_builtin, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `).run(
        toolRow['id'], toolRow['name'], toolRow['category'], toolRow['description'],
        toolRow['script_path'], toolRow['input_schema'], toolRow['output_schema'],
        toolRow['requires_credential_type'], toolRow['implementation_guide'],
        toolRow['is_builtin'], toolRow['created_at'], toolRow['updated_at'],
      );
    }
    counts['tool_definitions'] = allTools.length;

    // Migrate persona_tools links (only for personas in this project)
    const personaIds = personas.map(p => p.id);
    if (personaIds.length > 0) {
      const placeholders = personaIds.map(() => '?').join(',');
      const toolLinks = legacyDb.prepare(`SELECT * FROM persona_tools WHERE persona_id IN (${placeholders})`).all(...personaIds) as Array<{ persona_id: string; tool_id: string }>;
      for (const link of toolLinks) {
        tenantDb.prepare('INSERT OR IGNORE INTO persona_tools (persona_id, tool_id) VALUES (?, ?)').run(link.persona_id, link.tool_id);
      }
      counts['tool_links'] = toolLinks.length;

      // Migrate credential links
      const credLinks = legacyDb.prepare(`SELECT * FROM persona_credential_links WHERE persona_id IN (${placeholders})`).all(...personaIds) as Array<{ persona_id: string; credential_id: string }>;
      for (const link of credLinks) {
        tenantDb.prepare('INSERT OR IGNORE INTO persona_credential_links (persona_id, credential_id) VALUES (?, ?)').run(link.persona_id, link.credential_id);
      }
      counts['credential_links'] = credLinks.length;
    }

    // Migrate credentials
    const creds = legacyDb.prepare('SELECT * FROM persona_credentials WHERE project_id = ?').all(projectId) as Record<string, unknown>[];
    for (const c of creds) {
      tenantDb.prepare(`
        INSERT OR IGNORE INTO persona_credentials (id, project_id, name, service_type, encrypted_data, iv, tag, metadata, last_used_at, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `).run(c['id'], c['project_id'], c['name'], c['service_type'], c['encrypted_data'], c['iv'], c['tag'], c['metadata'], c['last_used_at'], c['created_at'], c['updated_at']);
    }
    counts['credentials'] = creds.length;

    // Migrate events
    const events = legacyDb.prepare('SELECT * FROM persona_events WHERE project_id = ?').all(projectId) as Record<string, unknown>[];
    for (const e of events) {
      tenantDb.prepare(`
        INSERT OR IGNORE INTO persona_events (id, project_id, event_type, source_type, source_id, target_persona_id, payload, status, error_message, processed_at, use_case_id, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `).run(e['id'], e['project_id'], e['event_type'], e['source_type'], e['source_id'], e['target_persona_id'], e['payload'], e['status'], e['error_message'], e['processed_at'], e['use_case_id'], e['created_at']);
    }
    counts['events'] = events.length;

    // Migrate subscriptions
    const subs = legacyDb.prepare('SELECT * FROM persona_event_subscriptions WHERE project_id = ?').all(projectId) as Record<string, unknown>[];
    for (const s of subs) {
      tenantDb.prepare(`
        INSERT OR IGNORE INTO persona_event_subscriptions (id, project_id, persona_id, event_type, source_filter, enabled, use_case_id, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
      `).run(s['id'], s['project_id'], s['persona_id'], s['event_type'], s['source_filter'], s['enabled'], s['use_case_id'], s['created_at'], s['updated_at']);
    }
    counts['subscriptions'] = subs.length;

    // Migrate triggers
    const triggers = legacyDb.prepare('SELECT * FROM persona_triggers WHERE project_id = ?').all(projectId) as Record<string, unknown>[];
    for (const t of triggers) {
      tenantDb.prepare(`
        INSERT OR IGNORE INTO persona_triggers (id, project_id, persona_id, trigger_type, config, enabled, last_triggered_at, next_trigger_at, use_case_id, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `).run(t['id'], t['project_id'], t['persona_id'], t['trigger_type'], t['config'], t['enabled'], t['last_triggered_at'], t['next_trigger_at'], t['use_case_id'], t['created_at'], t['updated_at']);
    }
    counts['triggers'] = triggers.length;

    // Migrate executions
    const execs = legacyDb.prepare('SELECT * FROM persona_executions WHERE project_id = ?').all(projectId) as Record<string, unknown>[];
    for (const ex of execs) {
      tenantDb.prepare(`
        INSERT OR IGNORE INTO persona_executions (id, project_id, persona_id, trigger_id, use_case_id, status, input_data, output_data, claude_session_id, model_used, input_tokens, output_tokens, cost_usd, error_message, duration_ms, retry_of_execution_id, retry_count, started_at, completed_at, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `).run(
        ex['id'], ex['project_id'], ex['persona_id'], ex['trigger_id'], ex['use_case_id'],
        ex['status'], ex['input_data'], ex['output_data'], ex['claude_session_id'], ex['model_used'],
        ex['input_tokens'], ex['output_tokens'], ex['cost_usd'], ex['error_message'], ex['duration_ms'],
        ex['retry_of_execution_id'], ex['retry_count'], ex['started_at'], ex['completed_at'], ex['created_at'],
      );
    }
    counts['executions'] = execs.length;

    stats[projectId] = counts;
    logger.info({ projectId, counts }, 'Project migration complete');
  }

  // Summary
  console.log('\n=== Migration Summary ===');
  for (const [pid, counts] of Object.entries(stats)) {
    console.log(`\nProject: ${pid}`);
    for (const [table, count] of Object.entries(counts)) {
      console.log(`  ${table}: ${count}`);
    }
  }
  console.log(`\nTotal projects migrated: ${Object.keys(stats).length}`);
  console.log(`Legacy DB preserved at: ${oldDbPath}`);
  console.log('Verify data and remove legacy DB manually when ready.\n');

  mgr.close();
  legacyDb.close();
}

main().catch((err) => {
  logger.fatal({ err }, 'Migration failed');
  process.exit(1);
});
