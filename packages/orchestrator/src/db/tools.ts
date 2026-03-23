import Database from 'better-sqlite3';
import type { PersonaToolDefinition } from '@dac-cloud/shared';
import { stmt, createRowMapper } from './_helpers.js';

// ---------------------------------------------------------------------------
// Row mapper
// ---------------------------------------------------------------------------

export const rowToTool = createRowMapper<PersonaToolDefinition>({
  id: { col: 'id' },
  name: { col: 'name' },
  category: { col: 'category' },
  description: { col: 'description' },
  scriptPath: { col: 'script_path' },
  inputSchema: { col: 'input_schema', nullable: true },
  outputSchema: { col: 'output_schema', nullable: true },
  requiresCredentialType: { col: 'requires_credential_type', nullable: true },
  implementationGuide: { col: 'implementation_guide', nullable: true },
  isBuiltin: { col: 'is_builtin', bool: true },
  createdAt: { col: 'created_at' },
  updatedAt: { col: 'updated_at' },
});

// ---------------------------------------------------------------------------
// CRUD
// ---------------------------------------------------------------------------

export function getToolsForPersona(db: Database.Database, personaId: string): PersonaToolDefinition[] {
  const rows = stmt(db,`
    SELECT t.* FROM persona_tool_definitions t
    JOIN persona_tools pt ON pt.tool_id = t.id
    WHERE pt.persona_id = ?
  `).all(personaId) as Record<string, unknown>[];
  return rows.map(rowToTool);
}

/** Batch-fetch tools for multiple personas. Returns a Map keyed by persona ID. */
export function getToolsForPersonaIds(db: Database.Database, personaIds: string[]): Map<string, PersonaToolDefinition[]> {
  const map = new Map<string, PersonaToolDefinition[]>();
  if (personaIds.length === 0) return map;
  const placeholders = personaIds.map(() => '?').join(',');
  const rows = db.prepare(`
    SELECT t.*, pt.persona_id AS _persona_id FROM persona_tool_definitions t
    JOIN persona_tools pt ON pt.tool_id = t.id
    WHERE pt.persona_id IN (${placeholders})
  `).all(...personaIds) as Array<Record<string, unknown>>;
  for (const row of rows) {
    const personaId = row._persona_id as string;
    const tool = rowToTool(row);
    let arr = map.get(personaId);
    if (!arr) {
      arr = [];
      map.set(personaId, arr);
    }
    arr.push(tool);
  }
  return map;
}

export function upsertToolDefinition(db: Database.Database, t: PersonaToolDefinition): void {
  stmt(db,`
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
  stmt(db,'INSERT OR IGNORE INTO persona_tools (persona_id, tool_id) VALUES (?, ?)').run(personaId, toolId);
}

export function unlinkTool(db: Database.Database, personaId: string, toolId: string): void {
  stmt(db,'DELETE FROM persona_tools WHERE persona_id = ? AND tool_id = ?').run(personaId, toolId);
}
