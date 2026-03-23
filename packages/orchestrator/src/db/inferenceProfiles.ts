import Database from 'better-sqlite3';
import type { InferenceProfile } from '@dac-cloud/shared';
import { stmt, createRowMapper } from './_helpers.js';

// ---------------------------------------------------------------------------
// Row mapper
// ---------------------------------------------------------------------------

const rowToInferenceProfile = createRowMapper<InferenceProfile>({
  id: { col: 'id' },
  projectId: { col: 'project_id', default: 'default' },
  name: { col: 'name' },
  provider: { col: 'provider' },
  model: { col: 'model', optional: true },
  baseUrl: { col: 'base_url', optional: true },
  authTokenEncrypted: { col: 'auth_token_encrypted', optional: true },
  authTokenIv: { col: 'auth_token_iv', optional: true },
  authTokenTag: { col: 'auth_token_tag', optional: true },
  envMappings: { col: 'env_mappings', json: true, default: '[]' },
  removeEnvKeys: { col: 'remove_env_keys', json: true, default: '[]' },
  isPreset: { col: 'is_preset', bool: true },
  createdAt: { col: 'created_at' },
  updatedAt: { col: 'updated_at' },
});

// ---------------------------------------------------------------------------
// CRUD
// ---------------------------------------------------------------------------

export function getInferenceProfile(db: Database.Database, id: string): InferenceProfile | undefined {
  const row = stmt(db,'SELECT * FROM inference_profiles WHERE id = ?').get(id) as Record<string, unknown> | undefined;
  return row ? rowToInferenceProfile(row) : undefined;
}

export function listInferenceProfiles(db: Database.Database, projectId?: string): InferenceProfile[] {
  if (projectId) {
    const rows = stmt(db,'SELECT * FROM inference_profiles WHERE project_id = ? ORDER BY name').all(projectId) as Record<string, unknown>[];
    return rows.map(rowToInferenceProfile);
  }
  return (stmt(db,'SELECT * FROM inference_profiles ORDER BY name').all() as Record<string, unknown>[]).map(rowToInferenceProfile);
}

export function upsertInferenceProfile(db: Database.Database, p: InferenceProfile): void {
  stmt(db,`
    INSERT INTO inference_profiles (id, project_id, name, provider, model, base_url, auth_token_encrypted, auth_token_iv, auth_token_tag, env_mappings, remove_env_keys, is_preset, created_at, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(id) DO UPDATE SET
      name = excluded.name, provider = excluded.provider, model = excluded.model, base_url = excluded.base_url,
      auth_token_encrypted = excluded.auth_token_encrypted, auth_token_iv = excluded.auth_token_iv, auth_token_tag = excluded.auth_token_tag,
      env_mappings = excluded.env_mappings, remove_env_keys = excluded.remove_env_keys, is_preset = excluded.is_preset, updated_at = excluded.updated_at
  `).run(
    p.id, p.projectId, p.name, p.provider, p.model ?? null, p.baseUrl ?? null,
    p.authTokenEncrypted ?? null, p.authTokenIv ?? null, p.authTokenTag ?? null,
    JSON.stringify(p.envMappings), JSON.stringify(p.removeEnvKeys ?? []),
    p.isPreset ? 1 : 0, p.createdAt, p.updatedAt,
  );
}

export function deleteInferenceProfile(db: Database.Database, id: string): void {
  stmt(db,'DELETE FROM inference_profiles WHERE id = ?').run(id);
}
