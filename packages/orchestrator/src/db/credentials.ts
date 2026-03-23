import Database from 'better-sqlite3';
import { nanoid } from 'nanoid';
import type { PersonaCredential } from '@dac-cloud/shared';
import { stmt, createRowMapper } from './_helpers.js';

// ---------------------------------------------------------------------------
// Row mappers
// ---------------------------------------------------------------------------

const rowToCredential = createRowMapper<PersonaCredential>({
  id: { col: 'id' },
  projectId: { col: 'project_id', default: 'default' },
  name: { col: 'name' },
  serviceType: { col: 'service_type' },
  encryptedData: { col: 'encrypted_data' },
  iv: { col: 'iv' },
  tag: { col: 'tag' },
  metadata: { col: 'metadata', nullable: true },
  lastUsedAt: { col: 'last_used_at', nullable: true },
  createdAt: { col: 'created_at' },
  updatedAt: { col: 'updated_at' },
});

export const rowToRedactedCredential = createRowMapper<Omit<PersonaCredential, 'encryptedData' | 'iv' | 'tag'>>({
  id: { col: 'id' },
  projectId: { col: 'project_id', default: 'default' },
  name: { col: 'name' },
  serviceType: { col: 'service_type' },
  metadata: { col: 'metadata', nullable: true },
  lastUsedAt: { col: 'last_used_at', nullable: true },
  createdAt: { col: 'created_at' },
  updatedAt: { col: 'updated_at' },
});

// ---------------------------------------------------------------------------
// CRUD
// ---------------------------------------------------------------------------

export function createCredential(db: Database.Database, input: {
  name: string; serviceType: string; encryptedData: string; iv: string; tag: string; metadata?: string | null; projectId?: string;
}): PersonaCredential {
  const now = new Date().toISOString();
  const id = nanoid();
  const projectId = input.projectId ?? 'default';
  const metadata = input.metadata ?? null;
  stmt(db,`
    INSERT INTO persona_credentials (id, project_id, name, service_type, encrypted_data, iv, tag, metadata, created_at, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `).run(id, projectId, input.name, input.serviceType, input.encryptedData, input.iv, input.tag, metadata, now, now);
  return {
    id,
    projectId,
    name: input.name,
    serviceType: input.serviceType,
    encryptedData: input.encryptedData,
    iv: input.iv,
    tag: input.tag,
    metadata,
    lastUsedAt: null,
    createdAt: now,
    updatedAt: now,
  };
}

export function getCredential(db: Database.Database, id: string): PersonaCredential | undefined {
  const row = stmt(db,'SELECT * FROM persona_credentials WHERE id = ?').get(id) as Record<string, unknown> | undefined;
  return row ? rowToCredential(row) : undefined;
}

export function listCredentialsForPersona(db: Database.Database, personaId: string): PersonaCredential[] {
  const rows = stmt(db,`
    SELECT c.* FROM persona_credentials c
    JOIN persona_credential_links cl ON cl.credential_id = c.id
    WHERE cl.persona_id = ?
  `).all(personaId) as Record<string, unknown>[];
  return rows.map(rowToCredential);
}

export function linkCredential(db: Database.Database, personaId: string, credentialId: string): void {
  stmt(db,'INSERT OR IGNORE INTO persona_credential_links (persona_id, credential_id) VALUES (?, ?)').run(personaId, credentialId);
}

export function deleteCredential(db: Database.Database, id: string, projectId?: string): number {
  if (projectId) {
    const result = stmt(db,'DELETE FROM persona_credentials WHERE id = ? AND project_id = ?').run(id, projectId);
    return result.changes;
  }
  const result = stmt(db,'DELETE FROM persona_credentials WHERE id = ?').run(id);
  return result.changes;
}
