import Database from 'better-sqlite3';
import { nanoid } from 'nanoid';
import type { CloudDeployment, DeploymentStatus } from '@dac-cloud/shared';
import { stmt, createRowMapper } from './_helpers.js';

// ---------------------------------------------------------------------------
// Row mapper
// ---------------------------------------------------------------------------

const rowToDeployment = createRowMapper<CloudDeployment>({
  id: { col: 'id' },
  projectId: { col: 'project_id', default: 'default' },
  personaId: { col: 'persona_id' },
  slug: { col: 'slug' },
  label: { col: 'label' },
  status: { col: 'status', default: 'active' },
  webhookEnabled: { col: 'webhook_enabled', bool: true },
  webhookSecret: { col: 'webhook_secret', nullable: true },
  invocationCount: { col: 'invocation_count', default: 0 },
  lastInvokedAt: { col: 'last_invoked_at', nullable: true },
  maxMonthlyBudgetUsd: { col: 'max_monthly_budget_usd', nullable: true },
  currentMonthCostUsd: { col: 'current_month_cost_usd', default: 0 },
  budgetMonth: { col: 'budget_month', nullable: true },
  createdAt: { col: 'created_at' },
  updatedAt: { col: 'updated_at' },
});

// ---------------------------------------------------------------------------
// CRUD
// ---------------------------------------------------------------------------

export function listDeployments(db: Database.Database, projectId?: string): CloudDeployment[] {
  if (projectId) {
    const rows = stmt(db, 'SELECT * FROM cloud_deployments WHERE project_id = ? ORDER BY created_at DESC').all(projectId) as Record<string, unknown>[];
    return rows.map(rowToDeployment);
  }
  const rows = stmt(db, 'SELECT * FROM cloud_deployments ORDER BY created_at DESC').all() as Record<string, unknown>[];
  return rows.map(rowToDeployment);
}

export function getDeployment(db: Database.Database, id: string): CloudDeployment | undefined {
  const row = stmt(db, 'SELECT * FROM cloud_deployments WHERE id = ?').get(id) as Record<string, unknown> | undefined;
  return row ? rowToDeployment(row) : undefined;
}

export function getDeploymentByPersona(db: Database.Database, personaId: string): CloudDeployment | undefined {
  const row = stmt(db, 'SELECT * FROM cloud_deployments WHERE persona_id = ?').get(personaId) as Record<string, unknown> | undefined;
  return row ? rowToDeployment(row) : undefined;
}

export function getDeploymentBySlug(db: Database.Database, slug: string): CloudDeployment | undefined {
  const row = stmt(db, 'SELECT * FROM cloud_deployments WHERE slug = ?').get(slug) as Record<string, unknown> | undefined;
  return row ? rowToDeployment(row) : undefined;
}

export function createDeployment(db: Database.Database, input: {
  personaId: string;
  slug: string;
  label: string;
  webhookSecret?: string | null;
  projectId?: string;
  maxMonthlyBudgetUsd?: number | null;
}): CloudDeployment {
  const now = new Date().toISOString();
  const id = nanoid();
  const projectId = input.projectId ?? 'default';
  const webhookSecret = input.webhookSecret ?? null;
  const maxMonthlyBudgetUsd = input.maxMonthlyBudgetUsd ?? null;
  stmt(db, `
    INSERT INTO cloud_deployments (id, project_id, persona_id, slug, label, status, webhook_enabled, webhook_secret, invocation_count, max_monthly_budget_usd, current_month_cost_usd, created_at, updated_at)
    VALUES (?, ?, ?, ?, ?, 'active', 1, ?, 0, ?, 0, ?, ?)
  `).run(id, projectId, input.personaId, input.slug, input.label, webhookSecret, maxMonthlyBudgetUsd, now, now);
  return {
    id,
    projectId,
    personaId: input.personaId,
    slug: input.slug,
    label: input.label,
    status: 'active',
    webhookEnabled: true,
    webhookSecret,
    invocationCount: 0,
    lastInvokedAt: null,
    maxMonthlyBudgetUsd,
    currentMonthCostUsd: 0,
    budgetMonth: null,
    createdAt: now,
    updatedAt: now,
  };
}

export function updateDeploymentStatus(db: Database.Database, id: string, status: DeploymentStatus): void {
  const now = new Date().toISOString();
  stmt(db, 'UPDATE cloud_deployments SET status = ?, updated_at = ? WHERE id = ?').run(status, now, id);
}

export function deleteDeployment(db: Database.Database, id: string, projectId?: string): void {
  if (projectId) {
    stmt(db, 'DELETE FROM cloud_deployments WHERE id = ? AND project_id = ?').run(id, projectId);
  } else {
    stmt(db, 'DELETE FROM cloud_deployments WHERE id = ?').run(id);
  }
}

export function incrementDeploymentInvocation(db: Database.Database, id: string): void {
  const now = new Date().toISOString();
  stmt(db, 'UPDATE cloud_deployments SET invocation_count = invocation_count + 1, last_invoked_at = ?, updated_at = ? WHERE id = ?').run(now, now, id);
}
