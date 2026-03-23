import http from 'node:http';
import { nanoid } from 'nanoid';
import { encrypt, createPersonaWithDefaults, createToolDefinitionWithDefaults, type ExecRequest, type Persona, type PersonaToolDefinition } from '@dac-cloud/shared';
import type Database from 'better-sqlite3';
import type { Auth, RequestContext } from './auth.js';
import type { Dispatcher, SubmitResult } from './dispatcher.js';
import type { WorkerPool } from './workerPool.js';
import type { TokenManager } from './tokenManager.js';
import type { OAuthManager } from './oauth.js';
import type { KafkaClient } from './kafka.js';
import { handleOAuthRoute } from './oauthHttpHandlers.js';
import { handleTriggerRoute } from './triggerHttpHandlers.js';
import type { TriggerSchedulerHandle } from './triggerScheduler.js';
import type { TenantDbManager } from './tenantDbManager.js';
import type { TenantKeyManager } from './tenantKeyManager.js';
import type { AuditLog, AuditEntry, AuditAction, AuditLogHealth } from './auditLog.js';
import type { EventProcessorHealth } from './eventProcessor.js';
import type { RetentionHealth } from './retention.js';
import type { Logger } from 'pino';
import * as db from './db/index.js';
import { readBody, json } from './httpUtils.js';

// --- Route types ---

interface ParsedRoute {
  resource: string;
  id?: string;
  subResource?: string;
}

type RouteHandler = (
  req: http.IncomingMessage,
  res: http.ServerResponse,
  ctx: RequestContext,
  route: ParsedRoute | null,
  queryString: string | undefined,
  tenantDb: Database.Database | undefined,
  scopedProjectId: string | undefined,
  reqLogger: Logger,
) => Promise<void> | void;

interface RouteEntry {
  handler: RouteHandler;
  needsDb?: boolean;
  adminOnly?: boolean;
}

/** Build a lookup key for resource-based routes: `METHOD:resource[:id][:subResource]` */
function resourceKey(method: string, resource: string, hasId: boolean, subResource?: string): string {
  return `${method}:${resource}${hasId ? ':id' : ''}${subResource ? ':' + subResource : ''}`;
}

/** Parse `/api/{resource}[/{id}[/{subResource}]]` into structured parts. */
function parseRoute(pathname: string): ParsedRoute | null {
  if (!pathname.startsWith('/api/')) return null;
  const segments = pathname.slice(5).split('/').filter(Boolean);
  if (segments.length === 0) return null;
  return {
    resource: segments[0]!,
    id: segments[1],
    subResource: segments[2],
  };
}

export function createHttpApi(
  auth: Auth,
  dispatcher: Dispatcher,
  pool: WorkerPool,
  tokenManager: TokenManager,
  oauth: OAuthManager,
  logger: Logger,
  database?: Database.Database,
  tenantDbManager?: TenantDbManager,
  tenantKeyManager?: TenantKeyManager,
  auditLog?: AuditLog,
  getEventProcessorHealth?: () => EventProcessorHealth,
  kafka?: KafkaClient,
  onEventPublished?: () => void,
  triggerScheduler?: TriggerSchedulerHandle,
  getRetentionHealth?: () => RetentionHealth,
): http.Server {

  /** Wrap a mutation handler to emit an audit event on successful response. */
  function withHttpAudit(handler: RouteHandler, action: AuditAction, resourceType: string): RouteHandler {
    return async (req, res, ctx, route, qs, tenantDb, scopedProjectId, reqLogger) => {
      await handler(req, res, ctx, route, qs, tenantDb, scopedProjectId, reqLogger);
      if (res.statusCode >= 200 && res.statusCode < 300 && auditLog) {
        try {
          auditLog.record({
            action,
            tenantId: ctx.projectId || 'default',
            actor: ctx.userEmail || ctx.projectId,
            resourceType,
            resourceId: route?.id,
            ipAddress: req.socket.remoteAddress,
          } as AuditEntry);
        } catch { /* logged inside record() */ }
      }
    };
  }

  // ── Exact-path route table (O(1) lookup by `METHOD:/path`) ──
  const exactRoutes = new Map<string, RouteEntry>();

  // OAuth + token injection (admin-only, delegated to oauthHttpHandlers)
  for (const [path, method] of [
    ['/api/oauth/authorize', 'POST'],
    ['/api/oauth/callback', 'POST'],
    ['/api/oauth/status', 'GET'],
    ['/api/oauth/refresh', 'POST'],
    ['/api/oauth/disconnect', 'DELETE'],
    ['/api/token', 'POST'],
  ] as const) {
    exactRoutes.set(`${method}:${path}`, {
      adminOnly: true,
      handler: async (req, res, _ctx, _route, _qs, _tenantDb, _scopedProjectId, reqLogger) => {
        await handleOAuthRoute(path, method, req, res, oauth, tokenManager, reqLogger, auditLog);
      },
    });
  }

  exactRoutes.set('GET:/api/status', {
    handler: (_req, res) => {
      handleStatus(res, pool, dispatcher, tokenManager, oauth);
    },
  });

  exactRoutes.set('POST:/api/execute', {
    handler: withHttpAudit(async (req, res, ctx, _route, _qs, _tenantDb, _scopedProjectId, reqLogger) => {
      await handleExecute(req, res, dispatcher, reqLogger, ctx);
    }, 'execution:submit', 'execution'),
  });

  exactRoutes.set('POST:/api/gitlab/webhook', {
    needsDb: true,
    handler: withHttpAudit(async (req, res, _ctx, _route, _qs, tenantDb) => {
      const body = await readBody(req);
      let parsed: Record<string, unknown> = {};
      try { parsed = JSON.parse(body); } catch { /* raw payload */ }
      const objectKind = (parsed['object_kind'] as string) || 'unknown';
      const projectPath = (parsed['project'] as Record<string, unknown>)?.['path_with_namespace'] as string | undefined;
      const event = db.publishEvent(tenantDb!, {
        eventType: `gitlab_${objectKind}`,
        sourceType: 'gitlab',
        sourceId: projectPath ?? null,
        payload: body,
      });
      onEventPublished?.();
      json(res, 201, event);
    }, 'event:publish', 'event'),
  });

  // Admin routes
  exactRoutes.set('GET:/api/admin/users', {
    adminOnly: true,
    handler: (_req, res) => {
      if (tenantDbManager) {
        json(res, 200, { projectIds: tenantDbManager.listTenantIds() });
      } else if (database) {
        json(res, 200, { projectIds: db.listDistinctProjectIds(database) });
      } else {
        json(res, 200, { projectIds: [] });
      }
    },
  });

  exactRoutes.set('GET:/api/admin/personas', {
    adminOnly: true,
    handler: (_req, res) => {
      if (tenantDbManager) {
        const allPersonas: import('@dac-cloud/shared').PersonaSummary[] = [];
        tenantDbManager.forEachTenant((tDb) => { allPersonas.push(...db.listPersonasSummary(tDb)); });
        json(res, 200, allPersonas);
      } else if (database) {
        json(res, 200, db.listPersonasSummary(database));
      } else {
        json(res, 200, []);
      }
    },
  });

  exactRoutes.set('GET:/api/admin/executions', {
    adminOnly: true,
    handler: (_req, res, _ctx, _route, queryString) => {
      const params = new URLSearchParams(queryString || '');
      const limit = clampQueryInt(params.get('limit'), 100, MAX_QUERY_LIMIT);
      if (tenantDbManager) {
        const allExecs: import('@dac-cloud/shared').PersonaExecution[] = [];
        tenantDbManager.forEachTenant((tDb) => {
          allExecs.push(...db.listExecutions(tDb, { limit }));
        });
        allExecs.sort((a, b) => b.createdAt.localeCompare(a.createdAt));
        json(res, 200, allExecs.slice(0, limit));
      } else if (database) {
        json(res, 200, db.listExecutions(database, {
          limit,
          offset: clampQueryInt(params.get('offset'), 0, MAX_QUERY_OFFSET),
        }));
      } else {
        json(res, 200, []);
      }
    },
  });

  exactRoutes.set('GET:/api/admin/audit', {
    adminOnly: true,
    handler: (_req, res, _ctx, _route, queryString) => {
      if (!auditLog) { json(res, 200, []); return; }
      const params = new URLSearchParams(queryString || '');
      json(res, 200, auditLog.query({
        tenantId: params.get('tenantId') ?? undefined,
        action: (params.get('action') ?? undefined) as AuditAction | undefined,
        since: params.get('since') ?? undefined,
        until: params.get('until') ?? undefined,
        actor: params.get('actor') ?? undefined,
        resourceType: params.get('resourceType') ?? undefined,
        resourceId: params.get('resourceId') ?? undefined,
        limit: clampQueryInt(params.get('limit'), 100, MAX_QUERY_LIMIT),
        offset: clampQueryInt(params.get('offset'), 0, MAX_QUERY_OFFSET),
      }));
    },
  });

  exactRoutes.set('GET:/api/admin/audit/stats', {
    adminOnly: true,
    handler: (_req, res, _ctx, _route, queryString) => {
      if (!auditLog) { json(res, 200, { byAction: [], byTenant: [], topActors: [], trend: [] }); return; }
      const params = new URLSearchParams(queryString || '');
      const bucket = params.get('bucket') === 'hour' ? 'hour' : 'day';
      json(res, 200, auditLog.stats({
        tenantId: params.get('tenantId') ?? undefined,
        since: params.get('since') ?? undefined,
        until: params.get('until') ?? undefined,
        bucket,
      }));
    },
  });

  exactRoutes.set('POST:/api/admin/audit/verify', {
    adminOnly: true,
    handler: async (req, res) => {
      if (!auditLog) { json(res, 200, { valid: true, verifiedCount: 0 }); return; }
      const body = await readBody(req);
      let opts: { since?: string; until?: string } | undefined;
      if (body) {
        try {
          const parsed = JSON.parse(body) as { since?: string; until?: string };
          if (parsed.since || parsed.until) opts = { since: parsed.since, until: parsed.until };
        } catch { /* empty body or non-JSON — verify entire chain */ }
      }
      json(res, 200, auditLog.verify(opts));
    },
  });

  // Daily metrics endpoint — accessible to authenticated users (scoped to their tenant)
  // and admins (system-wide aggregation across all tenants).
  exactRoutes.set('GET:/api/metrics/daily', {
    handler: (_req, res, ctx, _route, queryString, tenantDb, _scopedProjectId) => {
      const params = new URLSearchParams(queryString || '');
      const from = params.get('from') ?? undefined;
      const to = params.get('to') ?? undefined;
      const opts = { from, to };

      if (ctx.authType === 'admin' && tenantDbManager) {
        // Admin: aggregate across all tenants
        const allDaily: db.DailyMetricsRow[] = [];
        const allByPersona: db.DailyMetricsByPersonaRow[] = [];
        tenantDbManager.forEachTenant((tDb) => {
          allDaily.push(...db.queryDailyMetrics(tDb, opts));
          allByPersona.push(...db.queryDailyMetricsByPersona(tDb, opts));
        });

        // Merge daily rows by date
        const dayMap = new Map<string, db.DailyMetricsRow>();
        for (const row of allDaily) {
          const existing = dayMap.get(row.date);
          if (existing) {
            existing.totalExecutions += row.totalExecutions;
            existing.completed += row.completed;
            existing.failed += row.failed;
            existing.cancelled += row.cancelled;
            existing.totalCostUsd += row.totalCostUsd;
            existing.totalDurationMs += row.totalDurationMs;
            existing.totalInputTokens += row.totalInputTokens;
            existing.totalOutputTokens += row.totalOutputTokens;
            existing.avgDurationMs = existing.totalExecutions > 0
              ? Math.round(existing.totalDurationMs / existing.totalExecutions) : null;
          } else {
            dayMap.set(row.date, { ...row });
          }
        }
        const merged = [...dayMap.values()].sort((a, b) => b.date.localeCompare(a.date));

        json(res, 200, { daily: merged, byPersona: allByPersona });
      } else {
        // Tenant-scoped
        const targetDb = tenantDb ?? database;
        if (!targetDb) { json(res, 200, { daily: [], byPersona: [] }); return; }
        json(res, 200, {
          daily: db.queryDailyMetrics(targetDb, opts),
          byPersona: db.queryDailyMetricsByPersona(targetDb, opts),
        });
      }
    },
  });

  // ── Resource route table (O(1) lookup by `METHOD:resource[:id][:sub]`) ──
  const resourceRoutes = new Map<string, RouteEntry>();

  // --- Persona CRUD ---
  resourceRoutes.set(resourceKey('GET', 'personas', false), {
    needsDb: true,
    handler: (_req, res, _ctx, _route, _qs, tenantDb, scopedProjectId) => {
      json(res, 200, db.listPersonasSummary(tenantDb!, scopedProjectId));
    },
  });

  resourceRoutes.set(resourceKey('POST', 'personas', false), {
    needsDb: true,
    handler: async (req, res, ctx, _route, _qs, tenantDb, _scopedProjectId, reqLogger) => {
      const body = JSON.parse(await readBody(req)) as Partial<Persona>;
      const persona = createPersonaWithDefaults({
        ...body,
        projectIdOverride: ctx.authType === 'user' ? ctx.projectId : undefined,
      });
      db.upsertPersona(tenantDb!, persona);
      try { auditLog?.record({ action: 'persona:create', tenantId: ctx.projectId || 'default', actor: ctx.userEmail || ctx.projectId, resourceType: 'persona', resourceId: persona.id, ipAddress: req.socket.remoteAddress }); } catch { /* logged inside record() */ }
      reqLogger.info({ op: 'persona.create', resourceType: 'persona', resourceId: persona.id, projectId: ctx.projectId, status: 201 }, 'Persona created');
      json(res, 201, persona);
    },
  });

  resourceRoutes.set(resourceKey('GET', 'personas', true, 'tools'), {
    needsDb: true,
    handler: (_req, res, _ctx, route, _qs, tenantDb) => {
      json(res, 200, db.getToolsForPersona(tenantDb!, route!.id!));
    },
  });

  resourceRoutes.set(resourceKey('POST', 'personas', true, 'tools'), {
    needsDb: true,
    handler: async (req, res, ctx, route, _qs, tenantDb, scopedProjectId, reqLogger) => {
      const persona = withScopedEntity(db.getPersona(tenantDb!, route!.id!), scopedProjectId, res);
      if (!persona) return;
      const body = JSON.parse(await readBody(req)) as { toolId: string };
      db.linkTool(tenantDb!, route!.id!, body.toolId);
      try { auditLog?.record({ action: 'tool:link', tenantId: ctx.projectId || 'default', actor: ctx.userEmail || ctx.projectId, resourceType: 'persona', resourceId: route!.id!, detail: { toolId: body.toolId }, ipAddress: req.socket.remoteAddress }); } catch { /* logged inside record() */ }
      reqLogger.info({ op: 'persona.linkTool', resourceType: 'persona', resourceId: route!.id!, toolId: body.toolId, projectId: ctx.projectId, status: 200 }, 'Tool linked to persona');
      json(res, 200, { linked: true });
    },
  });

  resourceRoutes.set(resourceKey('GET', 'personas', true, 'full'), {
    needsDb: true,
    handler: (_req, res, _ctx, route, _qs, tenantDb, scopedProjectId, reqLogger) => {
      const hydrated = withScopedEntity(db.getHydratedPersona(tenantDb!, route!.id!, reqLogger), scopedProjectId, res);
      if (!hydrated) return;
      json(res, 200, hydrated);
    },
  });

  resourceRoutes.set(resourceKey('GET', 'personas', true), {
    needsDb: true,
    handler: (_req, res, _ctx, route, _qs, tenantDb, scopedProjectId) => {
      const persona = withScopedEntity(db.getPersona(tenantDb!, route!.id!), scopedProjectId, res);
      if (!persona) return;
      json(res, 200, persona);
    },
  });

  resourceRoutes.set(resourceKey('DELETE', 'personas', true), {
    needsDb: true,
    handler: (req, res, ctx, route, _qs, tenantDb, scopedProjectId, reqLogger) => {
      db.deletePersona(tenantDb!, route!.id!, scopedProjectId);
      try { auditLog?.record({ action: 'persona:delete', tenantId: ctx.projectId || 'default', actor: ctx.userEmail || ctx.projectId, resourceType: 'persona', resourceId: route!.id!, ipAddress: req.socket.remoteAddress }); } catch { /* logged inside record() */ }
      reqLogger.info({ op: 'persona.delete', resourceType: 'persona', resourceId: route!.id!, projectId: ctx.projectId, status: 200 }, 'Persona deleted');
      json(res, 200, { deleted: true });
    },
  });

  // --- Persona sub-resources: credentials ---
  resourceRoutes.set(resourceKey('GET', 'personas', true, 'credentials'), {
    needsDb: true,
    handler: (req, res, ctx, route, _qs, tenantDb) => {
      const creds = db.listCredentialsForPersona(tenantDb!, route!.id!);
      try { auditLog?.record({ action: 'key:access', tenantId: ctx.projectId || 'default', actor: ctx.userEmail || ctx.projectId, resourceType: 'credential', resourceId: route!.id!, ipAddress: req.socket.remoteAddress }); } catch { /* logged inside record() */ }
      json(res, 200, creds.map(c => ({ ...c, encryptedData: '[REDACTED]', iv: '[REDACTED]', tag: '[REDACTED]' })));
    },
  });

  resourceRoutes.set(resourceKey('POST', 'personas', true, 'credentials'), {
    needsDb: true,
    handler: async (req, res, ctx, route, _qs, tenantDb, scopedProjectId, reqLogger) => {
      const persona = withScopedEntity(db.getPersona(tenantDb!, route!.id!), scopedProjectId, res);
      if (!persona) return;
      const body = JSON.parse(await readBody(req)) as { credentialId: string };
      db.linkCredential(tenantDb!, route!.id!, body.credentialId);
      try { auditLog?.record({ action: 'credential:link', tenantId: ctx.projectId || 'default', actor: ctx.userEmail || ctx.projectId, resourceType: 'persona', resourceId: route!.id!, detail: { credentialId: body.credentialId }, ipAddress: req.socket.remoteAddress }); } catch { /* logged inside record() */ }
      reqLogger.info({ op: 'persona.linkCredential', resourceType: 'persona', resourceId: route!.id!, credentialId: body.credentialId, projectId: ctx.projectId, status: 200 }, 'Credential linked to persona');
      json(res, 200, { linked: true });
    },
  });

  // --- Persona sub-resources: subscriptions ---
  resourceRoutes.set(resourceKey('GET', 'personas', true, 'subscriptions'), {
    needsDb: true,
    handler: (_req, res, _ctx, route, _qs, tenantDb) => {
      json(res, 200, db.listSubscriptionsForPersona(tenantDb!, route!.id!));
    },
  });

  // --- Tool Definitions ---
  resourceRoutes.set(resourceKey('POST', 'tool-definitions', false), {
    needsDb: true,
    handler: async (req, res, ctx, _route, _qs, tenantDb, _scopedProjectId, reqLogger) => {
      const body = JSON.parse(await readBody(req)) as Partial<PersonaToolDefinition>;
      const tool = createToolDefinitionWithDefaults(body);
      db.upsertToolDefinition(tenantDb!, tool);
      try { auditLog?.record({ action: 'tool-definition:create', tenantId: ctx.projectId || 'default', actor: ctx.userEmail || ctx.projectId, resourceType: 'toolDefinition', resourceId: tool.id, ipAddress: req.socket.remoteAddress }); } catch { /* logged inside record() */ }
      reqLogger.info({ op: 'toolDefinition.create', resourceType: 'toolDefinition', resourceId: tool.id, projectId: ctx.projectId, status: 201 }, 'Tool definition created');
      json(res, 201, tool);
    },
  });

  // --- Credentials ---
  resourceRoutes.set(resourceKey('POST', 'credentials', false), {
    needsDb: true,
    handler: async (req, res, ctx, _route, _qs, tenantDb, _scopedProjectId, reqLogger) => {
      const body = JSON.parse(await readBody(req));
      if (ctx.authType === 'user') body.projectId = ctx.projectId;

      if (!body.plaintext || typeof body.plaintext !== 'string') {
        json(res, 400, { error: 'Missing required field: plaintext. Credentials must be submitted as plaintext for server-side encryption.' });
        return;
      }
      if (body.plaintext.length > MAX_PLAINTEXT_CHARS) {
        json(res, 400, { error: `Plaintext exceeds maximum allowed size of ${MAX_PLAINTEXT_CHARS} characters` });
        return;
      }
      if (!tenantKeyManager) {
        json(res, 503, { error: 'Credential encryption is unavailable. Server key manager is not configured.' });
        return;
      }

      delete body.encryptedData;
      delete body.iv;
      delete body.tag;

      const tenantId = body.projectId || ctx.projectId || 'default';
      const key = await tenantKeyManager.getKey(tenantId);
      const encrypted = encrypt(body.plaintext, key);
      body.encryptedData = encrypted.encrypted;
      body.iv = encrypted.iv;
      body.tag = encrypted.tag;
      delete body.plaintext;

      const cred = db.createCredential(tenantDb!, body);
      try { auditLog?.record({ action: 'key:encrypt', tenantId: body.projectId || ctx.projectId || 'default', actor: ctx.userEmail || ctx.projectId, resourceType: 'credential', resourceId: cred.id, ipAddress: req.socket.remoteAddress }); } catch { /* logged inside record() */ }
      reqLogger.info({ op: 'credential.create', resourceType: 'credential', resourceId: cred.id, projectId: ctx.projectId, status: 201 }, 'Credential created');
      json(res, 201, cred);
    },
  });

  resourceRoutes.set(resourceKey('DELETE', 'credentials', true), {
    needsDb: true,
    handler: (req, res, ctx, route, _qs, tenantDb, scopedProjectId, reqLogger) => {
      const changes = db.deleteCredential(tenantDb!, route!.id!, scopedProjectId);
      if (changes === 0) { json(res, 404, { error: 'Not found' }); return; }
      try { auditLog?.record({ action: 'key:delete', tenantId: ctx.projectId || 'default', actor: ctx.userEmail || ctx.projectId, resourceType: 'credential', resourceId: route!.id!, ipAddress: req.socket.remoteAddress }); } catch { /* logged inside record() */ }
      reqLogger.info({ op: 'credential.delete', resourceType: 'credential', resourceId: route!.id!, projectId: ctx.projectId, status: 200 }, 'Credential deleted');
      json(res, 200, { deleted: true });
    },
  });

  // --- Event Subscriptions ---
  resourceRoutes.set(resourceKey('POST', 'subscriptions', false), {
    needsDb: true,
    handler: async (req, res, ctx, _route, _qs, tenantDb) => {
      const body = JSON.parse(await readBody(req));
      if (ctx.authType === 'user') body.projectId = ctx.projectId;
      const missing: string[] = [];
      if (!body.personaId || typeof body.personaId !== 'string') missing.push('personaId');
      if (!body.eventType || typeof body.eventType !== 'string') missing.push('eventType');
      if (missing.length > 0) { json(res, 400, { error: `Missing required fields: ${missing.join(', ')}` }); return; }
      const sub = db.createSubscription(tenantDb!, body);
      try { auditLog?.record({ action: 'subscription:create', tenantId: ctx.projectId || 'default', actor: ctx.userEmail || ctx.projectId, resourceType: 'subscription', resourceId: sub.id, detail: { personaId: body.personaId, eventType: body.eventType }, ipAddress: req.socket.remoteAddress }); } catch { /* logged inside record() */ }
      json(res, 201, sub);
    },
  });

  resourceRoutes.set(resourceKey('PUT', 'subscriptions', true), {
    needsDb: true,
    handler: async (req, res, ctx, route, _qs, tenantDb) => {
      const body = JSON.parse(await readBody(req));
      db.updateSubscription(tenantDb!, route!.id!, body);
      const updated = db.getSubscription(tenantDb!, route!.id!);
      try { auditLog?.record({ action: 'subscription:update', tenantId: ctx.projectId || 'default', actor: ctx.userEmail || ctx.projectId, resourceType: 'subscription', resourceId: route!.id!, ipAddress: req.socket.remoteAddress }); } catch { /* logged inside record() */ }
      json(res, 200, updated);
    },
  });

  resourceRoutes.set(resourceKey('DELETE', 'subscriptions', true), {
    needsDb: true,
    handler: (req, res, ctx, route, _qs, tenantDb) => {
      db.deleteSubscription(tenantDb!, route!.id!);
      try { auditLog?.record({ action: 'subscription:delete', tenantId: ctx.projectId || 'default', actor: ctx.userEmail || ctx.projectId, resourceType: 'subscription', resourceId: route!.id!, ipAddress: req.socket.remoteAddress }); } catch { /* logged inside record() */ }
      json(res, 200, { deleted: true });
    },
  });

  // --- Triggers (delegated to triggerHttpHandlers) ---
  const triggerHandler: RouteHandler = async (req, res, ctx, route, _qs, tenantDb) => {
    await handleTriggerRoute(route!, req.method!, req, res, tenantDb!, ctx, triggerScheduler);
  };
  resourceRoutes.set(resourceKey('POST', 'triggers', false), { needsDb: true, handler: withHttpAudit(triggerHandler, 'trigger:create', 'trigger') });
  resourceRoutes.set(resourceKey('PUT', 'triggers', true), { needsDb: true, handler: withHttpAudit(triggerHandler, 'trigger:update', 'trigger') });
  resourceRoutes.set(resourceKey('DELETE', 'triggers', true), { needsDb: true, handler: withHttpAudit(triggerHandler, 'trigger:delete', 'trigger') });
  resourceRoutes.set(resourceKey('GET', 'triggers', true, 'history'), { needsDb: true, handler: triggerHandler });
  resourceRoutes.set(resourceKey('GET', 'triggers', true, 'stats'), { needsDb: true, handler: triggerHandler });
  resourceRoutes.set(resourceKey('GET', 'personas', true, 'triggers'), { needsDb: true, handler: triggerHandler });

  // --- Events ---
  resourceRoutes.set(resourceKey('GET', 'events', false), {
    needsDb: true,
    handler: (_req, res, _ctx, _route, queryString, tenantDb, scopedProjectId) => {
      const params = new URLSearchParams(queryString || '');
      const events = db.listEvents(tenantDb!, {
        eventType: params.get('eventType') ?? undefined,
        status: (params.get('status') ?? undefined) as import('@dac-cloud/shared').EventStatus | undefined,
        limit: clampQueryInt(params.get('limit'), 50, MAX_QUERY_LIMIT),
        offset: clampQueryInt(params.get('offset'), 0, MAX_QUERY_OFFSET),
        projectId: scopedProjectId,
      });
      json(res, 200, events);
    },
  });

  resourceRoutes.set(resourceKey('POST', 'events', false), {
    needsDb: true,
    handler: withHttpAudit(async (req, res, ctx, _route, _qs, tenantDb) => {
      const body = JSON.parse(await readBody(req));
      if (ctx.authType === 'user') body.projectId = ctx.projectId;
      const missing: string[] = [];
      if (!body.eventType || typeof body.eventType !== 'string') missing.push('eventType');
      if (!body.sourceType || typeof body.sourceType !== 'string') missing.push('sourceType');
      if (missing.length > 0) { json(res, 400, { error: `Missing required fields: ${missing.join(', ')}` }); return; }
      const event = db.publishEvent(tenantDb!, body);
      onEventPublished?.();
      json(res, 201, event);
    }, 'event:publish', 'event'),
  });

  resourceRoutes.set(resourceKey('PUT', 'events', true), {
    needsDb: true,
    handler: withHttpAudit(async (req, res, _ctx, route, _qs, tenantDb) => {
      const body = JSON.parse(await readBody(req)) as { status: import('@dac-cloud/shared').EventStatus; metadata?: string };
      const updated = db.updateEventWithMetadata(tenantDb!, route!.id!, body.status, body.metadata);
      if (updated) {
        json(res, 200, updated);
      } else {
        res.writeHead(404, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Event not found' }));
      }
    }, 'event:update', 'event'),
  });

  // --- Webhooks ---
  resourceRoutes.set(resourceKey('POST', 'webhooks', true), {
    needsDb: true,
    handler: withHttpAudit(async (req, res, _ctx, route, _qs, tenantDb) => {
      const personaId = route!.id!;
      const body = await readBody(req);
      const event = db.publishEvent(tenantDb!, {
        eventType: 'webhook_received',
        sourceType: 'webhook',
        sourceId: personaId,
        targetPersonaId: personaId,
        payload: body,
      });
      onEventPublished?.();
      json(res, 201, event);
    }, 'webhook:receive', 'webhook'),
  });

  // --- Executions ---
  resourceRoutes.set(resourceKey('GET', 'executions', false), {
    needsDb: true,
    handler: (_req, res, _ctx, _route, queryString, tenantDb, scopedProjectId) => {
      const params = new URLSearchParams(queryString || '');
      const executions = db.listExecutions(tenantDb!, {
        personaId: params.get('personaId') ?? undefined,
        status: params.get('status') ?? undefined,
        limit: clampQueryInt(params.get('limit'), 50, MAX_QUERY_LIMIT),
        offset: clampQueryInt(params.get('offset'), 0, MAX_QUERY_OFFSET),
        projectId: scopedProjectId,
      });
      json(res, 200, executions);
    },
  });

  resourceRoutes.set(resourceKey('POST', 'executions', true, 'cancel'), {
    handler: (_req, res, _ctx, route) => {
      handleCancelExecution(res, dispatcher, route!.id!);
    },
  });

  resourceRoutes.set(resourceKey('GET', 'executions', true), {
    handler: (_req, res, _ctx, route, queryString) => {
      const params = new URLSearchParams(queryString || '');
      const offset = parseInt(params.get('offset') || '0', 10);
      handleGetExecution(res, dispatcher, route!.id!, offset);
    },
  });

  // ── Request handler ──
  const server = http.createServer(async (req, res) => {
    // Request correlation ID
    const reqId = nanoid();
    const reqLogger = logger.child({ reqId });
    res.setHeader('X-Request-Id', reqId);

    // Security headers
    res.setHeader('X-Content-Type-Options', 'nosniff');

    // CORS headers
    const origin = req.headers.origin ?? '';
    const allowedOrigin = process.env['CORS_ALLOWED_ORIGIN'] || '*';
    const isAdminRoute = (req.url ?? '').startsWith('/api/admin');
    if (isAdminRoute) {
      if (origin && origin === allowedOrigin) {
        res.setHeader('Access-Control-Allow-Origin', origin);
      }
    } else {
      res.setHeader('Access-Control-Allow-Origin', allowedOrigin);
    }
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization, X-User-Token');

    if (req.method === 'OPTIONS') {
      res.writeHead(204);
      res.end();
      return;
    }

    const url = req.url || '/';
    const [pathname, queryString] = url.split('?');
    const route = parseRoute(pathname ?? '');

    // Unauthenticated endpoint
    if (pathname === '/health' && req.method === 'GET') {
      await handleHealth(res, pool, dispatcher, tokenManager, oauth, getEventProcessorHealth, database, tenantDbManager, kafka, triggerScheduler, auditLog, getRetentionHealth);
      return;
    }

    // Auth check
    const ctx = auth.validateAndExtractContext(req);
    if (!ctx) {
      res.writeHead(401, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Unauthorized', reqId }));
      return;
    }

    const scopedProjectId = ctx.authType === 'admin' ? undefined : ctx.projectId;

    const resolveDatabase = (projectId?: string): Database.Database | undefined => {
      if (tenantDbManager && projectId) {
        return tenantDbManager.getTenantDb(projectId);
      }
      return database;
    };
    const tenantDb = resolveDatabase(ctx.projectId);

    try {
      // O(1) exact-path lookup
      const exactEntry = exactRoutes.get(`${req.method}:${pathname}`);
      if (exactEntry) {
        if (exactEntry.adminOnly && requireAdmin(ctx, res)) return;
        if (exactEntry.needsDb && !tenantDb) { json(res, 404, { error: 'Not found', reqId }); return; }
        await exactEntry.handler(req, res, ctx, route, queryString, tenantDb, scopedProjectId, reqLogger);
        return;
      }

      // O(1) resource-route lookup
      if (route) {
        const resKey = resourceKey(req.method!, route.resource, !!route.id, route.subResource);
        const resEntry = resourceRoutes.get(resKey);
        if (resEntry) {
          if (resEntry.adminOnly && requireAdmin(ctx, res)) return;
          if (resEntry.needsDb && !tenantDb) { json(res, 404, { error: 'Not found', reqId }); return; }
          await resEntry.handler(req, res, ctx, route, queryString, tenantDb, scopedProjectId, reqLogger);
          return;
        }
      }

      // No matching route
      json(res, 404, { error: 'Not found', reqId });
    } catch (err) {
      if (err instanceof Error && err.message === 'Payload too large') {
        res.writeHead(413, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Payload too large', reqId }));
        return;
      }
      if (err instanceof Error && err.message.includes('UNIQUE constraint failed')) {
        res.writeHead(409, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Conflict: a resource with the same unique identifier already exists', reqId }));
        return;
      }
      reqLogger.error({ err, url }, 'HTTP handler error');
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Internal server error', reqId }));
    }
  });

  return server;
}

/**
 * Checks that an entity exists and belongs to the scoped project (if scoping is active).
 * Returns the entity on success, or null after sending a 404 response.
 */
function withScopedEntity<T extends { projectId?: string | null }>(
  entity: T | undefined,
  scopedProjectId: string | undefined,
  res: http.ServerResponse,
): T | null {
  if (!entity || (scopedProjectId && entity.projectId !== scopedProjectId)) {
    json(res, 404, { error: 'Not found' });
    return null;
  }
  return entity;
}

/** Returns true (and sends 403) if the caller is NOT an admin. */
function requireAdmin(ctx: RequestContext, res: http.ServerResponse): boolean {
  if (ctx.authType !== 'admin') {
    res.writeHead(403, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: 'Admin access required' }));
    return true;
  }
  return false;
}

// --- Existing handlers ---

/** Heartbeat age (ms) beyond which a worker is considered stale. */
const STALE_HEARTBEAT_THRESHOLD_MS = 60_000;

async function handleHealth(
  res: http.ServerResponse,
  pool: WorkerPool,
  dispatcher: Dispatcher,
  tokenManager: TokenManager,
  oauth: OAuthManager,
  getEventProcessorHealth?: () => EventProcessorHealth,
  database?: Database.Database,
  tenantDbManager?: TenantDbManager,
  kafka?: KafkaClient,
  triggerScheduler?: TriggerSchedulerHandle,
  auditLog?: AuditLog,
  getRetentionHealth?: () => RetentionHealth,
): Promise<void> {
  const issues: string[] = [];

  const counts = pool.getWorkerCount();
  const workers = pool.getWorkers();
  const now = Date.now();
  const staleWorkers = workers
    .filter(w => (now - w.lastHeartbeat) > STALE_HEARTBEAT_THRESHOLD_MS)
    .map(w => ({ workerId: w.workerId, heartbeatAgeMs: now - w.lastHeartbeat }));

  if (staleWorkers.length > 0) {
    issues.push(`${staleWorkers.length} worker(s) with stale heartbeat`);
  }

  const dbHealth: Record<string, unknown> = { status: 'unavailable' };
  if (database) {
    try {
      database.prepare('SELECT 1').get();
      dbHealth.status = 'ok';
    } catch {
      dbHealth.status = 'error';
      issues.push('database unreachable');
    }
  }
  if (tenantDbManager) {
    try {
      const tenantIds = tenantDbManager.listTenantIds();
      dbHealth.tenantCount = tenantIds.length;
      if (dbHealth.status === 'unavailable') dbHealth.status = 'ok';
    } catch {
      dbHealth.tenantCount = -1;
      if (dbHealth.status !== 'error') dbHealth.status = 'error';
      issues.push('tenant DB listing failed');
    }
  }

  let kafkaHealth: Record<string, unknown> = { status: 'disabled' };
  if (kafka) {
    const kh = await kafka.checkHealth();
    kafkaHealth = { status: kh.status, ...(kh.detail ? { detail: kh.detail } : {}) };
    if (kh.status === 'error') {
      issues.push(`kafka: ${kh.detail || 'error'}`);
    }
  }

  const queueDepth = dispatcher.getQueueLength();
  const activeExecutions = dispatcher.getActiveExecutions().length;

  let eventProcessorHealth: EventProcessorHealth | undefined;
  if (getEventProcessorHealth) {
    eventProcessorHealth = getEventProcessorHealth();
  }

  const tokenHealth = tokenManager.getHealth();
  if (tokenHealth.consecutiveDecryptionFailures > 0) {
    issues.push(`token decryption failures: ${tokenHealth.consecutiveDecryptionFailures} consecutive`);
  }

  let verdict: 'healthy' | 'degraded' | 'unhealthy';
  if (issues.some(i => i.includes('database unreachable') || i.includes('kafka: '))) {
    verdict = 'unhealthy';
  } else if (issues.length > 0) {
    verdict = 'degraded';
  } else {
    verdict = 'healthy';
  }

  const body: Record<string, unknown> = {
    status: verdict,
    timestamp: Date.now(),
    dependencies: {
      database: dbHealth,
      kafka: kafkaHealth,
      workers: {
        ...counts,
        staleWorkers: staleWorkers.length > 0 ? staleWorkers : undefined,
      },
      token: tokenHealth,
    },
    queue: {
      depth: queueDepth,
      activeExecutions,
    },
    hasSubscription: oauth.hasTokens(),
    oauthFunnel: oauth.getFunnelMetrics(),
    oauthErrors: oauth.getErrorMetrics(),
  };

  if (eventProcessorHealth) {
    (body.dependencies as Record<string, unknown>).eventProcessor = eventProcessorHealth;
  }

  if (triggerScheduler) {
    const schedulerMetrics = triggerScheduler.getMetrics();
    (body.dependencies as Record<string, unknown>).scheduler = schedulerMetrics;
    if (schedulerMetrics.triggers_missed_total > 0) {
      issues.push(`scheduler: ${schedulerMetrics.triggers_missed_total} missed triggers`);
    }
  }

  if (auditLog) {
    const auditHealth = auditLog.getHealth();
    (body.dependencies as Record<string, unknown>).audit = auditHealth;
    if (auditHealth.droppedEvents > 0) {
      issues.push(`audit: ${auditHealth.droppedEvents} dropped events`);
    }
  }

  if (getRetentionHealth) {
    const retHealth = getRetentionHealth();
    (body.dependencies as Record<string, unknown>).retention = retHealth;
    if (retHealth.totalErrors > 0) {
      issues.push(`retention: ${retHealth.totalErrors} total errors`);
    }
  }

  if (issues.length > 0) {
    body.issues = issues;
  }

  const httpStatus = verdict === 'unhealthy' ? 503 : 200;
  res.writeHead(httpStatus, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify(body));
}

function handleStatus(
  res: http.ServerResponse,
  pool: WorkerPool,
  dispatcher: Dispatcher,
  tokenManager: TokenManager,
  oauth: OAuthManager,
): void {
  const oauthTokens = oauth.getTokens();
  res.writeHead(200, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({
    workers: pool.getWorkers(),
    workerCounts: pool.getWorkerCount(),
    workerPoolMetrics: pool.getMetrics(),
    queueLength: dispatcher.getQueueLength(),
    activeExecutions: dispatcher.getActiveExecutions(),
    hasClaudeToken: tokenManager.hasToken(),
    oauth: {
      connected: oauth.hasTokens(),
      scopes: oauthTokens?.scopes || [],
      expiresAt: oauthTokens ? new Date(oauthTokens.expiresAt).toISOString() : null,
    },
  }));
}

async function handleExecute(
  req: http.IncomingMessage,
  res: http.ServerResponse,
  dispatcher: Dispatcher,
  logger: Logger,
  ctx?: RequestContext,
): Promise<void> {
  const body = await readBody(req);
  let parsed: { prompt?: string; personaId?: string; timeoutMs?: number };

  try {
    parsed = JSON.parse(body);
  } catch {
    res.writeHead(400, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: 'Invalid JSON body' }));
    return;
  }

  if (!parsed.prompt) {
    res.writeHead(400, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: 'Missing required field: prompt' }));
    return;
  }

  const request: ExecRequest = {
    executionId: nanoid(),
    personaId: parsed.personaId || 'manual',
    prompt: parsed.prompt,
    projectId: ctx?.authType === 'user' ? ctx.projectId : undefined,
    config: {
      timeoutMs: parsed.timeoutMs || 300_000,
    },
  };

  const result: SubmitResult = dispatcher.submit(request);

  if (!result.accepted) {
    logger.warn({ executionId: request.executionId, reason: result.reason }, 'Execution rejected by backpressure');
    res.writeHead(429, {
      'Content-Type': 'application/json',
      'Retry-After': String(result.retryAfterSeconds),
    });
    res.end(JSON.stringify({
      error: result.reason === 'tenant_quota_exceeded'
        ? 'Per-tenant queue quota exceeded'
        : 'Queue depth limit exceeded',
      retryAfterSeconds: result.retryAfterSeconds,
    }));
    return;
  }

  logger.info({ executionId: request.executionId }, 'Execution submitted via HTTP');

  res.writeHead(202, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({
    executionId: request.executionId,
    status: 'queued',
  }));
}

function handleGetExecution(
  res: http.ServerResponse,
  dispatcher: Dispatcher,
  executionId: string,
  offset: number = 0,
): void {
  const exec = dispatcher.getExecution(executionId);

  if (!exec) {
    res.writeHead(404, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: 'Execution not found' }));
    return;
  }

  const output = offset > 0 ? exec.output.slice(offset) : exec.output;

  res.writeHead(200, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({
    executionId,
    status: exec.status,
    outputLines: exec.output.length,
    output,
    durationMs: exec.durationMs,
    sessionId: exec.sessionId,
    totalCostUsd: exec.totalCostUsd,
    ...(exec.phaseTimings ? { phaseTimings: exec.phaseTimings } : {}),
  }));
}

function handleCancelExecution(
  res: http.ServerResponse,
  dispatcher: Dispatcher,
  executionId: string,
): void {
  const cancelled = dispatcher.cancelExecution(executionId);

  if (!cancelled) {
    res.writeHead(404, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: 'Execution not found or not running' }));
    return;
  }

  res.writeHead(200, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({ executionId, status: 'cancelling' }));
}

const MAX_PLAINTEXT_CHARS = 64 * 1024; // 64 KB
const MAX_QUERY_LIMIT = 500;
const MAX_QUERY_OFFSET = 100_000;

/** Parse an integer query param and clamp it to safe bounds. */
function clampQueryInt(value: string | null, defaultValue: number, max: number): number {
  if (!value) return defaultValue;
  const parsed = parseInt(value, 10);
  if (!Number.isFinite(parsed) || parsed < 0) return defaultValue;
  return Math.min(parsed, max);
}
