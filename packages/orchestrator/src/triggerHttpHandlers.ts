import http from 'node:http';
import type Database from 'better-sqlite3';
import type { RequestContext } from './auth.js';
import { readBody, json } from './httpUtils.js';
import * as db from './db/index.js';
import type { TriggerSchedulerHandle } from './triggerScheduler.js';

interface ParsedRoute {
  resource: string;
  id?: string;
  subResource?: string;
}

/**
 * Attempt to handle a trigger CRUD route.
 * Caller is responsible for auth checks before invoking this.
 * Sends a 404 for unrecognised trigger sub-paths.
 *
 * When a `scheduler` handle is provided, CRUD operations update the
 * in-memory trigger heap so the scheduler fires without a DB scan.
 */
export async function handleTriggerRoute(
  route: ParsedRoute,
  method: string,
  req: http.IncomingMessage,
  res: http.ServerResponse,
  tenantDb: Database.Database,
  ctx: RequestContext,
  scheduler?: TriggerSchedulerHandle,
): Promise<void> {
  const tenantId = ctx.projectId ?? 'default';

  // POST /api/triggers — create trigger
  if (route.resource === 'triggers' && !route.id && method === 'POST') {
    const body = JSON.parse(await readBody(req));
    if (ctx.authType === 'user') body.projectId = ctx.projectId;
    const trigger = db.createTrigger(tenantDb, body);
    scheduler?.heapPush(trigger.id, tenantId, trigger.nextTriggerAt);
    scheduler?.nudge();
    json(res, 201, trigger);
    return;
  }

  // PUT /api/triggers/:id — update trigger
  if (route.resource === 'triggers' && route.id && !route.subResource && method === 'PUT') {
    const body = JSON.parse(await readBody(req));
    db.updateTrigger(tenantDb, route.id, body);
    const updated = db.getTrigger(tenantDb, route.id);
    if (updated) {
      if (updated.enabled && updated.nextTriggerAt) {
        scheduler?.heapPush(updated.id, tenantId, updated.nextTriggerAt);
      } else {
        scheduler?.heapRemove(updated.id);
      }
      scheduler?.nudge();
    }
    json(res, 200, updated);
    return;
  }

  // DELETE /api/triggers/:id — delete trigger
  if (route.resource === 'triggers' && route.id && !route.subResource && method === 'DELETE') {
    scheduler?.heapRemove(route.id);
    db.deleteTrigger(tenantDb, route.id);
    json(res, 200, { deleted: true });
    return;
  }

  // GET /api/personas/:id/triggers — list triggers for persona
  if (route.resource === 'personas' && route.id && route.subResource === 'triggers' && method === 'GET') {
    json(res, 200, db.listTriggersForPersona(tenantDb, route.id));
    return;
  }

  // GET /api/triggers/:id/history — fire history for a trigger
  if (route.resource === 'triggers' && route.id && route.subResource === 'history' && method === 'GET') {
    const trigger = db.getTrigger(tenantDb, route.id);
    if (!trigger) { json(res, 404, { error: 'Trigger not found' }); return; }
    const url = new URL(req.url ?? '/', `http://${req.headers.host ?? 'localhost'}`);
    const limit = Math.min(Math.max(parseInt(url.searchParams.get('limit') ?? '50', 10) || 50, 1), 500);
    json(res, 200, db.listTriggerFires(tenantDb, route.id, limit));
    return;
  }

  // GET /api/triggers/:id/stats — aggregate stats for a trigger
  if (route.resource === 'triggers' && route.id && route.subResource === 'stats' && method === 'GET') {
    const trigger = db.getTrigger(tenantDb, route.id);
    if (!trigger) { json(res, 404, { error: 'Trigger not found' }); return; }
    json(res, 200, db.getTriggerStats(tenantDb, route.id));
    return;
  }

  // Unrecognised trigger sub-path
  json(res, 404, { error: 'Not found' });
}
