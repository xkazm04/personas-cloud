import http from 'node:http';
import { timingSafeEqual } from 'node:crypto';
import { nanoid } from 'nanoid';
import type { ExecRequest, Persona, PersonaToolDefinition } from '@dac-cloud/shared';
import type Database from 'better-sqlite3';
import type { Auth, RequestContext } from './auth.js';
import type { Dispatcher } from './dispatcher.js';
import type { WorkerPool } from './workerPool.js';
import type { TokenManager } from './tokenManager.js';
import type { OAuthManager } from './oauth.js';
import type { Logger } from 'pino';
import * as db from './db.js';

export function createHttpApi(
  auth: Auth,
  dispatcher: Dispatcher,
  pool: WorkerPool,
  tokenManager: TokenManager,
  oauth: OAuthManager,
  logger: Logger,
  database?: Database.Database,
  gitlabWebhookSecret?: string,
  corsOrigins?: string[],
): http.Server {
  const allowedOrigins = new Set(corsOrigins ?? []);

  const server = http.createServer(async (req, res) => {
    // CORS headers — only allow explicitly configured origins
    const requestOrigin = req.headers['origin'];
    if (requestOrigin && allowedOrigins.has(requestOrigin)) {
      res.setHeader('Access-Control-Allow-Origin', requestOrigin);
      res.setHeader('Vary', 'Origin');
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

    // Unauthenticated endpoints
    if (pathname === '/health' && req.method === 'GET') {
      handleHealth(res, pool, oauth);
      return;
    }

    // GitLab webhook — verified via X-Gitlab-Token (not API key auth)
    if (pathname === '/api/gitlab/webhook' && req.method === 'POST' && database) {
      if (!gitlabWebhookSecret) {
        logger.warn('GitLab webhook received but GITLAB_WEBHOOK_SECRET is not configured');
        res.writeHead(403, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'GitLab webhook secret not configured' }));
        return;
      }

      const token = req.headers['x-gitlab-token'] as string | undefined;
      if (!token || !safeCompareTokens(token, gitlabWebhookSecret)) {
        res.writeHead(401, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Invalid or missing X-Gitlab-Token' }));
        return;
      }

      const body = await readBody(req);
      let parsed: Record<string, unknown> = {};
      try { parsed = JSON.parse(body); } catch { /* raw payload */ }
      const objectKind = (parsed['object_kind'] as string) || 'unknown';
      const projectPath = (parsed['project'] as Record<string, unknown>)?.['path_with_namespace'] as string | undefined;
      const event = db.publishEvent(database, {
        eventType: `gitlab_${objectKind}`,
        sourceType: 'gitlab',
        sourceId: projectPath ?? null,
        payload: body,
      });
      json(res, 201, event);
      return;
    }

    // Auth check for everything else — dual auth (API key + optional JWT)
    const ctx = auth.validateAndExtractContext(req);
    if (!ctx) {
      res.writeHead(401, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Unauthorized' }));
      return;
    }

    // Helper: project scoping (admin sees all, user sees own project)
    const scopedProjectId = ctx.authType === 'admin' ? undefined : ctx.projectId;

    try {
      // --- OAuth endpoints ---
      if (pathname === '/api/oauth/authorize' && req.method === 'POST') {
        handleOAuthAuthorize(res, oauth);
      } else if (pathname === '/api/oauth/callback' && req.method === 'POST') {
        await handleOAuthCallback(req, res, oauth, tokenManager, logger);
      } else if (pathname === '/api/oauth/status' && req.method === 'GET') {
        handleOAuthStatus(res, oauth);
      } else if (pathname === '/api/oauth/refresh' && req.method === 'POST') {
        await handleOAuthRefresh(res, oauth, tokenManager, logger);
      } else if (pathname === '/api/oauth/disconnect' && req.method === 'DELETE') {
        handleOAuthDisconnect(res, oauth, tokenManager);
      }
      // --- Token injection (fallback for setup-token / direct paste) ---
      else if (pathname === '/api/token' && req.method === 'POST') {
        await handleSetToken(req, res, tokenManager, logger);
      }
      // --- Persona CRUD ---
      else if (pathname === '/api/personas' && req.method === 'GET' && database) {
        json(res, 200, db.listPersonas(database, scopedProjectId));
      } else if (pathname === '/api/personas' && req.method === 'POST' && database) {
        const body = JSON.parse(await readBody(req)) as Partial<Persona>;
        const promptError = validatePromptFields(body);
        if (promptError) {
          res.writeHead(400, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ error: promptError }));
          return;
        }
        const now = new Date().toISOString();
        const persona: Persona = {
          id: body.id ?? nanoid(),
          projectId: ctx.authType === 'user' ? ctx.projectId : (body.projectId ?? 'default'),
          name: body.name ?? 'Untitled',
          description: body.description ?? null,
          systemPrompt: body.systemPrompt ?? '',
          structuredPrompt: body.structuredPrompt ?? null,
          icon: body.icon ?? null,
          color: body.color ?? null,
          enabled: body.enabled ?? true,
          maxConcurrent: body.maxConcurrent ?? 1,
          timeoutMs: body.timeoutMs ?? 300_000,
          modelProfile: body.modelProfile ?? null,
          maxBudgetUsd: body.maxBudgetUsd ?? null,
          maxTurns: body.maxTurns ?? null,
          designContext: body.designContext ?? null,
          groupId: body.groupId ?? null,
          createdAt: body.createdAt ?? now,
          updatedAt: now,
        };
        db.upsertPersona(database, persona);
        json(res, 201, persona);
      } else if (matchRoute(pathname, '/api/personas/') && req.method === 'GET' && database) {
        const id = pathname!.replace('/api/personas/', '');
        const persona = db.getPersona(database, id);
        if (!persona || (scopedProjectId && persona.projectId !== scopedProjectId)) { json(res, 404, { error: 'Not found' }); return; }
        json(res, 200, persona);
      } else if (matchRoute(pathname, '/api/personas/') && pathname!.endsWith('/tools') && req.method === 'GET' && database) {
        const id = pathname!.replace('/api/personas/', '').replace('/tools', '');
        json(res, 200, db.getToolsForPersona(database, id));
      } else if (matchRoute(pathname, '/api/personas/') && pathname!.endsWith('/tools') && req.method === 'POST' && database) {
        const id = pathname!.replace('/api/personas/', '').replace('/tools', '');
        const body = JSON.parse(await readBody(req)) as { toolId: string };
        db.linkTool(database, id, body.toolId);
        json(res, 200, { linked: true });
      } else if (matchRoute(pathname, '/api/personas/') && !pathname!.endsWith('/tools') && !pathname!.endsWith('/credentials') && !pathname!.endsWith('/subscriptions') && !pathname!.endsWith('/triggers') && req.method === 'DELETE' && database) {
        const id = pathname!.replace('/api/personas/', '');
        db.deletePersona(database, id, scopedProjectId);
        json(res, 200, { deleted: true });
      }
      // --- Tool Definitions ---
      else if (pathname === '/api/tool-definitions' && req.method === 'POST' && database) {
        const body = JSON.parse(await readBody(req)) as Partial<PersonaToolDefinition>;
        const now = new Date().toISOString();
        const tool: PersonaToolDefinition = {
          id: body.id ?? nanoid(),
          name: body.name ?? 'unnamed',
          category: body.category ?? 'general',
          description: body.description ?? '',
          scriptPath: body.scriptPath ?? '',
          inputSchema: body.inputSchema ?? null,
          outputSchema: body.outputSchema ?? null,
          requiresCredentialType: body.requiresCredentialType ?? null,
          implementationGuide: body.implementationGuide ?? null,
          isBuiltin: body.isBuiltin ?? false,
          createdAt: body.createdAt ?? now,
          updatedAt: now,
        };
        db.upsertToolDefinition(database, tool);
        json(res, 201, tool);
      }
      // --- Credentials ---
      else if (pathname === '/api/credentials' && req.method === 'POST' && database) {
        const body = JSON.parse(await readBody(req));
        if (ctx.authType === 'user') body.projectId = ctx.projectId;
        const cred = db.createCredential(database, body);
        json(res, 201, cred);
      } else if (matchRoute(pathname, '/api/credentials/') && req.method === 'DELETE' && database) {
        const id = pathname!.replace('/api/credentials/', '');
        db.deleteCredential(database, id);
        json(res, 200, { deleted: true });
      } else if (matchRoute(pathname, '/api/personas/') && pathname!.endsWith('/credentials') && req.method === 'GET' && database) {
        const id = pathname!.replace('/api/personas/', '').replace('/credentials', '');
        const creds = db.listCredentialsForPersona(database, id);
        // Strip encrypted data from response
        json(res, 200, creds.map(c => ({ ...c, encryptedData: '[REDACTED]', iv: '[REDACTED]', tag: '[REDACTED]' })));
      } else if (matchRoute(pathname, '/api/personas/') && pathname!.endsWith('/credentials') && req.method === 'POST' && database) {
        const id = pathname!.replace('/api/personas/', '').replace('/credentials', '');
        const body = JSON.parse(await readBody(req)) as { credentialId: string };
        db.linkCredential(database, id, body.credentialId);
        json(res, 200, { linked: true });
      }
      // --- Event Subscriptions ---
      else if (pathname === '/api/subscriptions' && req.method === 'POST' && database) {
        const body = JSON.parse(await readBody(req));
        if (ctx.authType === 'user') body.projectId = ctx.projectId;
        const sub = db.createSubscription(database, body);
        json(res, 201, sub);
      } else if (matchRoute(pathname, '/api/subscriptions/') && req.method === 'PUT' && database) {
        const id = pathname!.replace('/api/subscriptions/', '');
        const body = JSON.parse(await readBody(req));
        db.updateSubscription(database, id, body);
        const updated = db.getSubscription(database, id);
        json(res, 200, updated);
      } else if (matchRoute(pathname, '/api/subscriptions/') && req.method === 'DELETE' && database) {
        const id = pathname!.replace('/api/subscriptions/', '');
        db.deleteSubscription(database, id);
        json(res, 200, { deleted: true });
      } else if (matchRoute(pathname, '/api/personas/') && pathname!.endsWith('/subscriptions') && req.method === 'GET' && database) {
        const id = pathname!.replace('/api/personas/', '').replace('/subscriptions', '');
        json(res, 200, db.listSubscriptionsForPersona(database, id));
      }
      // --- Triggers ---
      else if (pathname === '/api/triggers' && req.method === 'POST' && database) {
        const body = JSON.parse(await readBody(req));
        if (ctx.authType === 'user') body.projectId = ctx.projectId;
        const trigger = db.createTrigger(database, body);
        json(res, 201, trigger);
      } else if (matchRoute(pathname, '/api/triggers/') && req.method === 'PUT' && database) {
        const id = pathname!.replace('/api/triggers/', '');
        const body = JSON.parse(await readBody(req));
        db.updateTrigger(database, id, body);
        const updated = db.getTrigger(database, id);
        json(res, 200, updated);
      } else if (matchRoute(pathname, '/api/triggers/') && req.method === 'DELETE' && database) {
        const id = pathname!.replace('/api/triggers/', '');
        db.deleteTrigger(database, id);
        json(res, 200, { deleted: true });
      } else if (matchRoute(pathname, '/api/personas/') && pathname!.endsWith('/triggers') && req.method === 'GET' && database) {
        const id = pathname!.replace('/api/personas/', '').replace('/triggers', '');
        json(res, 200, db.listTriggersForPersona(database, id));
      }
      // --- Events ---
      else if (pathname === '/api/events' && req.method === 'GET' && database) {
        const params = new URLSearchParams(queryString || '');
        const events = db.listEvents(database, {
          eventType: params.get('eventType') ?? undefined,
          status: params.get('status') ?? undefined,
          limit: clampInt(params.get('limit'), 50, 1, MAX_LIMIT),
          offset: clampInt(params.get('offset'), 0, 0, Number.MAX_SAFE_INTEGER),
          projectId: scopedProjectId,
        });
        json(res, 200, events);
      } else if (pathname === '/api/events' && req.method === 'POST' && database) {
        const body = JSON.parse(await readBody(req));
        if (ctx.authType === 'user') body.projectId = ctx.projectId;
        const event = db.publishEvent(database, body);
        json(res, 201, event);
      } else if (matchRoute(pathname, '/api/events/') && req.method === 'PUT' && database) {
        const id = pathname!.replace('/api/events/', '');
        const body = JSON.parse(await readBody(req)) as { status: string; metadata?: string };
        const updated = db.updateEventWithMetadata(database, id, body.status, body.metadata);
        if (updated) {
          json(res, 200, updated);
        } else {
          res.writeHead(404, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ error: 'Event not found' }));
        }
      }
      // --- Webhooks ---
      else if (matchRoute(pathname, '/api/webhooks/') && req.method === 'POST' && database) {
        const personaId = pathname!.replace('/api/webhooks/', '');
        const body = await readBody(req);
        const event = db.publishEvent(database, {
          eventType: 'webhook_received',
          sourceType: 'webhook',
          sourceId: personaId,
          targetPersonaId: personaId,
          payload: body,
        });
        json(res, 201, event);
      }
      // --- Executions ---
      else if (pathname === '/api/executions' && req.method === 'GET' && database) {
        const params = new URLSearchParams(queryString || '');
        const executions = db.listExecutions(database, {
          personaId: params.get('personaId') ?? undefined,
          status: params.get('status') ?? undefined,
          limit: clampInt(params.get('limit'), 50, 1, MAX_LIMIT),
          offset: clampInt(params.get('offset'), 0, 0, Number.MAX_SAFE_INTEGER),
          projectId: scopedProjectId,
        });
        json(res, 200, executions);
      }
      // --- Existing execution endpoints ---
      else if (pathname === '/api/status' && req.method === 'GET') {
        handleStatus(res, pool, dispatcher, tokenManager, oauth);
      } else if (pathname === '/api/execute' && req.method === 'POST') {
        await handleExecute(req, res, dispatcher, logger, ctx);
      } else if (matchRoute(pathname, '/api/executions/') && pathname!.endsWith('/cancel') && req.method === 'POST') {
        const executionId = pathname!.replace('/api/executions/', '').replace('/cancel', '');
        handleCancelExecution(res, dispatcher, executionId || '');
      } else if (matchRoute(pathname, '/api/executions/') && req.method === 'GET') {
        const executionId = pathname!.replace('/api/executions/', '');
        const params = new URLSearchParams(queryString || '');
        const offset = clampInt(params.get('offset'), 0, 0, Number.MAX_SAFE_INTEGER);
        handleGetExecution(res, dispatcher, executionId, offset);
      }
      // --- Admin-only routes ---
      else if (pathname === '/api/admin/users' && req.method === 'GET' && database) {
        if (requireAdmin(ctx, res)) return;
        json(res, 200, { projectIds: db.listDistinctProjectIds(database) });
      } else if (pathname === '/api/admin/personas' && req.method === 'GET' && database) {
        if (requireAdmin(ctx, res)) return;
        json(res, 200, db.listPersonas(database));
      } else if (pathname === '/api/admin/executions' && req.method === 'GET' && database) {
        if (requireAdmin(ctx, res)) return;
        const params = new URLSearchParams(queryString || '');
        json(res, 200, db.listExecutions(database, {
          limit: clampInt(params.get('limit'), 100, 1, MAX_LIMIT),
          offset: clampInt(params.get('offset'), 0, 0, Number.MAX_SAFE_INTEGER),
        }));
      } else {
        res.writeHead(404, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Not found' }));
      }
    } catch (err) {
      if (err instanceof Error && err.message === 'Payload too large') {
        res.writeHead(413, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Payload too large' }));
        return;
      }
      logger.error({ err, url }, 'HTTP handler error');
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Internal server error' }));
    }
  });

  return server;
}

function json(res: http.ServerResponse, status: number, data: unknown): void {
  res.writeHead(status, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify(data));
}

function matchRoute(pathname: string | undefined, prefix: string): boolean {
  return !!pathname && pathname.startsWith(prefix) && pathname.length > prefix.length;
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

// --- OAuth handlers ---

function handleOAuthAuthorize(res: http.ServerResponse, oauth: OAuthManager): void {
  const { url, state } = oauth.generateAuthUrl();
  res.writeHead(200, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({
    authUrl: url,
    state,
    instructions: [
      '1. Open the authUrl in a browser',
      '2. Log in with your Anthropic account and authorize',
      '3. You will be redirected to a page showing an authorization code',
      '4. Copy the code from the URL (the "code" query parameter)',
      '5. POST it to /api/oauth/callback with { "code": "...", "state": "..." }',
    ],
  }));
}

async function handleOAuthCallback(
  req: http.IncomingMessage,
  res: http.ServerResponse,
  oauth: OAuthManager,
  tokenManager: TokenManager,
  logger: Logger,
): Promise<void> {
  const body = await readBody(req);
  let parsed: { code?: string; state?: string };

  try {
    parsed = JSON.parse(body);
  } catch {
    res.writeHead(400, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: 'Invalid JSON body' }));
    return;
  }

  if (!parsed.code || !parsed.state) {
    res.writeHead(400, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: 'Missing required fields: code, state' }));
    return;
  }

  const tokens = await oauth.exchangeCode(parsed.code, parsed.state);
  if (!tokens) {
    res.writeHead(401, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: 'OAuth token exchange failed' }));
    return;
  }

  // Also store the access token in TokenManager for backward compatibility
  tokenManager.storeClaudeToken(tokens.accessToken);

  logger.info('OAuth flow completed, subscription connected');

  res.writeHead(200, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({
    status: 'connected',
    scopes: tokens.scopes,
    expiresAt: new Date(tokens.expiresAt).toISOString(),
  }));
}

function handleOAuthStatus(res: http.ServerResponse, oauth: OAuthManager): void {
  const tokens = oauth.getTokens();
  res.writeHead(200, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({
    connected: oauth.hasTokens(),
    scopes: tokens?.scopes || [],
    expiresAt: tokens ? new Date(tokens.expiresAt).toISOString() : null,
    isExpired: tokens ? Date.now() > tokens.expiresAt : null,
  }));
}

async function handleOAuthRefresh(
  res: http.ServerResponse,
  oauth: OAuthManager,
  tokenManager: TokenManager,
  logger: Logger,
): Promise<void> {
  const tokens = await oauth.refreshAccessToken();
  if (!tokens) {
    res.writeHead(500, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: 'Token refresh failed' }));
    return;
  }

  tokenManager.storeClaudeToken(tokens.accessToken);
  logger.info('OAuth token refreshed via API');

  res.writeHead(200, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({
    status: 'refreshed',
    expiresAt: new Date(tokens.expiresAt).toISOString(),
  }));
}

function handleOAuthDisconnect(
  res: http.ServerResponse,
  oauth: OAuthManager,
  tokenManager: TokenManager,
): void {
  oauth.clearTokens();
  tokenManager.clearToken();
  res.writeHead(200, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({ status: 'disconnected' }));
}

// --- Token injection (for setup-token / manual paste) ---

async function handleSetToken(
  req: http.IncomingMessage,
  res: http.ServerResponse,
  tokenManager: TokenManager,
  logger: Logger,
): Promise<void> {
  const body = await readBody(req);
  let parsed: { token?: string };

  try {
    parsed = JSON.parse(body);
  } catch {
    res.writeHead(400, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: 'Invalid JSON body' }));
    return;
  }

  if (!parsed.token) {
    res.writeHead(400, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: 'Missing required field: token' }));
    return;
  }

  tokenManager.storeClaudeToken(parsed.token);
  logger.info('Claude token set via direct injection');

  res.writeHead(200, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({ status: 'token_stored' }));
}

// --- Existing handlers ---

function handleHealth(res: http.ServerResponse, pool: WorkerPool, oauth: OAuthManager): void {
  const counts = pool.getWorkerCount();
  res.writeHead(200, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({
    status: 'ok',
    workers: counts,
    hasSubscription: oauth.hasTokens(),
    timestamp: Date.now(),
  }));
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
      timeoutMs: Math.max(1000, Math.min(MAX_TIMEOUT_MS, parsed.timeoutMs || MAX_TIMEOUT_MS)),
    },
  };

  dispatcher.submit(request);

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

/** Constant-time token comparison to prevent timing side-channel attacks. */
function safeCompareTokens(a: string, b: string): boolean {
  if (a.length !== b.length) return false;
  return timingSafeEqual(Buffer.from(a), Buffer.from(b));
}

// --- Query parameter bounds ---

const MAX_LIMIT = 1000;
const MAX_TIMEOUT_MS = 120_000;

/** Parse an integer query param, clamping it within [min, max]. */
function clampInt(raw: string | null, fallback: number, min: number, max: number): number {
  if (raw === null) return fallback;
  const n = parseInt(raw, 10);
  if (Number.isNaN(n)) return fallback;
  return Math.max(min, Math.min(max, n));
}

// --- Prompt validation ---

const MAX_PROMPT_BYTES = 50 * 1024; // 50 KB

/** Regex matching null bytes and C0 control chars (U+0001-U+001F) except tab, newline, CR. */
const DANGEROUS_CHARS = /[\x00-\x08\x0B\x0C\x0E-\x1F]/;

function validatePromptFields(body: Partial<Persona>): string | null {
  if (body.systemPrompt !== undefined && body.systemPrompt !== null) {
    if (typeof body.systemPrompt !== 'string') return 'systemPrompt must be a string';
    if (body.systemPrompt.length > MAX_PROMPT_BYTES) return `systemPrompt exceeds maximum size of ${MAX_PROMPT_BYTES / 1024} KB`;
    if (DANGEROUS_CHARS.test(body.systemPrompt)) return 'systemPrompt contains invalid control characters';
  }
  if (body.structuredPrompt !== undefined && body.structuredPrompt !== null) {
    if (typeof body.structuredPrompt !== 'string') return 'structuredPrompt must be a string';
    if (body.structuredPrompt.length > MAX_PROMPT_BYTES) return `structuredPrompt exceeds maximum size of ${MAX_PROMPT_BYTES / 1024} KB`;
    if (DANGEROUS_CHARS.test(body.structuredPrompt)) return 'structuredPrompt contains invalid control characters';
    try { JSON.parse(body.structuredPrompt); } catch { return 'structuredPrompt must be valid JSON'; }
  }
  return null;
}

const MAX_BODY_BYTES = 1024 * 1024; // 1 MB

function readBody(req: http.IncomingMessage): Promise<string> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    let totalBytes = 0;
    req.on('data', (chunk: Buffer) => {
      totalBytes += chunk.length;
      if (totalBytes > MAX_BODY_BYTES) {
        req.destroy();
        reject(new Error('Payload too large'));
        return;
      }
      chunks.push(chunk);
    });
    req.on('end', () => resolve(Buffer.concat(chunks).toString()));
    req.on('error', reject);
  });
}
