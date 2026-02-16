import http from 'node:http';
import { nanoid } from 'nanoid';
import type { ExecRequest } from '@dac-cloud/shared';
import type { Auth } from './auth.js';
import type { Dispatcher } from './dispatcher.js';
import type { WorkerPool } from './workerPool.js';
import type { TokenManager } from './tokenManager.js';
import type { OAuthManager } from './oauth.js';
import type { Logger } from 'pino';

export function createHttpApi(
  auth: Auth,
  dispatcher: Dispatcher,
  pool: WorkerPool,
  tokenManager: TokenManager,
  oauth: OAuthManager,
  logger: Logger,
): http.Server {
  const server = http.createServer(async (req, res) => {
    // CORS headers
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, DELETE, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');

    if (req.method === 'OPTIONS') {
      res.writeHead(204);
      res.end();
      return;
    }

    const url = req.url || '/';

    // Unauthenticated endpoints
    if (url === '/health' && req.method === 'GET') {
      handleHealth(res, pool, oauth);
      return;
    }

    // Auth check for everything else
    if (!auth.validateRequest(req)) {
      res.writeHead(401, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Unauthorized' }));
      return;
    }

    try {
      // --- OAuth endpoints ---
      if (url === '/api/oauth/authorize' && req.method === 'POST') {
        handleOAuthAuthorize(res, oauth);
      } else if (url === '/api/oauth/callback' && req.method === 'POST') {
        await handleOAuthCallback(req, res, oauth, tokenManager, logger);
      } else if (url === '/api/oauth/status' && req.method === 'GET') {
        handleOAuthStatus(res, oauth);
      } else if (url === '/api/oauth/refresh' && req.method === 'POST') {
        await handleOAuthRefresh(res, oauth, tokenManager, logger);
      } else if (url === '/api/oauth/disconnect' && req.method === 'DELETE') {
        handleOAuthDisconnect(res, oauth, tokenManager);
      }
      // --- Token injection (fallback for setup-token / direct paste) ---
      else if (url === '/api/token' && req.method === 'POST') {
        await handleSetToken(req, res, tokenManager, logger);
      }
      // --- Execution endpoints ---
      else if (url === '/api/status' && req.method === 'GET') {
        handleStatus(res, pool, dispatcher, tokenManager, oauth);
      } else if (url === '/api/execute' && req.method === 'POST') {
        await handleExecute(req, res, dispatcher, logger);
      } else if (url.startsWith('/api/executions/') && url.endsWith('/cancel') && req.method === 'POST') {
        const executionId = url.replace('/api/executions/', '').replace('/cancel', '');
        handleCancelExecution(res, dispatcher, executionId || '');
      } else if (url.startsWith('/api/executions/') && req.method === 'GET') {
        const parts = url.split('?');
        const executionId = (parts[0] || '').replace('/api/executions/', '');
        const params = new URLSearchParams(parts[1] || '');
        const offset = parseInt(params.get('offset') || '0', 10);
        handleGetExecution(res, dispatcher, executionId, offset);
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
    config: {
      timeoutMs: parsed.timeoutMs || 300_000,
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
