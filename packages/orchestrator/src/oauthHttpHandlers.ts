import http from 'node:http';
import type { OAuthManager } from './oauth.js';
import type { TokenManager } from './tokenManager.js';
import type { AuditLog } from './auditLog.js';
import type { Logger } from 'pino';
import { readBody, json } from './httpUtils.js';

/**
 * Attempt to handle an OAuth or token-injection route.
 * Returns `true` if the route was matched and handled, `false` otherwise.
 * Caller is responsible for auth/admin checks before invoking this.
 */
export async function handleOAuthRoute(
  pathname: string,
  method: string,
  req: http.IncomingMessage,
  res: http.ServerResponse,
  oauth: OAuthManager,
  tokenManager: TokenManager,
  logger: Logger,
  auditLog?: AuditLog,
): Promise<boolean> {
  const ip = req.socket.remoteAddress;
  if (pathname === '/api/oauth/authorize' && method === 'POST') {
    handleOAuthAuthorize(res, oauth, auditLog, ip);
    return true;
  }
  if (pathname === '/api/oauth/callback' && method === 'POST') {
    await handleOAuthCallback(req, res, oauth, tokenManager, logger, auditLog, ip);
    return true;
  }
  if (pathname === '/api/oauth/status' && method === 'GET') {
    handleOAuthStatus(res, oauth);
    return true;
  }
  if (pathname === '/api/oauth/refresh' && method === 'POST') {
    await handleOAuthRefresh(res, oauth, tokenManager, logger, auditLog, ip);
    return true;
  }
  if (pathname === '/api/oauth/disconnect' && method === 'DELETE') {
    await handleOAuthDisconnect(res, oauth, tokenManager, logger, auditLog, ip);
    return true;
  }
  if (pathname === '/api/token' && method === 'POST') {
    await handleSetToken(req, res, tokenManager, logger, auditLog, ip);
    return true;
  }
  // Unrecognised OAuth/token sub-path
  json(res, 404, { error: 'Not found' });
  return false;
}

// --- OAuth handlers ---

function handleOAuthAuthorize(res: http.ServerResponse, oauth: OAuthManager, auditLog?: AuditLog, ip?: string): void {
  const { url, state } = oauth.generateAuthUrl();
  try { auditLog?.record({ action: 'oauth:authorize', resourceType: 'oauth', resourceId: state, ipAddress: ip }); } catch { /* logged inside record() */ }
  json(res, 200, {
    authUrl: url,
    state,
    instructions: [
      '1. Open the authUrl in a browser',
      '2. Log in with your Anthropic account and authorize',
      '3. You will be redirected to a page showing an authorization code',
      '4. Copy the code from the URL (the "code" query parameter)',
      '5. POST it to /api/oauth/callback with { "code": "...", "state": "..." }',
    ],
  });
}

async function handleOAuthCallback(
  req: http.IncomingMessage,
  res: http.ServerResponse,
  oauth: OAuthManager,
  tokenManager: TokenManager,
  logger: Logger,
  auditLog?: AuditLog,
  ip?: string,
): Promise<void> {
  const body = await readBody(req);
  let parsed: { code?: string; state?: string };

  try {
    parsed = JSON.parse(body);
  } catch {
    json(res, 400, { error: 'Invalid JSON body' });
    return;
  }

  if (!parsed.code || !parsed.state) {
    json(res, 400, { error: 'Missing required fields: code, state' });
    return;
  }

  const result = await oauth.exchangeCode(parsed.code, parsed.state);
  if (!result.ok) {
    const status = result.error.errorType === 'rate_limited' ? 429
      : result.error.errorType === 'server_error' ? 502
      : 401;
    json(res, status, { error: 'OAuth token exchange failed', errorType: result.error.errorType, transient: result.error.transient });
    return;
  }

  tokenManager.storeClaudeToken(result.tokens.accessToken);
  logger.info('OAuth flow completed, subscription connected');

  try {
    auditLog?.record({
      action: 'oauth:exchange',
      resourceType: 'oauth',
      detail: { scopes: result.tokens.scopes, expiresAt: new Date(result.tokens.expiresAt).toISOString() },
      ipAddress: ip,
    });
  } catch { /* logged inside record() */ }

  json(res, 200, {
    status: 'connected',
    scopes: result.tokens.scopes,
    expiresAt: new Date(result.tokens.expiresAt).toISOString(),
  });
}

function handleOAuthStatus(res: http.ServerResponse, oauth: OAuthManager): void {
  const tokens = oauth.getTokens();
  json(res, 200, {
    connected: oauth.hasTokens(),
    scopes: tokens?.scopes || [],
    expiresAt: tokens ? new Date(tokens.expiresAt).toISOString() : null,
    isExpired: tokens ? Date.now() > tokens.expiresAt : null,
  });
}

async function handleOAuthRefresh(
  res: http.ServerResponse,
  oauth: OAuthManager,
  tokenManager: TokenManager,
  logger: Logger,
  auditLog?: AuditLog,
  ip?: string,
): Promise<void> {
  const result = await oauth.refreshAccessToken();
  if (!result.ok) {
    const status = result.error.errorType === 'rate_limited' ? 429
      : result.error.errorType === 'server_error' ? 502
      : result.error.errorType === 'invalid_grant' ? 401
      : 500;
    try { auditLog?.record({ action: 'oauth:refresh_failed', resourceType: 'oauth', detail: { reason: result.error.message, errorType: result.error.errorType }, ipAddress: ip }); } catch { /* logged inside record() */ }
    json(res, status, { error: 'Token refresh failed', errorType: result.error.errorType, transient: result.error.transient });
    return;
  }

  tokenManager.storeClaudeToken(result.tokens.accessToken);
  logger.info('OAuth token refreshed via API');

  try { auditLog?.record({ action: 'oauth:refresh', resourceType: 'oauth', detail: { expiresAt: new Date(result.tokens.expiresAt).toISOString() }, ipAddress: ip }); } catch { /* logged inside record() */ }

  json(res, 200, {
    status: 'refreshed',
    expiresAt: new Date(result.tokens.expiresAt).toISOString(),
  });
}

async function handleOAuthDisconnect(
  res: http.ServerResponse,
  oauth: OAuthManager,
  tokenManager: TokenManager,
  logger: Logger,
  auditLog?: AuditLog,
  ip?: string,
): Promise<void> {
  const { revoked } = await oauth.revokeAndClearTokens();
  tokenManager.clearToken();

  if (!revoked) {
    logger.warn('OAuth tokens cleared locally but server-side revocation failed or was skipped');
  }

  try { auditLog?.record({ action: 'oauth:disconnect', resourceType: 'oauth', detail: { serverRevoked: revoked }, ipAddress: ip }); } catch { /* logged inside record() */ }

  json(res, 200, { status: 'disconnected', serverRevoked: revoked });
}

// --- Token injection ---

async function handleSetToken(
  req: http.IncomingMessage,
  res: http.ServerResponse,
  tokenManager: TokenManager,
  logger: Logger,
  auditLog?: AuditLog,
  ip?: string,
): Promise<void> {
  const body = await readBody(req);
  let parsed: { token?: string };

  try {
    parsed = JSON.parse(body);
  } catch {
    json(res, 400, { error: 'Invalid JSON body' });
    return;
  }

  const token = typeof parsed.token === 'string' ? parsed.token.trim() : '';

  if (!token) {
    json(res, 400, { error: 'Missing required field: token' });
    return;
  }

  if (!token.startsWith('sk-ant-')) {
    json(res, 400, { error: 'Invalid token format: expected Anthropic API key starting with sk-ant-' });
    return;
  }

  tokenManager.storeClaudeToken(token);
  logger.info('Claude token set via direct injection');

  try { auditLog?.record({ action: 'token:inject', resourceType: 'token', ipAddress: ip }); } catch { /* logged inside record() */ }

  json(res, 200, { status: 'token_stored' });
}

