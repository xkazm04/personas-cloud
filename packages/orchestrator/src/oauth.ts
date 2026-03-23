import { randomBytes, createHash } from 'node:crypto';
import type { Logger } from 'pino';
import type { AuditLog } from './auditLog.js';

// Anthropic OAuth constants (from Claude Code CLI)
const OAUTH_AUTHORIZE_URL = 'https://claude.ai/oauth/authorize';
const OAUTH_TOKEN_URL = 'https://console.anthropic.com/v1/oauth/token';
const OAUTH_REVOKE_URL = 'https://console.anthropic.com/v1/oauth/revoke';
const CLIENT_ID = '9d1c250a-e61b-44d9-88ed-5944d1962f5e';
const REDIRECT_URI = 'https://console.anthropic.com/oauth/code/callback';
const SCOPES = 'org:create_api_key user:profile user:inference';

export interface OAuthTokens {
  accessToken: string;
  refreshToken: string;
  expiresAt: number;    // unix ms
  scopes: string[];
}

// ---------------------------------------------------------------------------
// OAuth error classification
// ---------------------------------------------------------------------------

/** Classified OAuth error types for pattern detection and alerting. */
export type OAuthErrorType =
  | 'invalid_grant'   // refresh token consumed/expired — user must re-authenticate
  | 'rate_limited'    // 429 — back off and retry
  | 'server_error'    // 5xx — transient, retry with backoff
  | 'network_error'   // fetch threw (DNS, timeout, connection refused) — transient
  | 'invalid_request'; // 400 — likely a bug in request construction

export interface OAuthError {
  errorType: OAuthErrorType;
  status?: number;       // HTTP status when available
  message: string;       // human-readable summary
  transient: boolean;    // true = self-healing, false = requires intervention
  raw?: string;          // raw error body (truncated)
}

/**
 * Classify an HTTP error response from the OAuth token endpoint.
 */
function classifyHttpError(status: number, body: string): OAuthError {
  const truncated = body.length > 512 ? body.slice(0, 512) + '…' : body;

  // 401 or body contains invalid_grant → refresh token consumed/expired
  if (status === 401 || /invalid_grant/i.test(body)) {
    return {
      errorType: 'invalid_grant',
      status,
      message: 'Refresh token expired or consumed — user must re-authenticate',
      transient: false,
      raw: truncated,
    };
  }

  // 429 → rate limited
  if (status === 429) {
    return {
      errorType: 'rate_limited',
      status,
      message: 'Rate limited by OAuth provider — back off and retry',
      transient: true,
      raw: truncated,
    };
  }

  // 5xx → server error (transient)
  if (status >= 500) {
    return {
      errorType: 'server_error',
      status,
      message: `OAuth provider returned ${status} — transient server error`,
      transient: true,
      raw: truncated,
    };
  }

  // 400 or other 4xx → invalid request (likely a bug)
  return {
    errorType: 'invalid_request',
    status,
    message: `OAuth request rejected with ${status} — check request parameters`,
    transient: false,
    raw: truncated,
  };
}

/**
 * Classify a network-level error (fetch threw before receiving a response).
 */
function classifyNetworkError(err: unknown): OAuthError {
  const message = err instanceof Error ? err.message : String(err);
  return {
    errorType: 'network_error',
    message: `Network error: ${message}`,
    transient: true,
  };
}

/** Result of postTokenEndpoint: either tokens or a classified error. */
export type TokenEndpointResult =
  | { ok: true; tokens: OAuthTokens }
  | { ok: false; error: OAuthError };

export interface OAuthState {
  state: string;
  codeVerifier: string;
  createdAt: number;
  expiresAt: number;    // unix ms, 10 minute window
}

/** OAuth flow funnel metrics. */
export interface OAuthFunnelMetrics {
  flowsStarted: number;
  flowsCompleted: number;
  flowsExpired: number;
  flowsFailed: number;
}

/** Per-error-type counters for OAuth token endpoint failures. */
export interface OAuthErrorMetrics {
  invalid_grant: number;
  rate_limited: number;
  server_error: number;
  network_error: number;
  invalid_request: number;
  total: number;
}

export class OAuthManager {
  private pendingStates = new Map<string, OAuthState>();
  private tokens: OAuthTokens | null = null;
  private refreshPromise: Promise<TokenEndpointResult> | null = null;
  private sweepTimer: ReturnType<typeof setInterval> | null = null;
  private auditLog?: AuditLog;
  private expiryWarningMs = 30 * 60 * 1000; // default 30 minutes
  private expiryWarningEmitted = false;

  // OAuth flow funnel counters
  private flowsStarted = 0;
  private flowsCompleted = 0;
  private flowsExpired = 0;
  private flowsFailed = 0;

  // Per-error-type counters
  private errorCounts: Record<OAuthErrorType, number> = {
    invalid_grant: 0,
    rate_limited: 0,
    server_error: 0,
    network_error: 0,
    invalid_request: 0,
  };

  private static readonly DEFAULT_SCOPES = ['user:inference', 'user:profile'];

  constructor(private logger: Logger) {
    // Sweep expired pending states every 60 seconds
    this.sweepTimer = setInterval(() => this.sweepExpiredStates(), 60_000);
    this.sweepTimer.unref();
  }

  /**
   * Wire in the audit log and configurable expiry warning threshold.
   * Called after construction once the audit log is available.
   */
  setAuditLog(auditLog: AuditLog, warningMinutes?: number): void {
    this.auditLog = auditLog;
    if (warningMinutes !== undefined && warningMinutes > 0) {
      this.expiryWarningMs = warningMinutes * 60 * 1000;
    }
  }

  private sweepExpiredStates(): void {
    const now = Date.now();
    let swept = 0;
    for (const [key, entry] of this.pendingStates) {
      if (now > entry.expiresAt) {
        this.pendingStates.delete(key);
        this.flowsExpired++;
        swept++;
        this.logger.info({ stateId: key, ageMs: now - entry.createdAt }, 'OAuth flow expired — state swept');
      }
    }
    if (swept > 0) {
      this.logger.info({ swept, remaining: this.pendingStates.size }, 'Swept expired OAuth states');
    }
  }

  /**
   * POST to the OAuth token endpoint, parse the response, and map to OAuthTokens.
   * Callers supply only the grant-specific body fields and a scope fallback.
   * Returns a discriminated result with classified error info on failure.
   */
  private async postTokenEndpoint(
    body: Record<string, string>,
    fallbackScopes: string[],
    context: string,
  ): Promise<TokenEndpointResult> {
    try {
      const response = await fetch(OAUTH_TOKEN_URL, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Accept': 'application/json',
        },
        body: JSON.stringify(body),
      });

      if (!response.ok) {
        const errorText = await response.text();
        const classified = classifyHttpError(response.status, errorText);
        this.errorCounts[classified.errorType]++;
        this.logger.error(
          { status: response.status, errorType: classified.errorType, transient: classified.transient },
          `OAuth ${context} failed: ${classified.message}`,
        );
        try {
          this.auditLog?.record({
            action: 'oauth:token_error',
            resourceType: 'oauth',
            detail: { errorType: classified.errorType, status: response.status, transient: classified.transient, context },
          });
        } catch { /* logged inside record() */ }
        return { ok: false, error: classified };
      }

      const data = await response.json() as {
        access_token: string;
        refresh_token: string;
        expires_in: number;
        scope?: string;
      };

      return {
        ok: true,
        tokens: {
          accessToken: data.access_token,
          refreshToken: data.refresh_token,
          expiresAt: Date.now() + data.expires_in * 1000,
          scopes: data.scope ? data.scope.split(' ') : fallbackScopes,
        },
      };
    } catch (err) {
      const classified = classifyNetworkError(err);
      this.errorCounts[classified.errorType]++;
      this.logger.error(
        { err, errorType: classified.errorType, transient: classified.transient },
        `OAuth ${context} failed: ${classified.message}`,
      );
      try {
        this.auditLog?.record({
          action: 'oauth:token_error',
          resourceType: 'oauth',
          detail: { errorType: classified.errorType, transient: classified.transient, context },
        });
      } catch { /* logged inside record() */ }
      return { ok: false, error: classified };
    }
  }

  /**
   * Step 1: Generate the authorization URL for the user to visit.
   * Returns the URL and stores PKCE state internally.
   */
  generateAuthUrl(): { url: string; state: string } {
    const state = randomBytes(32).toString('hex');
    const codeVerifier = randomBytes(32).toString('base64url');
    const codeChallenge = createHash('sha256')
      .update(codeVerifier)
      .digest('base64url');

    const oauthState: OAuthState = {
      state,
      codeVerifier,
      createdAt: Date.now(),
      expiresAt: Date.now() + 10 * 60 * 1000, // 10 minutes
    };

    this.pendingStates.set(state, oauthState);

    const params = new URLSearchParams({
      code: 'true',
      client_id: CLIENT_ID,
      response_type: 'code',
      redirect_uri: REDIRECT_URI,
      scope: SCOPES,
      code_challenge: codeChallenge,
      code_challenge_method: 'S256',
      state,
    });

    const url = `${OAUTH_AUTHORIZE_URL}?${params.toString()}`;

    this.flowsStarted++;
    this.logger.info({ stateId: state }, 'OAuth flow started — authorization URL generated');
    return { url, state };
  }

  /**
   * Step 2: Exchange the authorization code for tokens.
   * Called after user completes the OAuth flow and provides the code.
   */
  async exchangeCode(authorizationCode: string, state: string): Promise<TokenEndpointResult> {
    // Validate state
    const pending = this.pendingStates.get(state);
    if (!pending) {
      this.flowsFailed++;
      this.logger.info({ stateId: state }, 'OAuth flow failed — state not found or already consumed');
      return { ok: false, error: { errorType: 'invalid_request', message: 'OAuth state not found or already consumed', transient: false } };
    }

    if (Date.now() > pending.expiresAt) {
      this.flowsFailed++;
      this.pendingStates.delete(state);
      this.logger.info({ stateId: state }, 'OAuth flow failed — state expired before code exchange');
      return { ok: false, error: { errorType: 'invalid_request', message: 'OAuth state expired', transient: false } };
    }

    const cleanedCode = authorizationCode.split('#')[0]?.split('&')[0] ?? authorizationCode;

    const result = await this.postTokenEndpoint(
      {
        grant_type: 'authorization_code',
        client_id: CLIENT_ID,
        code: cleanedCode,
        redirect_uri: REDIRECT_URI,
        code_verifier: pending.codeVerifier,
        state: pending.state,
      },
      OAuthManager.DEFAULT_SCOPES,
      'token exchange',
    );

    this.pendingStates.delete(state);

    if (result.ok) {
      this.flowsCompleted++;
      this.tokens = result.tokens;
      this.logger.info({
        stateId: state,
        scopes: this.tokens.scopes,
        expiresAt: new Date(this.tokens.expiresAt).toISOString(),
      }, 'OAuth flow completed — token exchange successful');
    } else {
      this.flowsFailed++;
      this.logger.info({ stateId: state, errorType: result.error.errorType }, 'OAuth flow failed — token exchange error');
    }

    return result;
  }

  /**
   * Refresh the access token using the refresh token.
   * Called automatically before dispatching if token is near expiry.
   * Uses promise dedup so concurrent callers share one in-flight refresh,
   * preventing single-use rotating refresh tokens from being consumed twice.
   */
  async refreshAccessToken(): Promise<TokenEndpointResult> {
    if (!this.tokens) {
      this.logger.error('No tokens to refresh');
      return { ok: false, error: { errorType: 'invalid_request', message: 'No tokens to refresh', transient: false } };
    }

    if (this.refreshPromise) {
      return this.refreshPromise;
    }

    this.refreshPromise = this.executeRefresh().finally(() => {
      this.refreshPromise = null;
    });
    return this.refreshPromise;
  }

  private async executeRefresh(): Promise<TokenEndpointResult> {
    const previousScopes = this.tokens!.scopes;

    // Refresh tokens are single-use rotation — store the new one
    const result = await this.postTokenEndpoint(
      {
        grant_type: 'refresh_token',
        client_id: CLIENT_ID,
        refresh_token: this.tokens!.refreshToken,
      },
      previousScopes,
      'token refresh',
    );

    if (result.ok) {
      this.tokens = result.tokens;
      this.logger.info({
        expiresAt: new Date(this.tokens.expiresAt).toISOString(),
      }, 'OAuth token refreshed');
      try { this.auditLog?.record({ action: 'oauth:refresh', resourceType: 'oauth', detail: { expiresAt: new Date(this.tokens.expiresAt).toISOString() } }); } catch { /* logged inside record() */ }
    } else {
      try { this.auditLog?.record({ action: 'oauth:refresh_failed', resourceType: 'oauth', detail: { reason: result.error.message, errorType: result.error.errorType } }); } catch { /* logged inside record() */ }
    }

    return result;
  }

  /**
   * Get a valid access token, refreshing if necessary.
   * Returns null if no tokens available or refresh fails.
   */
  async getValidAccessToken(): Promise<string | null> {
    if (!this.tokens) return null;

    const now = Date.now();
    const remainingMs = this.tokens.expiresAt - now;

    // Emit token:expiring_soon once per token when within the warning threshold
    if (remainingMs > 0 && remainingMs <= this.expiryWarningMs && !this.expiryWarningEmitted) {
      this.expiryWarningEmitted = true;
      try {
        this.auditLog?.record({
          action: 'token:expiring_soon',
          resourceType: 'oauth',
          detail: {
            expiresAt: new Date(this.tokens.expiresAt).toISOString(),
            remainingMinutes: Math.round(remainingMs / 60_000),
          },
        });
      } catch { /* logged inside record() */ }
      this.logger.warn({ remainingMinutes: Math.round(remainingMs / 60_000) }, 'OAuth token expiring soon');
    }

    // Refresh if within 10 minutes of expiry
    const TEN_MINUTES = 10 * 60 * 1000;
    if (now > this.tokens.expiresAt - TEN_MINUTES) {
      this.logger.info('Access token near expiry, refreshing...');
      const result = await this.refreshAccessToken();
      if (!result.ok) return null;
      // Reset expiry warning flag after successful refresh
      this.expiryWarningEmitted = false;
    }

    return this.tokens.accessToken;
  }

  /**
   * Load tokens directly (e.g. from encrypted storage on startup).
   */
  loadTokens(tokens: OAuthTokens): void {
    this.tokens = tokens;
    this.logger.info({
      expiresAt: new Date(tokens.expiresAt).toISOString(),
      scopes: tokens.scopes,
    }, 'OAuth tokens loaded');
  }

  /**
   * Get current tokens for persistence/encryption.
   */
  getTokens(): OAuthTokens | null {
    return this.tokens ? { ...this.tokens } : null;
  }

  hasTokens(): boolean {
    return this.tokens !== null;
  }

  /**
   * Revoke tokens on the Anthropic server (RFC 7009) then clear local state.
   * Best-effort: local state is always cleared even if revocation fails.
   */
  async revokeAndClearTokens(): Promise<{ revoked: boolean }> {
    const tokensToRevoke = this.tokens;
    // Clear local state immediately so the caller never stays "connected"
    this.tokens = null;
    this.pendingStates.clear();

    if (!tokensToRevoke) {
      this.logger.info('No tokens to revoke, local state cleared');
      return { revoked: false };
    }

    let revoked = false;
    // Revoke the refresh token first (invalidates the grant); then the access token.
    for (const { token, hint } of [
      { token: tokensToRevoke.refreshToken, hint: 'refresh_token' },
      { token: tokensToRevoke.accessToken, hint: 'access_token' },
    ] as const) {
      try {
        const response = await fetch(OAUTH_REVOKE_URL, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            client_id: CLIENT_ID,
            token,
            token_type_hint: hint,
          }),
        });
        // RFC 7009 §2.2: the server responds with 200 even if the token was already invalid
        if (response.ok) {
          this.logger.info({ hint }, 'OAuth token revoked on server');
          revoked = true;
        } else {
          const errorText = await response.text();
          this.logger.warn({ hint, status: response.status, error: errorText }, 'OAuth token revocation returned non-OK');
        }
      } catch (err) {
        this.logger.warn({ err, hint }, 'OAuth token revocation request failed');
      }
    }

    this.logger.info({ revoked }, 'OAuth disconnect complete, local tokens cleared');
    return { revoked };
  }

  clearTokens(): void {
    this.tokens = null;
    this.pendingStates.clear();
    this.logger.info('OAuth tokens cleared');
  }

  /**
   * Return OAuth flow funnel metrics for observability.
   */
  getFunnelMetrics(): OAuthFunnelMetrics {
    return {
      flowsStarted: this.flowsStarted,
      flowsCompleted: this.flowsCompleted,
      flowsExpired: this.flowsExpired,
      flowsFailed: this.flowsFailed,
    };
  }

  /**
   * Return per-error-type metrics for pattern detection and alerting.
   */
  getErrorMetrics(): OAuthErrorMetrics {
    const counts = { ...this.errorCounts };
    return {
      ...counts,
      total: counts.invalid_grant + counts.rate_limited + counts.server_error + counts.network_error + counts.invalid_request,
    };
  }

  /**
   * Stop the background sweep timer. Call on shutdown.
   */
  dispose(): void {
    if (this.sweepTimer) {
      clearInterval(this.sweepTimer);
      this.sweepTimer = null;
    }
  }
}
