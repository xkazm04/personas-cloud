import { randomBytes, createHash } from 'node:crypto';
import type { Logger } from 'pino';

// Anthropic OAuth constants (from Claude Code CLI)
const OAUTH_AUTHORIZE_URL = 'https://claude.ai/oauth/authorize';
const OAUTH_TOKEN_URL = 'https://console.anthropic.com/v1/oauth/token';
const CLIENT_ID = '9d1c250a-e61b-44d9-88ed-5944d1962f5e';
const REDIRECT_URI = 'https://console.anthropic.com/oauth/code/callback';
const SCOPES = 'org:create_api_key user:profile user:inference';

export interface OAuthTokens {
  accessToken: string;
  refreshToken: string;
  expiresAt: number;    // unix ms
  scopes: string[];
}

export interface OAuthState {
  state: string;
  codeVerifier: string;
  createdAt: number;
  expiresAt: number;    // unix ms, 10 minute window
}

const MAX_PENDING_STATES = 50;

export class OAuthManager {
  private pendingStates = new Map<string, OAuthState>();
  private tokens: OAuthTokens | null = null;
  /** In-flight refresh promise shared by concurrent callers. */
  private refreshPromise: Promise<OAuthTokens | null> | null = null;

  constructor(private logger: Logger) {}

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

    // Prune expired entries to prevent unbounded growth
    const now = Date.now();
    for (const [key, entry] of this.pendingStates) {
      if (now > entry.expiresAt) this.pendingStates.delete(key);
    }

    // Cap total pending states to prevent memory DoS via rapid authorize calls
    if (this.pendingStates.size >= MAX_PENDING_STATES) {
      // Evict the oldest entry to make room
      const oldest = this.pendingStates.keys().next().value!;
      this.pendingStates.delete(oldest);
    }

    this.pendingStates.set(state, {
      state,
      codeVerifier,
      createdAt: now,
      expiresAt: now + 10 * 60 * 1000, // 10 minutes
    });

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

    this.logger.info('Generated OAuth authorization URL');
    return { url, state };
  }

  /**
   * Step 2: Exchange the authorization code for tokens.
   * Called after user completes the OAuth flow and provides the code.
   */
  async exchangeCode(authorizationCode: string, state: string): Promise<OAuthTokens | null> {
    // Validate state
    const pending = this.pendingStates.get(state);
    if (!pending) {
      this.logger.error('No pending OAuth state for this state parameter — call generateAuthUrl first');
      return null;
    }

    if (Date.now() > pending.expiresAt) {
      this.logger.error('OAuth state expired');
      this.pendingStates.delete(state);
      return null;
    }

    // Consume the state so it cannot be replayed
    this.pendingStates.delete(state);

    const cleanedCode = authorizationCode.split('#')[0]?.split('&')[0] ?? authorizationCode;

    const body = {
      grant_type: 'authorization_code',
      client_id: CLIENT_ID,
      code: cleanedCode,
      redirect_uri: REDIRECT_URI,
      code_verifier: pending.codeVerifier,
      state: pending.state,
    };

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
        this.logger.error({ status: response.status, error: errorText }, 'OAuth token exchange failed');
        return null;
      }

      const data = await response.json() as {
        access_token: string;
        refresh_token: string;
        expires_in: number;
        scope?: string;
      };

      this.tokens = {
        accessToken: data.access_token,
        refreshToken: data.refresh_token,
        expiresAt: Date.now() + data.expires_in * 1000,
        scopes: data.scope ? data.scope.split(' ') : ['user:inference', 'user:profile'],
      };

      this.logger.info({
        scopes: this.tokens.scopes,
        expiresAt: new Date(this.tokens.expiresAt).toISOString(),
      }, 'OAuth token exchange successful');

      return this.tokens;

    } catch (err) {
      this.logger.error({ err }, 'OAuth token exchange request failed');
      return null;
    }
  }

  /**
   * Refresh the access token using the refresh token.
   * Called automatically before dispatching if token is near expiry.
   */
  async refreshAccessToken(): Promise<OAuthTokens | null> {
    if (!this.tokens) {
      this.logger.error('No tokens to refresh');
      return null;
    }

    const body = {
      grant_type: 'refresh_token',
      client_id: CLIENT_ID,
      refresh_token: this.tokens.refreshToken,
    };

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
        this.logger.error({ status: response.status, error: errorText }, 'OAuth token refresh failed');
        return null;
      }

      const data = await response.json() as {
        access_token: string;
        refresh_token: string;
        expires_in: number;
        scope?: string;
      };

      // Refresh tokens are single-use rotation — store the new one
      this.tokens = {
        accessToken: data.access_token,
        refreshToken: data.refresh_token,
        expiresAt: Date.now() + data.expires_in * 1000,
        scopes: data.scope ? data.scope.split(' ') : this.tokens.scopes,
      };

      this.logger.info({
        expiresAt: new Date(this.tokens.expiresAt).toISOString(),
      }, 'OAuth token refreshed');

      return this.tokens;

    } catch (err) {
      this.logger.error({ err }, 'OAuth token refresh request failed');
      return null;
    }
  }

  /**
   * Get a valid access token, refreshing if necessary.
   *
   * Uses a pending-promise mutex so concurrent callers share a single
   * in-flight refresh instead of racing against each other.  This is
   * critical because OAuth refresh tokens are single-use rotation tokens:
   * if two callers both call refreshAccessToken() concurrently, the second
   * one sends an already-invalidated refresh token, causing a 401 and
   * permanently breaking the token chain.
   *
   * Returns null if no tokens available or refresh fails.
   */
  async getValidAccessToken(): Promise<string | null> {
    if (!this.tokens) return null;

    // Refresh if within 10 minutes of expiry
    const TEN_MINUTES = 10 * 60 * 1000;
    if (Date.now() > this.tokens.expiresAt - TEN_MINUTES) {
      // If a refresh is already in-flight, wait for it instead of starting another.
      // Both the first caller (who starts the refresh) and subsequent callers
      // go through the same await path to avoid TOCTOU gaps.
      if (!this.refreshPromise) {
        this.logger.info('Access token near expiry, refreshing...');
        this.refreshPromise = this.refreshAccessToken().finally(() => {
          this.refreshPromise = null;
        });
      } else {
        this.logger.info('Token refresh already in-flight, waiting...');
      }

      const result = await this.refreshPromise;
      if (!result) return null;
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

  clearTokens(): void {
    this.tokens = null;
    this.pendingStates.clear();
    this.logger.info('OAuth tokens cleared');
  }
}
