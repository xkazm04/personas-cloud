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

export class OAuthManager {
  private pendingState: OAuthState | null = null;
  private tokens: OAuthTokens | null = null;

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

    this.pendingState = {
      state,
      codeVerifier,
      createdAt: Date.now(),
      expiresAt: Date.now() + 10 * 60 * 1000, // 10 minutes
    };

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
    if (!this.pendingState) {
      this.logger.error('No pending OAuth state — call generateAuthUrl first');
      return null;
    }

    if (this.pendingState.state !== state) {
      this.logger.error('OAuth state mismatch');
      return null;
    }

    if (Date.now() > this.pendingState.expiresAt) {
      this.logger.error('OAuth state expired');
      this.pendingState = null;
      return null;
    }

    const cleanedCode = authorizationCode.split('#')[0]?.split('&')[0] ?? authorizationCode;

    const body = {
      grant_type: 'authorization_code',
      client_id: CLIENT_ID,
      code: cleanedCode,
      redirect_uri: REDIRECT_URI,
      code_verifier: this.pendingState.codeVerifier,
      state: this.pendingState.state,
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

      this.pendingState = null;

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
   * Returns null if no tokens available or refresh fails.
   */
  async getValidAccessToken(): Promise<string | null> {
    if (!this.tokens) return null;

    // Refresh if within 10 minutes of expiry
    const TEN_MINUTES = 10 * 60 * 1000;
    if (Date.now() > this.tokens.expiresAt - TEN_MINUTES) {
      this.logger.info('Access token near expiry, refreshing...');
      const refreshed = await this.refreshAccessToken();
      if (!refreshed) return null;
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
    this.pendingState = null;
    this.logger.info('OAuth tokens cleared');
  }
}
