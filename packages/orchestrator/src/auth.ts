import { createHmac } from 'node:crypto';
import { hashApiKey } from '@dac-cloud/shared';
import type { IncomingMessage } from 'node:http';

// ---------------------------------------------------------------------------
// RequestContext — identifies the caller for multi-tenant isolation
// ---------------------------------------------------------------------------

export interface RequestContext {
  authType: 'user' | 'admin';
  /** Supabase user UUID for 'user', '__admin__' for API-key-only 'admin' */
  projectId: string;
  userEmail?: string;
}

// ---------------------------------------------------------------------------
// JWT helpers (HS256 verification without extra dependencies)
// ---------------------------------------------------------------------------

function base64UrlDecode(str: string): Buffer {
  // Restore standard base64 padding
  const padded = str.replace(/-/g, '+').replace(/_/g, '/');
  return Buffer.from(padded, 'base64');
}

interface JwtPayload {
  sub: string;
  email?: string;
  exp?: number;
  [key: string]: unknown;
}

function verifySupabaseJwt(token: string, secret: string): JwtPayload | null {
  const parts = token.split('.');
  if (parts.length !== 3) return null;

  const [headerB64, payloadB64, signatureB64] = parts;

  // Verify HS256 signature
  const expected = createHmac('sha256', secret)
    .update(`${headerB64}.${payloadB64}`)
    .digest();

  const actual = base64UrlDecode(signatureB64!);
  if (!expected.equals(actual)) return null;

  // Decode payload
  let payload: JwtPayload;
  try {
    payload = JSON.parse(base64UrlDecode(payloadB64!).toString('utf8'));
  } catch {
    return null;
  }

  // Check expiration
  if (payload.exp && payload.exp * 1000 < Date.now()) return null;

  if (!payload.sub) return null;

  return payload;
}

// ---------------------------------------------------------------------------
// Auth factory
// ---------------------------------------------------------------------------

export function createAuth(teamApiKey: string, supabaseJwtSecret?: string) {
  const expectedHash = hashApiKey(teamApiKey);

  function validateApiKey(req: IncomingMessage): boolean {
    const authHeader = req.headers['authorization'];
    if (!authHeader) return false;
    const parts = authHeader.split(' ');
    if (parts.length !== 2 || parts[0] !== 'Bearer') return false;
    return hashApiKey(parts[1]!) === expectedHash;
  }

  return {
    /** Legacy: simple boolean API-key check */
    validateRequest(req: IncomingMessage): boolean {
      return validateApiKey(req);
    },

    validateToken(token: string): boolean {
      return hashApiKey(token) === expectedHash;
    },

    validateWorkerToken(token: string, expectedWorkerToken: string): boolean {
      return token === expectedWorkerToken;
    },

    /**
     * Dual auth: validates API key + optional Supabase JWT.
     * Returns a RequestContext describing the caller.
     *
     * - JWT present + valid → user context (projectId = Supabase UUID)
     * - No JWT or JWT disabled → admin context (projectId = '__admin__')
     * - Invalid API key → null (reject)
     */
    validateAndExtractContext(req: IncomingMessage): RequestContext | null {
      // API key is always required
      if (!validateApiKey(req)) return null;

      // Check for user JWT
      const userToken = req.headers['x-user-token'] as string | undefined;
      if (userToken && supabaseJwtSecret) {
        const jwt = verifySupabaseJwt(userToken, supabaseJwtSecret);
        if (jwt) {
          return {
            authType: 'user',
            projectId: jwt.sub,
            userEmail: jwt.email,
          };
        }
        // Invalid/expired JWT → reject (don't silently fall back to admin)
        return null;
      }

      // No JWT or JWT auth disabled → admin context
      return { authType: 'admin', projectId: '__admin__' };
    },
  };
}

export type Auth = ReturnType<typeof createAuth>;
