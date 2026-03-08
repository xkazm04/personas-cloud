import { createHmac, timingSafeEqual } from 'node:crypto';
import { hashApiKey, verifyApiKey } from '@dac-cloud/shared';
import type { IncomingMessage } from 'node:http';

/**
 * Constant-time string comparison to prevent timing side-channel attacks.
 * Falls back to false when lengths differ (length is already leaked by most
 * network protocols, so this is acceptable).
 */
export function safeCompare(a: string, b: string): boolean {
  if (a.length !== b.length) return false;
  return timingSafeEqual(Buffer.from(a), Buffer.from(b));
}

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
  iss?: string;
  aud?: string;
  [key: string]: unknown;
}

interface JwtVerifyOptions {
  /** Expected issuer (iss) claim. Skipped if undefined. */
  expectedIssuer?: string;
  /** Expected audience (aud) claim. Skipped if undefined. */
  expectedAudience?: string;
}

function verifySupabaseJwt(
  token: string,
  secret: string,
  options: JwtVerifyOptions = {},
): JwtPayload | null {
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

  // Validate issuer claim
  if (options.expectedIssuer && payload.iss !== options.expectedIssuer) return null;

  // Validate audience claim
  if (options.expectedAudience && payload.aud !== options.expectedAudience) return null;

  return payload;
}

// ---------------------------------------------------------------------------
// Auth factory
// ---------------------------------------------------------------------------

export interface AuthOptions {
  supabaseJwtSecret?: string;
  /** Supabase project ref, used to derive the expected JWT issuer. */
  supabaseProjectRef?: string;
  /** When true, requests without a JWT fall back to admin context. Defaults to false. */
  enableAdminFallback?: boolean;
}

export function createAuth(teamApiKey: string, options: AuthOptions = {}) {
  const { supabaseJwtSecret, supabaseProjectRef, enableAdminFallback = false } = options;
  const jwtVerifyOptions: JwtVerifyOptions = {
    expectedAudience: supabaseJwtSecret ? 'authenticated' : undefined,
    expectedIssuer: supabaseProjectRef
      ? `https://${supabaseProjectRef}.supabase.co/auth/v1`
      : undefined,
  };
  // Pre-compute salted hash once at startup for efficient verification
  const expectedHash = hashApiKey(teamApiKey);

  function validateApiKey(req: IncomingMessage): boolean {
    const authHeader = req.headers['authorization'];
    if (!authHeader) return false;
    const parts = authHeader.split(' ');
    if (parts.length !== 2 || parts[0] !== 'Bearer') return false;
    return verifyApiKey(parts[1]!, expectedHash);
  }

  return {
    /** Legacy: simple boolean API-key check */
    validateRequest(req: IncomingMessage): boolean {
      return validateApiKey(req);
    },

    validateToken(token: string): boolean {
      return verifyApiKey(token, expectedHash);
    },

    validateWorkerToken(token: string, expectedWorkerToken: string): boolean {
      return safeCompare(token, expectedWorkerToken);
    },

    /**
     * Dual auth: validates API key + Supabase JWT.
     * Returns a RequestContext describing the caller.
     *
     * - JWT present + valid → user context (projectId = Supabase UUID)
     * - No JWT + enableAdminFallback → admin context (projectId = '__admin__')
     * - No JWT + !enableAdminFallback → null (reject)
     * - Invalid API key → null (reject)
     */
    validateAndExtractContext(req: IncomingMessage): RequestContext | null {
      // API key is always required
      if (!validateApiKey(req)) return null;

      // Check for user JWT
      const userToken = req.headers['x-user-token'] as string | undefined;
      if (userToken && supabaseJwtSecret) {
        const jwt = verifySupabaseJwt(userToken, supabaseJwtSecret, jwtVerifyOptions);
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

      // No JWT provided (or JWT auth not configured)
      // Only grant admin context if explicitly enabled
      if (enableAdminFallback) {
        return { authType: 'admin', projectId: '__admin__' };
      }

      // Admin fallback disabled — reject requests without a valid JWT
      return null;
    },
  };
}

export type Auth = ReturnType<typeof createAuth>;
