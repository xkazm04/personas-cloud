import http from 'node:http';
import { createHmac, randomBytes } from 'node:crypto';
import { nanoid } from 'nanoid';
import type { ZodSchema } from 'zod';
import { encrypt, createPersonaWithDefaults, createToolDefinitionWithDefaults, assembleCompilationPrompt, assembleBatchCompilationPrompt, validatePayloadFilter, parseModelProfile, parsePermissionPolicy, decrypt, deriveMasterKey } from '@dac-cloud/shared';
import type { ExecRequest, Persona, PersonaToolDefinition, CompileRequest, BatchCompileRequest, CompileResult, BatchCompileResult, BatchCompileItemResult, EncryptedPayload } from '@dac-cloud/shared';
import type Database from 'better-sqlite3';
import { safeCompare } from './auth.js';
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
=======
import * as db from './db.js';
import { gitlabEventType } from './gitlab.js';
import {
  OAuthCallbackSchema, SetTokenSchema, ExecuteSchema,
  PersonaCreateSchema, PersonaUpdateSchema, LinkToolSchema, ToolDefinitionCreateSchema,
  CredentialCreateSchema, LinkCredentialSchema,
  SubscriptionCreateSchema, SubscriptionUpdateSchema,
  TriggerCreateSchema, TriggerUpdateSchema,
  EventCreateSchema, EventUpdateSchema,
  CompileSchema, BatchCompileSchema,
  DeploymentCreateSchema, formatZodError,
} from './schemas.js';

// ---------------------------------------------------------------------------
// Rate limiter — O(1) sliding window counter per IP with per-category limits
// ---------------------------------------------------------------------------

/** Endpoint category for rate limiting purposes. */
type RateCategory = 'auth' | 'execute' | 'webhook' | 'crud';

/** Sliding window counter using two fixed time buckets (current + previous). */
interface RateCounter {
  current: number;
  prev: number;
  lastId: number;
}

interface RateBucket {
  /** Per-category counters */
  categories: Record<RateCategory, RateCounter>;
  /** General (aggregate) counter */
  general: RateCounter;
  /** Consecutive auth failure count (for progressive backoff) */
  authFailures: number;
  /** Timestamp of last auth failure reset */
  lastFailureReset: number;
}

const RATE_WINDOW_MS = 60_000;        // 1-minute sliding window
const RATE_LIMIT_GENERAL = 120;       // 120 requests/min per IP (aggregate ceiling)
const RATE_LIMIT_AUTH_FAIL_THRESHOLD = 10; // After 10 auth failures, block for backoff period
const AUTH_FAIL_BACKOFF_MS = 60_000;  // 1 minute backoff per failure wave

/** Per-category rate limits (requests per minute per IP). */
const CATEGORY_LIMITS: Record<RateCategory, number> = {
  auth: 10,       // /api/token, /api/oauth/*
  execute: 30,    // /api/execute, /api/executions/*/cancel
  webhook: 60,    // /api/gitlab/webhook, /api/webhooks/*
  crud: 100,      // all other CRUD endpoints
};

function createRateLimiter() {
  const buckets = new Map<string, RateBucket>();

  function createCounter(nowId: number): RateCounter {
    return { current: 0, prev: 0, lastId: nowId };
  }

  function emptyCategories(nowId: number): Record<RateCategory, RateCounter> {
    return {
      auth: createCounter(nowId),
      execute: createCounter(nowId),
      webhook: createCounter(nowId),
      crud: createCounter(nowId),
    };
  }

  /** Calculate weighted estimate of requests in the last window. */
  function getEstimate(counter: RateCounter, now: number): number {
    const bucketId = Math.floor(now / RATE_WINDOW_MS);
    let current = counter.current;
    let prev = counter.prev;

    if (counter.lastId !== bucketId) {
      prev = counter.lastId === bucketId - 1 ? counter.current : 0;
      current = 0;
    }

    const weight = (RATE_WINDOW_MS - (now % RATE_WINDOW_MS)) / RATE_WINDOW_MS;
    return current + (prev * weight);
  }

  /** Increment the counter for the current bucket. */
  function increment(counter: RateCounter, now: number): void {
    const bucketId = Math.floor(now / RATE_WINDOW_MS);
    if (counter.lastId !== bucketId) {
      counter.prev = counter.lastId === bucketId - 1 ? counter.current : 0;
      counter.current = 0;
      counter.lastId = bucketId;
    }
    counter.current++;
  }

  // Periodic cleanup of stale entries every 5 minutes
  setInterval(() => {
    const now = Date.now();
    const currentId = Math.floor(now / RATE_WINDOW_MS);
    for (const [ip, bucket] of buckets) {
      const isStale = (c: RateCounter) => c.lastId < currentId - 1;
      const allStale = isStale(bucket.general) &&
                       (Object.values(bucket.categories) as RateCounter[]).every(isStale);

      if (allStale && bucket.authFailures === 0 && now - bucket.lastFailureReset > AUTH_FAIL_BACKOFF_MS * 2) {
        buckets.delete(ip);
      }
    }
  }, 5 * 60_000).unref();

  function getBucket(ip: string): RateBucket {
    let bucket = buckets.get(ip);
    if (!bucket) {
      const nowId = Math.floor(Date.now() / RATE_WINDOW_MS);
      bucket = {
        categories: emptyCategories(nowId),
        general: createCounter(nowId),
        authFailures: 0,
        lastFailureReset: Date.now()
      };
      buckets.set(ip, bucket);
    }
    return bucket;
  }

  return {
    allow(ip: string, category: RateCategory): boolean {
      const now = Date.now();
      const bucket = getBucket(ip);

      // Check auth failure backoff
      if (bucket.authFailures >= RATE_LIMIT_AUTH_FAIL_THRESHOLD) {
        const backoffEnd = bucket.lastFailureReset + AUTH_FAIL_BACKOFF_MS;
        if (now < backoffEnd) return false;
        bucket.authFailures = 0;
        bucket.lastFailureReset = now;
      }

      // Check per-category and aggregate limits
      if (getEstimate(bucket.categories[category], now) >= CATEGORY_LIMITS[category]) return false;
      if (getEstimate(bucket.general, now) >= RATE_LIMIT_GENERAL) return false;

      increment(bucket.categories[category], now);
      increment(bucket.general, now);
      return true;
    },

    recordAuthFailure(ip: string): void {
      const bucket = getBucket(ip);
      bucket.authFailures++;
      if (bucket.authFailures === 1) {
        bucket.lastFailureReset = Date.now();
      }
    },
  };
}

// ---------------------------------------------------------------------------
// Declarative route-to-category map — makes rate-limit assignments explicit
// ---------------------------------------------------------------------------

interface RouteRule {
  /** HTTP method (or '*' for any method). */
  method: string;
  /** Exact path match, or prefix match when `prefix` is true. */
  path: string;
  /** If true, `path` is treated as a prefix. */
  prefix?: boolean;
  /** Optional suffix the pathname must end with (combined with prefix match). */
  suffix?: string;
  category: RateCategory;
}

/**
 * Declarative route-to-rate-limit-category table.
 * Rules are evaluated top-to-bottom; first match wins.
 * When adding a new endpoint, add a corresponding entry here so it doesn't
 * silently fall through to the default 'crud' bucket.
 */
const ROUTE_CATEGORY_MAP: RouteRule[] = [
  // --- Auth ---
  { method: 'POST', path: '/api/token', category: 'auth' },
  { method: '*', path: '/api/oauth/', prefix: true, category: 'auth' },

  // --- Execution (expensive LLM calls) ---
  { method: 'POST', path: '/api/execute', category: 'execute' },
  { method: 'POST', path: '/api/personas/compile', category: 'execute' },
  { method: 'POST', path: '/api/personas/compile/batch', category: 'execute' },
  { method: 'GET', path: '/api/personas/compile/batch/', prefix: true, category: 'execute' },
  { method: 'GET', path: '/api/personas/compile/', prefix: true, category: 'execute' },
  { method: 'POST', path: '/api/executions/', prefix: true, suffix: '/cancel', category: 'execute' },

  // --- Webhooks ---
  { method: '*', path: '/api/gitlab/webhook', category: 'webhook' },
  { method: 'POST', path: '/api/webhooks/', prefix: true, category: 'webhook' },
  { method: 'POST', path: '/api/deployed/', prefix: true, category: 'webhook' },
];

/**
 * Classify a request pathname + method into a rate limit category.
 * Uses the declarative ROUTE_CATEGORY_MAP; falls back to 'crud' if no rule matches.
 */
function classifyEndpoint(pathname: string, method: string): RateCategory {
  for (const rule of ROUTE_CATEGORY_MAP) {
    if (rule.method !== '*' && rule.method !== method) continue;
    if (rule.prefix) {
      if (!pathname.startsWith(rule.path)) continue;
      if (rule.suffix && !pathname.endsWith(rule.suffix)) continue;
    } else {
      if (pathname !== rule.path) continue;
    }
    return rule.category;
  }
  return 'crud';
}

/**
 * Known route prefixes that the server handles.
 * Used at startup to verify every known route has an explicit rate-limit
 * category assignment. When a new route is added to the server but not to
 * ROUTE_CATEGORY_MAP, a warning is logged so it doesn't silently inherit
 * the default 'crud' bucket.
 */
const KNOWN_ROUTE_PREFIXES: Array<{ method: string; path: string }> = [
  // Unauthenticated
  { method: 'GET', path: '/health' },
  { method: 'GET', path: '/metrics' },
  { method: 'GET', path: '/api/metrics' },
  // Auth
  { method: 'POST', path: '/api/token' },
  { method: 'POST', path: '/api/oauth/authorize' },
  { method: 'POST', path: '/api/oauth/callback' },
  { method: 'GET', path: '/api/oauth/status' },
  { method: 'POST', path: '/api/oauth/refresh' },
  { method: 'DELETE', path: '/api/oauth/disconnect' },
  // Execution
  { method: 'POST', path: '/api/execute' },
  { method: 'POST', path: '/api/personas/compile' },
  { method: 'POST', path: '/api/personas/compile/batch' },
  { method: 'GET', path: '/api/personas/compile/batch/:batchId' },
  { method: 'GET', path: '/api/personas/compile/:id' },
  { method: 'POST', path: '/api/executions/:id/cancel' },
  // Webhooks
  { method: 'POST', path: '/api/gitlab/webhook' },
  { method: 'POST', path: '/api/webhooks/:id' },
  { method: 'POST', path: '/api/deployed/:slug' },
  // Personas
  { method: 'GET', path: '/api/personas' },
  { method: 'POST', path: '/api/personas' },
  { method: 'GET', path: '/api/personas/:id' },
  { method: 'PATCH', path: '/api/personas/:id' },
  { method: 'PUT', path: '/api/personas/:id' },
  { method: 'DELETE', path: '/api/personas/:id' },
  { method: 'POST', path: '/api/personas/:id/validate' },
  { method: 'GET', path: '/api/personas/:id/tools' },
  { method: 'POST', path: '/api/personas/:id/tools' },
  { method: 'GET', path: '/api/personas/:id/credentials' },
  { method: 'POST', path: '/api/personas/:id/credentials' },
  { method: 'GET', path: '/api/personas/:id/subscriptions' },
  { method: 'GET', path: '/api/personas/:id/triggers' },
  // Tool definitions
  { method: 'POST', path: '/api/tool-definitions' },
  // Credentials
  { method: 'POST', path: '/api/credentials' },
  { method: 'DELETE', path: '/api/credentials/:id' },
  // Subscriptions
  { method: 'POST', path: '/api/subscriptions' },
  { method: 'PUT', path: '/api/subscriptions/:id' },
  { method: 'DELETE', path: '/api/subscriptions/:id' },
  // Triggers
  { method: 'POST', path: '/api/triggers' },
  { method: 'PUT', path: '/api/triggers/:id' },
  { method: 'DELETE', path: '/api/triggers/:id' },
  { method: 'GET', path: '/api/triggers/:id/firings' },
  { method: 'GET', path: '/api/triggers/:id/stats' },
  // Events
  { method: 'GET', path: '/api/events' },
  { method: 'POST', path: '/api/events' },
  { method: 'PUT', path: '/api/events/:id' },
  { method: 'GET', path: '/api/events/:id/audit' },
  { method: 'POST', path: '/api/events/:id/retry' },
  // Executions
  { method: 'GET', path: '/api/executions' },
  { method: 'GET', path: '/api/executions/stats' },
  { method: 'GET', path: '/api/executions/:id' },
  { method: 'GET', path: '/api/executions/:id/stream' },
  { method: 'GET', path: '/api/executions/:id/reviews' },
  { method: 'POST', path: '/api/executions/:id/reviews/:reviewId/respond' },
  // Reviews
  { method: 'GET', path: '/api/reviews/pending' },
  // Status
  { method: 'GET', path: '/api/status' },
  // Deployments
  { method: 'GET', path: '/api/deployments' },
  { method: 'POST', path: '/api/deployments' },
  { method: 'GET', path: '/api/deployments/:id' },
  { method: 'POST', path: '/api/deployments/:id/pause' },
  { method: 'POST', path: '/api/deployments/:id/resume' },
  { method: 'DELETE', path: '/api/deployments/:id' },
  // Admin
  { method: 'GET', path: '/api/admin/users' },
  { method: 'GET', path: '/api/admin/personas' },
  { method: 'GET', path: '/api/admin/executions' },
];

/**
 * Verify that every known route has an explicit rate-limit category.
 * Logs a warning for routes falling through to the default 'crud' bucket
 * so new endpoints don't silently inherit permissive limits.
 * Call once at startup.
 */
function verifyRouteCategoryAssignments(logger: Logger): void {
  // Routes that intentionally use 'crud' (explicitly documented here)
  const intentionalCrud = new Set([
    'GET:/health', 'GET:/metrics', 'GET:/api/metrics', 'GET:/api/status',
  ]);

  for (const route of KNOWN_ROUTE_PREFIXES) {
    // Resolve parameterized paths to a representative pathname for classification
    const testPath = route.path
      .replace(/:id/g, 'test-id')
      .replace(/:slug/g, 'test-slug')
      .replace(/:reviewId/g, 'test-review');
    const category = classifyEndpoint(testPath, route.method);
    const routeKey = `${route.method}:${route.path}`;

    if (category === 'crud' && !intentionalCrud.has(routeKey)) {
      // Only warn for API routes — health/metrics are intentionally unclassified
      if (route.path.startsWith('/api/')) {
        logger.debug({ route: routeKey, assignedCategory: category }, 'Route uses default rate-limit category — consider adding an explicit mapping in ROUTE_CATEGORY_MAP');
      }
    }
  }
}

// ---------------------------------------------------------------------------
// Idempotency key cache — prevents duplicate mutations from retried requests
// ---------------------------------------------------------------------------

interface IdempotencyEntry {
  status: number;
  body: string;
  createdAt: number;
}

const IDEMPOTENCY_TTL_MS = 10 * 60_000; // 10 minutes
const IDEMPOTENCY_MAX_SIZE = 1000;

function createIdempotencyCache() {
  const cache = new Map<string, IdempotencyEntry>();

  // Periodic cleanup every 2 minutes
  setInterval(() => {
    const now = Date.now();
    for (const [key, entry] of cache) {
      if (now - entry.createdAt > IDEMPOTENCY_TTL_MS) cache.delete(key);
    }
  }, 2 * 60_000).unref();

  return {
    get(key: string): IdempotencyEntry | undefined {
      const entry = cache.get(key);
      if (entry && Date.now() - entry.createdAt > IDEMPOTENCY_TTL_MS) {
        cache.delete(key);
        return undefined;
      }
      return entry;
    },
    set(key: string, status: number, body: string): void {
      // FIFO eviction
      if (cache.size >= IDEMPOTENCY_MAX_SIZE) {
        const firstKey = cache.keys().next().value!;
        cache.delete(firstKey);
      }
      cache.set(key, { status, body, createdAt: Date.now() });
    },
  };
}

// ---------------------------------------------------------------------------
// Declarative authenticated route table
// ---------------------------------------------------------------------------

/**
 * Context object passed to every authenticated route handler.
 * Centralises access to all dependencies so handlers don't need bespoke parameter lists.
 */
interface RouteCtx {
  req: http.IncomingMessage;
  res: http.ServerResponse;
  ctx: RequestContext;
  scopedProjectId: string | undefined;
  database: Database.Database | null;
  dispatcher: Dispatcher;
  pool: WorkerPool;
  tokenManager: TokenManager;
  oauth: OAuthManager;
  logger: Logger;
  pathname: string;
  queryString: string | undefined;
  onEventPublished: (() => void) | undefined;
  /** Extract the ID parameter from a prefix-matched route (strip prefix and optional suffix). */
  param: (prefix: string, suffix?: string) => string;
}

/** A single entry in the authenticated route table. */
interface AuthRoute {
  method: string;
  /**
   * How to match the pathname:
   * - `exact`:  pathname === path
   * - `prefix`: pathname starts with path and has additional characters (like matchRoute)
   * - `regex`:  pathname matches the regex pattern
   */
  match: 'exact' | 'prefix' | 'regex';
  /** Exact path, prefix string, or regex pattern string. */
  path: string | RegExp;
  /** Optional: pathname must end with this suffix (for prefix matches). */
  suffix?: string;
  /** Optional: pathname must NOT end with any of these (for prefix matches). */
  excludeSuffixes?: string[];
  /** If true, the handler requires a non-null database. Default false. */
  requiresDb?: boolean;
  /** If true, the route is admin-only. Default false. */
  admin?: boolean;
  /** Handler function. Returns void or a Promise<void>. */
  handler: (rc: RouteCtx) => void | Promise<void>;
}

/**
 * Match a request against the route table.  Returns the first matching route, or undefined.
 */
function findRoute(routes: AuthRoute[], method: string, pathname: string, hasDb: boolean): AuthRoute | undefined {
  for (const route of routes) {
    if (route.method !== method) continue;
    if (route.requiresDb && !hasDb) continue;

    switch (route.match) {
      case 'exact':
        if (pathname !== route.path) continue;
        break;
      case 'prefix': {
        const prefix = route.path as string;
        if (!pathname.startsWith(prefix) || pathname.length <= prefix.length) continue;
        if (route.suffix && !pathname.endsWith(route.suffix)) continue;
        if (route.excludeSuffixes?.some(s => pathname.endsWith(s))) continue;
        break;
      }
      case 'regex':
        if (!(route.path as RegExp).test(pathname)) continue;
        break;
    }

    return route;
  }
  return undefined;
}

// ---------------------------------------------------------------------------
// Batch compilation tracking — maps batchId → executionId
// ---------------------------------------------------------------------------

/** Maximum number of batch entries before FIFO eviction. */
const BATCH_TRACKING_MAX_SIZE = 500;
const batchTracker = new Map<string, { execId: string; count: number; projectId?: string }>();

function trackBatch(batchId: string, execId: string, count: number, projectId?: string): void {
  if (batchTracker.size >= BATCH_TRACKING_MAX_SIZE) {
    const oldest = batchTracker.keys().next().value!;
    batchTracker.delete(oldest);
  }
  batchTracker.set(batchId, { execId, count, projectId });
}

/**
 * Authenticated route table — evaluated top-to-bottom, first match wins.
 *
 * Each entry declares method, pattern, and a handler that receives a RouteCtx.
 * The order mirrors the original if/else chain to preserve route priority
 * (e.g. `/api/personas/compile` before `/api/personas/:id`).
 */
const AUTHENTICATED_ROUTES: AuthRoute[] = [
  // --- OAuth endpoints ---
  { method: 'POST', match: 'exact', path: '/api/oauth/authorize',
    handler: (rc) => handleOAuthAuthorize(rc.res, rc.oauth) },
  { method: 'POST', match: 'exact', path: '/api/oauth/callback',
    handler: (rc) => handleOAuthCallback(rc.req, rc.res, rc.oauth, rc.tokenManager, rc.logger) },
  { method: 'GET', match: 'exact', path: '/api/oauth/status',
    handler: (rc) => handleOAuthStatus(rc.res, rc.oauth) },
  { method: 'POST', match: 'exact', path: '/api/oauth/refresh',
    handler: (rc) => handleOAuthRefresh(rc.res, rc.oauth, rc.tokenManager, rc.logger) },
  { method: 'DELETE', match: 'exact', path: '/api/oauth/disconnect',
    handler: (rc) => handleOAuthDisconnect(rc.res, rc.oauth, rc.tokenManager) },

  // --- Token injection ---
  { method: 'POST', match: 'exact', path: '/api/token',
    handler: (rc) => handleSetToken(rc.req, rc.res, rc.tokenManager, rc.logger) },

  // --- Compilation API (must precede generic /api/personas/ CRUD routes) ---
  { method: 'POST', match: 'exact', path: '/api/personas/compile',
    handler: (rc) => handleCompile(rc.req, rc.res, rc.dispatcher, rc.logger, rc.ctx, rc.database ?? undefined) },
  { method: 'POST', match: 'exact', path: '/api/personas/compile/batch',
    handler: (rc) => handleBatchCompile(rc.req, rc.res, rc.dispatcher, rc.logger, rc.ctx, rc.database ?? undefined) },
  { method: 'GET', match: 'prefix', path: '/api/personas/compile/batch/',
    handler: (rc) => {
      const batchId = rc.param('/api/personas/compile/batch/');
      handleGetBatchCompilation(rc.res, rc.dispatcher, batchId);
    } },
  { method: 'GET', match: 'prefix', path: '/api/personas/compile/',
    handler: (rc) => {
      const compileId = rc.param('/api/personas/compile/');
      handleGetCompilation(rc.res, rc.dispatcher, compileId);
    } },

  // --- Persona Validation ---
  { method: 'POST', match: 'prefix', path: '/api/personas/', suffix: '/validate', requiresDb: true,
    handler: (rc) => {
      const id = rc.param('/api/personas/', '/validate');
      const persona = requireOwnership(rc.res, db.getPersona(rc.database!, id), rc.scopedProjectId);
      if (!persona) return;
      const report = validatePersona(persona, rc.database!, rc.dispatcher.getMasterKey(), rc.logger);
      json(rc.res, 200, report);
    } },

  // --- Persona CRUD ---
  { method: 'GET', match: 'exact', path: '/api/personas', requiresDb: true,
    handler: (rc) => json(rc.res, 200, db.listPersonas(rc.database!, rc.scopedProjectId)) },
  { method: 'POST', match: 'exact', path: '/api/personas', requiresDb: true,
    handler: async (rc) => {
      const result = await parseAndValidate(rc.req, PersonaCreateSchema);
      if (!result.ok) { json(rc.res, 400, { error: result.error }); return; }
      const body: Partial<Persona> & { id?: string } = result.data;
      if (rc.ctx.authType !== 'admin') delete body.projectId;
      const now = new Date().toISOString();
      const resolvedProjectId = rc.ctx.authType === 'user' ? rc.ctx.projectId : (body.projectId ?? 'default');

      // If a client-provided ID exists, check for an existing persona to update
      const existing = body.id ? db.getPersona(rc.database!, body.id) : null;
      if (existing && rc.scopedProjectId && existing.projectId !== rc.scopedProjectId) {
        json(rc.res, 403, { error: 'Persona belongs to a different project' }); return;
      }

      const persona: Persona = {
        id: body.id || nanoid(),
        projectId: existing?.projectId ?? resolvedProjectId,
        name: body.name ?? existing?.name ?? 'Untitled',
        description: body.description ?? existing?.description ?? null,
        systemPrompt: body.systemPrompt ?? existing?.systemPrompt ?? '',
        structuredPrompt: body.structuredPrompt ?? existing?.structuredPrompt ?? null,
        icon: body.icon ?? existing?.icon ?? null,
        color: body.color ?? existing?.color ?? null,
        enabled: body.enabled ?? existing?.enabled ?? true,
        maxConcurrent: body.maxConcurrent ?? existing?.maxConcurrent ?? 1,
        timeoutMs: body.timeoutMs ?? existing?.timeoutMs ?? 300_000,
        modelProfile: body.modelProfile ?? existing?.modelProfile ?? null,
        maxBudgetUsd: body.maxBudgetUsd ?? existing?.maxBudgetUsd ?? null,
        maxTurns: body.maxTurns ?? existing?.maxTurns ?? null,
        designContext: body.designContext ?? existing?.designContext ?? null,
        groupId: body.groupId ?? existing?.groupId ?? null,
        permissionPolicy: body.permissionPolicy ?? existing?.permissionPolicy ?? null,
        webhookSecret: existing?.webhookSecret ?? randomBytes(32).toString('hex'),
        createdAt: existing?.createdAt ?? now,
        updatedAt: now,
      };
      db.upsertPersona(rc.database!, persona);
      json(rc.res, existing ? 200 : 201, persona);
    } },
  { method: 'GET', match: 'prefix', path: '/api/personas/', requiresDb: true,
    excludeSuffixes: ['/tools', '/credentials', '/subscriptions', '/triggers', '/validate'],
    handler: (rc) => {
      const id = rc.param('/api/personas/');
      const persona = requireOwnership(rc.res, db.getPersona(rc.database!, id), rc.scopedProjectId);
      if (!persona) return;
      json(rc.res, 200, persona);
    } },
  { method: 'PATCH', match: 'prefix', path: '/api/personas/', requiresDb: true,
    handler: async (rc) => {
      const id = rc.param('/api/personas/');
      const result = await parseAndValidate(rc.req, PersonaUpdateSchema);
      if (!result.ok) { json(rc.res, 400, { error: result.error }); return; }
      if (Object.keys(result.data).length === 0) { json(rc.res, 400, { error: 'No fields to update' }); return; }
      const updated = db.updatePersona(rc.database!, id, result.data as Record<string, unknown>, rc.scopedProjectId);
      if (!updated) { json(rc.res, 404, { error: 'Not found' }); return; }
      json(rc.res, 200, updated);
    } },
  { method: 'PUT', match: 'prefix', path: '/api/personas/', requiresDb: true,
    excludeSuffixes: ['/tools', '/credentials', '/subscriptions', '/triggers'],
    handler: async (rc) => {
      const id = rc.param('/api/personas/');
      const result = await parseAndValidate(rc.req, PersonaUpdateSchema);
      if (!result.ok) { json(rc.res, 400, { error: result.error }); return; }
      if (Object.keys(result.data).length === 0) { json(rc.res, 400, { error: 'No fields to update' }); return; }
      const updated = db.updatePersona(rc.database!, id, result.data as Record<string, unknown>, rc.scopedProjectId);
      if (!updated) { json(rc.res, 404, { error: 'Not found' }); return; }
      json(rc.res, 200, updated);
    } },
  { method: 'GET', match: 'prefix', path: '/api/personas/', suffix: '/tools', requiresDb: true,
    handler: (rc) => {
      const id = rc.param('/api/personas/', '/tools');
      if (rc.scopedProjectId && !requireOwnership(rc.res, db.getPersona(rc.database!, id), rc.scopedProjectId)) return;
      json(rc.res, 200, db.getToolsForPersona(rc.database!, id));
    } },
  { method: 'POST', match: 'prefix', path: '/api/personas/', suffix: '/tools', requiresDb: true,
    handler: async (rc) => {
      const id = rc.param('/api/personas/', '/tools');
      if (rc.scopedProjectId && !requireOwnership(rc.res, db.getPersona(rc.database!, id), rc.scopedProjectId)) return;
      const result = await parseAndValidate(rc.req, LinkToolSchema);
      if (!result.ok) { json(rc.res, 400, { error: result.error }); return; }
      db.linkTool(rc.database!, id, result.data.toolId);
      json(rc.res, 200, { linked: true });
    } },
  { method: 'DELETE', match: 'prefix', path: '/api/personas/', requiresDb: true,
    excludeSuffixes: ['/tools', '/credentials', '/subscriptions', '/triggers'],
    handler: (rc) => {
      const id = rc.param('/api/personas/');
      db.deletePersona(rc.database!, id, rc.scopedProjectId);
      json(rc.res, 200, { deleted: true });
    } },

  // --- Tool Definitions ---
  { method: 'POST', match: 'exact', path: '/api/tool-definitions', requiresDb: true,
    handler: async (rc) => {
      const result = await parseAndValidate(rc.req, ToolDefinitionCreateSchema);
      if (!result.ok) { json(rc.res, 400, { error: result.error }); return; }
      const body = result.data;
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
        createdAt: now,
        updatedAt: now,
      };
      db.upsertToolDefinition(rc.database!, tool);
      json(rc.res, 201, tool);
    } },

  // --- Credentials ---
  { method: 'POST', match: 'exact', path: '/api/credentials', requiresDb: true,
    handler: async (rc) => {
      const result = await parseAndValidate(rc.req, CredentialCreateSchema);
      if (!result.ok) { json(rc.res, 400, { error: result.error }); return; }
      const credBody: Record<string, unknown> = { ...result.data };
      if (rc.ctx.authType === 'user') credBody.projectId = rc.ctx.projectId;
      const cred = db.createCredential(rc.database!, credBody as Parameters<typeof db.createCredential>[1]);
      json(rc.res, 201, cred);
    } },
  { method: 'DELETE', match: 'prefix', path: '/api/credentials/', requiresDb: true,
    handler: (rc) => {
      const id = rc.param('/api/credentials/');
      db.deleteCredential(rc.database!, id, rc.scopedProjectId);
      json(rc.res, 200, { deleted: true });
    } },
  { method: 'GET', match: 'prefix', path: '/api/personas/', suffix: '/credentials', requiresDb: true,
    handler: (rc) => {
      const id = rc.param('/api/personas/', '/credentials');
      if (rc.scopedProjectId && !requireOwnership(rc.res, db.getPersona(rc.database!, id), rc.scopedProjectId)) return;
      const creds = db.listCredentialsForPersona(rc.database!, id);
      json(rc.res, 200, creds.map(c => ({ ...c, encryptedData: '[REDACTED]', iv: '[REDACTED]', tag: '[REDACTED]', salt: '[REDACTED]' })));
    } },
  { method: 'POST', match: 'prefix', path: '/api/personas/', suffix: '/credentials', requiresDb: true,
    handler: async (rc) => {
      const id = rc.param('/api/personas/', '/credentials');
      if (rc.scopedProjectId && !requireOwnership(rc.res, db.getPersona(rc.database!, id), rc.scopedProjectId)) return;
      const linkResult = await parseAndValidate(rc.req, LinkCredentialSchema);
      if (!linkResult.ok) { json(rc.res, 400, { error: linkResult.error }); return; }
      const credentialId = linkResult.data.credentialId;
      if (rc.scopedProjectId && !requireOwnership(rc.res, db.getCredential(rc.database!, credentialId), rc.scopedProjectId, 'Credential not found')) return;
      db.linkCredential(rc.database!, id, credentialId);
      json(rc.res, 200, { linked: true });
    } },

  // --- Event Subscriptions ---
  { method: 'POST', match: 'exact', path: '/api/subscriptions', requiresDb: true,
    handler: async (rc) => {
      const result = await parseAndValidate(rc.req, SubscriptionCreateSchema);
      if (!result.ok) { json(rc.res, 400, { error: result.error }); return; }
      const subBody: Record<string, unknown> = { ...result.data };
      if (rc.ctx.authType === 'user') subBody.projectId = rc.ctx.projectId;
      const sub = db.createSubscription(rc.database!, subBody as Parameters<typeof db.createSubscription>[1]);
      json(rc.res, 201, sub);
    } },
  { method: 'PUT', match: 'prefix', path: '/api/subscriptions/', requiresDb: true,
    handler: async (rc) => {
      const id = rc.param('/api/subscriptions/');
      if (rc.scopedProjectId) {
        const sub = db.getSubscription(rc.database!, id);
        if (!sub) { json(rc.res, 404, { error: 'Not found' }); return; }
        if (!requireOwnership(rc.res, db.getPersona(rc.database!, sub.personaId), rc.scopedProjectId)) return;
      }
      const subUpResult = await parseAndValidate(rc.req, SubscriptionUpdateSchema);
      if (!subUpResult.ok) { json(rc.res, 400, { error: subUpResult.error }); return; }
      const subUpdates: Record<string, unknown> = { ...subUpResult.data };
      db.updateSubscription(rc.database!, id, subUpdates as Parameters<typeof db.updateSubscription>[2], rc.scopedProjectId);
      const updated = db.getSubscription(rc.database!, id);
      json(rc.res, 200, updated);
    } },
  { method: 'DELETE', match: 'prefix', path: '/api/subscriptions/', requiresDb: true,
    handler: async (rc) => {
      const id = rc.param('/api/subscriptions/');
      if (rc.scopedProjectId) {
        const sub = db.getSubscription(rc.database!, id);
        if (!sub) { json(rc.res, 404, { error: 'Not found' }); return; }
        if (!requireOwnership(rc.res, db.getPersona(rc.database!, sub.personaId), rc.scopedProjectId)) return;
      }
      db.deleteSubscription(rc.database!, id, rc.scopedProjectId);
      json(rc.res, 200, { deleted: true });
    } },
  { method: 'GET', match: 'prefix', path: '/api/personas/', suffix: '/subscriptions', requiresDb: true,
    handler: (rc) => {
      const id = rc.param('/api/personas/', '/subscriptions');
      if (rc.scopedProjectId && !requireOwnership(rc.res, db.getPersona(rc.database!, id), rc.scopedProjectId)) return;
      json(rc.res, 200, db.listSubscriptionsForPersona(rc.database!, id));
    } },

  // --- Triggers ---
  { method: 'POST', match: 'exact', path: '/api/triggers', requiresDb: true,
    handler: async (rc) => {
      const result = await parseAndValidate(rc.req, TriggerCreateSchema);
      if (!result.ok) { json(rc.res, 400, { error: result.error }); return; }
      const trigBody: Record<string, unknown> = { ...result.data };
      if (rc.ctx.authType === 'user') trigBody.projectId = rc.ctx.projectId;
      const trigger = db.createTrigger(rc.database!, trigBody as Parameters<typeof db.createTrigger>[1]);
      json(rc.res, 201, trigger);
    } },
  { method: 'PUT', match: 'prefix', path: '/api/triggers/', requiresDb: true,
    excludeSuffixes: ['/firings', '/stats'],
    handler: async (rc) => {
      const id = rc.param('/api/triggers/');
      if (rc.scopedProjectId) {
        const triggerProjectId = db.getTriggerProjectId(rc.database!, id);
        if (!triggerProjectId || triggerProjectId !== rc.scopedProjectId) {
          json(rc.res, 404, { error: 'Not found' }); return;
        }
      }
      const trigUpResult = await parseAndValidate(rc.req, TriggerUpdateSchema);
      if (!trigUpResult.ok) { json(rc.res, 400, { error: trigUpResult.error }); return; }
      const trigUpdates: Record<string, unknown> = { ...trigUpResult.data };
      db.updateTrigger(rc.database!, id, trigUpdates as Parameters<typeof db.updateTrigger>[2], rc.scopedProjectId);
      const updated = db.getTrigger(rc.database!, id);
      json(rc.res, 200, updated);
    } },
  { method: 'GET', match: 'prefix', path: '/api/triggers/', suffix: '/firings', requiresDb: true,
    handler: (rc) => {
      const id = rc.param('/api/triggers/', '/firings');
      if (rc.scopedProjectId) {
        const triggerProjectId = db.getTriggerProjectId(rc.database!, id);
        if (!triggerProjectId || triggerProjectId !== rc.scopedProjectId) {
          json(rc.res, 404, { error: 'Not found' }); return;
        }
      }
      const params = new URLSearchParams(rc.queryString || '');
      const firings = db.listTriggerFirings(rc.database!, {
        triggerId: id,
        status: params.get('status') ?? undefined,
        limit: clampInt(params.get('limit'), 50, 1, MAX_LIMIT),
        offset: clampInt(params.get('offset'), 0, 0, Number.MAX_SAFE_INTEGER),
      });
      json(rc.res, 200, firings);
    } },
  { method: 'GET', match: 'prefix', path: '/api/triggers/', suffix: '/stats', requiresDb: true,
    handler: (rc) => {
      const id = rc.param('/api/triggers/', '/stats');
      if (rc.scopedProjectId) {
        const triggerProjectId = db.getTriggerProjectId(rc.database!, id);
        if (!triggerProjectId || triggerProjectId !== rc.scopedProjectId) {
          json(rc.res, 404, { error: 'Not found' }); return;
        }
      }
      json(rc.res, 200, db.getTriggerFiringStats(rc.database!, id));
    } },
  { method: 'DELETE', match: 'prefix', path: '/api/triggers/', requiresDb: true,
    excludeSuffixes: ['/firings', '/stats'],
    handler: (rc) => {
      const id = rc.param('/api/triggers/');
      db.deleteTrigger(rc.database!, id, rc.scopedProjectId);
      json(rc.res, 200, { deleted: true });
    } },
  { method: 'GET', match: 'prefix', path: '/api/personas/', suffix: '/triggers', requiresDb: true,
    handler: (rc) => {
      const id = rc.param('/api/personas/', '/triggers');
      if (rc.scopedProjectId && !requireOwnership(rc.res, db.getPersona(rc.database!, id), rc.scopedProjectId)) return;
      json(rc.res, 200, db.listTriggersForPersona(rc.database!, id));
    } },

  // --- Events ---
  { method: 'GET', match: 'exact', path: '/api/events', requiresDb: true,
    handler: (rc) => {
      const params = new URLSearchParams(rc.queryString || '');
      const events = db.listEvents(rc.database!, {
        eventType: params.get('eventType') ?? undefined,
        status: params.get('status') ?? undefined,
        limit: clampInt(params.get('limit'), 50, 1, MAX_LIMIT),
        offset: clampInt(params.get('offset'), 0, 0, Number.MAX_SAFE_INTEGER),
        projectId: rc.scopedProjectId,
      });
      json(rc.res, 200, events);
    } },
  { method: 'POST', match: 'exact', path: '/api/events', requiresDb: true,
    handler: async (rc) => {
      const result = await parseAndValidate(rc.req, EventCreateSchema);
      if (!result.ok) { json(rc.res, 400, { error: result.error }); return; }
      const evtBody: Record<string, unknown> = { ...result.data };
      if (rc.ctx.authType === 'user') evtBody.projectId = rc.ctx.projectId;
      const event = db.publishEvent(rc.database!, evtBody as Parameters<typeof db.publishEvent>[1]);
      rc.onEventPublished?.();
      json(rc.res, 201, event);
    } },
  { method: 'GET', match: 'regex', path: /^\/api\/events\/[^/]+\/audit$/, requiresDb: true,
    handler: (rc) => {
      const id = rc.pathname.replace('/api/events/', '').replace('/audit', '');
      const event = requireOwnership(rc.res, db.getEvent(rc.database!, id), rc.scopedProjectId, 'Event not found');
      if (!event) return;
      const auditLog = db.getEventAuditLog(rc.database!, id);
      json(rc.res, 200, { event, audit: auditLog });
    } },
  { method: 'POST', match: 'regex', path: /^\/api\/events\/[^/]+\/retry$/, requiresDb: true,
    handler: (rc) => {
      const id = rc.pathname.replace('/api/events/', '').replace('/retry', '');
      const event = requireOwnership(rc.res, db.getEvent(rc.database!, id), rc.scopedProjectId, 'Event not found');
      if (!event) return;
      if (event.status !== 'dead_letter' && event.status !== 'failed') {
        json(rc.res, 400, { error: `Cannot retry event in '${event.status}' status — only dead_letter or failed events can be retried` }); return;
      }
      db.updateEventStatus(rc.database!, id, 'pending');
      db.clearEventDispatches(rc.database!, id);
      const updated = db.getEvent(rc.database!, id);
      db.appendEventAudit(rc.database!, {
        eventId: id,
        action: 'manual_retry',
        detail: JSON.stringify({ previousStatus: event.status, retryCount: event.retryCount }),
      });
      rc.logger.info({ eventId: id, previousStatus: event.status }, 'Dead letter event manually retried');
      json(rc.res, 200, updated);
    } },
  { method: 'PUT', match: 'prefix', path: '/api/events/', requiresDb: true,
    handler: async (rc) => {
      const id = rc.param('/api/events/');
      const evtUpResult = await parseAndValidate(rc.req, EventUpdateSchema);
      if (!evtUpResult.ok) { json(rc.res, 400, { error: evtUpResult.error }); return; }
      const updated = db.updateEventWithMetadata(rc.database!, id, evtUpResult.data.status, evtUpResult.data.metadata);
      if (updated) {
        json(rc.res, 200, updated);
      } else {
        rc.res.writeHead(404, { 'Content-Type': 'application/json' });
        rc.res.end(JSON.stringify({ error: 'Event not found' }));
      }
    } },

  // --- Executions ---
  { method: 'GET', match: 'exact', path: '/api/executions', requiresDb: true,
    handler: (rc) => {
      const params = new URLSearchParams(rc.queryString || '');
      const executions = db.listExecutions(rc.database!, {
        personaId: params.get('personaId') ?? undefined,
        status: params.get('status') ?? undefined,
        limit: clampInt(params.get('limit'), 50, 1, MAX_LIMIT),
        offset: clampInt(params.get('offset'), 0, 0, Number.MAX_SAFE_INTEGER),
        projectId: rc.scopedProjectId,
      });
      json(rc.res, 200, executions);
    } },
  { method: 'GET', match: 'exact', path: '/api/executions/stats', requiresDb: true,
    handler: (rc) => {
      const params = new URLSearchParams(rc.queryString || '');
      const stats = db.getExecutionStats(rc.database!, {
        personaId: params.get('personaId') ?? undefined,
        projectId: rc.scopedProjectId,
        periodDays: clampInt(params.get('period'), 7, 1, 90),
      });
      json(rc.res, 200, stats);
    } },
  { method: 'GET', match: 'exact', path: '/api/status',
    handler: (rc) => handleStatus(rc.res, rc.pool, rc.dispatcher, rc.tokenManager, rc.oauth) },
  { method: 'POST', match: 'exact', path: '/api/execute',
    handler: (rc) => handleExecute(rc.req, rc.res, rc.dispatcher, rc.logger, rc.ctx) },
  { method: 'POST', match: 'prefix', path: '/api/executions/', suffix: '/cancel',
    handler: (rc) => {
      const executionId = rc.param('/api/executions/', '/cancel');
      handleCancelExecution(rc.res, rc.dispatcher, executionId || '');
    } },

  // --- Human-in-the-loop review endpoints ---
  { method: 'GET', match: 'exact', path: '/api/reviews/pending',
    handler: (rc) => {
      const reviews = rc.dispatcher.getAllPendingReviews(rc.scopedProjectId);
      json(rc.res, 200, reviews);
    } },
  { method: 'POST', match: 'regex', path: /^\/api\/executions\/[^/]+\/reviews\/[^/]+\/respond$/,
    handler: async (rc) => {
      const segments = rc.pathname.replace('/api/executions/', '').split('/');
      const executionId = segments[0];
      const reviewId = segments[2]; // segments: [execId, 'reviews', reviewId, 'respond']
      let body: { decision?: string; message?: string };
      try {
        body = JSON.parse(await readBody(rc.req));
      } catch {
        json(rc.res, 400, { error: 'Invalid JSON body' }); return;
      }
      if (!body.decision || !['approved', 'rejected'].includes(body.decision)) {
        json(rc.res, 400, { error: 'decision must be "approved" or "rejected"' }); return;
      }
      const responded = rc.dispatcher.respondToReview(executionId, reviewId, body.decision as 'approved' | 'rejected', body.message || '', rc.scopedProjectId);
      if (!responded) { json(rc.res, 404, { error: 'Review not found or already resolved' }); return; }
      json(rc.res, 200, { ok: true, executionId, reviewId, decision: body.decision });
    } },
  { method: 'GET', match: 'prefix', path: '/api/executions/', suffix: '/reviews',
    excludeSuffixes: ['/respond'],
    handler: (rc) => {
      const executionId = rc.param('/api/executions/', '/reviews');
      const reviews = rc.dispatcher.getReviewsForExecution(executionId);
      json(rc.res, 200, reviews);
    } },
  { method: 'GET', match: 'prefix', path: '/api/executions/', suffix: '/stream',
    handler: (rc) => {
      const executionId = rc.param('/api/executions/', '/stream');
      handleExecutionStream(rc.res, rc.dispatcher, executionId);
    } },
  { method: 'GET', match: 'prefix', path: '/api/executions/',
    excludeSuffixes: ['/cancel', '/reviews', '/respond', '/stream'],
    handler: (rc) => {
      const executionId = rc.param('/api/executions/');
      const params = new URLSearchParams(rc.queryString || '');
      const offset = clampInt(params.get('offset'), 0, 0, Number.MAX_SAFE_INTEGER);
      handleGetExecution(rc.res, rc.dispatcher, executionId, offset);
    } },

  // --- Cloud Deployments ---
  { method: 'GET', match: 'exact', path: '/api/deployments', requiresDb: true,
    handler: (rc) => json(rc.res, 200, db.listDeployments(rc.database!, rc.scopedProjectId).map(redactDeployment)) },
  { method: 'POST', match: 'exact', path: '/api/deployments', requiresDb: true,
    handler: async (rc) => {
      const result = await parseAndValidate(rc.req, DeploymentCreateSchema);
      if (!result.ok) { json(rc.res, 400, { error: result.error }); return; }
      const personaId = result.data.personaId;
      const persona = requireOwnership(rc.res, db.getPersona(rc.database!, personaId), rc.scopedProjectId, 'Persona not found');
      if (!persona) return;
      const existing = db.getDeploymentByPersona(rc.database!, personaId);
      if (existing) { json(rc.res, 409, { error: 'Persona already has a deployment', deployment: redactDeployment(existing) }); return; }
      const rawLabel = result.data.label || persona.name;
      const slug = toSlug(rawLabel);
      if (db.getDeploymentBySlug(rc.database!, slug)) {
        json(rc.res, 409, { error: `Slug "${slug}" is already in use` }); return;
      }
      const webhookSecret = persona.webhookSecret || randomBytes(32).toString('hex');
      if (!persona.webhookSecret) {
        db.upsertPersona(rc.database!, { ...persona, webhookSecret, updatedAt: new Date().toISOString() });
      }
      const deployment = db.createDeployment(rc.database!, {
        personaId,
        slug,
        label: rawLabel,
        webhookSecret,
        projectId: rc.ctx.authType === 'user' ? rc.ctx.projectId : persona.projectId,
        maxMonthlyBudgetUsd: result.data.maxMonthlyBudgetUsd ?? null,
      });
      rc.logger.info({ deploymentId: deployment.id, slug, personaId }, 'Deployment created');
      json(rc.res, 201, redactDeployment(deployment));
    } },
  { method: 'GET', match: 'prefix', path: '/api/deployments/', requiresDb: true,
    excludeSuffixes: ['/pause', '/resume'],
    handler: (rc) => {
      const id = rc.param('/api/deployments/');
      const deployment = requireOwnership(rc.res, db.getDeployment(rc.database!, id), rc.scopedProjectId);
      if (!deployment) return;
      json(rc.res, 200, redactDeployment(deployment));
    } },
  { method: 'POST', match: 'prefix', path: '/api/deployments/', suffix: '/pause', requiresDb: true,
    handler: (rc) => {
      const id = rc.param('/api/deployments/', '/pause');
      const deployment = requireOwnership(rc.res, db.getDeployment(rc.database!, id), rc.scopedProjectId);
      if (!deployment) return;
      db.updateDeploymentStatus(rc.database!, id, 'paused');
      json(rc.res, 200, redactDeployment({ ...deployment, status: 'paused' as const }));
    } },
  { method: 'POST', match: 'prefix', path: '/api/deployments/', suffix: '/resume', requiresDb: true,
    handler: (rc) => {
      const id = rc.param('/api/deployments/', '/resume');
      const deployment = requireOwnership(rc.res, db.getDeployment(rc.database!, id), rc.scopedProjectId);
      if (!deployment) return;
      db.updateDeploymentStatus(rc.database!, id, 'active');
      json(rc.res, 200, redactDeployment({ ...deployment, status: 'active' as const }));
    } },
  { method: 'DELETE', match: 'prefix', path: '/api/deployments/', requiresDb: true,
    handler: (rc) => {
      const id = rc.param('/api/deployments/');
      const deployment = requireOwnership(rc.res, db.getDeployment(rc.database!, id), rc.scopedProjectId);
      if (!deployment) return;
      db.deleteDeployment(rc.database!, id, rc.scopedProjectId);
      rc.logger.info({ deploymentId: id }, 'Deployment deleted');
      json(rc.res, 200, { deleted: true });
    } },

  // --- Admin-only routes ---
  { method: 'GET', match: 'exact', path: '/api/admin/users', requiresDb: true, admin: true,
    handler: (rc) => json(rc.res, 200, { projectIds: db.listDistinctProjectIds(rc.database!) }) },
  { method: 'GET', match: 'exact', path: '/api/admin/personas', requiresDb: true, admin: true,
    handler: (rc) => json(rc.res, 200, db.listPersonas(rc.database!)) },
  { method: 'GET', match: 'exact', path: '/api/admin/executions', requiresDb: true, admin: true,
    handler: (rc) => {
      const params = new URLSearchParams(rc.queryString || '');
      json(rc.res, 200, db.listExecutions(rc.database!, {
        limit: clampInt(params.get('limit'), 100, 1, MAX_LIMIT),
        offset: clampInt(params.get('offset'), 0, 0, Number.MAX_SAFE_INTEGER),
      }));
    } },
];

// ---------------------------------------------------------------------------
// Content-Type enforcement
// ---------------------------------------------------------------------------

function hasJsonContentType(req: http.IncomingMessage): boolean {
  const ct = req.headers['content-type'] || '';
  return ct.includes('application/json');
}

// ---------------------------------------------------------------------------
// Client IP extraction — X-Forwarded-For spoof prevention
// ---------------------------------------------------------------------------

/**
 * Extract the real client IP from the request.
 *
 * When `trustedProxyCount` is 0 (default), the X-Forwarded-For header is
 * ignored entirely and req.socket.remoteAddress is used.  This is the safe
 * default when the orchestrator is not behind a reverse proxy.
 *
 * When `trustedProxyCount` > 0, the client IP is taken from the Nth-from-right
 * entry in the X-Forwarded-For chain.  For example, with trustedProxyCount=1
 * (one proxy, e.g. nginx or ALB), the rightmost entry is the one added by the
 * trusted proxy and represents the real client IP.  Any entries to the left of
 * that are client-supplied and cannot be trusted.
 *
 * This prevents the trivial spoofing attack where a client sends
 * `X-Forwarded-For: fake-ip` to bypass rate limiting.
 */
function getClientIp(req: http.IncomingMessage, trustedProxyCount: number): string {
  if (trustedProxyCount > 0) {
    const xff = req.headers['x-forwarded-for'];
    if (xff) {
      const ips = (Array.isArray(xff) ? xff.join(',') : xff)
        .split(',')
        .map(s => s.trim())
        .filter(Boolean);
      // Pick the entry at position (length - trustedProxyCount).
      // With 1 proxy: rightmost IP.  With 2 proxies: second from right, etc.
      const idx = ips.length - trustedProxyCount;
      if (idx >= 0 && ips[idx]) {
        return ips[idx];
      }
      // Fewer XFF entries than expected proxies — fall back to remoteAddress
    }
  }
  return req.socket.remoteAddress || 'unknown';
}

// ---------------------------------------------------------------------------
// HTTP API
// ---------------------------------------------------------------------------

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
=======
  gitlabWebhookSecret?: string,
  corsOrigins?: string[],
  onEventPublished?: () => void,
  trustedProxyCount: number = 0,
  corsWildcard: boolean = false,
): http.Server {
  const allowedOrigins = new Set(corsOrigins ?? []);
  const rateLimiter = createRateLimiter();
  const idempotencyCache = createIdempotencyCache();

  // Verify all known routes have explicit rate-limit category assignments
  verifyRouteCategoryAssignments(logger);

  const server = http.createServer(async (req, res) => {
    // CORS headers — only allow explicitly configured origins.
    // Method/header headers are scoped inside the origin check so non-matching
    // origins (or same-origin requests) never receive permissive CORS signals.
    const requestOrigin = req.headers['origin'];
    const originAllowed = !!(requestOrigin && (corsWildcard || allowedOrigins.has(requestOrigin)));
    if (originAllowed) {
      res.setHeader('Access-Control-Allow-Origin', corsWildcard ? '*' : requestOrigin!);
      res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
      res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization, X-User-Token, Idempotency-Key');
      res.setHeader('Vary', 'Origin');
    }

    if (req.method === 'OPTIONS') {
      // Preflight: only return 204 if origin is allowed; otherwise 403.
      if (!originAllowed) {
        res.writeHead(403, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Origin not allowed' }));
        return;
      }
      res.writeHead(204);
      res.end();
      return;
    }

    const url = req.url || '/';
    const [pathname, queryString] = url.split('?');
    const route = parseRoute(pathname ?? '');

    // Unauthenticated endpoint
=======
    // Rate limiting (by IP, per endpoint category)
    const clientIp = getClientIp(req, trustedProxyCount);
    const rateCategory = classifyEndpoint(pathname ?? '/', req.method ?? 'GET');
    if (pathname !== '/health' && !rateLimiter.allow(clientIp, rateCategory)) {
      res.writeHead(429, { 'Content-Type': 'application/json', 'Retry-After': '60' });
      res.end(JSON.stringify({ error: 'Too many requests' }));
      return;
    }

    // Idempotency key — replay cached response for duplicate mutations
    const isMutation = req.method === 'POST' || req.method === 'PUT' || req.method === 'DELETE';
    const idempotencyKey = req.headers['idempotency-key'] as string | undefined;
    if (idempotencyKey && isMutation) {
      const cached = idempotencyCache.get(idempotencyKey);
      if (cached) {
        res.writeHead(cached.status, { 'Content-Type': 'application/json', 'X-Idempotency-Replay': '1' });
        res.end(cached.body);
        return;
      }
      // Intercept response to cache it
      const origEnd = res.end.bind(res);
      let capturedStatus = 200;
      const origWriteHead = res.writeHead.bind(res);
      res.writeHead = ((...args: [number, ...unknown[]]) => {
        capturedStatus = args[0];
        return origWriteHead(...args as Parameters<typeof res.writeHead>);
      }) as typeof res.writeHead;
      res.end = ((chunk?: unknown, ...rest: unknown[]) => {
        if (capturedStatus >= 200 && capturedStatus < 300 && typeof chunk === 'string') {
          idempotencyCache.set(idempotencyKey, capturedStatus, chunk);
        }
        return (origEnd as (...args: unknown[]) => unknown)(chunk, ...rest);
      }) as typeof res.end;
    }

    // Content-Type enforcement for POST/PUT with body
    if ((req.method === 'POST' || req.method === 'PUT') && pathname !== '/health') {
      if (!hasJsonContentType(req)) {
        res.writeHead(415, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Content-Type must be application/json' }));
        return;
      }
    }

    // Unauthenticated endpoints
    if (pathname === '/health' && req.method === 'GET') {
      await handleHealth(res, pool, dispatcher, tokenManager, oauth, getEventProcessorHealth, database, tenantDbManager, kafka, triggerScheduler, auditLog, getRetentionHealth);
      return;
    }

    // Auth check
=======
    // Prometheus-compatible metrics endpoint
    if (pathname === '/metrics' && req.method === 'GET') {
      if (!auth.validateRequest(req)) {
        res.writeHead(401, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Unauthorized' }));
        return;
      }
      const metricsText = dispatcher.metrics.toPrometheus();
      res.writeHead(200, { 'Content-Type': 'text/plain; version=0.0.4; charset=utf-8' });
      res.end(metricsText);
      return;
    }

    // JSON metrics endpoint
    if (pathname === '/api/metrics' && req.method === 'GET') {
      if (!auth.validateRequest(req)) {
        res.writeHead(401, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Unauthorized' }));
        return;
      }
      json(res, 200, dispatcher.metrics.toJSON());
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
      if (!token || !safeCompare(token, gitlabWebhookSecret)) {
        res.writeHead(401, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Invalid or missing X-Gitlab-Token' }));
        return;
      }

      const body = await readBody(req, MAX_WEBHOOK_BODY_BYTES);
      let parsed: Record<string, unknown> = {};
      try { parsed = JSON.parse(body); } catch { /* raw payload */ }
      const objectKind = (parsed['object_kind'] as string) || 'unknown';
      const projectPath = (parsed['project'] as Record<string, unknown>)?.['path_with_namespace'] as string | undefined;
      const event = db.publishEvent(database, {
        eventType: gitlabEventType(objectKind),
        sourceType: 'gitlab',
        sourceId: projectPath ?? null,
        payload: body,
      });
      onEventPublished?.();
      json(res, 201, event);
      return;
    }

    // Generic webhook — verified via HMAC signature (per-persona secret)
    if (matchRoute(pathname, '/api/webhooks/') && req.method === 'POST' && database) {
      const personaId = pathname!.replace('/api/webhooks/', '');
      const persona = db.getPersona(database, personaId);
      if (!persona) { json(res, 404, { error: 'Persona not found' }); return; }

      const body = await readBody(req, MAX_WEBHOOK_BODY_BYTES);

      // If the persona has a webhook secret, require HMAC signature verification.
      // If no secret is configured, fall back to requiring API key auth (handled below).
      if (persona.webhookSecret) {
        const signature = req.headers['x-webhook-signature'] as string | undefined;
        if (!signature) {
          json(res, 401, { error: 'Missing X-Webhook-Signature header' });
          return;
        }
        const expected = createHmac('sha256', persona.webhookSecret).update(body).digest('hex');
        if (!safeCompare(signature, expected)) {
          json(res, 401, { error: 'Invalid webhook signature' });
          return;
        }
      } else {
        // No webhook secret — require standard API key authentication
        const ctx = auth.validateAndExtractContext(req);
        if (!ctx) {
          rateLimiter.recordAuthFailure(clientIp);
          json(res, 401, { error: 'Unauthorized — configure a webhook secret or provide API key' });
          return;
        }
        // Verify project scoping for authenticated requests
        const scoped = ctx.authType === 'admin' ? undefined : ctx.projectId;
        if (scoped && persona.projectId !== scoped) {
          json(res, 404, { error: 'Persona not found' }); return;
        }
      }

      const event = db.publishEvent(database, {
        eventType: 'webhook_received',
        sourceType: 'webhook',
        sourceId: personaId,
        targetPersonaId: personaId,
        projectId: persona.projectId,
        payload: body,
      });
      onEventPublished?.();
      json(res, 201, event);
      return;
    }

    // Deployed persona API — invoked externally via HMAC-signed requests
    // Route: POST /api/deployed/{slug}
    if (matchRoute(pathname, '/api/deployed/') && req.method === 'POST' && database) {
      const slug = pathname!.replace('/api/deployed/', '');
      const deployment = db.getDeploymentBySlug(database, slug);
      if (!deployment) { json(res, 404, { error: 'Deployment not found' }); return; }
      if (deployment.status !== 'active') { json(res, 503, { error: 'Deployment is paused' }); return; }

      // Budget enforcement — reject if monthly budget exhausted
      if (deployment.maxMonthlyBudgetUsd != null) {
        const currentMonth = new Date().toISOString().slice(0, 7);
        const monthCost = deployment.budgetMonth === currentMonth ? deployment.currentMonthCostUsd : 0;
        if (monthCost >= deployment.maxMonthlyBudgetUsd) {
          json(res, 402, { error: 'Monthly budget exhausted', budgetUsd: deployment.maxMonthlyBudgetUsd, spentUsd: monthCost }); return;
        }
        // Warn when approaching limit (>= 80%)
        if (monthCost >= deployment.maxMonthlyBudgetUsd * 0.8) {
          res.setHeader('X-Budget-Warning', 'approaching-limit');
        }
      }

      const body = await readBody(req, MAX_WEBHOOK_BODY_BYTES);

      // Verify HMAC signature if webhook secret is set
      if (deployment.webhookSecret) {
        const signature = req.headers['x-webhook-signature'] as string | undefined;
        if (!signature) { json(res, 401, { error: 'Missing X-Webhook-Signature header' }); return; }
        const expected = createHmac('sha256', deployment.webhookSecret).update(body).digest('hex');
        if (!safeCompare(signature, expected)) { json(res, 401, { error: 'Invalid webhook signature' }); return; }
      } else {
        // No webhook secret — require API key authentication
        const deployCtx = auth.validateAndExtractContext(req);
        if (!deployCtx) { json(res, 401, { error: 'Unauthorized — configure a webhook secret or provide API key' }); return; }
        // Verify project scoping — prevent cross-tenant access
        const scoped = deployCtx.authType === 'admin' ? undefined : deployCtx.projectId;
        if (scoped && deployment.projectId !== scoped) {
          json(res, 404, { error: 'Deployment not found' }); return;
        }
      }

      // Build and submit execution
      const persona = db.getPersona(database, deployment.personaId);
      if (!persona) { json(res, 500, { error: 'Persona not found for deployment' }); return; }

      // Parse input payload as JSON if possible for structured prompt interpolation
      let inputData: Record<string, unknown> | undefined;
      if (body) {
        try {
          inputData = JSON.parse(body);
        } catch {
          // If not valid JSON, pass as a 'raw' field
          inputData = { raw: body };
        }
      }

      const execRequest: ExecRequest = {
        executionId: nanoid(),
        personaId: persona.id,
        prompt: persona.systemPrompt, // Fallback if persona not found in DB during dispatch
        inputData,
        projectId: persona.projectId,
        config: {
          timeoutMs: Math.max(1000, Math.min(MAX_TIMEOUT_MS, persona.timeoutMs || MAX_TIMEOUT_MS)),
        },
      };

      if (!dispatcher.submit(execRequest)) {
        json(res, 429, { error: 'Execution queue is full — try again later' });
        return;
      }
      db.incrementDeploymentInvocation(database, deployment.id);

      logger.info({ executionId: execRequest.executionId, slug, deploymentId: deployment.id }, 'Deployed API invocation');

      json(res, 202, {
        executionId: execRequest.executionId,
        status: 'queued',
        deploymentId: deployment.id,
      });
      return;
    }

    // Auth check for everything else — dual auth (API key + optional JWT)
    const ctx = auth.validateAndExtractContext(req);
    if (!ctx) {
      rateLimiter.recordAuthFailure(clientIp);
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
=======
    // Dispatch to the declarative route table
    const route = findRoute(AUTHENTICATED_ROUTES, req.method ?? 'GET', pathname ?? '/', !!database);
    if (!route) {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Not found' }));
      return;
    }

    // Admin gate
    if (route.admin && requireAdmin(ctx, res)) return;

    const routeCtx: RouteCtx = {
      req, res, ctx, scopedProjectId,
      database: database ?? null,
      dispatcher, pool, tokenManager, oauth, logger,
      pathname: pathname ?? '/',
      queryString,
      onEventPublished,
      param(prefix: string, suffix?: string) {
        let id = (pathname ?? '/').slice(prefix.length);
        if (suffix && id.endsWith(suffix)) id = id.slice(0, -suffix.length);
        return id;
      },
    };

    try {
      await route.handler(routeCtx);
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
=======
/** Strip webhookSecret from deployment objects in API responses. */
function redactDeployment<T extends { webhookSecret?: unknown }>(deployment: T): Omit<T, 'webhookSecret'> & { hasWebhookSecret: boolean } {
  const { webhookSecret, ...rest } = deployment;
  return { ...rest, hasWebhookSecret: webhookSecret != null && webhookSecret !== '' };
}

function json(res: http.ServerResponse, status: number, data: unknown): void {
  res.writeHead(status, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify(data));
}

function matchRoute(pathname: string | undefined, prefix: string): boolean {
  return !!pathname && pathname.startsWith(prefix) && pathname.length > prefix.length;
}

/**
 * Generic ownership guard — fetches an entity and verifies project scoping.
 * Returns the entity if ownership passes, or null (after sending 404).
 * When scopedProjectId is undefined (admin), skips the project check.
 */
function requireOwnership<T extends { projectId?: string }>(
  res: http.ServerResponse,
  entity: T | undefined | null,
  scopedProjectId: string | undefined,
  errorMsg = 'Not found',
): T | null {
  if (!entity) {
    json(res, 404, { error: errorMsg });
    return null;
  }
  if (scopedProjectId && entity.projectId !== scopedProjectId) {
    json(res, 404, { error: errorMsg });
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

=======
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
  const result = await parseAndValidate(req, OAuthCallbackSchema);
  if (!result.ok) { json(res, 400, { error: result.error }); return; }
  const parsed = result.data;

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
  const result = await parseAndValidate(req, SetTokenSchema);
  if (!result.ok) { json(res, 400, { error: result.error }); return; }
  const parsed = result.data;

  tokenManager.storeClaudeToken(parsed.token);
  logger.info('Claude token set via direct injection');

  res.writeHead(200, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({ status: 'token_stored' }));
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
  const result = await parseAndValidate(req, ExecuteSchema);
  if (!result.ok) { json(res, 400, { error: result.error }); return; }
  const parsed = result.data;

  const request: ExecRequest = {
    executionId: nanoid(),
    personaId: parsed.personaId || 'manual',
    prompt: parsed.prompt,
    projectId: ctx?.authType === 'user' ? ctx.projectId : undefined,
    config: {
      timeoutMs: Math.max(1000, Math.min(MAX_TIMEOUT_MS, parsed.timeoutMs || MAX_TIMEOUT_MS)),
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
=======
  if (!dispatcher.submit(request)) {
    json(res, 429, { error: 'Execution queue is full — try again later' });
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
    totalOutputLines: exec.totalOutputLines,
    outputLines: exec.totalOutputLines, // backward-compat alias for desktop client
    tailBufferLines: exec.output.length,
    output,
    outputEvicted: exec.outputEvicted,
    durationMs: exec.durationMs,
    sessionId: exec.sessionId,
    totalCostUsd: exec.totalCostUsd,
    ...(exec.phaseTimings ? { phaseTimings: exec.phaseTimings } : {}),
=======
    costUsd: exec.totalCostUsd, // backward-compat alias for desktop client
    progress: exec.source === 'live' ? exec.progress ?? null : null,
    pendingReviews: exec.source === 'live'
      ? exec.pendingReviews?.filter(r => r.status === 'pending') ?? []
      : [],
    source: exec.source,
  }));
}

/**
 * SSE streaming endpoint for execution progress.
 * Sends events: progress, output, review_request, complete.
 * Auto-closes when execution reaches a terminal status or client disconnects.
 */
function handleExecutionStream(
  res: http.ServerResponse,
  dispatcher: Dispatcher,
  executionId: string,
): void {
  const exec = dispatcher.getExecution(executionId);
  if (!exec) {
    res.writeHead(404, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: 'Execution not found' }));
    return;
  }

  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    'X-Accel-Buffering': 'no', // disable nginx buffering
  });

  let lastOutputOffset = 0;
  let closed = false;

  const sendEvent = (event: string, data: unknown) => {
    if (closed) return;
    res.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`);
  };

  // Send initial state
  sendEvent('init', {
    executionId,
    status: exec.status,
    totalOutputLines: exec.totalOutputLines,
    output: exec.output,
    progress: exec.source === 'live' ? exec.progress ?? null : null,
  });
  lastOutputOffset = exec.output.length;

  // If already terminal, close immediately
  const terminalStatuses = new Set(['completed', 'failed', 'cancelled']);
  if (terminalStatuses.has(exec.status)) {
    sendEvent('complete', {
      executionId,
      status: exec.status,
      durationMs: exec.durationMs,
      totalCostUsd: exec.totalCostUsd,
    });
    res.end();
    return;
  }

  // Poll for updates every 500ms
  const interval = setInterval(() => {
    if (closed) { clearInterval(interval); return; }
    const current = dispatcher.getExecution(executionId);
    if (!current) { clearInterval(interval); sendEvent('error', { error: 'Execution not found' }); res.end(); closed = true; return; }

    // Send new output lines
    if (current.output.length > lastOutputOffset) {
      const newLines = current.output.slice(lastOutputOffset);
      sendEvent('output', { lines: newLines, totalOutputLines: current.totalOutputLines });
      lastOutputOffset = current.output.length;
    }

    // Send progress if available
    if (current.source === 'live' && current.progress) {
      sendEvent('progress', current.progress);
    }

    // Send pending reviews
    if (current.source === 'live' && current.pendingReviews?.some(r => r.status === 'pending')) {
      sendEvent('review_request', current.pendingReviews.filter(r => r.status === 'pending'));
    }

    // Check if terminal
    if (terminalStatuses.has(current.status)) {
      clearInterval(interval);
      sendEvent('complete', {
        executionId,
        status: current.status,
        durationMs: current.durationMs,
        totalCostUsd: current.totalCostUsd,
        sessionId: current.sessionId,
      });
      res.end();
      closed = true;
    }
  }, 500);

  // Clean up on client disconnect
  res.on('close', () => {
    closed = true;
    clearInterval(interval);
  });
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
=======
// --- Compilation handlers ---

async function handleCompile(
  req: http.IncomingMessage,
  res: http.ServerResponse,
  dispatcher: Dispatcher,
  logger: Logger,
  ctx?: RequestContext,
  database?: Database.Database,
): Promise<void> {
  const result = await parseAndValidate(req, CompileSchema);
  if (!result.ok) { json(res, 400, { error: result.error }); return; }

  const body = result.data as CompileRequest;

  // Resolve available tool definitions from DB if tool names are provided
  let availableTools: PersonaToolDefinition[] | undefined;
  if (body.tools && body.tools.length > 0 && database) {
    const allTools = db.listToolDefinitions(database);
    const toolNames = new Set(body.tools);
    availableTools = allTools.filter(t => toolNames.has(t.name));
  }

  // Build the compilation prompt
  const compilationPrompt = assembleCompilationPrompt(body, availableTools);

  const compileId = nanoid();
  const projectId = ctx?.authType === 'user' ? ctx.projectId : undefined;

  // Submit as an execution — the compilation runs as a regular LLM call
  const execRequest: ExecRequest = {
    executionId: compileId,
    personaId: 'system:compiler',
    prompt: compilationPrompt,
    projectId,
    config: {
      timeoutMs: Math.min(MAX_TIMEOUT_MS, 120_000),
    },
  };

  if (!dispatcher.submit(execRequest)) {
    json(res, 429, { error: 'Execution queue is full — try again later' });
    return;
  }

  logger.info({ compileId, intent: body.intent.slice(0, 100) }, 'Compilation submitted');

  const compileResult: CompileResult = {
    compileId,
    status: 'queued',
    stage: null,
    persona: null,
    error: null,
  };

  json(res, 202, compileResult);
}

async function handleBatchCompile(
  req: http.IncomingMessage,
  res: http.ServerResponse,
  dispatcher: Dispatcher,
  logger: Logger,
  ctx?: RequestContext,
  _database?: Database.Database,
): Promise<void> {
  const result = await parseAndValidate(req, BatchCompileSchema);
  if (!result.ok) { json(res, 400, { error: result.error }); return; }

  const body = result.data as BatchCompileRequest;
  const count = Math.max(1, Math.min(10, body.count ?? 3));

  // Build the batch compilation prompt
  const batchPrompt = assembleBatchCompilationPrompt(
    body.description,
    count,
    body.tools,
    body.connectors,
  );

  const batchId = nanoid();
  const compileIds: string[] = [];
  const projectId = ctx?.authType === 'user' ? ctx.projectId : undefined;

  // Submit a single execution for the batch — the LLM produces all personas in one call
  const execId = nanoid();
  compileIds.push(execId);

  const execRequest: ExecRequest = {
    executionId: execId,
    personaId: 'system:compiler:batch',
    prompt: batchPrompt,
    projectId,
    config: {
      timeoutMs: Math.min(MAX_TIMEOUT_MS, 120_000),
    },
  };

  if (!dispatcher.submit(execRequest)) {
    json(res, 429, { error: 'Execution queue is full — try again later' });
    return;
  }

  logger.info({ batchId, execId, count, description: body.description.slice(0, 100) }, 'Batch compilation submitted');

  trackBatch(batchId, execId, count, projectId);

  const batchResult: BatchCompileResult = {
    batchId,
    compileIds,
    status: 'queued',
  };

  json(res, 202, batchResult);
}

function handleGetCompilation(
  res: http.ServerResponse,
  dispatcher: Dispatcher,
  compileId: string,
): void {
  const exec = dispatcher.getExecution(compileId);

  if (!exec) {
    json(res, 404, { error: 'Compilation not found' });
    return;
  }

  // Map execution status to compilation status
  let status: CompileResult['status'];
  let stage: CompileResult['stage'] = null;
  let persona: CompileResult['persona'] | null = null;
  let error: string | null = null;

  switch (exec.status) {
    case 'running':
      status = 'running';
      stage = 'llm_generation';
      break;
    case 'completed':
      status = 'completed';
      stage = 'persist';
      try {
        // Use getExecutionOutput to handle evicted cache transparently
        const outputLines = exec.outputEvicted
          ? (dispatcher.getExecutionOutput(compileId) ?? [])
          : exec.output;
        const fullOutput = outputLines.join('\n');
        const parsed = extractJsonFromOutput(fullOutput);
        if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
          persona = parsedDesignToPersona(parsed as Record<string, unknown>, compileId, 'default');
        }
      } catch {
        error = 'Compilation completed but output could not be parsed into a persona';
      }
      break;
    case 'failed':
    case 'cancelled':
      status = 'failed';
      error = 'Compilation execution ' + exec.status;
      break;
    default:
      status = 'queued';
  }

  const compileResult: CompileResult = {
    compileId,
    status,
    stage,
    persona: persona as Persona | null,
    error,
  };

  json(res, 200, compileResult);
}

/** Extract a JSON block (possibly fenced) from LLM output. */
function extractJsonFromOutput(output: string): unknown | null {
  const jsonMatch = output.match(/```json\s*([\s\S]*?)\s*```/) ?? output.match(/(\[[\s\S]*\])/) ?? output.match(/(\{[\s\S]*\})/);
  if (!jsonMatch?.[1]) return null;
  return JSON.parse(jsonMatch[1]);
}

/** Build a Persona object from a parsed LLM design result. */
function parsedDesignToPersona(parsed: Record<string, unknown>, id: string, projectId: string): Persona {
  return {
    id,
    projectId,
    name: (parsed.name as string) ?? 'Compiled Persona',
    description: (parsed.description as string) ?? (parsed.summary as string) ?? null,
    systemPrompt: (parsed.full_prompt_markdown as string) ?? '',
    structuredPrompt: parsed.structured_prompt ? JSON.stringify(parsed.structured_prompt) : null,
    icon: null,
    color: null,
    enabled: true,
    maxConcurrent: 1,
    timeoutMs: 300_000,
    modelProfile: null,
    maxBudgetUsd: null,
    maxTurns: null,
    designContext: JSON.stringify(parsed),
    groupId: null,
    permissionPolicy: null,
    webhookSecret: randomBytes(32).toString('hex'),
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
  };
}

/**
 * GET /api/personas/compile/batch/:batchId
 *
 * Returns the batch compilation status with per-item results.
 * Partial-failure contract: always returns 200 with a results array.
 * Each item in results has its own status ('completed' | 'failed'),
 * persona, and error fields so callers can handle mixed outcomes.
 */
function handleGetBatchCompilation(
  res: http.ServerResponse,
  dispatcher: Dispatcher,
  batchId: string,
): void {
  const batch = batchTracker.get(batchId);
  if (!batch) {
    json(res, 404, { error: 'Batch compilation not found' });
    return;
  }

  const exec = dispatcher.getExecution(batch.execId);
  if (!exec) {
    json(res, 404, { error: 'Batch execution not found' });
    return;
  }

  const result: BatchCompileResult = {
    batchId,
    compileIds: [batch.execId],
    status: 'queued',
  };

  switch (exec.status) {
    case 'running':
      result.status = 'running';
      break;

    case 'completed': {
      // Use getExecutionOutput to handle evicted cache transparently
      const outputLines = exec.outputEvicted
        ? (dispatcher.getExecutionOutput(batch.execId) ?? [])
        : exec.output;
      const fullOutput = outputLines.join('\n');
      const results: BatchCompileItemResult[] = [];
      const projectId = batch.projectId ?? 'default';

      try {
        const parsed = extractJsonFromOutput(fullOutput);

        // Expect a JSON array of persona designs
        const items: unknown[] = Array.isArray(parsed) ? parsed : [parsed];

        for (let i = 0; i < items.length; i++) {
          const item = items[i];
          try {
            if (!item || typeof item !== 'object') {
              throw new Error('Item is not an object');
            }
            const persona = parsedDesignToPersona(
              item as Record<string, unknown>,
              nanoid(),
              projectId,
            );
            results.push({ index: i, status: 'completed', persona, error: null });
          } catch (err) {
            results.push({
              index: i,
              status: 'failed',
              persona: null,
              error: `Failed to parse persona at index ${i}: ${err instanceof Error ? err.message : String(err)}`,
            });
          }
        }
      } catch {
        // Entire output unparseable
        results.push({
          index: 0,
          status: 'failed',
          persona: null,
          error: 'Batch output could not be parsed as JSON',
        });
      }

      const succeeded = results.filter(r => r.status === 'completed').length;
      const total = results.length;

      if (succeeded === 0) {
        result.status = 'failed';
        result.error = 'All items failed to parse';
      } else if (succeeded < total) {
        result.status = 'partial';
      } else {
        result.status = 'completed';
      }

      result.results = results;
      break;
    }

    case 'failed':
    case 'cancelled':
      result.status = 'failed';
      result.error = 'Batch execution ' + exec.status;
      break;

    default:
      result.status = 'queued';
  }

  json(res, 200, result);
}

// ---------------------------------------------------------------------------
// Persona validation — dry-run config checks without execution
// ---------------------------------------------------------------------------

interface ValidationIssue {
  level: 'error' | 'warning' | 'info';
  category: string;
  message: string;
}

interface ValidationReport {
  personaId: string;
  personaName: string;
  valid: boolean;
  errors: number;
  warnings: number;
  infos: number;
  issues: ValidationIssue[];
}

/** Approximate token count — rough 4-chars-per-token heuristic. */
function estimateTokens(text: string): number {
  return Math.ceil(text.length / 4);
}

/** Known model context window sizes (tokens). */
const MODEL_CONTEXT_LIMITS: Record<string, number> = {
  'claude-sonnet-4-5-20250929': 200_000,
  'claude-opus-4-6': 200_000,
  'claude-haiku-4-5-20251001': 200_000,
  default: 200_000,
};

function validatePersona(
  persona: Persona,
  database: Database.Database,
  masterKeyHex: string | null,
  logger: Logger,
): ValidationReport {
  // Pre-fetch all related data in one batch
  const data = db.getPersonaValidationData(database, persona.id);
  const issues: ValidationIssue[] = [];

  // 1. System prompt checks
  if (!persona.systemPrompt && !persona.structuredPrompt) {
    issues.push({ level: 'error', category: 'prompt', message: 'Persona has no system prompt or structured prompt configured' });
  }

  if (persona.systemPrompt) {
    const tokens = estimateTokens(persona.systemPrompt);
    const modelName = parseModelProfile(persona.modelProfile)?.model ?? 'default';
    const contextLimit = MODEL_CONTEXT_LIMITS[modelName] ?? MODEL_CONTEXT_LIMITS['default'];
    // System prompt should not consume more than 50% of context window
    if (tokens > contextLimit * 0.5) {
      issues.push({
        level: 'warning',
        category: 'prompt',
        message: `System prompt is ~${tokens} tokens, consuming over 50% of the ${contextLimit}-token context window for model ${modelName}`,
      });
    }
    if (tokens > contextLimit) {
      issues.push({
        level: 'error',
        category: 'prompt',
        message: `System prompt is ~${tokens} tokens, exceeding the ${contextLimit}-token context window for model ${modelName}`,
      });
    }
  }

  // 2. Structured prompt validation
  if (persona.structuredPrompt) {
    try {
      JSON.parse(persona.structuredPrompt);
    } catch {
      issues.push({ level: 'error', category: 'prompt', message: 'structuredPrompt is not valid JSON' });
    }
  }

  // 3. Model profile validation
  if (persona.modelProfile) {
    const profile = parseModelProfile(persona.modelProfile);
    if (!profile) {
      issues.push({ level: 'error', category: 'model', message: 'modelProfile is not valid JSON' });
    } else {
      if (profile.provider && !['anthropic', 'ollama', 'litellm', 'custom'].includes(profile.provider)) {
        issues.push({ level: 'warning', category: 'model', message: `Unknown model provider "${profile.provider}" — expected anthropic, ollama, litellm, or custom` });
      }
      if (profile.provider && profile.provider !== 'anthropic' && !profile.baseUrl) {
        issues.push({ level: 'warning', category: 'model', message: `Non-Anthropic provider "${profile.provider}" configured but no baseUrl set` });
      }
    }
  }

  // 4. Permission policy validation
  if (persona.permissionPolicy) {
    const policy = parsePermissionPolicy(persona.permissionPolicy);
    if (!policy) {
      issues.push({ level: 'error', category: 'permissions', message: 'permissionPolicy is not valid JSON' });
    } else if (policy.skipAllPermissions) {
      issues.push({ level: 'warning', category: 'permissions', message: 'skipAllPermissions is enabled — all tool calls will be auto-approved without restriction' });
    }
  }

  // 5. Linked tools validation
  const { tools, credentials: creds, subscriptions: subs, triggers, deployment } = data;
  for (const tool of tools) {
    if (!tool.description) {
      issues.push({ level: 'warning', category: 'tools', message: `Tool "${tool.name}" has no description — the LLM may not know when to use it` });
    }
    if (tool.requiresCredentialType) {
      const hasCred = creds.some(c => c.serviceType === tool.requiresCredentialType);
      if (!hasCred) {
        issues.push({
          level: 'error',
          category: 'tools',
          message: `Tool "${tool.name}" requires a credential of type "${tool.requiresCredentialType}" but none is linked to this persona`,
        });
      }
    }
  }

  // 6. Credentials validation
  for (const cred of creds) {
    if (!cred.encryptedData || !cred.iv || !cred.tag) {
      issues.push({ level: 'error', category: 'credentials', message: `Credential "${cred.name}" has missing encryption fields (encryptedData, iv, or tag)` });
      continue;
    }

    // Try to decrypt the credential to verify it's valid
    if (masterKeyHex) {
      try {
        const credSalt = cred.salt ? Buffer.from(cred.salt, 'hex') : undefined;
        const { key } = deriveMasterKey(masterKeyHex, credSalt, cred.iter);
        const payload: EncryptedPayload = {
          encrypted: cred.encryptedData,
          iv: cred.iv,
          tag: cred.tag,
          salt: cred.salt,
          iter: cred.iter,
        };
        decrypt(payload, key);
      } catch (err) {
        issues.push({
          level: 'error',
          category: 'credentials',
          message: `Credential "${cred.name}" failed to decrypt — it may be corrupted or encrypted with a different master key`,
        });
        logger.debug({ err, credentialId: cred.id }, 'Credential decryption failed during validation');
      }
    } else {
      issues.push({ level: 'warning', category: 'credentials', message: `Cannot verify credential "${cred.name}" — no master key configured` });
    }
  }

  // 7. Subscriptions validation
  for (const sub of subs) {
    if (!sub.eventType) {
      issues.push({ level: 'error', category: 'subscriptions', message: `Subscription ${sub.id} has no eventType configured` });
    }
    if (sub.payloadFilter) {
      const filterError = validatePayloadFilter(sub.payloadFilter);
      if (filterError) {
        issues.push({
          level: 'error',
          category: 'subscriptions',
          message: `Subscription ${sub.id} has invalid payloadFilter: ${filterError}`,
        });
      }
    }
  }

  // 8. Triggers validation
  for (const trigger of triggers) {
    if (!trigger.triggerType) {
      issues.push({ level: 'error', category: 'triggers', message: `Trigger ${trigger.id} has no triggerType configured` });
    }
    if (trigger.triggerType === 'cron' && trigger.config) {
      try {
        const config = JSON.parse(trigger.config);
        if (!config.schedule) {
          issues.push({ level: 'error', category: 'triggers', message: `Cron trigger ${trigger.id} has no schedule in config` });
        }
      } catch {
        issues.push({ level: 'error', category: 'triggers', message: `Trigger ${trigger.id} config is not valid JSON` });
      }
    } else if (trigger.triggerType === 'cron' && !trigger.config) {
      issues.push({ level: 'error', category: 'triggers', message: `Cron trigger ${trigger.id} has no config (schedule is required)` });
    }
  }

  // 9. Deployment checks
  if (deployment) {
    if (!deployment.webhookSecret && !persona.webhookSecret) {
      issues.push({
        level: 'warning',
        category: 'deployment',
        message: 'Deployment exists but no webhook secret is configured — API endpoint will require API key auth for every request',
      });
    }
    if (deployment.status === 'paused') {
      issues.push({ level: 'warning', category: 'deployment', message: 'Deployment is paused — incoming requests will be rejected' });
    }
  }

  // 10. General persona config checks
  if (!persona.enabled) {
    issues.push({ level: 'warning', category: 'config', message: 'Persona is disabled — it will not process events or triggers' });
  }
  if (persona.timeoutMs < 5_000) {
    issues.push({ level: 'warning', category: 'config', message: `Timeout is very low (${persona.timeoutMs}ms) — execution may be killed before completing` });
  }

  // Info-level findings — advisory suggestions for improvement
  if (tools.length === 0) {
    issues.push({ level: 'info', category: 'tools', message: 'No tools linked — the persona will rely solely on its system prompt without tool access' });
  }
  if (subs.length === 0 && triggers.length === 0 && !deployment) {
    issues.push({ level: 'info', category: 'config', message: 'No subscriptions, triggers, or deployments — the persona can only be executed manually via the API' });
  }
  if (!persona.description) {
    issues.push({ level: 'info', category: 'config', message: 'No description set — adding one helps identify the persona in dashboards and logs' });
  }
  if (!persona.modelProfile) {
    issues.push({ level: 'info', category: 'model', message: 'No model profile configured — the default model and parameters will be used' });
  }
  if (persona.maxConcurrent > 5) {
    issues.push({ level: 'info', category: 'config', message: `High maxConcurrent (${persona.maxConcurrent}) — ensure sufficient worker capacity for this level of parallelism` });
  }

  const errors = issues.filter(i => i.level === 'error').length;
  const warnings = issues.filter(i => i.level === 'warning').length;
  const infos = issues.filter(i => i.level === 'info').length;

  return {
    personaId: persona.id,
    personaName: persona.name,
    valid: errors === 0,
    errors,
    warnings,
    infos,
    issues,
  };
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

// ---------------------------------------------------------------------------
// Zod-based body parser + validator
// ---------------------------------------------------------------------------

/**
 * Parse the request body as JSON and validate it against a Zod schema.
 * Returns the validated & typed data, or an error string with field-level
 * details. This replaces the old `parseJsonBody` + manual `validate*` functions
 * and unsafe `JSON.parse(body) as T` patterns.
 */
async function parseAndValidate<T>(
  req: http.IncomingMessage,
  schema: ZodSchema<T>,
): Promise<{ ok: true; data: T } | { ok: false; error: string }> {
  const raw = await readBody(req);
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return { ok: false, error: 'Invalid JSON body' };
  }
  if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) {
    return { ok: false, error: 'Request body must be a JSON object' };
  }
  const result = schema.safeParse(parsed);
  if (!result.success) {
    return { ok: false, error: formatZodError(result.error) };
  }
  return { ok: true, data: result.data };
}

/** Convert a name to a URL-safe slug. */
function toSlug(name: string): string {
  return name
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 64) || 'api';
}

const MAX_BODY_BYTES = 1024 * 1024; // 1 MB — CRUD endpoints
const MAX_WEBHOOK_BODY_BYTES = 256 * 1024; // 256 KB — webhook endpoints

function readBody(req: http.IncomingMessage, maxBytes: number = MAX_BODY_BYTES): Promise<string> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    let totalBytes = 0;
    let destroyed = false;
    req.on('data', (chunk: Buffer) => {
      if (destroyed) return;
      totalBytes += chunk.length;
      if (totalBytes > maxBytes) {
        destroyed = true;
        req.destroy();
        reject(new Error('Payload too large'));
        return;
      }
      chunks.push(chunk);
    });
    req.on('end', () => {
      if (!destroyed) resolve(Buffer.concat(chunks).toString());
    });
    req.on('error', reject);
  });
}
