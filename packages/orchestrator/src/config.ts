import 'dotenv/config';
import path from 'node:path';

export interface OrchestratorConfig {
  masterKey: string;
  claudeToken: string;
  teamApiKey: string;
  workerToken: string;
  supabaseJwtSecret: string;
  supabaseProjectRef: string;
  gitlabWebhookSecret: string;
  corsOrigins: string[];
  /** When true, all origins are allowed (CORS_ORIGINS="*"). */
  corsWildcard: boolean;
  wsPort: number;
  httpPort: number;
  // TLS
  tlsEnabled: boolean;
  tlsCertPath: string;
  tlsKeyPath: string;
  tlsCaPath: string;
  tlsRequireClientCert: boolean;
  // Output scrubbing
  scrubberEnabled: boolean;
  // Audit
  auditDbPath: string;
  auditRetentionDays: number;
  auditChainVerifyIntervalMinutes: number;
  tokenExpiryWarningMinutes: number;
  // Retention
  retentionDays: number;
  metricsRetentionDays: number;
  // Data directory
  dataDir: string;
  // Worker verification
  allowedImageDigests: string[];
  rejectUnverifiedWorkers: boolean;
  // Queue backpressure
  maxQueueDepth: number;
  queueWarningThreshold: number;
  queueCriticalThreshold: number;
  perTenantQueueQuota: number;
  queueRetryAfterSeconds: number;
  /** When true, requests without a JWT fall back to admin context. Defaults to false. */
  enableAdminFallback: boolean;
  /**
   * Number of trusted reverse proxies in front of the orchestrator.
   * When > 0, the client IP is extracted from the Nth-from-right entry in
   * the X-Forwarded-For header (e.g. 1 for a single proxy like nginx/ALB).
   * When 0 (default), X-Forwarded-For is ignored and req.socket.remoteAddress
   * is used directly, preventing IP spoofing via header injection.
   */
  trustedProxyCount: number;
  // Fly Machines (on-demand workers)
  flyEnabled: boolean;
  flyAppName: string;
  flyApiToken: string;
  flyRegion: string;
  flyImageRef: string;
  flyOrchestratorWsUrl: string;
  flyCpuKind: 'shared' | 'performance';
  flyCpus: number;
  flyMemoryMb: number;
}

function required(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return value;
}

function optional(name: string, fallback: string): string {
  return process.env[name] || fallback;
}

export function loadConfig(): OrchestratorConfig {
  const dataDir = process.env['DAC_DATA_DIR'] || path.join(process.cwd(), 'data');

  const config: OrchestratorConfig = {
    masterKey: required('MASTER_KEY'),
    claudeToken: process.env['CLAUDE_TOKEN'] || '',  // Optional: use OAuth flow if not provided
    teamApiKey: required('TEAM_API_KEY'),
    workerToken: required('WORKER_TOKEN'),
    supabaseJwtSecret: process.env['SUPABASE_JWT_SECRET'] || '',
    supabaseProjectRef: process.env['SUPABASE_PROJECT_REF'] || '',
    gitlabWebhookSecret: process.env['GITLAB_WEBHOOK_SECRET'] || '',
    corsOrigins: (process.env['CORS_ORIGINS'] || '').split(',').map(s => s.trim()).filter(Boolean),
    corsWildcard: false, // Set by validateConfig if CORS_ORIGINS="*"
    wsPort: parseInt(optional('WS_PORT', '8443'), 10),
    httpPort: parseInt(optional('HTTP_PORT', '3001'), 10),
    // TLS
    tlsEnabled: process.env['TLS_ENABLED'] !== 'false',
    tlsCertPath: process.env['TLS_CERT_PATH'] || '',
    tlsKeyPath: process.env['TLS_KEY_PATH'] || '',
    tlsCaPath: process.env['TLS_CA_PATH'] || '',
    tlsRequireClientCert: process.env['TLS_REQUIRE_CLIENT_CERT'] === 'true',
    // Output scrubbing
    scrubberEnabled: process.env['SCRUBBER_ENABLED'] !== 'false',
    // Audit
    auditDbPath: process.env['AUDIT_DB_PATH'] || path.join(dataDir, 'audit.db'),
    auditRetentionDays: parseInt(process.env['AUDIT_RETENTION_DAYS'] || '90', 10),
    auditChainVerifyIntervalMinutes: parseInt(process.env['AUDIT_CHAIN_VERIFY_INTERVAL_MINUTES'] || '60', 10),
    tokenExpiryWarningMinutes: parseInt(process.env['TOKEN_EXPIRY_WARNING_MINUTES'] || '30', 10),
    // Retention
    retentionDays: parseInt(process.env['RETENTION_DAYS'] || '30', 10),
    metricsRetentionDays: parseInt(process.env['METRICS_RETENTION_DAYS'] || '365', 10),
    // Data directory
    dataDir,
    // Worker verification
    allowedImageDigests: (process.env['ALLOWED_IMAGE_DIGESTS'] || '').split(',').map(s => s.trim()).filter(Boolean),
    rejectUnverifiedWorkers: process.env['REJECT_UNVERIFIED_WORKERS'] === 'true',
    // Queue backpressure
    maxQueueDepth: parseInt(process.env['MAX_QUEUE_DEPTH'] || '1000', 10),
    queueWarningThreshold: parseInt(process.env['QUEUE_WARNING_THRESHOLD'] || '500', 10),
    queueCriticalThreshold: parseInt(process.env['QUEUE_CRITICAL_THRESHOLD'] || '800', 10),
    perTenantQueueQuota: parseInt(process.env['PER_TENANT_QUEUE_QUOTA'] || '0', 10),
    queueRetryAfterSeconds: parseInt(process.env['QUEUE_RETRY_AFTER_SECONDS'] || '30', 10),
    enableAdminFallback: process.env['ENABLE_ADMIN_FALLBACK'] === '1',
    trustedProxyCount: Math.max(0, parseInt(process.env['TRUSTED_PROXY_COUNT'] || '0', 10) || 0),
    // Fly Machines
    flyEnabled: process.env['FLY_MACHINES_ENABLED'] === 'true',
    flyAppName: process.env['FLY_APP_NAME'] || '',
    flyApiToken: process.env['FLY_API_TOKEN'] || '',
    flyRegion: process.env['FLY_REGION'] || 'iad',
    flyImageRef: process.env['FLY_WORKER_IMAGE'] || '',
    flyOrchestratorWsUrl: process.env['FLY_ORCHESTRATOR_WS_URL'] || '',
    flyCpuKind: (process.env['FLY_CPU_KIND'] === 'performance' ? 'performance' : 'shared') as 'shared' | 'performance',
    flyCpus: parseInt(process.env['FLY_CPUS'] || '1', 10),
    flyMemoryMb: parseInt(process.env['FLY_MEMORY_MB'] || '1024', 10),
  };

  validateConfig(config);

  return config;
}

/**
 * Validate config after loading — fail fast on missing critical secrets,
 * warn on misconfigured optional integrations.
 */
function validateConfig(config: OrchestratorConfig): void {
  const errors: string[] = [];
  const warnings: string[] = [];

  // JWT auth: supabaseJwtSecret is required for user-scoped auth.
  // Without it, requests without a JWT cannot be validated.
  // ENABLE_ADMIN_FALLBACK=1 explicitly opts in to admin-only mode.
  if (!config.supabaseJwtSecret && !config.enableAdminFallback) {
    errors.push(
      'SUPABASE_JWT_SECRET is not set and ENABLE_ADMIN_FALLBACK is not enabled — ' +
      'the orchestrator cannot authenticate users. Either set SUPABASE_JWT_SECRET for multi-tenant JWT auth, ' +
      'or set ENABLE_ADMIN_FALLBACK=1 to explicitly run in single-tenant admin-only mode.',
    );
  }

  if (config.enableAdminFallback) {
    warnings.push(
      'ENABLE_ADMIN_FALLBACK=1 — requests without a valid JWT will be granted admin context (__admin__). ' +
      'Do not use this in multi-tenant production deployments.',
    );
  }

  // Fly Machines — validate required fields when enabled
  if (config.flyEnabled) {
    if (!config.flyAppName) errors.push('FLY_MACHINES_ENABLED=true but FLY_APP_NAME is not set.');
    if (!config.flyApiToken) errors.push('FLY_MACHINES_ENABLED=true but FLY_API_TOKEN is not set.');
    if (!config.flyImageRef) errors.push('FLY_MACHINES_ENABLED=true but FLY_WORKER_IMAGE is not set.');
    if (!config.flyOrchestratorWsUrl) errors.push('FLY_MACHINES_ENABLED=true but FLY_ORCHESTRATOR_WS_URL is not set.');
  }

  // Trusted proxy count validation — wrong value silently breaks rate limiting and audit logs
  const MAX_SANE_PROXY_COUNT = 5;
  if (config.trustedProxyCount === 0) {
    const isProduction = process.env['NODE_ENV'] === 'production';
    if (isProduction) {
      warnings.push(
        'TRUSTED_PROXY_COUNT is 0 in production — X-Forwarded-For headers are ignored and ' +
        'req.socket.remoteAddress is used for client IPs. If the orchestrator is behind a reverse proxy ' +
        '(nginx, ALB, Cloudflare), rate limiting will use the proxy IP instead of client IPs. ' +
        'Set TRUSTED_PROXY_COUNT to the number of trusted proxies in front of this service.',
      );
    }
    console.info(
      '[config] Client IP strategy: direct connection (remoteAddress). ' +
      'Set TRUSTED_PROXY_COUNT > 0 if behind a reverse proxy.',
    );
  } else if (config.trustedProxyCount > MAX_SANE_PROXY_COUNT) {
    warnings.push(
      `TRUSTED_PROXY_COUNT is ${config.trustedProxyCount}, which is unusually high. ` +
      `Typical deployments have 1-3 proxies. A wrong value extracts the wrong IP from X-Forwarded-For, ` +
      `silently breaking rate limiting and audit logs.`,
    );
  } else {
    console.info(
      `[config] Client IP strategy: X-Forwarded-For (${config.trustedProxyCount} trusted ` +
      `${config.trustedProxyCount === 1 ? 'proxy' : 'proxies'}, extracting ${config.trustedProxyCount === 1 ? 'rightmost' : `${config.trustedProxyCount}th from right`} entry).`,
    );
  }

  // CORS origin validation — explicit handling of edge cases
  const rawCorsEnv = process.env['CORS_ORIGINS'];
  const hasWildcard = config.corsOrigins.includes('*');

  if (hasWildcard && config.corsOrigins.length > 1) {
    errors.push(
      'CORS_ORIGINS contains both "*" and specific origins — this is ambiguous. ' +
      'Use either "*" alone to allow all origins (not recommended for production), ' +
      'or list specific origins only.',
    );
  } else if (hasWildcard) {
    config.corsOrigins = [];
    config.corsWildcard = true;
    warnings.push(
      'CORS_ORIGINS is set to "*" — all origins will be allowed. ' +
      'This is suitable for development but should not be used in production.',
    );
  }

  if (config.corsOrigins.length === 0 && !hasWildcard) {
    if (rawCorsEnv !== undefined && rawCorsEnv.trim() === '') {
      // Explicit empty string — treat as "no CORS origins allowed"
      console.info('[config] CORS policy: no origins allowed (CORS_ORIGINS is empty). Cross-origin requests will be blocked.');
    } else if (rawCorsEnv === undefined) {
      console.info('[config] CORS policy: no origins configured (CORS_ORIGINS not set). Cross-origin requests will be blocked.');
    }
  } else if (hasWildcard) {
    console.info('[config] CORS policy: all origins allowed (wildcard).');
  } else {
    console.info(`[config] CORS policy: ${config.corsOrigins.length} allowed origin(s): ${config.corsOrigins.join(', ')}`);
  }

  // Validate individual CORS origins are well-formed URLs (scheme + host, no path/trailing slash)
  for (const origin of config.corsOrigins) {
    try {
      const parsed = new URL(origin);
      if (parsed.origin !== origin) {
        warnings.push(
          `CORS_ORIGINS entry "${origin}" will be normalised to "${parsed.origin}" by browsers. ` +
          `Use the exact origin (scheme + host + optional port, no trailing slash or path).`,
        );
      }
    } catch {
      errors.push(
        `CORS_ORIGINS contains an invalid origin: "${origin}". ` +
        `Each entry must be a valid URL origin (e.g. "https://app.example.com" or "http://localhost:3000").`,
      );
    }
  }

  // Emit warnings via stderr (pino not available here, keep config module dependency-free)
  for (const w of warnings) {
    console.warn(`[config] WARNING: ${w}`);
  }

  if (errors.length > 0) {
    const message = [
      'Startup config validation failed:',
      ...errors.map((e, i) => `  ${i + 1}. ${e}`),
    ].join('\n');
    throw new Error(message);
  }
}
