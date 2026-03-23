import 'dotenv/config';
import path from 'node:path';

export interface OrchestratorConfig {
  masterKey: string;
  claudeToken: string;
  teamApiKey: string;
  workerToken: string;
  kafkaBrokers: string;
  kafkaUsername: string;
  kafkaPassword: string;
  kafkaEnabled: boolean;
  supabaseJwtSecret: string;
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
  const kafkaBrokers = process.env['KAFKA_BROKERS'] || '';

  const dataDir = process.env['DAC_DATA_DIR'] || path.join(process.cwd(), 'data');

  return {
    masterKey: required('MASTER_KEY'),
    claudeToken: process.env['CLAUDE_TOKEN'] || '',  // Optional: use OAuth flow if not provided
    teamApiKey: required('TEAM_API_KEY'),
    workerToken: required('WORKER_TOKEN'),
    kafkaBrokers,
    kafkaUsername: process.env['KAFKA_USERNAME'] || '',
    kafkaPassword: process.env['KAFKA_PASSWORD'] || '',
    kafkaEnabled: kafkaBrokers.length > 0,
    supabaseJwtSecret: process.env['SUPABASE_JWT_SECRET'] || '',
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
  };
}
