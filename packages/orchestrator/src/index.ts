import pino from 'pino';
import { loadConfig } from './config.js';
import { createAuth } from './auth.js';
import { createTokenManager } from './tokenManager.js';
import { OAuthManager } from './oauth.js';
import { WorkerPool, type WorkerPoolEvents } from './workerPool.js';
import { createKafkaClient, createNoopKafkaClient } from './kafka.js';
import { Dispatcher } from './dispatcher.js';
import { createHttpApi } from './httpApi.js';
import { TenantDbManager } from './tenantDbManager.js';
import { TenantKeyManager, type TenantKeyEvents } from './tenantKeyManager.js';
import { AuditLog } from './auditLog.js';
import { AuditPolicy } from './auditPolicy.js';
import { AuditRetentionPolicy } from './auditRetentionPolicy.js';
import { AuditChainVerifier } from './auditChainVerifier.js';
import { startRetentionJob } from './retention.js';
import { startEventProcessor } from './eventProcessor.js';
import { startTriggerScheduler } from './triggerScheduler.js';
import { loadTlsConfig } from './tls.js';
import { TOPICS, type ExecRequest } from '@dac-cloud/shared';

const logger = pino({ level: process.env['LOG_LEVEL'] || 'info' });

async function main() {
  logger.info('Starting DAC Cloud Orchestrator...');

  // Load configuration
  const config = loadConfig();
  logger.info({
    kafkaEnabled: config.kafkaEnabled,
    wsPort: config.wsPort,
    httpPort: config.httpPort,
    hasDirectToken: !!config.claudeToken,
  }, 'Configuration loaded');

  // Initialize components
  const auth = createAuth(config.teamApiKey, config.supabaseJwtSecret || undefined);
  const tokenManager = await createTokenManager(config.masterKey, logger);
  const oauth = new OAuthManager(logger);

  // If a direct Claude token is provided (from setup-token), store it
  if (config.claudeToken) {
    tokenManager.storeClaudeToken(config.claudeToken);
    logger.info('Direct Claude token loaded from environment');
  } else {
    logger.info('No direct Claude token — use OAuth flow via POST /api/oauth/authorize');
  }

  // Tenant-isolated database manager
  const tenantDbManager = new TenantDbManager(config.dataDir, logger);
  logger.info({ dataDir: config.dataDir }, 'Tenant DB manager initialized');

  // Worker pool (WebSocket server)
  const pool = new WorkerPool(config.workerToken, logger, {
    allowedDigests: config.allowedImageDigests,
    rejectUnverified: config.rejectUnverifiedWorkers,
  });

  // Kafka client
  const kafka = config.kafkaEnabled
    ? createKafkaClient(config.kafkaBrokers, config.kafkaUsername, config.kafkaPassword, logger)
    : createNoopKafkaClient(logger);

  // Audit log (separate append-only DB)
  const auditLog = new AuditLog(config.auditDbPath, logger);
  const auditRetention = new AuditRetentionPolicy(auditLog, config.auditRetentionDays, logger);
  auditRetention.startDailyPurge();

  // Periodic hash chain integrity verification
  const auditChainVerifier = new AuditChainVerifier(
    auditLog, logger, config.auditChainVerifyIntervalMinutes * 60_000,
  );
  auditChainVerifier.start();

  // Wire audit log into OAuth manager for token lifecycle events
  oauth.setAuditLog(auditLog, config.tokenExpiryWarningMinutes);

  // Per-tenant key manager (cached key derivation with TTL)
  const tenantKeyManager = new TenantKeyManager(config.masterKey, logger);

  // Declarative audit policy — maps emitter events to audit entries
  const auditPolicy = new AuditPolicy(auditLog);

  auditPolicy.attach<TenantKeyEvents>(tenantKeyManager, {
    'key:derived': (e) => ({ action: 'key:derive', tenantId: e.tenantId }),
  });

  auditPolicy.attach<WorkerPoolEvents>(pool, {
    'worker-connected': (info) => ({ action: 'worker:connected', detail: { workerId: info.workerId } }),
    'worker-disconnected': (workerId, executionId) => ({ action: 'worker:disconnected', detail: { workerId }, resourceId: executionId }),
  });

  // Wire tenant ID source for key pre-warming and proactive renewal
  await tenantKeyManager.setTenantIdSource(() => tenantDbManager.listTenantIds());
  // Eagerly warm key cache when a new tenant is registered
  tenantDbManager.setTenantCreatedCallback((tenantId) => tenantKeyManager.warmTenant(tenantId));

  // Dispatcher — uses OAuth manager for token refresh
  const dispatcher = new Dispatcher(pool, tokenManager, oauth, kafka, logger, {
    maxQueueDepth: config.maxQueueDepth,
    warningThreshold: config.queueWarningThreshold,
    criticalThreshold: config.queueCriticalThreshold,
    perTenantQuota: config.perTenantQueueQuota,
    retryAfterSeconds: config.queueRetryAfterSeconds,
  });
  dispatcher.setTenantDbManager(tenantDbManager);
  await dispatcher.setMasterKey(config.masterKey);
  dispatcher.setTenantKeyManager(tenantKeyManager);
  dispatcher.setAuditLog(auditLog);

  // Connect Kafka
  if (config.kafkaEnabled) {
    await kafka.connect();

    // Subscribe to execution requests
    await kafka.subscribe(async (topic: string, value: string) => {
      if (topic === TOPICS.EXEC_REQUESTS) {
        try {
          const request: ExecRequest = JSON.parse(value);
          const result = dispatcher.submit(request);
          if (!result.accepted) {
            logger.warn({ executionId: request.executionId, reason: result.reason }, 'Kafka execution rejected by backpressure, sending to DLQ');
            await kafka.produce(TOPICS.DLQ, JSON.stringify({
              originalTopic: TOPICS.EXEC_REQUESTS,
              reason: result.reason,
              payload: request,
              timestamp: Date.now(),
            }), request.executionId);
          }
        } catch (err) {
          logger.error({ err }, 'Failed to parse execution request from Kafka');
        }
      }
    });
  }

  // Load TLS config and start WebSocket server for workers
  const tlsConfig = loadTlsConfig(logger);
  pool.listen(config.wsPort, tlsConfig.enabled ? tlsConfig : undefined);

  // Start HTTP API (now includes OAuth endpoints + persona CRUD)
  // Set master key on Kafka for envelope encryption
  await kafka.setMasterKey(config.masterKey);

  // Start event processor (demand-driven with 30s backstop) — tenant-aware
  const eventProcessor = startEventProcessor(null, dispatcher, logger, 30_000, tenantDbManager);

  // Start heap-driven trigger scheduler — builds in-memory min-heap from DB on startup
  const triggerScheduler = startTriggerScheduler(null, logger, undefined, tenantDbManager, eventProcessor.nudge);

  // Start daily retention job (purge old executions/events, aggregate metrics)
  const retentionJob = startRetentionJob(tenantDbManager, config.retentionDays, config.metricsRetentionDays, logger);

  const httpServer = createHttpApi(auth, dispatcher, pool, tokenManager, oauth, logger, undefined, tenantDbManager, tenantKeyManager, auditLog, eventProcessor.getHealth, kafka, eventProcessor.nudge, triggerScheduler, retentionJob.getHealth);
  httpServer.listen(config.httpPort, () => {
    logger.info({ port: config.httpPort }, 'HTTP API listening');
  });

  // Token refresh loop — refresh OAuth token proactively every 30 minutes
  setInterval(async () => {
    if (oauth.hasTokens()) {
      const token = await oauth.getValidAccessToken();
      if (token) {
        tokenManager.storeClaudeToken(token);
      }
    }
  }, 30 * 60 * 1000);

  // Log status periodically
  setInterval(() => {
    const counts = pool.getWorkerCount();
    const queueLen = dispatcher.getQueueLength();
    const execCounters = dispatcher.snapshotAndResetCounters();
    const epCounters = eventProcessor.snapshotAndResetCounters();

    logger.info({
      workers: counts,
      queueLength: queueLen,
      activeExecutions: dispatcher.getActiveExecutions().length,
      hasOAuth: oauth.hasTokens(),
      interval: {
        execCompleted: execCounters.completed,
        execFailed: execCounters.failed,
        costUsd: execCounters.costUsd,
        eventsFetched: epCounters.eventsFetched,
        eventsDelivered: epCounters.delivered,
        eventsSkipped: epCounters.skipped,
        eventsFailed: epCounters.failed,
        eventTicks: epCounters.ticks,
      },
    }, 'Status');

    // Database health stats — early warning for unbounded growth
    try {
      const dbHealth = tenantDbManager.getHealthStats();

      // Audit DB size
      let auditSizeBytes = 0;
      let auditRowCount = 0;
      try {
        const auditDb = auditLog.getDb();
        const sizeRow = auditDb.prepare('SELECT page_count * page_size AS size FROM pragma_page_count(), pragma_page_size()').get() as { size: number } | undefined;
        auditSizeBytes = sizeRow?.size ?? 0;
        const countRow = auditDb.prepare('SELECT COUNT(*) AS cnt FROM audit_events').get() as { cnt: number };
        auditRowCount = countRow.cnt;
      } catch { /* audit DB stats are best-effort */ }

      const oldestAgeDays = dbHealth.oldestUnpurgedAgeMs !== null
        ? Math.round(dbHealth.oldestUnpurgedAgeMs / (1000 * 60 * 60 * 24) * 10) / 10
        : null;

      logger.info({
        totalSizeMB: Math.round(dbHealth.totalSizeBytes / (1024 * 1024) * 100) / 100,
        auditSizeMB: Math.round(auditSizeBytes / (1024 * 1024) * 100) / 100,
        tenantCount: dbHealth.tenantCount,
        executionRows: dbHealth.totalExecutionRows,
        eventRows: dbHealth.totalEventRows,
        auditRows: auditRowCount,
        oldestUnpurgedDays: oldestAgeDays,
        retentionDays: config.retentionDays,
      }, 'DB health');
    } catch (err) {
      logger.warn({ err }, 'Failed to collect DB health stats');
    }
  }, 60_000);

  // Graceful shutdown
  const shutdown = async () => {
    logger.info('Shutting down...');
    clearInterval(eventProcessor.timer);
    triggerScheduler.close();
    clearInterval(retentionJob.timer);
    dispatcher.shutdown();
    pool.shutdown();
    await kafka.disconnect();
    httpServer.close();
    tenantKeyManager.close();
    auditChainVerifier.close();
    auditRetention.close();
    auditLog.close();
    tenantDbManager.close();
    oauth.dispose();
    process.exit(0);
  };

  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);

  logger.info('DAC Cloud Orchestrator ready');
  logger.info(`  HTTP API: http://localhost:${config.httpPort}`);
  logger.info(`  Worker WS: ${tlsConfig.enabled ? 'wss' : 'ws'}://localhost:${config.wsPort}`);
  if (!config.claudeToken) {
    logger.info('  Connect subscription: POST /api/oauth/authorize');
  }
}

main().catch((err) => {
  logger.fatal({ err }, 'Failed to start orchestrator');
  process.exit(1);
});
