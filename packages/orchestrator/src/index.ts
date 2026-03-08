import path from 'node:path';
import pino from 'pino';
import { loadConfig } from './config.js';
import { createAuth } from './auth.js';
import { createTokenManager } from './tokenManager.js';
import { OAuthManager } from './oauth.js';
import { WorkerPool } from './workerPool.js';
import { createKafkaClient, createNoopKafkaClient } from './kafka.js';
import { Dispatcher } from './dispatcher.js';
import { createHttpApi } from './httpApi.js';
import { initDb } from './db.js';
import { startEventProcessor } from './eventProcessor.js';
import { startTriggerScheduler } from './triggerScheduler.js';
import { TOPICS, projectTopic, type ExecRequest } from '@dac-cloud/shared';

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
  const auth = createAuth(config.teamApiKey, {
    supabaseJwtSecret: config.supabaseJwtSecret || undefined,
    supabaseProjectRef: config.supabaseProjectRef || undefined,
    enableAdminFallback: config.enableAdminFallback,
  });
  const tokenManager = createTokenManager(config.masterKey, logger);
  const oauth = new OAuthManager(logger);

  // If a direct Claude token is provided (from setup-token), store it
  if (config.claudeToken) {
    tokenManager.storeClaudeToken(config.claudeToken);
    logger.info('Direct Claude token loaded from environment');
  } else {
    logger.info('No direct Claude token — use OAuth flow via POST /api/oauth/authorize');
  }

  // Database
  const dbPath = process.env['DAC_DB_PATH'] || path.join(process.cwd(), 'data', 'personas.db');
  const database = initDb(dbPath, logger);

  // Worker pool (WebSocket server)
  const pool = new WorkerPool(config.workerToken, logger);

  // Kafka client (deployment ID isolates consumer groups in multi-deployment setups)
  const kafka = config.kafkaEnabled
    ? createKafkaClient(
        config.kafkaBrokers,
        config.kafkaUsername,
        config.kafkaPassword,
        config.kafkaSigningKey,
        logger,
        config.kafkaDeploymentId || undefined,
        config.kafkaBatchSize,
        config.kafkaLingerMs,
      )
    : createNoopKafkaClient(logger);

  // Dispatcher — uses OAuth manager for token refresh
  const dispatcher = new Dispatcher(pool, tokenManager, oauth, kafka, logger);
  dispatcher.setDatabase(database);
  dispatcher.setMasterKey(config.masterKey);

  // Recover queued executions from DB (crash recovery)
  dispatcher.recoverQueue();

  // Connect Kafka
  if (config.kafkaEnabled) {
    await kafka.connect();

    // Subscribe to execution requests (base topic handles requests without projectId)
    await kafka.subscribe(async (topic: string, value: string) => {
      try {
        const request: ExecRequest = JSON.parse(value);

        // Validate that the message's projectId matches the topic it was sent to.
        // Messages on the base topic must not carry a projectId (or use 'default').
        // Messages on a project-scoped topic must have a matching projectId.
        const expectedTopic = projectTopic(TOPICS.EXEC_REQUESTS, request.projectId);
        if (topic !== expectedTopic) {
          logger.warn({ topic, expectedTopic, projectId: request.projectId, executionId: request.executionId },
            'ExecRequest projectId does not match topic — dropped');
          return;
        }

        if (!dispatcher.submit(request)) {
          logger.warn({ executionId: request.executionId }, 'Kafka execution rejected — queue full');
        }
      } catch (err) {
        logger.error({ err }, 'Failed to parse execution request from Kafka');
      }
    });
  }

  // Start WebSocket server for workers
  pool.listen(config.wsPort);

  // Start event processor (change-driven with 30s fallback poll)
  const eventProcessor = startEventProcessor(database, dispatcher, logger);

  // When an execution completes, notify the event processor so concurrency-blocked
  // events can be retried immediately instead of waiting for the fallback poll.
  dispatcher.onExecutionComplete(() => eventProcessor.notify());

  // Start HTTP API (now includes OAuth endpoints + persona CRUD)
  // Pass eventProcessor.notify so event insertions trigger immediate processing.
  const httpServer = createHttpApi(auth, dispatcher, pool, tokenManager, oauth, logger, database, config.gitlabWebhookSecret || undefined, config.corsOrigins, eventProcessor.notify, config.trustedProxyCount, config.corsWildcard);
  httpServer.listen(config.httpPort, () => {
    logger.info({ port: config.httpPort }, 'HTTP API listening');
  });

  // Start trigger scheduler (5s tick) — fires events that also notify the processor
  const triggerSchedulerTimer = startTriggerScheduler(database, logger, undefined, eventProcessor.notify);

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
    if (counts.total > 0 || queueLen > 0) {
      logger.info({
        workers: counts,
        queueLength: queueLen,
        activeExecutions: dispatcher.getActiveExecutions().length,
        hasOAuth: oauth.hasTokens(),
      }, 'Status');
    }
  }, 60_000);

  // Graceful shutdown
  const shutdown = async () => {
    logger.info('Shutting down...');
    eventProcessor.stop();
    clearInterval(triggerSchedulerTimer);
    httpServer.close(); // Stop accepting new HTTP connections first
    await dispatcher.shutdown(); // Drain in-flight executions (up to 60s)
    pool.shutdown();
    await kafka.disconnect();
    database.close();
    process.exit(0);
  };

  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);

  logger.info('DAC Cloud Orchestrator ready');
  logger.info(`  HTTP API: http://localhost:${config.httpPort}`);
  logger.info(`  Worker WS: ws://localhost:${config.wsPort}`);
  if (!config.claudeToken) {
    logger.info('  Connect subscription: POST /api/oauth/authorize');
  }
}

main().catch((err) => {
  logger.fatal({ err }, 'Failed to start orchestrator');
  process.exit(1);
});
