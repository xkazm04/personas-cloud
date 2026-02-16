import pino from 'pino';
import { loadConfig } from './config.js';
import { createAuth } from './auth.js';
import { createTokenManager } from './tokenManager.js';
import { OAuthManager } from './oauth.js';
import { WorkerPool } from './workerPool.js';
import { createKafkaClient, createNoopKafkaClient } from './kafka.js';
import { Dispatcher } from './dispatcher.js';
import { createHttpApi } from './httpApi.js';
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
  const auth = createAuth(config.teamApiKey);
  const tokenManager = createTokenManager(config.masterKey, logger);
  const oauth = new OAuthManager(logger);

  // If a direct Claude token is provided (from setup-token), store it
  if (config.claudeToken) {
    tokenManager.storeClaudeToken(config.claudeToken);
    logger.info('Direct Claude token loaded from environment');
  } else {
    logger.info('No direct Claude token — use OAuth flow via POST /api/oauth/authorize');
  }

  // Worker pool (WebSocket server)
  const pool = new WorkerPool(config.workerToken, logger);

  // Kafka client
  const kafka = config.kafkaEnabled
    ? createKafkaClient(config.kafkaBrokers, config.kafkaUsername, config.kafkaPassword, logger)
    : createNoopKafkaClient(logger);

  // Dispatcher — uses OAuth manager for token refresh
  const dispatcher = new Dispatcher(pool, tokenManager, oauth, kafka, logger);

  // Connect Kafka
  if (config.kafkaEnabled) {
    await kafka.connect();

    // Subscribe to execution requests
    await kafka.subscribe(async (topic: string, value: string) => {
      if (topic === TOPICS.EXEC_REQUESTS) {
        try {
          const request: ExecRequest = JSON.parse(value);
          dispatcher.submit(request);
        } catch (err) {
          logger.error({ err }, 'Failed to parse execution request from Kafka');
        }
      }
    });
  }

  // Start WebSocket server for workers
  pool.listen(config.wsPort);

  // Start HTTP API (now includes OAuth endpoints)
  const httpServer = createHttpApi(auth, dispatcher, pool, tokenManager, oauth, logger);
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
    dispatcher.shutdown();
    pool.shutdown();
    await kafka.disconnect();
    httpServer.close();
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
