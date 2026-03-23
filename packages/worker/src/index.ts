import pino from 'pino';
import { loadConfig } from './config.js';
import { Connection, type WorkerTlsConfig } from './connection.js';
import { resolveClaudeCommand } from './executor.js';
import { ExecutionManager } from './execution-manager.js';
import { MetricsCollector } from './metrics.js';
import { installCredentialExitHandler, sweepOrphanedCredentialFiles } from './credentialInjector.js';
import { startHealthProbe } from './healthProbe.js';
import type { ExecAssign } from '@dac-cloud/shared';

const logger = pino({ level: process.env['LOG_LEVEL'] || 'info' });

async function main() {
  logger.info('Starting DAC Cloud Worker...');

  const config = loadConfig();
  logger.info({
    workerId: config.workerId,
    orchestratorUrl: config.orchestratorUrl,
    maxConcurrentExecutions: config.maxConcurrentExecutions,
  }, 'Configuration loaded');

  // Clean up any credential files left behind by a previous crash
  installCredentialExitHandler(logger);
  await sweepOrphanedCredentialFiles(logger);

  // Resolve the Claude CLI command once at startup to avoid a blocking
  // execSync call on every execution.
  const claudeCommand = resolveClaudeCommand();
  logger.info({ command: claudeCommand.command, shell: claudeCommand.shell }, 'Resolved Claude CLI at startup');

  // Unified execution lifecycle manager
  const executionManager = new ExecutionManager(logger, config.maxConcurrentExecutions, claudeCommand);

  // System health metrics collector
  const metrics = new MetricsCollector();

  const connection = new Connection(
    config.orchestratorUrl,
    config.workerId,
    config.workerToken,
    {
      onAssign(msg: ExecAssign) {
        if (executionManager.availableSlots() <= 0) {
          logger.warn({ executionId: msg.executionId }, 'Received assignment but no slots available — rejecting');
          connection.sendComplete(msg.executionId, 'failed', 1, 0);
          return;
        }

        // Create a dedicated Executor instance for this execution
        const executor = executionManager.createExecutor(msg.executionId);

        // Guard against sending duplicate complete messages to the orchestrator.
        // If onComplete succeeds, the catch handler must not send a second complete.
        let completed = false;

        executor.execute(msg, {
          onMessage(emsg) {
            switch (emsg.type) {
              case 'stdout':
                connection.sendStdout(msg.executionId, emsg.chunk);
                break;
              case 'stderr':
                logger.warn({ executionId: msg.executionId, stderr: emsg.chunk }, 'CLI stderr');
                connection.sendStderr(msg.executionId, emsg.chunk);
                break;
              case 'event':
                connection.sendEvent(msg.executionId, emsg.eventType, emsg.payload);
                break;
              case 'progress':
                connection.sendProgress(msg.executionId, emsg.progress);
                break;
              case 'flush_output':
                connection.flushOutput(msg.executionId);
                break;
              case 'review_request':
                connection.sendReviewRequest(msg.executionId, emsg.reviewId, emsg.payload);
                break;
              case 'complete':
                completed = true;
                connection.sendComplete(msg.executionId, emsg.status, emsg.exitCode, emsg.durationMs, emsg.sessionId, emsg.totalCostUsd);
                metrics.recordExecution(emsg.durationMs);
                executionManager.removeExecution(msg.executionId);
                connection.sendReady(executionManager.availableSlots(), config.maxConcurrentExecutions);
                break;
            }
          },
        }).catch((err) => {
          logger.error({ err, executionId: msg.executionId }, 'Execution error');
          if (!completed) {
            connection.sendComplete(msg.executionId, 'failed', 1, 0);
            executionManager.removeExecution(msg.executionId);
            connection.sendReady(executionManager.availableSlots(), config.maxConcurrentExecutions);
          }
        });
      },

      onCancel(msg) {
        const executor = executionManager.getExecutor(msg.executionId);
        if (executor) {
          logger.info({ executionId: msg.executionId }, 'Cancelling execution');
          executor.kill();
        }
      },

      onReviewResponse(msg) {
        const executor = executionManager.getExecutor(msg.executionId);
        if (executor) {
          const resolved = executor.resolveReview(msg.reviewId, msg.message);
          if (!resolved) {
            logger.warn({ executionId: msg.executionId, reviewId: msg.reviewId }, 'Review response for unknown review ID');
          }
        } else {
          logger.warn({ executionId: msg.executionId }, 'Review response for unknown execution');
        }
      },

      onShutdown(msg) {
        logger.info({ reason: msg.reason, gracePeriodMs: msg.gracePeriodMs }, 'Shutdown requested');
        executionManager.drain(msg.gracePeriodMs).then(shutdown);
      },

      getActiveExecutionIds() {
        return executionManager.getActiveExecutionIds();
      },
    },
    logger,
    // Pass TLS config if CA or client certs are configured
    (config.tlsCaPath || config.tlsCertPath) ? {
      caPath: config.tlsCaPath,
      certPath: config.tlsCertPath,
      keyPath: config.tlsKeyPath,
      rejectUnauthorized: config.tlsRejectUnauthorized,
    } as WorkerTlsConfig : undefined,
    { imageDigest: config.imageDigest, claudeCliVersion: config.claudeCliVersion },
    config.maxConcurrentExecutions,
  );

  // Wire health metrics into heartbeats
  connection.setHealthMetricsProvider(() => metrics.collect());

  // Connect to orchestrator
  connection.connect();

  // Start health probe if configured
  let healthServer: import('node:http').Server | undefined;
  if (config.healthPort > 0) {
    healthServer = startHealthProbe(
      config.healthPort,
      connection,
      executionManager,
      config.workerId,
      logger,
    );
  }

  // Graceful shutdown — waits for sandbox/credential cleanup before exiting
  let shuttingDown = false;
  async function shutdown() {
    if (shuttingDown) return;   // prevent double-shutdown
    shuttingDown = true;
    logger.info('Shutting down worker...');
    healthServer?.close();
    connection.disconnect();
    process.exit(0);
  }

  // SIGINT: immediate kill (interactive terminal interrupt)
  process.on('SIGINT', () => {
    logger.info({ activeCount: executionManager.activeCount() }, 'SIGINT received');
    executionManager.killAll();
    shutdown();
  });

  // SIGTERM: graceful drain with configurable grace period
  process.on('SIGTERM', () => {
    logger.info(
      { activeCount: executionManager.activeCount(), gracePeriodMs: config.shutdownGracePeriodMs },
      'SIGTERM received',
    );
    executionManager.drain(config.shutdownGracePeriodMs).then(shutdown);
  });

  logger.info('DAC Cloud Worker started');
}

main().catch((err) => {
  logger.fatal({ err }, 'Failed to start worker');
  process.exit(1);
});
