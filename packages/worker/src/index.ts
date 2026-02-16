import pino from 'pino';
import { loadConfig } from './config.js';
import { Connection } from './connection.js';
import { Executor } from './executor.js';
import type { ExecAssign } from '@dac-cloud/shared';

const logger = pino({ level: process.env['LOG_LEVEL'] || 'info' });

async function main() {
  logger.info('Starting DAC Cloud Worker...');

  const config = loadConfig();
  logger.info({ workerId: config.workerId, orchestratorUrl: config.orchestratorUrl }, 'Configuration loaded');

  const executor = new Executor(logger);
  let currentExecutionId: string | null = null;

  const connection = new Connection(
    config.orchestratorUrl,
    config.workerId,
    config.workerToken,
    {
      onAssign(msg: ExecAssign) {
        currentExecutionId = msg.executionId;

        executor.execute(msg, {
          onStdout(chunk: string, timestamp: number) {
            connection.sendStdout(msg.executionId, chunk);
          },

          onStderr(chunk: string, timestamp: number) {
            logger.warn({ executionId: msg.executionId, stderr: chunk }, 'CLI stderr');
            connection.sendStderr(msg.executionId, chunk);
          },

          onEvent(eventType: string, payload: unknown) {
            connection.sendEvent(msg.executionId, eventType, payload);
          },

          onComplete(status, exitCode, durationMs, sessionId, totalCostUsd) {
            connection.sendComplete(msg.executionId, status, exitCode, durationMs, sessionId, totalCostUsd);
            currentExecutionId = null;

            // Signal ready for next assignment
            connection.sendReady();
          },
        }).catch((err) => {
          logger.error({ err, executionId: msg.executionId }, 'Execution error');
          connection.sendComplete(msg.executionId, 'failed', 1, 0);
          currentExecutionId = null;
          connection.sendReady();
        });
      },

      onCancel(msg) {
        if (currentExecutionId === msg.executionId) {
          logger.info({ executionId: msg.executionId }, 'Cancelling execution');
          executor.kill();
        }
      },

      onShutdown(msg) {
        logger.info({ reason: msg.reason, gracePeriodMs: msg.gracePeriodMs }, 'Shutdown requested');

        if (currentExecutionId) {
          // Wait for current execution to finish, then exit
          logger.info('Waiting for current execution to complete before shutdown');
          const checkInterval = setInterval(() => {
            if (!currentExecutionId) {
              clearInterval(checkInterval);
              shutdown();
            }
          }, 1000);

          // Force shutdown after grace period
          setTimeout(() => {
            clearInterval(checkInterval);
            executor.kill();
            shutdown();
          }, msg.gracePeriodMs);
        } else {
          shutdown();
        }
      },
    },
    logger,
  );

  // Connect to orchestrator
  connection.connect();

  // Graceful shutdown
  function shutdown() {
    logger.info('Shutting down worker...');
    connection.disconnect();
    process.exit(0);
  }

  process.on('SIGINT', () => {
    if (currentExecutionId) {
      logger.info('SIGINT received, killing current execution');
      executor.kill();
    }
    shutdown();
  });

  process.on('SIGTERM', () => {
    if (currentExecutionId) {
      logger.info('SIGTERM received, killing current execution');
      executor.kill();
    }
    shutdown();
  });

  logger.info('DAC Cloud Worker started');
}

main().catch((err) => {
  logger.fatal({ err }, 'Failed to start worker');
  process.exit(1);
});
