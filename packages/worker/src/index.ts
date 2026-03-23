import pino from 'pino';
import { loadConfig } from './config.js';
import { Connection, type WorkerTlsConfig } from './connection.js';
import { Executor } from './executor.js';
import { installCredentialExitHandler, sweepOrphanedCredentialFiles } from './credentialInjector.js';
import { startHealthProbe } from './healthProbe.js';
import type { ExecAssign } from '@dac-cloud/shared';

const logger = pino({ level: process.env['LOG_LEVEL'] || 'info' });

async function main() {
  logger.info('Starting DAC Cloud Worker...');

  const config = loadConfig();
  logger.info({ workerId: config.workerId, orchestratorUrl: config.orchestratorUrl }, 'Configuration loaded');

  // Clean up any credential files left behind by a previous crash
  installCredentialExitHandler(logger);
  await sweepOrphanedCredentialFiles(logger);

  const executor = new Executor(logger);
  let currentExecutionId: string | null = null;

  const connection = new Connection(
    config.orchestratorUrl,
    config.workerId,
    config.workerToken,
    {
      onAssign(msg: ExecAssign) {
        if (currentExecutionId || executor.isBusy()) {
          logger.warn(
            { executionId: msg.executionId, currentExecutionId },
            'Received assignment while busy, sending busy nack',
          );
          connection.sendBusy(msg.executionId, 'Worker is already executing');
          return;
        }

        currentExecutionId = msg.executionId;

        executor.execute(msg, {
          onStdout(chunk: string, timestamp: number) {
            connection.sendStdout(msg.executionId, chunk);
          },

          onStderr(chunk: string, timestamp: number) {
            logger.warn({ executionId: msg.executionId, stderr: chunk }, 'CLI stderr');
            connection.sendStderr(msg.executionId, chunk);
          },

          onEvent(eventType, payload) {
            connection.sendEvent(msg.executionId, eventType, payload);
          },

          onProgress(phase: string, percent: number, detail?: string) {
            connection.sendProgress(msg.executionId, phase, percent, detail);
          },

          onComplete(status, exitCode, durationMs, sessionId, totalCostUsd, phaseTimings) {
            connection.sendComplete(msg.executionId, status, exitCode, durationMs, sessionId, totalCostUsd, phaseTimings);
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
    // Pass TLS config if CA or client certs are configured
    (config.tlsCaPath || config.tlsCertPath) ? {
      caPath: config.tlsCaPath,
      certPath: config.tlsCertPath,
      keyPath: config.tlsKeyPath,
      rejectUnauthorized: config.tlsRejectUnauthorized,
    } as WorkerTlsConfig : undefined,
    { imageDigest: config.imageDigest, claudeCliVersion: config.claudeCliVersion },
  );

  // Connect to orchestrator
  connection.connect();

  // Start health probe if configured
  let healthServer: import('node:http').Server | undefined;
  if (config.healthPort > 0) {
    healthServer = startHealthProbe(
      config.healthPort,
      connection,
      executor,
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

    // Drain the executor's finally-block cleanup (sandbox dirs, credential
    // files, proxy) before calling process.exit so nothing is leaked.
    await executor.waitForCleanup();
    process.exit(0);
  }

  const onTermSignal = () => {
    if (currentExecutionId) {
      logger.info('Termination signal received, killing current execution');
      executor.kill();
    }
    shutdown();
  };

  process.on('SIGINT', onTermSignal);
  process.on('SIGTERM', onTermSignal);

  logger.info('DAC Cloud Worker started');
}

main().catch((err) => {
  logger.fatal({ err }, 'Failed to start worker');
  process.exit(1);
});
