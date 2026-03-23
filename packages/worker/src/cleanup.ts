import fs from 'node:fs';
import path from 'node:path';
import type { Logger } from 'pino';
import { isValidExecutionId, ensureContainedPath } from './validation.js';

// Remove temporary execution directory
export function cleanupTempDir(dirPath: string, logger: Logger): void {
  try {
    if (fs.existsSync(dirPath)) {
      fs.rmSync(dirPath, { recursive: true, force: true });
      logger.debug({ dirPath }, 'Cleaned up temp directory');
    }
  } catch (err) {
    logger.warn({ err, dirPath }, 'Failed to clean up temp directory');
  }
}

// Create a temporary execution directory
export function createTempDir(executionId: string): string {
  if (!isValidExecutionId(executionId)) {
    throw new Error(`Invalid execution ID format: "${executionId}"`);
  }

  const tmpBase = process.env['TMPDIR'] || process.env['TMP'] || '/tmp';
  const dirName = `dac-exec-${executionId}`;
  const dirPath = ensureContainedPath(tmpBase, path.join(tmpBase, dirName));
  fs.mkdirSync(dirPath, { recursive: true });
  return dirPath;
}

/**
 * Create a per-execution isolated home directory inside the exec temp dir.
 *
 * This prevents credential file races when multiple workers (or concurrent
 * executions) run on the same host. The Claude CLI reads/writes config and
 * credentials under $HOME/.claude/ — by giving each execution its own HOME,
 * they can never stomp on each other.
 *
 * Copies the global hasCompletedOnboarding config so the CLI skips onboarding.
 */
export function createIsolatedHome(execDir: string, logger: Logger): string {
  const isolatedHome = path.join(execDir, '.home');
  fs.mkdirSync(isolatedHome, { recursive: true });

  // Write .claude.json with hasCompletedOnboarding so CLI skips onboarding
  const configPath = path.join(isolatedHome, '.claude.json');
  fs.writeFileSync(configPath, JSON.stringify({ hasCompletedOnboarding: true }));

  // Create .claude directory for credential/config isolation
  const claudeDir = path.join(isolatedHome, '.claude');
  fs.mkdirSync(claudeDir, { recursive: true });

  logger.debug({ isolatedHome, executionDir: execDir }, 'Created isolated home for execution');
  return isolatedHome;
}
