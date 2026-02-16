import fs from 'node:fs';
import path from 'node:path';
import type { Logger } from 'pino';

// Remove credential-related env vars injected for execution
export function clearEnvVars(envKeys: string[], logger: Logger): void {
  for (const key of envKeys) {
    if (process.env[key]) {
      delete process.env[key];
      logger.debug({ key }, 'Cleared env var');
    }
  }
}

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
  const tmpBase = process.env['TMPDIR'] || process.env['TMP'] || '/tmp';
  const dirPath = path.join(tmpBase, `dac-exec-${executionId}`);
  fs.mkdirSync(dirPath, { recursive: true });
  return dirPath;
}

// Ensure ~/.claude.json exists with hasCompletedOnboarding
export function ensureClaudeConfig(logger: Logger): void {
  const home = process.env['HOME'] || process.env['USERPROFILE'] || '/root';
  const configPath = path.join(home, '.claude.json');

  try {
    let config: Record<string, unknown> = {};

    if (fs.existsSync(configPath)) {
      const content = fs.readFileSync(configPath, 'utf8');
      config = JSON.parse(content);
    }

    if (!config['hasCompletedOnboarding']) {
      config['hasCompletedOnboarding'] = true;
      fs.writeFileSync(configPath, JSON.stringify(config, null, 2));
      logger.info('Set hasCompletedOnboarding in ~/.claude.json');
    }
  } catch (err) {
    logger.warn({ err }, 'Failed to configure ~/.claude.json');
  }
}

/**
 * Write an OAuth access token to ~/.claude/.credentials.json
 * so the Claude CLI uses it for authentication.
 * The CLI reads from this file on startup (claudeAiOauth key).
 */
export function writeClaudeCredentials(accessToken: string, logger: Logger): void {
  const home = process.env['HOME'] || process.env['USERPROFILE'] || '/root';
  const claudeDir = path.join(home, '.claude');
  const credentialsPath = path.join(claudeDir, '.credentials.json');

  try {
    // Ensure ~/.claude/ directory exists
    fs.mkdirSync(claudeDir, { recursive: true });

    // Read existing credentials to preserve other fields
    let creds: Record<string, unknown> = {};
    if (fs.existsSync(credentialsPath)) {
      const content = fs.readFileSync(credentialsPath, 'utf8');
      creds = JSON.parse(content);
    }

    // Write OAuth token in the format Claude CLI expects
    creds['claudeAiOauth'] = {
      accessToken,
      refreshToken: '',
      expiresAt: Date.now() + 3600_000, // 1 hour from now
      scopes: ['user:inference', 'user:profile'],
    };

    fs.writeFileSync(credentialsPath, JSON.stringify(creds));
    logger.info('Wrote OAuth credentials to ~/.claude/.credentials.json');
  } catch (err) {
    logger.warn({ err }, 'Failed to write Claude credentials');
  }
}
