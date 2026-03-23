import fs from 'node:fs';
import fsp from 'node:fs/promises';
import path from 'node:path';
import os from 'node:os';
import crypto from 'node:crypto';
import type { Logger } from 'pino';

export interface InjectedCredentials {
  /** Path to the tmpfs-backed credential file */
  credentialFilePath: string;
  /** Environment variable key pointing to the file */
  envPointerKey: string;
  /** Call to delete the credential file */
  cleanup(): Promise<void>;
}

/** File name prefix used for all credential temp files. */
const CRED_FILE_PREFIX = 'dac-creds-';

/** Max age (ms) before an orphaned credential file is swept on startup. */
const ORPHAN_MAX_AGE_MS = 2 * 60 * 1000; // 2 minutes

/**
 * Registry of credential file paths created by this process.
 * Used by the exit handler to synchronously remove files on crash.
 */
const activeCredentialFiles = new Set<string>();

/** Whether the process exit handler has been installed. */
let exitHandlerInstalled = false;

/**
 * Returns the number of credential files currently tracked by this process.
 * Useful as a gauge metric to detect credential file leaks (monotonically
 * increasing count indicates cleanup failures).
 */
export function getActiveCredentialFileCount(): number {
  return activeCredentialFiles.size;
}

/**
 * Returns the directories where credential files may be written.
 */
function credentialDirs(): string[] {
  const dirs: string[] = [os.tmpdir()];
  if (process.platform === 'linux' && fs.existsSync('/dev/shm')) {
    dirs.push('/dev/shm');
  }
  return dirs;
}

/**
 * Install a synchronous process exit handler that removes any credential
 * files still tracked in the registry. This covers cases where the process
 * exits abnormally (uncaughtException, unhandledRejection) but the event
 * loop is still alive long enough to fire the 'exit' event.
 *
 * Call once during worker startup.
 */
export function installCredentialExitHandler(logger: Logger): void {
  if (exitHandlerInstalled) return;
  exitHandlerInstalled = true;

  process.on('exit', () => {
    const totalFiles = activeCredentialFiles.size;
    const results: { path: string; success: boolean }[] = [];

    for (const filePath of activeCredentialFiles) {
      try {
        fs.unlinkSync(filePath);
        results.push({ path: filePath, success: true });
      } catch {
        results.push({ path: filePath, success: false });
      }
    }

    const succeeded = results.filter((r) => r.success).length;
    const failed = results.filter((r) => !r.success).length;

    // Logger may not flush during 'exit', but try anyway
    if (totalFiles > 0) {
      logger.info(
        { totalFiles, succeeded, failed, files: results },
        'Exit handler credential file cleanup',
      );
    }
  });
}

/**
 * Scan credential temp directories for orphaned `dac-creds-*` files older
 * than ORPHAN_MAX_AGE_MS and remove them. Intended to be called once during
 * worker startup before any executions begin.
 */
export async function sweepOrphanedCredentialFiles(logger: Logger): Promise<number> {
  const now = Date.now();
  let swept = 0;

  for (const dir of credentialDirs()) {
    let entries: string[];
    try {
      entries = await fsp.readdir(dir);
    } catch (err) {
      logger.warn({ err, dir, operation: 'sweep_readdir' }, 'Failed to read credential directory');
      continue; // directory inaccessible
    }

    for (const entry of entries) {
      if (!entry.startsWith(CRED_FILE_PREFIX)) continue;

      const fullPath = path.join(dir, entry);
      try {
        const stat = await fsp.stat(fullPath);
        if (now - stat.mtimeMs > ORPHAN_MAX_AGE_MS) {
          await fsp.unlink(fullPath);
          swept++;
          logger.warn({ credentialFile: fullPath, ageMs: now - stat.mtimeMs }, 'Swept orphaned credential file');
        }
      } catch {
        // File disappeared between readdir and stat/unlink — fine
      }
    }
  }

  logger.info(
    { swept, activeCredentialFiles: activeCredentialFiles.size },
    'Credential file sweep complete',
  );

  return swept;
}

/**
 * Write credentials to a temporary file (preferring tmpfs/ramfs) instead of
 * injecting them as environment variables. This prevents credential exposure
 * via /proc/pid/environ or `ps auxe`.
 *
 * Returns the file path and a cleanup function. The child process receives
 * the DAC_CREDENTIALS_FILE env var pointing to the file.
 */
export async function injectCredentialsViaFile(
  env: Record<string, string>,
  executionId: string,
  logger: Logger,
): Promise<InjectedCredentials> {
  // Prefer /dev/shm (tmpfs, never hits disk) on Linux
  let tmpBase: string;
  if (process.platform === 'linux' && fs.existsSync('/dev/shm')) {
    tmpBase = '/dev/shm';
  } else {
    tmpBase = os.tmpdir();
  }

  const randomSuffix = crypto.randomBytes(8).toString('hex');
  const credFilePath = path.join(tmpBase, `${CRED_FILE_PREFIX}${executionId}-${randomSuffix}.json`);

  // Collect credential env vars into the file
  const credentials: Record<string, string> = {};
  const credentialKeys: string[] = [];

  for (const [key, value] of Object.entries(env)) {
    // Credential env vars follow the CONNECTOR_* pattern or are well-known secret keys
    if (
      key.startsWith('CONNECTOR_') ||
      key === 'ANTHROPIC_API_KEY' ||
      key.includes('TOKEN') ||
      key.includes('SECRET') ||
      key.includes('PASSWORD') ||
      key.includes('API_KEY')
    ) {
      credentials[key] = value;
      credentialKeys.push(key);
    }
  }

  // Write credential file asynchronously
  await fsp.writeFile(credFilePath, JSON.stringify(credentials), { mode: 0o600 });

  // Track the file so exit handler can clean it up on crash
  activeCredentialFiles.add(credFilePath);

  // Remove credential keys from the env dict (they'll be read from file instead)
  for (const key of credentialKeys) {
    delete env[key];
  }

  logger.debug({
    executionId,
    credentialFile: credFilePath,
    credentialCount: credentialKeys.length,
    onTmpfs: tmpBase === '/dev/shm',
  }, 'Credentials written to temporary file');

  return {
    credentialFilePath: credFilePath,
    envPointerKey: 'DAC_CREDENTIALS_FILE',
    async cleanup() {
      activeCredentialFiles.delete(credFilePath);
      try {
        await fsp.unlink(credFilePath);
        logger.debug({ credentialFile: credFilePath }, 'Credential file cleaned up');
      } catch (err) {
        // File may already be gone (e.g. sandbox cleanup removed it first)
        if ((err as NodeJS.ErrnoException).code !== 'ENOENT') {
          logger.warn({ err, credentialFile: credFilePath }, 'Failed to cleanup credential file');
        }
      }
    },
  };
}
