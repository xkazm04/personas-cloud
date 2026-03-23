import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import { execSync } from 'node:child_process';
import type { Logger } from 'pino';

import { buildCliArgs, type NetworkPolicy } from '@dac-cloud/shared';

export interface SandboxConfig {
  executionId: string;
  env: Record<string, string>;
  accessToken?: string;
  networkAccess: boolean;
  networkPolicy?: NetworkPolicy;
  timeoutMs: number;
  maxBudgetUsd?: number;
  maxTurns?: number;
}

export interface SandboxResult {
  command: string;
  args: string[];
  env: Record<string, string>;
  cwd: string;
  sandboxDir: string;
  proxyRequired?: boolean;
}

let _bwrapAvailable: boolean | null = null;

/**
 * Cached bwrap args for static system paths that never change between executions.
 * Computed once at module load time to avoid 6 synchronous fs.existsSync calls per execution.
 */
const _cachedStaticBwrapArgs: string[] = (() => {
  if (process.platform !== 'linux') return [];

  const args: string[] = [];
  const conditionalPaths: Array<{ path: string; args: string[] }> = [
    { path: '/lib', args: ['--ro-bind', '/lib', '/lib'] },
    { path: '/lib64', args: ['--ro-bind', '/lib64', '/lib64'] },
    { path: '/etc/ssl', args: ['--ro-bind', '/etc/ssl', '/etc/ssl'] },
    { path: '/etc/ca-certificates', args: ['--ro-bind', '/etc/ca-certificates', '/etc/ca-certificates'] },
    { path: '/usr/local/lib/node_modules', args: ['--ro-bind', '/usr/local/lib/node_modules', '/usr/local/lib/node_modules'] },
    { path: '/usr/local/bin/claude', args: ['--ro-bind', '/usr/local/bin/claude', '/usr/local/bin/claude'] },
  ];

  for (const entry of conditionalPaths) {
    if (fs.existsSync(entry.path)) {
      args.push(...entry.args);
    }
  }

  return args;
})();

/**
 * Check if bubblewrap (bwrap) is available on this system.
 * Cached after first call.
 */
export function isSandboxAvailable(): boolean {
  if (_bwrapAvailable !== null) return _bwrapAvailable;

  if (process.platform !== 'linux') {
    _bwrapAvailable = false;
    return false;
  }

  try {
    execSync('which bwrap', { stdio: 'ignore' });
    _bwrapAvailable = true;
  } catch {
    _bwrapAvailable = false;
  }
  return _bwrapAvailable;
}

/**
 * Resolve the claude CLI command path.
 */
function resolveClaudeCommand(): string {
  if (process.platform === 'win32') {
    try {
      return execSync('where claude', { encoding: 'utf8' }).trim().split('\n')[0]!;
    } catch { /* fall through */ }
  }
  return 'claude';
}

/**
 * Build a clean environment dict for the child process.
 * Never inherits from process.env — only explicit values.
 */
function buildCleanEnv(assignmentEnv: Record<string, string>): Record<string, string> {
  const clean: Record<string, string> = {
    PATH: '/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin',
    NODE_ENV: 'production',
    LANG: 'en_US.UTF-8',
  };

  // Copy assignment env vars (credentials, config)
  for (const [key, value] of Object.entries(assignmentEnv)) {
    if (key === 'ANTHROPIC_API_KEY') continue; // handled via credentials file
    clean[key] = value;
  }

  // Explicitly unset CLAUDECODE to prevent nested session guard
  delete clean['CLAUDECODE'];

  return clean;
}

/**
 * Create the per-execution sandbox directory structure and write
 * Claude config + credentials files.
 */
function createSandboxDir(
  executionId: string,
  accessToken: string | undefined,
  logger: Logger,
): string {
  const sandboxDir = path.join(os.tmpdir(), `dac-sandbox-${executionId}`);
  const homeDir = path.join(sandboxDir, 'home');
  const claudeDir = path.join(homeDir, '.claude');
  const workDir = path.join(sandboxDir, 'work');
  const tmpDir = path.join(sandboxDir, 'tmp');

  fs.mkdirSync(claudeDir, { recursive: true });
  fs.mkdirSync(workDir, { recursive: true });
  fs.mkdirSync(tmpDir, { recursive: true });

  // Claude onboarding config
  fs.writeFileSync(
    path.join(homeDir, '.claude.json'),
    JSON.stringify({ hasCompletedOnboarding: true }),
  );

  // OAuth credentials (per-execution, no race condition)
  if (accessToken) {
    const credPayload = {
      claudeAiOauth: {
        token_type: 'Bearer',
        access_token: accessToken,
        expires_at: Math.floor(Date.now() / 1000) + 3600,
      },
    };
    fs.writeFileSync(
      path.join(claudeDir, '.credentials.json'),
      JSON.stringify(credPayload, null, 2),
    );
    // Restrict permissions on Linux
    if (process.platform === 'linux') {
      try { fs.chmodSync(path.join(claudeDir, '.credentials.json'), 0o600); } catch (err) { logger.warn({ err, executionId, operation: 'chmod_credentials' }, 'Failed to restrict credentials file permissions'); }
    }
  }

  logger.debug({ sandboxDir, executionId }, 'Sandbox directory created');
  return sandboxDir;
}

/**
 * Prepare a sandboxed execution environment.
 * When bwrap is available: full namespace isolation.
 * Fallback: per-execution HOME without namespace isolation.
 */
export function prepareSandbox(config: SandboxConfig, logger: Logger): SandboxResult {
  const oauthToken = config.env['ANTHROPIC_API_KEY'];
  const accessToken = config.accessToken ??
    (oauthToken?.startsWith('sk-ant-oat') ? oauthToken : undefined);

  const sandboxDir = createSandboxDir(config.executionId, accessToken, logger);
  const homeDir = path.join(sandboxDir, 'home');
  const workDir = path.join(sandboxDir, 'work');

  const cleanEnv = buildCleanEnv(config.env);

  // Non-OAuth API keys (e.g. sk-ant-api*) must be passed via environment.
  // buildCleanEnv strips ANTHROPIC_API_KEY assuming it's handled via credentials
  // file, but only OAuth tokens (sk-ant-oat*) are written there. Re-inject
  // non-OAuth keys so the Claude CLI can authenticate.
  const apiKey = config.env['ANTHROPIC_API_KEY'];
  if (apiKey && !accessToken) {
    cleanEnv['ANTHROPIC_API_KEY'] = apiKey;
  }

  if (isSandboxAvailable()) {
    // Full bwrap sandbox
    const bwrapArgs: string[] = [
      '--unshare-pid',
      '--unshare-uts',
      '--die-with-parent',
      // Read-only system mounts
      '--ro-bind', '/usr', '/usr',
      '--ro-bind', '/bin', '/bin',
      '--ro-bind', '/sbin', '/sbin',
      '--ro-bind', '/etc/resolv.conf', '/etc/resolv.conf',
      // Conditional system paths (cached at module init)
      ..._cachedStaticBwrapArgs,
      // Proc, dev
      '--proc', '/proc',
      '--dev', '/dev',
      '--tmpfs', '/run',
      // Per-execution writable mounts
      '--bind', homeDir, '/home/exec',
      '--bind', workDir, '/workspace',
      '--bind', path.join(sandboxDir, 'tmp'), '/tmp',
      // Environment
      '--setenv', 'HOME', '/home/exec',
      '--setenv', 'TMPDIR', '/tmp',
      '--chdir', '/workspace',
      '--new-session',
    ];

    // Network isolation
    if (!config.networkAccess && config.networkPolicy?.mode !== 'allow-list') {
      bwrapArgs.push('--unshare-net');
    }

    // Inject credential env vars into bwrap
    for (const [key, value] of Object.entries(cleanEnv)) {
      if (key !== 'PATH' && key !== 'NODE_ENV' && key !== 'LANG') {
        bwrapArgs.push('--setenv', key, value);
      }
    }
    bwrapArgs.push('--setenv', 'PATH', cleanEnv['PATH']!);
    bwrapArgs.push('--setenv', 'NODE_ENV', 'production');

    // Separator + claude command
    const cli = buildCliArgs(
      config.maxBudgetUsd || config.maxTurns
        ? { maxBudgetUsd: config.maxBudgetUsd ?? null, maxTurns: config.maxTurns ?? null } as Parameters<typeof buildCliArgs>[0]
        : undefined,
    );
    bwrapArgs.push('--');
    bwrapArgs.push(cli.command, ...cli.args);

    logger.info({ executionId: config.executionId, sandbox: 'bwrap' }, 'Using bwrap sandbox');

    return {
      command: 'bwrap',
      args: bwrapArgs,
      env: cleanEnv,
      cwd: workDir,
      sandboxDir,
      proxyRequired: config.networkPolicy?.mode === 'allow-list',
    };
  }

  // Fallback: no namespace isolation — refuse if a network policy is present.
  // Without bwrap network namespaces, HTTP_PROXY env vars can be bypassed by
  // direct TCP connections, so the proxy cannot enforce allow-list policies.
  if (config.networkPolicy) {
    throw new Error(
      `Cannot enforce network policy (mode="${config.networkPolicy.mode}") in fallback mode: ` +
      `bwrap is unavailable and namespace isolation is required to restrict network access. ` +
      `Install bubblewrap (bwrap) on this host to execute personas with network policies.`,
    );
  }

  cleanEnv['HOME'] = homeDir;
  cleanEnv['TMPDIR'] = path.join(sandboxDir, 'tmp');

  const cli = buildCliArgs(
    config.maxBudgetUsd || config.maxTurns
      ? { maxBudgetUsd: config.maxBudgetUsd ?? null, maxTurns: config.maxTurns ?? null } as Parameters<typeof buildCliArgs>[0]
      : undefined,
  );
  const claudeCmd = resolveClaudeCommand();
  const claudeArgs = cli.args;

  logger.info({ executionId: config.executionId, sandbox: 'fallback' }, 'Using fallback (per-execution HOME, no namespace isolation)');

  return {
    command: claudeCmd,
    args: claudeArgs,
    env: cleanEnv,
    cwd: workDir,
    sandboxDir,
  };
}

/**
 * Clean up sandbox directory after execution completes.
 */
export function cleanupSandbox(sandboxDir: string, logger: Logger): void {
  try {
    fs.rmSync(sandboxDir, { recursive: true, force: true });
    logger.debug({ sandboxDir }, 'Sandbox cleaned up');
  } catch (err) {
    logger.warn({ err, sandboxDir }, 'Failed to cleanup sandbox directory');
  }
}
