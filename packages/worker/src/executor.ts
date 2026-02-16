import { spawn, execSync, type ChildProcess } from 'node:child_process';
import type { ExecAssign } from '@dac-cloud/shared';
import { LineBuffer, parseStreamLine, detectPersonaEvents } from './parser.js';
import { createTempDir, cleanupTempDir, clearEnvVars, ensureClaudeConfig, writeClaudeCredentials } from './cleanup.js';
import type { Logger } from 'pino';

/**
 * Resolve the Claude CLI command.
 * On Windows, the CLI may be installed as claude.exe (WinGet), claude.cmd (npm), or claude (PATH).
 * We try `where`/`which` to find the actual binary path for reliable spawning.
 */
function resolveClaudeCommand(): { command: string; shell: boolean } {
  const isWindows = process.platform === 'win32';

  if (isWindows) {
    // Try to find the actual binary path
    try {
      const resolved = execSync('where claude', { encoding: 'utf-8' }).trim().split('\n')[0]?.trim();
      if (resolved) {
        return { command: resolved, shell: false };
      }
    } catch { /* fall through */ }

    // Fallback to shell-based resolution
    return { command: 'claude', shell: true };
  }

  return { command: 'claude', shell: false };
}

export interface ExecutorCallbacks {
  onStdout(chunk: string, timestamp: number): void;
  onStderr(chunk: string, timestamp: number): void;
  onEvent(eventType: string, payload: unknown): void;
  onComplete(status: 'completed' | 'failed' | 'cancelled', exitCode: number, durationMs: number, sessionId?: string, totalCostUsd?: number): void;
}

export class Executor {
  private childProcess: ChildProcess | null = null;
  private killed = false;

  constructor(private logger: Logger) {}

  async execute(assignment: ExecAssign, callbacks: ExecutorCallbacks): Promise<void> {
    const startTime = Date.now();
    const execDir = createTempDir(assignment.executionId);
    let sessionId: string | undefined;
    let totalCostUsd: number | undefined;

    // Ensure Claude config exists
    ensureClaudeConfig(this.logger);

    // Write OAuth token to Claude CLI credentials file
    const oauthToken = assignment.env['ANTHROPIC_API_KEY'];
    if (oauthToken && oauthToken.startsWith('sk-ant-oat')) {
      writeClaudeCredentials(oauthToken, this.logger);
    }

    // Inject environment variables (except OAuth token which goes to credentials file)
    const injectedKeys: string[] = [];
    for (const [key, value] of Object.entries(assignment.env)) {
      if (key === 'ANTHROPIC_API_KEY' && value.startsWith('sk-ant-oat')) {
        continue; // Written to credentials file instead
      }
      process.env[key] = value;
      injectedKeys.push(key);
    }

    this.logger.info({
      executionId: assignment.executionId,
      personaId: assignment.personaId,
      execDir,
      timeoutMs: assignment.config.timeoutMs,
    }, 'Starting execution');

    try {
      const { command, shell } = resolveClaudeCommand();
      const args = [
        '-p', '-',
        '--output-format', 'stream-json',
        '--verbose',
        '--dangerously-skip-permissions',
      ];

      this.logger.info({ command, shell }, 'Resolved Claude CLI');

      // Build child process environment — inherit current env (which now includes injected vars)
      const childEnv = { ...process.env };
      // Unset CLAUDECODE to avoid "nested session" guard when running inside another Claude Code session
      delete childEnv['CLAUDECODE'];
      // Remove ANTHROPIC_API_KEY so CLI uses credentials file (OAuth) instead of API key auth
      delete childEnv['ANTHROPIC_API_KEY'];

      this.childProcess = spawn(command, args, {
        cwd: execDir,
        stdio: ['pipe', 'pipe', 'pipe'],
        shell,
        env: childEnv,
      });

      // Pipe prompt to stdin
      this.childProcess.stdin!.write(assignment.prompt);
      this.childProcess.stdin!.end();

      // Set up timeout
      const timeoutHandle = setTimeout(() => {
        this.logger.warn({
          executionId: assignment.executionId,
          timeoutMs: assignment.config.timeoutMs,
        }, 'Execution timeout, killing process');
        this.kill();
      }, assignment.config.timeoutMs);

      // Parse stdout
      const stdoutBuffer = new LineBuffer();
      this.childProcess.stdout!.on('data', (data: Buffer) => {
        const lines = stdoutBuffer.push(data.toString());
        for (const line of lines) {
          callbacks.onStdout(line, Date.now());

          // Parse structured data
          const parsed = parseStreamLine(line);
          if (parsed.sessionId) sessionId = parsed.sessionId;
          if (parsed.totalCostUsd) totalCostUsd = parsed.totalCostUsd;

          // Detect persona protocol events
          const events = detectPersonaEvents(line, this.logger);
          for (const event of events) {
            callbacks.onEvent(event.eventType, event.payload);
          }
        }
      });

      // Parse stderr
      const stderrBuffer = new LineBuffer();
      this.childProcess.stderr!.on('data', (data: Buffer) => {
        const lines = stderrBuffer.push(data.toString());
        for (const line of lines) {
          callbacks.onStderr(line, Date.now());
        }
      });

      // Wait for process to exit
      const exitCode = await new Promise<number>((resolve) => {
        this.childProcess!.on('close', (code: number | null) => {
          clearTimeout(timeoutHandle);
          resolve(code ?? 1);
        });

        this.childProcess!.on('error', (err: Error) => {
          clearTimeout(timeoutHandle);
          this.logger.error({ err, executionId: assignment.executionId }, 'Process spawn error');
          resolve(1);
        });
      });

      // Flush remaining buffered data
      for (const line of stdoutBuffer.flush()) {
        callbacks.onStdout(line, Date.now());
      }
      for (const line of stderrBuffer.flush()) {
        callbacks.onStderr(line, Date.now());
      }

      const durationMs = Date.now() - startTime;
      const status = this.killed ? 'cancelled' : (exitCode === 0 ? 'completed' : 'failed');

      this.logger.info({
        executionId: assignment.executionId,
        exitCode,
        durationMs,
        status,
        sessionId,
        totalCostUsd,
      }, 'Execution finished');

      callbacks.onComplete(status, exitCode, durationMs, sessionId, totalCostUsd);

    } finally {
      // Cleanup: remove env vars and temp directory
      clearEnvVars(injectedKeys, this.logger);
      cleanupTempDir(execDir, this.logger);
      this.childProcess = null;
      this.killed = false;
    }
  }

  kill(): void {
    if (this.childProcess && !this.killed) {
      this.killed = true;
      this.childProcess.kill('SIGTERM');

      // Force kill after 5 seconds
      setTimeout(() => {
        if (this.childProcess) {
          this.childProcess.kill('SIGKILL');
        }
      }, 5_000);
    }
  }
}
