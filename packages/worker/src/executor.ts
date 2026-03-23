import { spawn, execSync, type ChildProcess } from 'node:child_process';
import { nanoid } from 'nanoid';
import type { ExecAssign, PhaseTimings, ProtocolEventType } from '@dac-cloud/shared';
import { buildPermissionArgs } from '@dac-cloud/shared';
import { StreamProcessor } from './parser.js';
import type { StreamEvent } from './parser.js';
import type { ExecutionProgress } from '@dac-cloud/shared';
import { createTempDir, cleanupTempDir, createIsolatedHome } from './cleanup.js';
import { sanitizeEnvVars } from './validation.js';
import { prepareSandbox, cleanupSandbox, type SandboxConfig } from './sandbox.js';
import { startFilterProxy, type ProxyHandle } from './networkProxy.js';
import { injectCredentialsViaFile, type InjectedCredentials } from './credentialInjector.js';
import type { Logger } from 'pino';

/** Minimum interval between progress updates sent to the orchestrator (ms). */
const PROGRESS_THROTTLE_MS = 500;

export interface ResolvedClaudeCommand {
  command: string;
  shell: boolean;
}

/**
 * Resolve the Claude CLI command.
 * On Windows, the CLI may be installed as claude.exe (WinGet), claude.cmd (npm), or claude (PATH).
 * We try `where`/`which` to find the actual binary path for reliable spawning.
 *
 * The result is deterministic for the lifetime of the worker process, so it
 * should be called once at startup and the result cached.
 */
export function resolveClaudeCommand(): ResolvedClaudeCommand {
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

/**
 * Discriminated union of all messages the Executor can emit during execution.
 * Consumers receive a single `onMessage` callback and switch on `msg.type`.
 */
export type ExecutorMessage =
  | { type: 'stdout'; chunk: string; timestamp: number }
  | { type: 'stderr'; chunk: string; timestamp: number }
  | { type: 'event'; eventType: string; payload: unknown }
  | { type: 'progress'; progress: ExecutionProgress }
  | { type: 'review_request'; reviewId: string; payload: unknown }
  /** Synchronization point to flush micro-batching buffers before completion. */
  | { type: 'flush_output' }
  | { type: 'complete'; status: 'completed' | 'failed' | 'cancelled'; exitCode: number; durationMs: number; sessionId?: string; totalCostUsd?: number; phaseTimings?: PhaseTimings };

export interface ExecutorCallbacks {
  onMessage(msg: ExecutorMessage): void;
}

export interface ExecutorStatus {
  state: 'idle' | 'busy';
  executionId?: string;
  durationMs?: number;
}

export class Executor {
  private childProcess: ChildProcess | null = null;
  private killed = false;
  /** Set to true once the child process emits a 'close' event. */
  private exited = false;
  private currentExecutionId: string | null = null;
  private executionStartTime: number | null = null;
  /** Active review ID when paused for human review. */
  private pendingReviewId: string | null = null;
  /** Resolver for the pending review promise. */
  private reviewResolver: ((message: string) => void) | null = null;

  /**
   * Resolves when the current execution (including its finally-block cleanup)
   * finishes.  Null when idle.
   */
  private executionDone: Promise<void> | null = null;

  constructor(
    private logger: Logger,
    private claudeCommand: ResolvedClaudeCommand,
  ) {}

  getStatus(): ExecutorStatus {
    if (this.currentExecutionId && this.executionStartTime) {
      return {
        state: 'busy',
        executionId: this.currentExecutionId,
        durationMs: Date.now() - this.executionStartTime,
      };
    }
    return { state: 'idle' };
  }

  /**
   * Returns a promise that resolves once the in-flight execution's cleanup
   * (sandbox removal, credential deletion, proxy teardown) has finished.
   * If no execution is active, resolves immediately.
   *
   * An optional `timeoutMs` ensures the caller is never blocked forever
   * (defaults to 10 s).
   */
  waitForCleanup(timeoutMs = 10_000): Promise<void> {
    if (!this.executionDone) return Promise.resolve();

    return Promise.race([
      this.executionDone,
      new Promise<void>((resolve) => setTimeout(resolve, timeoutMs)),
    ]);
  }

  /** Returns true if an execution is currently in progress. */
  isBusy(): boolean {
    return this.childProcess !== null;
  }

  async execute(assignment: ExecAssign, callbacks: ExecutorCallbacks): Promise<void> {
    const run = this.doExecute(assignment, callbacks);
    this.executionDone = run.catch((err) => {
      this.logger.warn({ err, executionId: assignment.executionId, operation: 'execute' }, 'Execution promise swallowed');
    });   // swallow – callers handle errors via callbacks
    try {
      await run;
    } finally {
      this.executionDone = null;
    }
  }

  private async doExecute(assignment: ExecAssign, callbacks: ExecutorCallbacks): Promise<void> {
    const startTime = Date.now();
    this.currentExecutionId = assignment.executionId;
    this.executionStartTime = startTime;

    // Create a per-execution temp directory
    const execDir = createTempDir(assignment.executionId);

    // Sandbox setup (network policy, env isolation)
    const netPolicy = assignment.config.networkPolicy ?? undefined;
    const sandboxConfig: SandboxConfig = {
      executionId: assignment.executionId,
      env: { ...assignment.env },
      networkAccess: !netPolicy, // full access only if no policy set
      networkPolicy: netPolicy,
      timeoutMs: assignment.config.timeoutMs,
      maxBudgetUsd: assignment.config.maxBudgetUsd,
      maxTurns: assignment.config.maxTurns,
    };

    let t0 = Date.now();
    const sandbox = prepareSandbox(sandboxConfig, this.logger);
    const sandboxSetupMs = Date.now() - t0;

    let proxy: ProxyHandle | null = null;
    let injectedCreds: InjectedCredentials | null = null;
    let proxyStartupMs: number | null = null;
    let credentialInjectionMs = 0;
    let cliRuntimeMs = 0;
    let exitCode = 1;
    let sessionId: string | undefined;
    let totalCostUsd: number | undefined;

    const emit = (msg: ExecutorMessage) => callbacks.onMessage(msg);

    this.logger.info({
      executionId: assignment.executionId,
      personaId: assignment.personaId,
      sandboxDir: sandbox.sandboxDir,
      timeoutMs: assignment.config.timeoutMs,
    }, 'Starting execution');

    try {
      // Start filtering proxy for allow-list network policies
      if (sandbox.proxyRequired && netPolicy) {
        t0 = Date.now();
        proxy = await startFilterProxy(netPolicy, this.logger, assignment.executionId);
        proxyStartupMs = Date.now() - t0;
        const proxyUrl = `http://127.0.0.1:${proxy.port}`;
        sandbox.env['HTTP_PROXY'] = proxyUrl;
        sandbox.env['HTTPS_PROXY'] = proxyUrl;
        sandbox.env['http_proxy'] = proxyUrl;
        sandbox.env['https_proxy'] = proxyUrl;
        sandbox.env['NO_PROXY'] = '';
      }

      // Inject credentials via async file write (unblocks event loop)
      t0 = Date.now();
      injectedCreds = await injectCredentialsViaFile(sandbox.env, assignment.executionId, this.logger);
      credentialInjectionMs = Date.now() - t0;
      sandbox.env[injectedCreds.envPointerKey] = injectedCreds.credentialFilePath;

      const { command, shell } = this.claudeCommand;
      const permissionArgs = buildPermissionArgs(assignment.permissionPolicy);
      const args = [
        '-p', '-',
        '--output-format', 'stream-json',
        '--verbose',
        ...permissionArgs,
      ];

      this.logger.info({ command, shell, permissionArgs }, 'Resolved Claude CLI');

      // Build child process environment — inherit sandbox env, then inject
      // only validated credential vars (never into process.env)
      // to prevent cross-execution credential leakage and RCE via env hijack.
      const childEnv = { ...sandbox.env };

      // Sanitize user-supplied env vars: reject dangerous names like
      // NODE_OPTIONS, LD_PRELOAD, PATH, HOME, etc.
      const { safe: safeEnv, rejected } = sanitizeEnvVars(assignment.env);
      if (rejected.length > 0) {
        this.logger.warn({
          executionId: assignment.executionId,
          rejectedEnvVars: rejected,
        }, 'Blocked dangerous environment variable names from assignment');
      }
      for (const [key, value] of Object.entries(safeEnv)) {
        childEnv[key] = value;
      }

      // Unset CLAUDECODE to avoid "nested session" guard when running inside another Claude Code session
      delete childEnv['CLAUDECODE'];

      // Create a per-execution isolated home directory so the Claude CLI
      // reads/writes its own config and credential files. This prevents
      // credential file races between concurrent executions on the same host.
      const isolatedHome = createIsolatedHome(execDir, this.logger);
      childEnv['HOME'] = isolatedHome;
      childEnv['USERPROFILE'] = isolatedHome;

      const cliStartTime = Date.now();
      this.childProcess = spawn(command, args, {
        cwd: sandbox.cwd || execDir,
        stdio: ['pipe', 'pipe', 'pipe'],
        env: childEnv,
        shell,
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

      // Stream parsing pipeline — all line-buffering, JSON parsing, event
      // detection, and progress tracking are encapsulated in StreamProcessor.
      const stream = new StreamProcessor(this.logger);

      // Throttle progress updates: buffer the latest snapshot and send at most once per PROGRESS_THROTTLE_MS.
      let pendingProgress: ExecutionProgress | null = null;
      let lastProgressSentAt = 0;
      let progressTimer: ReturnType<typeof setTimeout> | null = null;

      const sendThrottledProgress = (progress: ExecutionProgress) => {
        const now = Date.now();
        const elapsed = now - lastProgressSentAt;

        if (elapsed >= PROGRESS_THROTTLE_MS) {
          lastProgressSentAt = now;
          pendingProgress = null;
          if (progressTimer) {
            clearTimeout(progressTimer);
            progressTimer = null;
          }
          emit({ type: 'progress', progress });
        } else {
          pendingProgress = progress;
          if (!progressTimer) {
            progressTimer = setTimeout(() => {
              progressTimer = null;
              if (pendingProgress) {
                lastProgressSentAt = Date.now();
                emit({ type: 'progress', progress: pendingProgress });
                pendingProgress = null;
              }
            }, PROGRESS_THROTTLE_MS - elapsed);
          }
        }
      };

      const dispatchStreamEvents = (events: StreamEvent[]) => {
        for (const evt of events) {
          switch (evt.type) {
            case 'stdout':
              emit({ type: 'stdout', chunk: evt.line, timestamp: evt.timestamp });
              break;
            case 'stderr':
              emit({ type: 'stderr', chunk: evt.line, timestamp: evt.timestamp });
              break;
            case 'session_id':
              sessionId = evt.sessionId;
              break;
            case 'cost':
              totalCostUsd = evt.totalCostUsd;
              break;
            case 'progress':
              sendThrottledProgress(evt.progress);
              break;
            case 'persona_event':
              emit({ type: 'event', eventType: evt.eventType, payload: evt.payload });
              // Pause execution for manual_review events
              if (evt.eventType === 'manual_review' && this.childProcess) {
                const reviewId = nanoid();
                this.pendingReviewId = reviewId;
                this.logger.info({ executionId: assignment.executionId, reviewId }, 'Pausing execution for manual review');
                this.childProcess.stdout!.pause();
                emit({ type: 'review_request', reviewId, payload: evt.payload });
              }
              break;
          }
        }
      };

      this.childProcess.stdout!.on('data', (data: Buffer) => {
        dispatchStreamEvents(stream.pushStdout(data.toString()));
      });

      this.childProcess.stderr!.on('data', (data: Buffer) => {
        dispatchStreamEvents(stream.pushStderr(data.toString()));
      });

      // Wait for process to exit
      exitCode = await new Promise<number>((resolve) => {
        this.childProcess!.on('close', (code: number | null) => {
          this.exited = true;
          clearTimeout(timeoutHandle);
          resolve(code ?? 1);
        });

        this.childProcess!.on('error', (err: Error) => {
          clearTimeout(timeoutHandle);
          this.logger.error({ err, executionId: assignment.executionId }, 'Process spawn error');
          resolve(1);
        });
      });

      // Flush remaining buffered data through the stream pipeline
      dispatchStreamEvents(stream.flush());

      // Flush any pending throttled progress update before completion
      if (progressTimer) {
        clearTimeout(progressTimer);
        progressTimer = null;
      }
      if (pendingProgress) {
        emit({ type: 'progress', progress: pendingProgress });
        pendingProgress = null;
      }

      cliRuntimeMs = Date.now() - cliStartTime;

      // Synchronize: flush any micro-batched output before sending completion.
      // This ensures the orchestrator receives all output before the status change,
      // preventing output arriving after the completion message due to timer interleave.
      emit({ type: 'flush_output' });

      const durationMs = Date.now() - startTime;
      // Guard timeout-kill vs natural-completion race: if the timeout fires and
      // sets killed=true but the process exits with code 0 in the same tick,
      // the exit code takes precedence — the execution completed successfully
      // before the SIGTERM was delivered.
      const status: 'completed' | 'failed' | 'cancelled' =
        exitCode === 0 ? 'completed' : this.killed ? 'cancelled' : 'failed';

      const phaseTimings: PhaseTimings = {
        sandboxSetupMs,
        credentialInjectionMs,
        proxyStartupMs,
        cliRuntimeMs,
        cleanupMs: 0, // Will be measured in finally block
      };

      this.logger.info({
        executionId: assignment.executionId,
        exitCode,
        durationMs,
        status,
        sessionId,
        totalCostUsd,
        phaseTimings,
      }, 'Execution finished');

      emit({ type: 'complete', status, exitCode, durationMs, sessionId, totalCostUsd, phaseTimings });

    } finally {
      // Cleanup: remove temp directory, sandbox, credentials, proxy
      const cleanupStart = Date.now();
      if (injectedCreds) await injectedCreds.cleanup().catch(() => {});
      if (proxy) proxy.cleanup();
      cleanupSandbox(sandbox.sandboxDir, this.logger);
      cleanupTempDir(execDir, this.logger);
      this.childProcess = null;
      this.killed = false;
      this.exited = false;
      this.currentExecutionId = null;
      this.executionStartTime = null;
      this.pendingReviewId = null;
      this.reviewResolver = null;
    }
  }

  kill(): void {
    if (this.childProcess && !this.killed && !this.exited) {
      this.killed = true;
      this.childProcess.kill('SIGTERM');

      // Force kill after 5 seconds
      setTimeout(() => {
        if (this.childProcess) {
          this.childProcess.kill('SIGKILL');
        }
      }, 5_000);
    }

    // Also resolve any pending review so the process can exit cleanly
    if (this.reviewResolver) {
      this.reviewResolver('');
      this.reviewResolver = null;
      this.pendingReviewId = null;
    }
  }

  /**
   * Resume a paused execution after human review.
   * Resumes the stdout stream so the child process can continue.
   */
  resolveReview(reviewId: string, message: string): boolean {
    if (this.pendingReviewId !== reviewId) return false;

    this.logger.info({ reviewId, message }, 'Resolving review — resuming execution');
    this.pendingReviewId = null;

    // Resume stdout stream — removes backpressure, allowing the child process to continue
    if (this.childProcess) {
      this.childProcess.stdout!.resume();
    }

    // Resolve the pending promise if one exists
    if (this.reviewResolver) {
      this.reviewResolver(message);
      this.reviewResolver = null;
    }

    return true;
  }
}
