import type { ChildProcess } from 'node:child_process';
import { spawn } from 'node:child_process';
import type { ExecAssign, PhaseTimings, ProtocolEventType } from '@dac-cloud/shared';
import { LineBuffer, extractStreamEvents } from './parser.js';
import { prepareSandbox, cleanupSandbox, type SandboxConfig } from './sandbox.js';
import { startFilterProxy, type ProxyHandle } from './networkProxy.js';
import { injectCredentialsViaFile, type InjectedCredentials } from './credentialInjector.js';
import { parseNetworkPolicy } from '@dac-cloud/shared';
import type { Logger } from 'pino';

export interface ExecutorCallbacks {
  onStdout(chunk: string, timestamp: number): void;
  onStderr(chunk: string, timestamp: number): void;
  onEvent(eventType: ProtocolEventType, payload: unknown): void;
  onProgress(phase: string, percent: number, detail?: string): void;
  onComplete(status: 'completed' | 'failed' | 'cancelled', exitCode: number, durationMs: number, sessionId?: string, totalCostUsd?: number, phaseTimings?: PhaseTimings): void;
}

export interface ExecutorStatus {
  state: 'idle' | 'busy';
  executionId?: string;
  durationMs?: number;
}

export class Executor {
  private childProcess: ChildProcess | null = null;
  private killed = false;
  private sessionId: string | undefined;
  private totalCostUsd: number | undefined;
  private currentExecutionId: string | null = null;
  private executionStartTime: number | null = null;

  /**
   * Resolves when the current execution (including its finally-block cleanup)
   * finishes.  Null when idle.
   */
  private executionDone: Promise<void> | null = null;

  constructor(private logger: Logger) {}

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

  private processStdoutLine(line: string, callbacks: ExecutorCallbacks): void {
    callbacks.onStdout(line, Date.now());

    const streamEvents = extractStreamEvents(line, this.logger);
    if (streamEvents.sessionId) this.sessionId = streamEvents.sessionId;
    if (streamEvents.totalCostUsd) this.totalCostUsd = streamEvents.totalCostUsd;
    for (const event of streamEvents.personaEvents) {
      callbacks.onEvent(event.eventType, event.payload);
    }
    if (streamEvents.progress) {
      callbacks.onProgress(streamEvents.progress.phase, streamEvents.progress.percent, streamEvents.progress.detail);
    }
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
    this.sessionId = undefined;
    this.totalCostUsd = undefined;

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
    let completedNormally = false;

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

      const cliStartTime = Date.now();
      this.childProcess = spawn(sandbox.command, sandbox.args, {
        cwd: sandbox.cwd,
        stdio: ['pipe', 'pipe', 'pipe'],
        env: sandbox.env,
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
          this.processStdoutLine(line, callbacks);
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
      exitCode = await new Promise<number>((resolve) => {
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
        this.processStdoutLine(line, callbacks);
      }
      for (const line of stderrBuffer.flush()) {
        callbacks.onStderr(line, Date.now());
      }

      cliRuntimeMs = Date.now() - cliStartTime;
      completedNormally = true;

    } finally {
      const wasKilled = this.killed;
      const cleanupStart = Date.now();
      if (injectedCreds) await injectedCreds.cleanup().catch(() => {});
      if (proxy) proxy.cleanup();
      cleanupSandbox(sandbox.sandboxDir, this.logger);
      this.childProcess = null;
      this.killed = false;
      this.currentExecutionId = null;
      this.executionStartTime = null;
      const cleanupMs = Date.now() - cleanupStart;

      const durationMs = Date.now() - startTime;
      const status = wasKilled ? 'cancelled' : (exitCode === 0 ? 'completed' : 'failed');

      const phaseTimings: PhaseTimings = {
        sandboxSetupMs,
        credentialInjectionMs,
        proxyStartupMs,
        cliRuntimeMs,
        cleanupMs,
      };

      this.logger.info({
        executionId: assignment.executionId,
        exitCode,
        durationMs,
        status,
        sessionId: this.sessionId,
        totalCostUsd: this.totalCostUsd,
        phaseTimings,
      }, 'Execution finished');

      if (completedNormally) {
        callbacks.onComplete(status, exitCode, durationMs, this.sessionId, this.totalCostUsd, phaseTimings);
      }
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
