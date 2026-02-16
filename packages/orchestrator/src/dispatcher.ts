import { type ExecAssign, type ExecRequest, TOPICS } from '@dac-cloud/shared';
import type { WorkerPool } from './workerPool.js';
import type { TokenManager } from './tokenManager.js';
import type { OAuthManager } from './oauth.js';
import type { KafkaClient } from './kafka.js';
import type { Logger } from 'pino';

interface QueuedExecution {
  request: ExecRequest;
  receivedAt: number;
}

interface TrackedExecution {
  workerId: string;
  startedAt: number;
  output: string[];
  status: 'running' | 'completed' | 'failed' | 'cancelled';
  completedAt?: number;
  exitCode?: number;
  durationMs?: number;
  sessionId?: string;
  totalCostUsd?: number;
}

const COMPLETED_RETENTION_MS = 10 * 60 * 1000; // 10 minutes

export class Dispatcher {
  private queue: QueuedExecution[] = [];
  private executions = new Map<string, TrackedExecution>();
  private cleanupTimer: ReturnType<typeof setInterval>;

  constructor(
    private pool: WorkerPool,
    private tokenManager: TokenManager,
    private oauth: OAuthManager | null,
    private kafka: KafkaClient,
    private logger: Logger,
  ) {
    // When a worker becomes ready, try to dispatch queued work
    this.pool.on('worker-ready', () => {
      this.processQueue();
    });

    // When a worker connects, try to dispatch
    this.pool.on('worker-connected', () => {
      this.processQueue();
    });

    // Track stdout for execution output
    this.pool.on('stdout', (_workerId: string, msg: { executionId: string; chunk: string; timestamp: number }) => {
      const exec = this.executions.get(msg.executionId);
      if (exec) {
        exec.output.push(msg.chunk);
      }

      // Forward to Kafka
      this.kafka.produce(TOPICS.EXEC_OUTPUT, JSON.stringify({
        executionId: msg.executionId,
        chunk: msg.chunk,
        timestamp: msg.timestamp,
      }), msg.executionId).catch(err => {
        this.logger.error({ err, executionId: msg.executionId }, 'Failed to produce output to Kafka');
      });
    });

    // Track stderr for debugging failed executions
    this.pool.on('stderr', (_workerId: string, msg: { executionId: string; chunk: string; timestamp: number }) => {
      const exec = this.executions.get(msg.executionId);
      if (exec) {
        exec.output.push(`[STDERR] ${msg.chunk}`);
      }
      this.logger.warn({ executionId: msg.executionId, stderr: msg.chunk }, 'Execution stderr');
    });

    // Handle execution completion
    this.pool.on('complete', (_workerId: string, msg: { executionId: string; status: string; exitCode: number; durationMs: number; sessionId?: string; totalCostUsd?: number }) => {
      const exec = this.executions.get(msg.executionId);

      this.kafka.produce(TOPICS.EXEC_LIFECYCLE, JSON.stringify({
        executionId: msg.executionId,
        status: msg.status,
        exitCode: msg.exitCode,
        durationMs: msg.durationMs,
        sessionId: msg.sessionId,
        totalCostUsd: msg.totalCostUsd,
      }), msg.executionId).catch(err => {
        this.logger.error({ err, executionId: msg.executionId }, 'Failed to produce lifecycle to Kafka');
      });

      // Mark as completed (retain for polling by Vibeman)
      if (exec) {
        exec.status = msg.status as TrackedExecution['status'];
        exec.completedAt = Date.now();
        exec.exitCode = msg.exitCode;
        exec.durationMs = msg.durationMs;
        exec.sessionId = msg.sessionId;
        exec.totalCostUsd = msg.totalCostUsd;
      }

      // Process next queued item
      this.processQueue();
    });

    // Forward persona events to Kafka
    this.pool.on('persona-event', (_workerId: string, msg: { executionId: string; eventType: string; payload: unknown }) => {
      this.kafka.produce(TOPICS.EVENTS, JSON.stringify({
        executionId: msg.executionId,
        eventType: msg.eventType,
        payload: msg.payload,
        timestamp: Date.now(),
      }), msg.executionId).catch(err => {
        this.logger.error({ err, executionId: msg.executionId }, 'Failed to produce event to Kafka');
      });
    });

    // Handle worker disconnect during execution
    this.pool.on('worker-disconnected', (_workerId: string, executionId?: string) => {
      if (executionId) {
        this.logger.error({ executionId }, 'Worker disconnected during execution');
        const exec = this.executions.get(executionId);
        if (exec) {
          exec.status = 'failed';
          exec.completedAt = Date.now();
        }

        this.kafka.produce(TOPICS.EXEC_LIFECYCLE, JSON.stringify({
          executionId,
          status: 'failed',
          error: 'Worker disconnected',
          durationMs: 0,
        }), executionId).catch(() => {});
      }
    });

    // Cleanup completed executions older than retention period
    this.cleanupTimer = setInterval(() => {
      const now = Date.now();
      for (const [id, exec] of this.executions) {
        if (exec.completedAt && now - exec.completedAt > COMPLETED_RETENTION_MS) {
          this.executions.delete(id);
        }
      }
    }, 60_000);
  }

  submit(request: ExecRequest): void {
    this.logger.info({
      executionId: request.executionId,
      personaId: request.personaId,
    }, 'Execution submitted');

    this.queue.push({ request, receivedAt: Date.now() });
    this.processQueue();
  }

  private async processQueue(): Promise<void> {
    if (this.queue.length === 0) return;

    const workerId = this.pool.getIdleWorker();
    if (!workerId) {
      this.logger.debug({ queueLength: this.queue.length }, 'No idle worker, execution queued');
      return;
    }

    const item = this.queue.shift()!;
    await this.dispatchToWorker(workerId, item.request);
  }

  private async dispatchToWorker(workerId: string, request: ExecRequest): Promise<void> {
    // Try to get a fresh token via OAuth (auto-refreshes if near expiry)
    let claudeToken: string | null = null;
    if (this.oauth?.hasTokens()) {
      claudeToken = await this.oauth.getValidAccessToken();
      if (claudeToken) {
        this.tokenManager.storeClaudeToken(claudeToken);
      }
    }
    // Fallback to stored token
    if (!claudeToken) {
      claudeToken = this.tokenManager.getClaudeToken();
    }
    if (!claudeToken) {
      this.logger.error({ executionId: request.executionId }, 'No Claude token available, re-queuing execution');
      this.queue.unshift({ request, receivedAt: Date.now() });
      return;
    }

    // Build environment variables for the worker
    // Pass OAuth access token as ANTHROPIC_API_KEY so Claude CLI uses it for auth
    const env: Record<string, string> = {
      ANTHROPIC_API_KEY: claudeToken,
    };

    const assignment: ExecAssign = {
      type: 'assign',
      executionId: request.executionId,
      personaId: request.personaId,
      prompt: request.prompt,
      env,
      config: {
        timeoutMs: request.config.timeoutMs || 300_000,
        maxOutputBytes: 10 * 1024 * 1024, // 10MB
      },
    };

    // Track execution
    this.executions.set(request.executionId, {
      workerId,
      startedAt: Date.now(),
      output: [],
      status: 'running',
    });

    const sent = this.pool.assign(workerId, assignment);
    if (!sent) {
      this.logger.error({ workerId, executionId: request.executionId }, 'Failed to assign to worker');
      this.executions.delete(request.executionId);
      // Re-queue
      this.queue.unshift({ request, receivedAt: Date.now() });
    } else {
      this.logger.info({
        workerId,
        executionId: request.executionId,
        personaId: request.personaId,
      }, 'Execution dispatched to worker');
    }
  }

  cancelExecution(executionId: string): boolean {
    const exec = this.executions.get(executionId);
    if (!exec || exec.status !== 'running') return false;

    return this.pool.send(exec.workerId, {
      type: 'cancel',
      executionId,
    });
  }

  getQueueLength(): number {
    return this.queue.length;
  }

  getActiveExecutions(): Array<{ executionId: string; workerId: string; startedAt: number }> {
    return Array.from(this.executions.entries())
      .filter(([, e]) => e.status === 'running')
      .map(([executionId, e]) => ({
        executionId,
        workerId: e.workerId,
        startedAt: e.startedAt,
      }));
  }

  getExecution(executionId: string): TrackedExecution | null {
    return this.executions.get(executionId) ?? null;
  }

  getExecutionOutput(executionId: string): string[] | null {
    return this.executions.get(executionId)?.output ?? null;
  }

  shutdown(): void {
    clearInterval(this.cleanupTimer);
  }
}
