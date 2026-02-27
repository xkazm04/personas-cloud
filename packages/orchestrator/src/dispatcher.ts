import { type ExecAssign, type ExecRequest, TOPICS, assemblePrompt, parseModelProfile, decrypt, deriveMasterKey, type EncryptedPayload } from '@dac-cloud/shared';
import type Database from 'better-sqlite3';
import type { WorkerPool } from './workerPool.js';
import type { TokenManager } from './tokenManager.js';
import type { OAuthManager } from './oauth.js';
import type { KafkaClient } from './kafka.js';
import type { Logger } from 'pino';
import * as db from './db.js';

interface QueuedExecution {
  request: ExecRequest;
  receivedAt: number;
}

// In-memory tracking for active executions (real-time output streaming)
interface ActiveExecution {
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

export class Dispatcher {
  private queue: QueuedExecution[] = [];
  // In-memory map for real-time output + cancel routing (active executions only)
  private active = new Map<string, ActiveExecution>();
  private database: Database.Database | null = null;
  private masterKey: Buffer | null = null;

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
      const exec = this.active.get(msg.executionId);
      if (exec) {
        exec.output.push(msg.chunk);
      }

      // Persist to DB
      if (this.database) {
        try { db.appendOutput(this.database, msg.executionId, msg.chunk + '\n'); } catch { /* best effort */ }
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
      const exec = this.active.get(msg.executionId);
      if (exec) {
        exec.output.push(`[STDERR] ${msg.chunk}`);
      }

      if (this.database) {
        try { db.appendOutput(this.database, msg.executionId, `[STDERR] ${msg.chunk}\n`); } catch { /* best effort */ }
      }

      this.logger.warn({ executionId: msg.executionId, stderr: msg.chunk }, 'Execution stderr');
    });

    // Handle execution completion
    this.pool.on('complete', (_workerId: string, msg: { executionId: string; status: string; exitCode: number; durationMs: number; sessionId?: string; totalCostUsd?: number }) => {
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

      // Update in-memory
      const exec = this.active.get(msg.executionId);
      if (exec) {
        exec.status = msg.status as ActiveExecution['status'];
        exec.completedAt = Date.now();
        exec.exitCode = msg.exitCode;
        exec.durationMs = msg.durationMs;
        exec.sessionId = msg.sessionId;
        exec.totalCostUsd = msg.totalCostUsd;
        // Remove from active after a short delay (allow polling to pick up final state)
        setTimeout(() => this.active.delete(msg.executionId), 30_000);
      }

      // Persist to DB
      if (this.database) {
        try {
          db.updateExecution(this.database, msg.executionId, {
            status: msg.status === 'completed' ? 'completed' : msg.status === 'cancelled' ? 'cancelled' : 'failed',
            completedAt: new Date().toISOString(),
            durationMs: msg.durationMs,
            claudeSessionId: msg.sessionId,
            costUsd: msg.totalCostUsd ?? 0,
          });
        } catch (err) {
          this.logger.error({ err, executionId: msg.executionId }, 'Failed to persist execution completion');
        }
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
        const exec = this.active.get(executionId);
        if (exec) {
          exec.status = 'failed';
          exec.completedAt = Date.now();
          setTimeout(() => this.active.delete(executionId), 30_000);
        }

        // Persist failure
        if (this.database) {
          try {
            db.updateExecution(this.database, executionId, {
              status: 'failed',
              errorMessage: 'Worker disconnected',
              completedAt: new Date().toISOString(),
            });
          } catch { /* best effort */ }
        }

        this.kafka.produce(TOPICS.EXEC_LIFECYCLE, JSON.stringify({
          executionId,
          status: 'failed',
          error: 'Worker disconnected',
          durationMs: 0,
        }), executionId).catch(() => {});
      }
    });
  }

  setDatabase(database: Database.Database): void {
    this.database = database;
  }

  setMasterKey(masterKeyHex: string): void {
    this.masterKey = deriveMasterKey(masterKeyHex);
  }

  submit(request: ExecRequest): void {
    this.logger.info({
      executionId: request.executionId,
      personaId: request.personaId,
    }, 'Execution submitted');

    // Create DB record immediately
    if (this.database) {
      try {
        db.createExecution(this.database, {
          id: request.executionId,
          personaId: request.personaId,
          inputData: request.prompt,
          projectId: request.projectId,
        });
      } catch (err) {
        this.logger.error({ err, executionId: request.executionId }, 'Failed to create execution record');
      }
    }

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
    const env: Record<string, string> = {
      ANTHROPIC_API_KEY: claudeToken,
    };

    // If persona exists in DB, assemble prompt from persona config and inject credentials
    let prompt = request.prompt;
    if (this.database) {
      const persona = db.getPersona(this.database, request.personaId);
      if (persona) {
        const tools = db.getToolsForPersona(this.database, persona.id);
        const creds = db.listCredentialsForPersona(this.database, persona.id);

        // Decrypt credentials and inject as env vars
        const credentialHints: string[] = [];
        for (const cred of creds) {
          const envName = `CONNECTOR_${cred.name.toUpperCase().replace(/[^A-Z0-9]/g, '_')}`;
          credentialHints.push(envName);

          // Decrypt and inject credential fields as env vars
          if (this.masterKey) {
            try {
              const payload: EncryptedPayload = {
                encrypted: cred.encryptedData,
                iv: cred.iv,
                tag: cred.tag,
              };
              const decrypted = decrypt(payload, this.masterKey);

              // Try to parse as JSON with multiple fields
              try {
                const fields = JSON.parse(decrypted) as Record<string, string>;
                for (const [fieldKey, fieldValue] of Object.entries(fields)) {
                  const fieldEnvName = `CONNECTOR_${cred.name.toUpperCase().replace(/[^A-Z0-9]/g, '_')}_${fieldKey.toUpperCase().replace(/[^A-Z0-9]/g, '_')}`;
                  env[fieldEnvName] = String(fieldValue);
                }
              } catch {
                // Not JSON — inject as single value
                env[envName] = decrypted;
              }
            } catch (err) {
              this.logger.error({ err, credentialId: cred.id, credentialName: cred.name }, 'Failed to decrypt credential');
            }
          }
        }

        const inputData = request.inputData ? (request.inputData as Record<string, unknown>) : undefined;
        prompt = assemblePrompt(persona, tools, inputData, credentialHints.length > 0 ? credentialHints : undefined);

        // Apply model profile env overrides
        const profile = parseModelProfile(persona.modelProfile);
        if (profile?.provider) {
          switch (profile.provider) {
            case 'ollama':
              if (profile.baseUrl) env['OLLAMA_BASE_URL'] = profile.baseUrl;
              if (profile.authToken) env['OLLAMA_API_KEY'] = profile.authToken;
              delete env['ANTHROPIC_API_KEY'];
              break;
            case 'litellm':
              if (profile.baseUrl) env['ANTHROPIC_BASE_URL'] = profile.baseUrl;
              if (profile.authToken) env['ANTHROPIC_AUTH_TOKEN'] = profile.authToken;
              delete env['ANTHROPIC_API_KEY'];
              break;
            case 'custom':
              if (profile.baseUrl) env['OPENAI_BASE_URL'] = profile.baseUrl;
              if (profile.authToken) env['OPENAI_API_KEY'] = profile.authToken;
              delete env['ANTHROPIC_API_KEY'];
              break;
          }
        }

        this.logger.info({ personaId: persona.id, toolCount: tools.length, credCount: creds.length }, 'Assembled prompt from DB persona');
      }
    }

    const assignment: ExecAssign = {
      type: 'assign',
      executionId: request.executionId,
      personaId: request.personaId,
      prompt,
      env,
      config: {
        timeoutMs: request.config.timeoutMs || 300_000,
        maxOutputBytes: 10 * 1024 * 1024, // 10MB
      },
    };

    // Track in-memory for real-time streaming
    this.active.set(request.executionId, {
      workerId,
      startedAt: Date.now(),
      output: [],
      status: 'running',
    });

    // Mark running in DB
    if (this.database) {
      try {
        db.updateExecution(this.database, request.executionId, {
          status: 'running',
          startedAt: new Date().toISOString(),
        });
      } catch { /* best effort */ }
    }

    const sent = this.pool.assign(workerId, assignment);
    if (!sent) {
      this.logger.error({ workerId, executionId: request.executionId }, 'Failed to assign to worker');
      this.active.delete(request.executionId);
      if (this.database) {
        try { db.updateExecution(this.database, request.executionId, { status: 'queued' }); } catch { /* best effort */ }
      }
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
    const exec = this.active.get(executionId);
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
    return Array.from(this.active.entries())
      .filter(([, e]) => e.status === 'running')
      .map(([executionId, e]) => ({
        executionId,
        workerId: e.workerId,
        startedAt: e.startedAt,
      }));
  }

  /**
   * Get execution state. Tries in-memory first (for real-time output),
   * then falls back to DB for completed/historical executions.
   */
  getExecution(executionId: string): ActiveExecution | null {
    // Check in-memory active executions first
    const active = this.active.get(executionId);
    if (active) return active;

    // Fall back to DB
    if (this.database) {
      const dbExec = db.getExecution(this.database, executionId);
      if (dbExec) {
        // Convert DB execution to ActiveExecution shape for backward compat
        return {
          workerId: '',
          startedAt: dbExec.startedAt ? new Date(dbExec.startedAt).getTime() : 0,
          output: dbExec.outputData ? dbExec.outputData.split('\n').filter(Boolean) : [],
          status: dbExec.status as ActiveExecution['status'],
          completedAt: dbExec.completedAt ? new Date(dbExec.completedAt).getTime() : undefined,
          durationMs: dbExec.durationMs ?? undefined,
          sessionId: dbExec.claudeSessionId ?? undefined,
          totalCostUsd: dbExec.costUsd ?? undefined,
        };
      }
    }

    return null;
  }

  getExecutionOutput(executionId: string): string[] | null {
    const exec = this.getExecution(executionId);
    return exec?.output ?? null;
  }

  shutdown(): void {
    // No cleanup timer needed — DB persists naturally
  }
}
