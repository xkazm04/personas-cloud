import type { Logger } from 'pino';
import { Executor, type ResolvedClaudeCommand } from './executor.js';

/**
 * Manages the lifecycle of active executions and provides a unified drain/shutdown contract.
 *
 * Owns the activeExecutions map, slot accounting, and a single drain() method that:
 * 1. Stops accepting new work (draining = true)
 * 2. Waits for running executions to complete within a configurable grace period
 * 3. Force-kills stragglers if the deadline expires
 * 4. Resolves when all executions have terminated
 */
export class ExecutionManager {
  private activeExecutions = new Map<string, Executor>();
  private draining = false;

  constructor(
    private logger: Logger,
    private maxConcurrentExecutions: number,
    private claudeCommand: ResolvedClaudeCommand,
  ) {}

  /** Number of available execution slots. Returns 0 when draining. */
  availableSlots(): number {
    if (this.draining) return 0;
    return this.maxConcurrentExecutions - this.activeExecutions.size;
  }

  /** Whether the manager is currently draining (not accepting new work). */
  isDraining(): boolean {
    return this.draining;
  }

  /** Number of currently active executions. */
  activeCount(): number {
    return this.activeExecutions.size;
  }

  /** Create and register a new Executor for the given assignment. */
  createExecutor(executionId: string): Executor {
    const executor = new Executor(this.logger, this.claudeCommand);
    this.activeExecutions.set(executionId, executor);
    return executor;
  }

  /** Remove a completed execution from tracking. */
  removeExecution(executionId: string): void {
    this.activeExecutions.delete(executionId);
  }

  /** Get an executor by execution ID. */
  getExecutor(executionId: string): Executor | undefined {
    return this.activeExecutions.get(executionId);
  }

  /** Get the IDs of all currently active executions. */
  getActiveExecutionIds(): string[] {
    return [...this.activeExecutions.keys()];
  }

  /**
   * Drain all active executions with a grace period.
   *
   * Stops accepting new work, waits for running executions to complete
   * within `gracePeriodMs`, then force-kills any remaining.
   *
   * Returns a promise that resolves when all executions have terminated.
   */
  drain(gracePeriodMs: number): Promise<void> {
    this.draining = true;

    if (this.activeExecutions.size === 0) {
      return Promise.resolve();
    }

    this.logger.info(
      { activeCount: this.activeExecutions.size, gracePeriodMs },
      'Draining active executions',
    );

    return new Promise<void>((resolve) => {
      // Poll for all executions to finish
      const checkInterval = setInterval(() => {
        if (this.activeExecutions.size === 0) {
          clearInterval(checkInterval);
          clearTimeout(forceKillTimer);
          resolve();
        }
      }, 1000);

      // Force-kill stragglers after the grace period
      const forceKillTimer = setTimeout(() => {
        clearInterval(checkInterval);
        if (this.activeExecutions.size > 0) {
          this.logger.warn(
            { activeCount: this.activeExecutions.size },
            'Grace period expired, force-killing remaining executions',
          );
          for (const executor of this.activeExecutions.values()) {
            executor.kill();
          }
        }
        resolve();
      }, gracePeriodMs);
    });
  }

  /** Immediately kill all active executions without waiting. */
  killAll(): void {
    this.draining = true;
    for (const executor of this.activeExecutions.values()) {
      executor.kill();
    }
  }
}
