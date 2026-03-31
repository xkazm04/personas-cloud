/**
 * Orchestrator-level metrics collector for execution observability.
 *
 * Tracks latency percentiles (p50/p95/p99), queue depth, worker utilization,
 * retry counts, cost trends, and per-persona execution heatmaps.
 * Exposes both Prometheus text format and JSON format.
 */

/** Sliding window size for latency percentile calculations. */
const LATENCY_WINDOW_SIZE = 1000;

/** Number of hourly buckets to keep for per-persona heatmaps (7 days). */
const HEATMAP_HOURS = 168;

interface PerPersonaStats {
  totalExecutions: number;
  completedExecutions: number;
  failedExecutions: number;
  totalDurationMs: number;
  totalCostUsd: number;
  /** Hourly execution counts for heatmap: hour-of-week (0-167) → count. */
  hourlyBuckets: number[];
}

export class OrchestratorMetrics {
  // --- Execution counters ---
  private executionsSubmitted = 0;
  private executionsDispatched = 0;
  private executionsCompleted = 0;
  private executionsFailed = 0;
  private executionsCancelled = 0;
  private executionsRejected = 0;

  // --- Latency tracking (sliding window) ---
  private latencies: number[] = [];

  // --- Queue depth samples ---
  private lastQueueDepth = 0;
  private peakQueueDepth = 0;

  // --- Worker utilization ---
  private lastWorkerUtilization = 0;
  private lastTotalSlots = 0;
  private lastAvailableSlots = 0;

  // --- Retry counters ---
  private executionRetries = 0;
  private eventRetries = 0;
  private concurrencyBlocks = 0;
  private deadLetterCount = 0;

  // --- Stale recovery ---
  private staleRecoveries = 0;

  // --- Cost tracking ---
  private totalCostUsd = 0;

  // --- Per-sink output error counters (for fan-out write path observability) ---
  private outputDbErrors = 0;

  // --- Per-persona breakdown ---
  private perPersona = new Map<string, PerPersonaStats>();

  // --- Uptime ---
  private startedAt = Date.now();

  // --- Execution timing (in-flight tracking for queue wait time) ---
  private queuedAt = new Map<string, number>();
  private queueWaitTimes: number[] = [];

  /** Record an execution submission (entered the queue). */
  recordSubmission(executionId: string): void {
    this.executionsSubmitted++;
    this.queuedAt.set(executionId, Date.now());
  }

  /** Record an execution dispatch (left the queue, sent to worker). */
  recordDispatch(executionId: string): void {
    this.executionsDispatched++;
    const queuedTime = this.queuedAt.get(executionId);
    if (queuedTime) {
      const waitMs = Date.now() - queuedTime;
      this.queueWaitTimes.push(waitMs);
      if (this.queueWaitTimes.length > LATENCY_WINDOW_SIZE) {
        this.queueWaitTimes.shift();
      }
      this.queuedAt.delete(executionId);
    }
  }

  /** Record an execution completion with duration, cost, and persona. */
  recordCompletion(
    executionId: string,
    status: 'completed' | 'failed' | 'cancelled',
    durationMs: number,
    costUsd: number,
    personaId?: string,
  ): void {
    this.queuedAt.delete(executionId);

    switch (status) {
      case 'completed':
        this.executionsCompleted++;
        break;
      case 'failed':
        this.executionsFailed++;
        break;
      case 'cancelled':
        this.executionsCancelled++;
        break;
    }

    // Track latency
    this.latencies.push(durationMs);
    if (this.latencies.length > LATENCY_WINDOW_SIZE) {
      this.latencies.shift();
    }

    // Track cost
    if (costUsd > 0) {
      this.totalCostUsd += costUsd;
    }

    // Per-persona stats
    if (personaId) {
      const stats = this.getOrCreatePersonaStats(personaId);
      stats.totalExecutions++;
      if (status === 'completed') stats.completedExecutions++;
      if (status === 'failed') stats.failedExecutions++;
      stats.totalDurationMs += durationMs;
      if (costUsd > 0) stats.totalCostUsd += costUsd;

      // Heatmap: bucket by hour-of-week (0 = Monday 00:00, 167 = Sunday 23:00)
      const now = new Date();
      const dayOfWeek = (now.getUTCDay() + 6) % 7; // Monday = 0
      const hourOfWeek = dayOfWeek * 24 + now.getUTCHours();
      stats.hourlyBuckets[hourOfWeek]++;
    }
  }

  /** Record a rejected submission (queue full). */
  recordRejection(): void {
    this.executionsRejected++;
  }

  /** Record an execution retry. */
  recordExecutionRetry(): void {
    this.executionRetries++;
  }

  /** Record an event retry (from eventProcessor). */
  recordEventRetry(): void {
    this.eventRetries++;
  }

  /** Record a concurrency block event. */
  recordConcurrencyBlock(): void {
    this.concurrencyBlocks++;
  }

  /** Record an event moved to dead letter queue. */
  recordDeadLetter(): void {
    this.deadLetterCount++;
  }

  /** Record stale event recoveries from the periodic sweep (not crash recovery). */
  recordStaleRecovery(count: number): void {
    this.staleRecoveries += count;
  }

  /** Record an output write error for the DB sink. */
  recordOutputDbError(): void {
    this.outputDbErrors++;
  }

  /** Update queue and worker snapshots (called periodically). */
  updateSnapshots(queueDepth: number, totalSlots: number, availableSlots: number): void {
    this.lastQueueDepth = queueDepth;
    if (queueDepth > this.peakQueueDepth) this.peakQueueDepth = queueDepth;
    this.lastTotalSlots = totalSlots;
    this.lastAvailableSlots = availableSlots;
    this.lastWorkerUtilization = totalSlots > 0
      ? Math.round(((totalSlots - availableSlots) / totalSlots) * 1000) / 10
      : 0;
  }

  /** Get JSON metrics snapshot. */
  toJSON(): Record<string, unknown> {
    const sorted = [...this.latencies].sort((a, b) => a - b);
    const queueWaitSorted = [...this.queueWaitTimes].sort((a, b) => a - b);

    const personaBreakdown: Record<string, unknown> = {};
    for (const [personaId, stats] of this.perPersona) {
      personaBreakdown[personaId] = {
        totalExecutions: stats.totalExecutions,
        completedExecutions: stats.completedExecutions,
        failedExecutions: stats.failedExecutions,
        avgDurationMs: stats.totalExecutions > 0
          ? Math.round(stats.totalDurationMs / stats.totalExecutions)
          : null,
        totalCostUsd: Math.round(stats.totalCostUsd * 1_000_000) / 1_000_000,
        successRate: stats.totalExecutions > 0
          ? Math.round((stats.completedExecutions / stats.totalExecutions) * 1000) / 10
          : null,
        hourlyHeatmap: stats.hourlyBuckets,
      };
    }

    return {
      uptime: {
        startedAt: new Date(this.startedAt).toISOString(),
        uptimeMs: Date.now() - this.startedAt,
      },
      executions: {
        submitted: this.executionsSubmitted,
        dispatched: this.executionsDispatched,
        completed: this.executionsCompleted,
        failed: this.executionsFailed,
        cancelled: this.executionsCancelled,
        rejected: this.executionsRejected,
      },
      latency: {
        sampleCount: sorted.length,
        p50Ms: percentile(sorted, 50),
        p95Ms: percentile(sorted, 95),
        p99Ms: percentile(sorted, 99),
        avgMs: sorted.length > 0 ? Math.round(sorted.reduce((a, b) => a + b, 0) / sorted.length) : null,
      },
      queueWait: {
        sampleCount: queueWaitSorted.length,
        p50Ms: percentile(queueWaitSorted, 50),
        p95Ms: percentile(queueWaitSorted, 95),
        p99Ms: percentile(queueWaitSorted, 99),
      },
      queue: {
        currentDepth: this.lastQueueDepth,
        peakDepth: this.peakQueueDepth,
      },
      workers: {
        totalSlots: this.lastTotalSlots,
        availableSlots: this.lastAvailableSlots,
        utilizationPercent: this.lastWorkerUtilization,
      },
      retries: {
        executionRetries: this.executionRetries,
        eventRetries: this.eventRetries,
        concurrencyBlocks: this.concurrencyBlocks,
        deadLetterCount: this.deadLetterCount,
        staleRecoveries: this.staleRecoveries,
      },
      cost: {
        totalUsd: Math.round(this.totalCostUsd * 1_000_000) / 1_000_000,
        avgPerExecutionUsd: this.executionsCompleted > 0
          ? Math.round((this.totalCostUsd / this.executionsCompleted) * 1_000_000) / 1_000_000
          : null,
      },
      outputSinks: {
        dbErrors: this.outputDbErrors,
      },
      perPersona: personaBreakdown,
    };
  }

  /** Render Prometheus-compatible text format. */
  toPrometheus(): string {
    const sorted = [...this.latencies].sort((a, b) => a - b);
    const queueWaitSorted = [...this.queueWaitTimes].sort((a, b) => a - b);
    const lines: string[] = [];

    const g = (name: string, help: string, value: number | null) => {
      lines.push(`# HELP ${name} ${help}`);
      lines.push(`# TYPE ${name} gauge`);
      lines.push(`${name} ${value ?? 0}`);
    };

    const c = (name: string, help: string, value: number) => {
      lines.push(`# HELP ${name} ${help}`);
      lines.push(`# TYPE ${name} counter`);
      lines.push(`${name} ${value}`);
    };

    // Uptime
    g('orchestrator_uptime_seconds', 'Orchestrator uptime in seconds', Math.floor((Date.now() - this.startedAt) / 1000));

    // Execution counters
    c('orchestrator_executions_submitted_total', 'Total executions submitted', this.executionsSubmitted);
    c('orchestrator_executions_dispatched_total', 'Total executions dispatched to workers', this.executionsDispatched);
    c('orchestrator_executions_completed_total', 'Total executions completed successfully', this.executionsCompleted);
    c('orchestrator_executions_failed_total', 'Total executions failed', this.executionsFailed);
    c('orchestrator_executions_cancelled_total', 'Total executions cancelled', this.executionsCancelled);
    c('orchestrator_executions_rejected_total', 'Total executions rejected (queue full)', this.executionsRejected);

    // Latency percentiles
    lines.push('# HELP orchestrator_execution_duration_ms Execution duration percentiles in milliseconds');
    lines.push('# TYPE orchestrator_execution_duration_ms summary');
    lines.push(`orchestrator_execution_duration_ms{quantile="0.5"} ${percentile(sorted, 50) ?? 0}`);
    lines.push(`orchestrator_execution_duration_ms{quantile="0.95"} ${percentile(sorted, 95) ?? 0}`);
    lines.push(`orchestrator_execution_duration_ms{quantile="0.99"} ${percentile(sorted, 99) ?? 0}`);
    lines.push(`orchestrator_execution_duration_ms_count ${sorted.length}`);
    lines.push(`orchestrator_execution_duration_ms_sum ${sorted.reduce((a, b) => a + b, 0)}`);

    // Queue wait percentiles
    lines.push('# HELP orchestrator_queue_wait_ms Queue wait time percentiles in milliseconds');
    lines.push('# TYPE orchestrator_queue_wait_ms summary');
    lines.push(`orchestrator_queue_wait_ms{quantile="0.5"} ${percentile(queueWaitSorted, 50) ?? 0}`);
    lines.push(`orchestrator_queue_wait_ms{quantile="0.95"} ${percentile(queueWaitSorted, 95) ?? 0}`);
    lines.push(`orchestrator_queue_wait_ms{quantile="0.99"} ${percentile(queueWaitSorted, 99) ?? 0}`);

    // Queue depth
    g('orchestrator_queue_depth', 'Current execution queue depth', this.lastQueueDepth);
    g('orchestrator_queue_peak_depth', 'Peak execution queue depth since startup', this.peakQueueDepth);

    // Worker utilization
    g('orchestrator_worker_slots_total', 'Total worker execution slots', this.lastTotalSlots);
    g('orchestrator_worker_slots_available', 'Available worker execution slots', this.lastAvailableSlots);
    g('orchestrator_worker_utilization_percent', 'Worker utilization percentage', this.lastWorkerUtilization);

    // Retries
    c('orchestrator_execution_retries_total', 'Total execution dispatch retries', this.executionRetries);
    c('orchestrator_event_retries_total', 'Total event processing retries', this.eventRetries);
    c('orchestrator_concurrency_blocks_total', 'Total concurrency-blocked event dispatches', this.concurrencyBlocks);
    c('orchestrator_dead_letter_total', 'Total events moved to dead letter queue', this.deadLetterCount);
    c('orchestrator_stale_recoveries_total', 'Total events recovered from stale processing state during runtime sweeps', this.staleRecoveries);

    // Cost
    g('orchestrator_cost_total_usd', 'Total execution cost in USD', Math.round(this.totalCostUsd * 1_000_000) / 1_000_000);

    // Per-sink output errors
    c('orchestrator_output_db_errors_total', 'Total DB write errors in output fan-out', this.outputDbErrors);

    // Per-persona execution counts
    for (const [personaId, stats] of this.perPersona) {
      lines.push(`orchestrator_persona_executions_total{persona="${personaId}"} ${stats.totalExecutions}`);
      lines.push(`orchestrator_persona_completed_total{persona="${personaId}"} ${stats.completedExecutions}`);
      lines.push(`orchestrator_persona_failed_total{persona="${personaId}"} ${stats.failedExecutions}`);
      lines.push(`orchestrator_persona_cost_usd{persona="${personaId}"} ${Math.round(stats.totalCostUsd * 1_000_000) / 1_000_000}`);
    }

    return lines.join('\n') + '\n';
  }

  private getOrCreatePersonaStats(personaId: string): PerPersonaStats {
    let stats = this.perPersona.get(personaId);
    if (!stats) {
      stats = {
        totalExecutions: 0,
        completedExecutions: 0,
        failedExecutions: 0,
        totalDurationMs: 0,
        totalCostUsd: 0,
        hourlyBuckets: new Array(HEATMAP_HOURS).fill(0),
      };
      this.perPersona.set(personaId, stats);
    }
    return stats;
  }
}

/** Calculate a percentile from a sorted array. Returns null if array is empty. */
function percentile(sorted: number[], p: number): number | null {
  if (sorted.length === 0) return null;
  const idx = Math.ceil((p / 100) * sorted.length) - 1;
  return sorted[Math.max(0, idx)];
}
