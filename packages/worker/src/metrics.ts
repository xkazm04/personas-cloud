import os from 'node:os';
import fs from 'node:fs';
import { tmpdir } from 'node:os';
import type { WorkerHealthMetrics } from '@dac-cloud/shared';

/**
 * Collects and tracks worker-level system health metrics.
 *
 * Tracks execution counts and durations over the worker's lifetime,
 * and samples system metrics (CPU, memory, disk) on demand.
 */
export class MetricsCollector {
  private executionsCompleted = 0;
  private totalDurationMs = 0;
  private startedAt = Date.now();
  private lastCpuSnapshot = os.cpus();
  private lastCpuSnapshotTime = Date.now();

  /** Record a completed execution's duration for average tracking. */
  recordExecution(durationMs: number): void {
    this.executionsCompleted++;
    this.totalDurationMs += durationMs;
  }

  /** Collect current system health metrics. */
  collect(): WorkerHealthMetrics {
    return {
      cpuUsagePercent: this.getCpuUsagePercent(),
      freeMemoryBytes: os.freemem(),
      totalMemoryBytes: os.totalmem(),
      freeDiskBytes: this.getFreeDiskBytes(),
      executionsCompleted: this.executionsCompleted,
      avgExecutionDurationMs: this.executionsCompleted > 0
        ? Math.round(this.totalDurationMs / this.executionsCompleted)
        : null,
      uptimeMs: Date.now() - this.startedAt,
    };
  }

  /**
   * Compute CPU usage percentage since last snapshot by comparing
   * idle vs total ticks across all cores.
   */
  private getCpuUsagePercent(): number {
    const cpus = os.cpus();
    const now = Date.now();

    // Need at least two snapshots to compute delta
    if (now - this.lastCpuSnapshotTime < 100) {
      // Too soon — return 0 on first call
      this.lastCpuSnapshot = cpus;
      this.lastCpuSnapshotTime = now;
      return 0;
    }

    let totalIdleDelta = 0;
    let totalTickDelta = 0;

    for (let i = 0; i < cpus.length; i++) {
      const prev = this.lastCpuSnapshot[i];
      const curr = cpus[i];
      if (!prev || !curr) continue;

      const prevTotal = prev.times.user + prev.times.nice + prev.times.sys + prev.times.idle + prev.times.irq;
      const currTotal = curr.times.user + curr.times.nice + curr.times.sys + curr.times.idle + curr.times.irq;

      totalIdleDelta += curr.times.idle - prev.times.idle;
      totalTickDelta += currTotal - prevTotal;
    }

    this.lastCpuSnapshot = cpus;
    this.lastCpuSnapshotTime = now;

    if (totalTickDelta === 0) return 0;
    const usagePercent = ((totalTickDelta - totalIdleDelta) / totalTickDelta) * 100;
    return Math.round(usagePercent * 10) / 10; // 1 decimal place
  }

  /**
   * Get available disk space in the temp directory.
   * Uses fs.statfsSync which is available in Node 18.15+.
   */
  private getFreeDiskBytes(): number | null {
    try {
      const stats = fs.statfsSync(tmpdir());
      return stats.bavail * stats.bsize;
    } catch {
      return null;
    }
  }
}
