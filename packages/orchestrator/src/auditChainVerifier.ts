import type { Logger } from 'pino';
import type { AuditLog } from './auditLog.js';

/** Default verification interval: 1 hour. */
const DEFAULT_INTERVAL_MS = 60 * 60 * 1000;

/**
 * Periodically verifies the audit log hash chain integrity.
 *
 * Follows the same setInterval lifecycle pattern as AuditRetentionPolicy.
 * On chain break detection, emits a high-severity (fatal-level) log event
 * with the broken row ID so external alerting systems can trigger.
 */
export class AuditChainVerifier {
  private timer: NodeJS.Timeout | null = null;
  private readonly intervalMs: number;

  constructor(
    private auditLog: AuditLog,
    private logger: Logger,
    intervalMs?: number,
  ) {
    this.intervalMs = intervalMs ?? DEFAULT_INTERVAL_MS;
  }

  /** Run a single verification pass and log the result. */
  runVerification(): void {
    try {
      const result = this.auditLog.verify();

      if (result.valid) {
        this.logger.info(
          { verifiedCount: result.verifiedCount },
          'Audit hash chain verification passed',
        );
      } else {
        this.logger.fatal(
          { verifiedCount: result.verifiedCount, brokenAtId: result.brokenAtId },
          'AUDIT CHAIN INTEGRITY BREACH — hash chain broken, possible tampering detected',
        );
      }
    } catch (err) {
      this.logger.error({ err }, 'Audit hash chain verification failed with error');
    }
  }

  /** Start the periodic verification timer. */
  start(): NodeJS.Timeout {
    // Run an initial verification on startup
    this.runVerification();

    this.timer = setInterval(() => this.runVerification(), this.intervalMs);
    this.logger.info(
      { intervalMinutes: Math.round(this.intervalMs / 60_000) },
      'Audit chain verification scheduler started',
    );
    return this.timer;
  }

  close(): void {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }
}
