import type { Logger } from 'pino';
import { getTableSpec, isEphemeral } from '@dac-cloud/shared';
import type { EphemeralTableSpec } from '@dac-cloud/shared';
import type { AuditLog } from './auditLog.js';
import { RetentionPolicy } from './retentionPolicy.js';

// Derive retention constraints from the table registry — single source of truth
const auditSpec = getTableSpec('audit_events');
if (!auditSpec || !isEphemeral(auditSpec)) {
  throw new Error('audit_events must be registered as ephemeral in TABLE_REGISTRY');
}

/** Minimum allowed retention to prevent accidental wipe of the compliance trail. */
export const MIN_AUDIT_RETENTION_DAYS = (auditSpec as EphemeralTableSpec).minRetentionDays ?? 90;

/** Shared RetentionPolicy instance for audit_events. */
const auditPolicy = new RetentionPolicy({ table: 'audit_events' });

/**
 * Manages retention / purge lifecycle for the audit log.
 *
 * Separated from AuditLog so that the log itself enforces append-only
 * semantics structurally — only this class can delete audit rows.
 */
export class AuditRetentionPolicy {
  private purgeTimer: NodeJS.Timeout | null = null;

  constructor(
    private auditLog: AuditLog,
    private retentionDays: number,
    private logger: Logger,
  ) {
    if (!Number.isFinite(retentionDays) || retentionDays < MIN_AUDIT_RETENTION_DAYS) {
      throw new Error(
        `auditRetentionDays must be >= ${MIN_AUDIT_RETENTION_DAYS}, got ${retentionDays}`,
      );
    }
  }

  /** Delete audit events older than retentionDays with volume safety cap. */
  async purgeOld(): Promise<number> {
    const cutoff = new Date(Date.now() - this.retentionDays * 86_400_000).toISOString();
    const totalDeleted = await auditPolicy.purge(this.auditLog.getDb(), cutoff, this.logger);
    if (totalDeleted > 0) {
      this.logger.info({ purged: totalDeleted, cutoff }, 'Purged old audit events');
    }
    return totalDeleted;
  }

  /** Start daily purge timer. Returns the timer for cleanup. */
  startDailyPurge(): NodeJS.Timeout {
    this.purgeTimer = setInterval(() => {
      this.purgeOld().catch((err) => {
        this.logger.error({ err }, 'Audit purge failed');
      });
    }, 24 * 60 * 60 * 1000);
    return this.purgeTimer;
  }

  close(): void {
    if (this.purgeTimer) clearInterval(this.purgeTimer);
  }
}
