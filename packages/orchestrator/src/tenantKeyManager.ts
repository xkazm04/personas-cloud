import { EventEmitter } from 'node:events';
import { deriveTenantKeyAsync } from '@dac-cloud/shared';
import type { Logger } from 'pino';

interface CachedKey {
  key: Buffer;
  createdAt: number;
}

export interface TenantKeyMetrics {
  hits: number;
  misses: number;
  cacheSize: number;
  derivations: number;
  avgDerivationMs: number;
}

const DEFAULT_TTL_MS = 30 * 60 * 1000; // 30 minutes
const DEFAULT_RENEW_AHEAD_MS = 5 * 60 * 1000; // renew 5 min before expiry

export type TenantIdSource = () => string[];

export interface KeyDerivedEvent { tenantId: string; timestamp: number }
export interface TenantKeyEvents {
  'key:derived': [KeyDerivedEvent];
}

/**
 * Manages per-tenant encryption keys with caching.
 *
 * Keys are derived from the master secret + tenant ID via PBKDF2 (shared/crypto.ts).
 * A single maintenance timer handles both renewal and eviction in a fixed order
 * (renew first, then evict) to prevent races between independent timers.
 *
 * Emits `key:derived` so audit concerns can subscribe externally.
 */
export class TenantKeyManager extends EventEmitter<TenantKeyEvents> {
  private cache = new Map<string, CachedKey>();
  private maintenanceTimer: NodeJS.Timeout;
  private tenantIdSource: TenantIdSource | null = null;
  private _hits = 0;
  private _misses = 0;
  private _derivations = 0;
  private _totalDerivationMs = 0;

  constructor(
    private masterSecret: string,
    private logger: Logger,
    private ttlMs: number = DEFAULT_TTL_MS,
    private renewAheadMs: number = DEFAULT_RENEW_AHEAD_MS,
  ) {
    super();
    // Single timer for cache maintenance: renewal before eviction
    this.maintenanceTimer = setInterval(() => this.maintainCache(), 60_000);
  }

  /**
   * Set a source function that returns all known tenant IDs.
   * Once set, pre-warms the cache. Proactive renewal runs automatically
   * via the unified maintenance timer when a tenant source is available.
   */
  async setTenantIdSource(source: TenantIdSource): Promise<void> {
    this.tenantIdSource = source;
    await this.warmAll();
  }

  /**
   * Pre-derive and cache keys for all known tenants.
   */
  async warmAll(): Promise<void> {
    if (!this.tenantIdSource) return;

    const tenantIds = this.tenantIdSource();
    let warmed = 0;
    const promises: Promise<void>[] = [];
    for (const tenantId of tenantIds) {
      const cached = this.cache.get(tenantId);
      if (!cached || Date.now() - cached.createdAt >= this.ttlMs) {
        promises.push(this.deriveAndCache(tenantId).then(() => { warmed++; }));
      }
    }
    await Promise.all(promises);
    if (warmed > 0) {
      this.logger.info({ warmed, total: tenantIds.length }, 'Pre-warmed tenant key cache');
    }
  }

  /**
   * Eagerly warm the key cache for a single tenant (e.g., on tenant creation).
   */
  async warmTenant(tenantId: string): Promise<void> {
    await this.deriveAndCache(tenantId);
    this.logger.debug({ tenantId }, 'Eagerly warmed tenant key');
  }

  /**
   * Get the encryption key for a tenant. Derives on first access, caches thereafter.
   */
  async getKey(tenantId: string): Promise<Buffer> {
    const cached = this.cache.get(tenantId);
    if (cached && Date.now() - cached.createdAt < this.ttlMs) {
      this._hits++;
      return cached.key;
    }

    this._misses++;
    return this.deriveAndCache(tenantId);
  }

  /**
   * Returns cache hit/miss metrics and derivation performance stats.
   */
  getMetrics(): TenantKeyMetrics {
    return {
      hits: this._hits,
      misses: this._misses,
      cacheSize: this.cache.size,
      derivations: this._derivations,
      avgDerivationMs: this._derivations > 0
        ? this._totalDerivationMs / this._derivations
        : 0,
    };
  }

  /**
   * Invalidate all cached keys (e.g., after master key rotation).
   */
  invalidateAll(): void {
    this.zeroAllKeys();
    this.cache.clear();
    this.logger.info('All cached tenant keys invalidated');
  }

  close(): void {
    clearInterval(this.maintenanceTimer);
    this.zeroAllKeys();
    this.cache.clear();
    this.removeAllListeners();
  }

  private async deriveAndCache(tenantId: string): Promise<Buffer> {
    const existing = this.cache.get(tenantId);
    if (existing) existing.key.fill(0);

    const { key, elapsedMs } = await deriveTenantKeyAsync(this.masterSecret, tenantId);

    this._derivations++;
    this._totalDerivationMs += elapsedMs;

    this.cache.set(tenantId, { key, createdAt: Date.now() });

    this.emit('key:derived', { tenantId, timestamp: Date.now() });

    this.logger.debug({ tenantId, derivationMs: Math.round(elapsedMs) }, 'Tenant key derived');
    return key;
  }

  /** Zero all cached key buffers. */
  private zeroAllKeys(): void {
    for (const cached of this.cache.values()) {
      cached.key.fill(0);
    }
  }

  /**
   * Unified cache maintenance: renew near-expiry keys first, then evict expired ones.
   * Running both phases in a single callback with fixed ordering prevents the race
   * where independent timers could evict a key before renewal refreshes it, or
   * renewal re-derives a key that eviction immediately deletes.
   */
  private async maintainCache(): Promise<void> {
    try {
      const now = Date.now();

      // Phase 1: Renew keys approaching expiry (only when a tenant source is set)
      if (this.tenantIdSource) {
        const renewThreshold = this.ttlMs - this.renewAheadMs;
        const toRenew: { tenantId: string; promise: Promise<void> }[] = [];

        for (const [tenantId, cached] of this.cache) {
          const age = now - cached.createdAt;
          if (age >= renewThreshold) {
            toRenew.push({ tenantId, promise: this.deriveAndCache(tenantId).then(() => {}) });
          }
        }

        // Pick up any new tenants not yet in cache
        for (const tenantId of this.tenantIdSource()) {
          if (!this.cache.has(tenantId)) {
            toRenew.push({ tenantId, promise: this.deriveAndCache(tenantId).then(() => {}) });
          }
        }

        const results = await Promise.allSettled(toRenew.map(r => r.promise));
        const failed = results
          .map((r, i) => r.status === 'rejected' ? { tenantId: toRenew[i].tenantId, reason: r.reason } : null)
          .filter(Boolean) as { tenantId: string; reason: unknown }[];

        if (failed.length > 0) {
          for (const { tenantId, reason } of failed) {
            this.logger.error({ tenantId, err: reason }, 'Failed to renew tenant key during maintenance');
          }
        }

        const succeeded = toRenew.length - failed.length;
        if (toRenew.length > 0) {
          this.logger.debug({ renewed: succeeded, failed: failed.length }, 'Proactively renewed tenant keys');
        }
      }

      // Phase 2: Evict keys still past TTL (those not renewed above)
      for (const [tenantId, cached] of this.cache) {
        if (now - cached.createdAt >= this.ttlMs) {
          cached.key.fill(0);
          this.cache.delete(tenantId);
          this.logger.debug({ tenantId }, 'Evicted expired tenant key');
        }
      }

      // Phase 3: Log cache metrics summary
      const metrics = this.getMetrics();
      const total = metrics.hits + metrics.misses;
      this.logger.info({
        hits: metrics.hits,
        misses: metrics.misses,
        hitRate: total > 0 ? `${((metrics.hits / total) * 100).toFixed(1)}%` : 'N/A',
        cacheSize: metrics.cacheSize,
        derivations: metrics.derivations,
        avgDerivationMs: Math.round(metrics.avgDerivationMs),
      }, 'Tenant key cache metrics');
    } catch (err) {
      this.logger.error({ err, cacheSize: this.cache.size }, 'Unexpected error during tenant key cache maintenance');
    }
  }
}
