import { Kafka, type Producer, type Consumer, type EachMessagePayload, logLevel } from 'kafkajs';
import { TOPICS, sealEnvelope, deriveMasterKeyAsync, signKafkaMessage, verifyKafkaMessage } from '@dac-cloud/shared';
import type { Logger } from 'pino';

export interface KafkaHealthStatus {
  status: 'ok' | 'error' | 'disabled';
  detail?: string;
}

export interface KafkaClient {
  producer: Producer;
  consumer: Consumer;
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  subscribe(handler: (topic: string, value: string, key?: string) => Promise<void>): Promise<void>;
  /** Subscribe to a project-scoped topic (in addition to the base EXEC_REQUESTS topic). */
  subscribeProjectTopic(topic: string, handler: (topic: string, value: string, key?: string) => Promise<void>): Promise<void>;
  produce(topic: string, value: string, key?: string): Promise<void>;
  /** Produce a message encrypted with per-tenant envelope encryption. */
  produceEncrypted(topic: string, payload: unknown, tenantId: string, key?: string): Promise<void>;
  setMasterKey(masterKeyHex: string): Promise<void>;
  /** Lightweight connectivity probe for health checks. */
  checkHealth(): Promise<KafkaHealthStatus>;
}

/** Maximum number of nonces to track for deduplication (≈5 min at high throughput). */
const MAX_NONCE_CACHE_SIZE = 100_000;

/**
 * Periodically prune the nonce set to prevent unbounded growth.
 * Called on an interval aligned with the message age window.
 */
function startNoncePruning(seenNonces: Set<string>, intervalMs: number): NodeJS.Timeout {
  return setInterval(() => {
    // Simple strategy: clear the entire set periodically.
    // Messages older than MAX_MESSAGE_AGE_MS (5 min) are rejected by timestamp
    // check anyway, so clearing every 5 min is safe — any replayed nonce from
    // before the clear will fail the timestamp check.
    if (seenNonces.size > 0) {
      seenNonces.clear();
    }
  }, intervalMs);
}

// ---------------------------------------------------------------------------
// ProducerOutbox — standalone batching layer, independently testable
// ---------------------------------------------------------------------------

interface OutboxMessage {
  topic: string;
  message: { key?: string; value: string };
  resolve: () => void;
  reject: (err: unknown) => void;
}

/**
 * Flush callback signature accepted by ProducerOutbox.
 * Receives messages grouped by topic and must resolve/reject via the per-message promises.
 */
export type FlushCallback = (topicMessages: Array<{ topic: string; messages: Array<{ key?: string; value: string }> }>) => Promise<void>;

/**
 * Standalone producer outbox that batches messages by byte size and linger
 * timeout, then flushes via a caller-provided callback.
 *
 * Extracted from createKafkaClient so the batching logic (pending queue,
 * linger timer, byte accounting, flush mutex, drain guard) is independently
 * testable without a real Kafka producer.
 */
export class ProducerOutbox {
  private pending: OutboxMessage[] = [];
  private lingerTimer: NodeJS.Timeout | null = null;
  private currentBatchSizeBytes = 0;
  private draining = false;
  /** Mutex: the in-flight flush promise, if any. Prevents concurrent flushes from racing. */
  private flushInProgress: Promise<void> | null = null;

  constructor(
    private flushCb: FlushCallback,
    private batchSizeBytes: number,
    private lingerMs: number,
    private logger: Logger,
  ) {}

  /**
   * Enqueue a message for batched production. Returns a promise that resolves
   * when the message has been successfully flushed to the underlying transport.
   */
  enqueue(topic: string, value: string, key?: string): Promise<void> {
    if (this.draining) {
      return Promise.reject(new Error('Outbox is shutting down — cannot enqueue new messages'));
    }

    return new Promise<void>((resolve, reject) => {
      const msg = { key: key || undefined, value };
      const msgSize = Buffer.byteLength(value, 'utf8') + (key ? Buffer.byteLength(key, 'utf8') : 0);

      this.pending.push({ topic, message: msg, resolve, reject });
      this.currentBatchSizeBytes += msgSize;

      if (this.currentBatchSizeBytes >= this.batchSizeBytes) {
        this.flush().catch(err => this.logger.error({ err }, 'Failed to flush outbox on size limit'));
      } else if (!this.lingerTimer) {
        this.lingerTimer = setTimeout(() => {
          this.flush().catch(err => this.logger.error({ err }, 'Failed to flush outbox on linger timeout'));
        }, this.lingerMs);
      }
    });
  }

  /** Drain all pending messages, then mark the outbox as closed. */
  async drain(): Promise<void> {
    this.draining = true;
    await this.flush();
  }

  /** Flush all pending messages through the flush callback. */
  async flush(): Promise<void> {
    // If a flush is already in-flight, wait for it then re-check for
    // messages that accumulated during the wait.
    if (this.flushInProgress) {
      await this.flushInProgress;
    }
    if (this.pending.length === 0) return;
    this.flushInProgress = this.flushOnce().finally(() => { this.flushInProgress = null; });
    await this.flushInProgress;
  }

  private async flushOnce(): Promise<void> {
    if (this.lingerTimer) {
      clearTimeout(this.lingerTimer);
      this.lingerTimer = null;
    }
    if (this.pending.length === 0) return;

    const batch = [...this.pending];
    this.pending.length = 0;
    this.currentBatchSizeBytes = 0;

    // Group by topic for batch send
    const topicGroups = new Map<string, Array<{ key?: string; value: string }>>();
    for (const item of batch) {
      let group = topicGroups.get(item.topic);
      if (!group) {
        group = [];
        topicGroups.set(item.topic, group);
      }
      group.push(item.message);
    }

    try {
      await this.flushCb(
        Array.from(topicGroups.entries()).map(([topic, messages]) => ({ topic, messages })),
      );
      batch.forEach(item => item.resolve());
    } catch (err) {
      this.logger.error({ err }, 'Outbox batch flush failed');
      batch.forEach(item => item.reject(err));
    }
  }
}

// ---------------------------------------------------------------------------
// KafkaClient factory — composes transport + outbox + signing
// ---------------------------------------------------------------------------

/**
 * Default Kafka linger window in milliseconds.
 * Set to 100ms (2× the worker's 50ms output micro-batch interval) so that
 * worker batches accumulate into fewer Kafka batches. This alignment reduces
 * network syscalls and broker round-trips while keeping output delivery
 * latency predictable — a worker batch arriving just after a flush waits at
 * most 100ms instead of being flushed immediately as a single-message batch.
 */
const DEFAULT_KAFKA_LINGER_MS = 100;

export function createKafkaClient(
  brokers: string,
  username: string,
  password: string,
  signingKey: string,
  logger: Logger,
  deploymentId?: string,
  batchSize: number = 16384,
  lingerMs: number = DEFAULT_KAFKA_LINGER_MS,
): KafkaClient {
  const clientId = deploymentId ? `dac-orchestrator-${deploymentId}` : 'dac-orchestrator';
  const groupId = deploymentId ? `dac-orchestrator-${deploymentId}` : 'dac-orchestrator';

  const kafka = new Kafka({
    clientId,
    brokers: brokers.split(','),
    ssl: true,
    sasl: {
      mechanism: 'scram-sha-256',
      username,
      password,
    },
    logLevel: logLevel.WARN,
  });

  const producer = kafka.producer();
  const consumer = kafka.consumer({ groupId });
  let masterKey: Buffer | null = null;
  let connected = false;
  let lastProduceError: string | null = null;

  // Track produce errors to surface in health checks
  producer.on('producer.network.request_timeout', () => {
    lastProduceError = 'request_timeout';
  });

  // Nonce deduplication set — tracks recently verified nonces to reject duplicates
  const seenNonces = new Set<string>();
  let noncePruneTimer: NodeJS.Timeout | null = null;

  // Compose batching via ProducerOutbox
  const outbox = new ProducerOutbox(
    async (topicMessages) => {
      await producer.sendBatch({ topicMessages });
    },
    batchSize,
    lingerMs,
    logger,
  );

  function wrapHandler(handler: (topic: string, value: string, key?: string) => Promise<void>) {
    return async ({ topic, message }: EachMessagePayload) => {
      const raw = message.value?.toString();
      const key = message.key?.toString();
      if (!raw) return;

      let value: string;
      if (signingKey) {
        // Enforce nonce cache size limit
        if (seenNonces.size >= MAX_NONCE_CACHE_SIZE) {
          seenNonces.clear();
        }

        const verified = verifyKafkaMessage(raw, signingKey, topic, seenNonces);
        if (verified === null) {
          logger.warn({ topic, key }, 'Kafka message failed HMAC verification — routing to DLQ');

          // Route rejected messages to DLQ for audit
          try {
            const dlqPayload = JSON.stringify({
              originalTopic: topic,
              originalKey: key,
              rawMessage: raw.slice(0, 4096), // truncate to prevent DLQ bloat
              reason: 'hmac_verification_failed',
              rejectedAt: Date.now(),
            });
            // DLQ messages are signed but we don't verify DLQ on consume — it's for audit only
            const dlqValue = signingKey ? signKafkaMessage(dlqPayload, signingKey, TOPICS.DLQ) : dlqPayload;
            await producer.send({
              topic: TOPICS.DLQ,
              messages: [{ key: key || undefined, value: dlqValue }],
            });
          } catch (dlqErr) {
            logger.error({ err: dlqErr, topic, key }, 'Failed to route rejected message to DLQ');
          }
          return;
        }
        value = verified;
      } else {
        value = raw;
      }

      try {
        await handler(topic, value, key);
      } catch (err) {
        logger.error({ err, topic, key }, 'Kafka message handler error');
      }
    };
  }

  return {
    producer,
    consumer,

    async setMasterKey(masterKeyHex: string) {
      masterKey = await deriveMasterKeyAsync(masterKeyHex);
    },

    async connect() {
      await producer.connect();
      await consumer.connect();
      connected = true;
      lastProduceError = null;
      // Start nonce pruning (every 5 minutes, aligned with message age window)
      noncePruneTimer = startNoncePruning(seenNonces, 5 * 60 * 1000);
      logger.info({ clientId, groupId }, 'Kafka connected');
    },

    async disconnect() {
      connected = false;
      if (noncePruneTimer) clearInterval(noncePruneTimer);
      seenNonces.clear();
      try {
        await outbox.drain();
      } finally {
        await producer.disconnect();
        await consumer.disconnect();
        logger.info('Kafka disconnected');
      }
    },

    async subscribe(handler: (topic: string, value: string, key?: string) => Promise<void>) {
      await consumer.subscribe({ topic: TOPICS.EXEC_REQUESTS, fromBeginning: false });

      await consumer.run({
        eachMessage: wrapHandler(handler),
      });

      logger.info({ topic: TOPICS.EXEC_REQUESTS }, 'Kafka consumer subscribed');
    },

    async subscribeProjectTopic(topic: string, handler: (topic: string, value: string, key?: string) => Promise<void>) {
      await consumer.subscribe({ topic, fromBeginning: false });
      // Note: consumer.run() was already called in subscribe() — KafkaJS routes
      // new topic subscriptions through the existing eachMessage handler automatically
      // when using subscribe() before run(). For topics added after run(), we rely
      // on the regex/pattern approach or re-subscription. In practice, project topics
      // should be subscribed before consumer.run().
      logger.info({ topic }, 'Kafka consumer subscribed to project topic');
    },

    async produce(topic: string, value: string, key?: string) {
      const payload = signingKey ? signKafkaMessage(value, signingKey, topic) : value;
      return outbox.enqueue(topic, payload, key);
    },

    async produceEncrypted(topic: string, payload: unknown, tenantId: string, key?: string) {
      if (!masterKey) {
        // Fallback to plaintext if no master key configured
        await this.produce(topic, JSON.stringify(payload), key);
        return;
      }
      const envelope = sealEnvelope(tenantId, payload, masterKey);
      await producer.send({
        topic,
        messages: [{
          key: key || undefined,
          value: JSON.stringify(envelope),
        }],
      });
    },

    async checkHealth(): Promise<KafkaHealthStatus> {
      if (!connected) return { status: 'error', detail: 'not connected' };
      if (lastProduceError) return { status: 'error', detail: lastProduceError };
      return { status: 'ok' };
    },
  };
}

// No-op Kafka client for when Kafka is not configured (local testing)
export function createNoopKafkaClient(logger: Logger): KafkaClient {
  logger.info('Kafka disabled — using no-op client');

  return {
    producer: {} as Producer,
    consumer: {} as Consumer,
    async setMasterKey() {},
    async connect() {},
    async disconnect() {},
    async subscribe() {},
    async subscribeProjectTopic() {},
    async produce(topic: string, value: string) {
      logger.debug({ topic, valueLength: value.length }, 'No-op Kafka produce');
    },
    async produceEncrypted(topic: string, payload: unknown) {
      logger.debug({ topic }, 'No-op Kafka produceEncrypted');
    },
    async checkHealth(): Promise<KafkaHealthStatus> {
      return { status: 'disabled' };
    },
  };
}
