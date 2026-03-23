import { Kafka, type Producer, type Consumer, type EachMessagePayload, logLevel } from 'kafkajs';
import { TOPICS, sealEnvelope, deriveMasterKeyAsync } from '@dac-cloud/shared';
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
  produce(topic: string, value: string, key?: string): Promise<void>;
  /** Produce a message encrypted with per-tenant envelope encryption. */
  produceEncrypted(topic: string, payload: unknown, tenantId: string, key?: string): Promise<void>;
  setMasterKey(masterKeyHex: string): Promise<void>;
  /** Lightweight connectivity probe for health checks. */
  checkHealth(): Promise<KafkaHealthStatus>;
}

export function createKafkaClient(
  brokers: string,
  username: string,
  password: string,
  logger: Logger,
): KafkaClient {
  const kafka = new Kafka({
    clientId: 'dac-orchestrator',
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
  const consumer = kafka.consumer({ groupId: 'dac-orchestrator' });
  let masterKey: Buffer | null = null;
  let connected = false;
  let lastProduceError: string | null = null;

  // Track produce errors to surface in health checks
  producer.on('producer.network.request_timeout', () => {
    lastProduceError = 'request_timeout';
  });

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
      logger.info('Kafka connected');
    },

    async disconnect() {
      connected = false;
      await producer.disconnect();
      await consumer.disconnect();
      logger.info('Kafka disconnected');
    },

    async subscribe(handler: (topic: string, value: string, key?: string) => Promise<void>) {
      await consumer.subscribe({ topic: TOPICS.EXEC_REQUESTS, fromBeginning: false });

      await consumer.run({
        eachMessage: async ({ topic, message }: EachMessagePayload) => {
          const value = message.value?.toString();
          const key = message.key?.toString();
          if (value) {
            try {
              await handler(topic, value, key);
            } catch (err) {
              logger.error({ err, topic, key }, 'Kafka message handler error');
            }
          }
        },
      });

      logger.info({ topic: TOPICS.EXEC_REQUESTS }, 'Kafka consumer subscribed');
    },

    async produce(topic: string, value: string, key?: string) {
      await producer.send({
        topic,
        messages: [{
          key: key || undefined,
          value,
        }],
      });
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
