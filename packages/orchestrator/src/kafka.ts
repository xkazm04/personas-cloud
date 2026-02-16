import { Kafka, type Producer, type Consumer, type EachMessagePayload, logLevel } from 'kafkajs';
import { TOPICS } from '@dac-cloud/shared';
import type { Logger } from 'pino';

export interface KafkaClient {
  producer: Producer;
  consumer: Consumer;
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  subscribe(handler: (topic: string, value: string, key?: string) => Promise<void>): Promise<void>;
  produce(topic: string, value: string, key?: string): Promise<void>;
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

  return {
    producer,
    consumer,

    async connect() {
      await producer.connect();
      await consumer.connect();
      logger.info('Kafka connected');
    },

    async disconnect() {
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
  };
}

// No-op Kafka client for when Kafka is not configured (local testing)
export function createNoopKafkaClient(logger: Logger): KafkaClient {
  logger.info('Kafka disabled — using no-op client');

  return {
    producer: {} as Producer,
    consumer: {} as Consumer,
    async connect() {},
    async disconnect() {},
    async subscribe() {},
    async produce(topic: string, value: string) {
      logger.debug({ topic, valueLength: value.length }, 'No-op Kafka produce');
    },
  };
}
