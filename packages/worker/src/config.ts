import 'dotenv/config';
import { nanoid } from 'nanoid';

export interface WorkerConfig {
  orchestratorUrl: string;
  workerToken: string;
  workerId: string;
}

function required(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return value;
}

export function loadConfig(): WorkerConfig {
  return {
    orchestratorUrl: required('ORCHESTRATOR_URL'),
    workerToken: required('WORKER_TOKEN'),
    workerId: process.env['WORKER_ID'] || `worker-${nanoid(8)}`,
  };
}
