import 'dotenv/config';
import { nanoid } from 'nanoid';

export interface WorkerConfig {
  orchestratorUrl: string;
  workerToken: string;
  workerId: string;
  // Image verification
  imageDigest: string;
  claudeCliVersion: string;
  // TLS (for connecting to orchestrator via wss://)
  tlsCaPath: string;
  tlsCertPath: string;
  tlsKeyPath: string;
  tlsRejectUnauthorized: boolean;
  // Health probe HTTP port (0 = disabled)
  healthPort: number;
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
    imageDigest: process.env['IMAGE_DIGEST'] || '',
    claudeCliVersion: process.env['CLAUDE_CLI_VERSION'] || '',
    tlsCaPath: process.env['TLS_CA_PATH'] || '',
    tlsCertPath: process.env['TLS_CERT_PATH'] || '',
    tlsKeyPath: process.env['TLS_KEY_PATH'] || '',
    tlsRejectUnauthorized: process.env['TLS_REJECT_UNAUTHORIZED'] !== 'false',
    healthPort: parseInt(process.env['HEALTH_PORT'] || '0', 10),
  };
}
