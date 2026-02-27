import 'dotenv/config';

export interface OrchestratorConfig {
  masterKey: string;
  claudeToken: string;
  teamApiKey: string;
  workerToken: string;
  kafkaBrokers: string;
  kafkaUsername: string;
  kafkaPassword: string;
  kafkaEnabled: boolean;
  supabaseJwtSecret: string;
  wsPort: number;
  httpPort: number;
}

function required(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return value;
}

function optional(name: string, fallback: string): string {
  return process.env[name] || fallback;
}

export function loadConfig(): OrchestratorConfig {
  const kafkaBrokers = process.env['KAFKA_BROKERS'] || '';

  return {
    masterKey: required('MASTER_KEY'),
    claudeToken: process.env['CLAUDE_TOKEN'] || '',  // Optional: use OAuth flow if not provided
    teamApiKey: required('TEAM_API_KEY'),
    workerToken: required('WORKER_TOKEN'),
    kafkaBrokers,
    kafkaUsername: process.env['KAFKA_USERNAME'] || '',
    kafkaPassword: process.env['KAFKA_PASSWORD'] || '',
    kafkaEnabled: kafkaBrokers.length > 0,
    supabaseJwtSecret: process.env['SUPABASE_JWT_SECRET'] || '',
    wsPort: parseInt(optional('WS_PORT', '8443'), 10),
    httpPort: parseInt(optional('HTTP_PORT', '3001'), 10),
  };
}
