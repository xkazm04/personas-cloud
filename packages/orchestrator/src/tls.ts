import fs from 'node:fs';
import type { Logger } from 'pino';

export interface TlsConfig {
  enabled: boolean;
  certPath: string;
  keyPath: string;
  caPath?: string;
  requireClientCert: boolean;
}

export function loadTlsConfig(logger: Logger): TlsConfig {
  const enabled = process.env['TLS_ENABLED'] !== 'false';
  const certPath = process.env['TLS_CERT_PATH'] || '';
  const keyPath = process.env['TLS_KEY_PATH'] || '';
  const caPath = process.env['TLS_CA_PATH'] || '';
  const requireClientCert = process.env['TLS_REQUIRE_CLIENT_CERT'] === 'true';

  if (enabled) {
    if (!certPath || !keyPath) {
      logger.warn('TLS_ENABLED=true but TLS_CERT_PATH or TLS_KEY_PATH not set — falling back to plaintext WS');
      return { enabled: false, certPath: '', keyPath: '', requireClientCert: false };
    }

    if (!fs.existsSync(certPath)) {
      throw new Error(`TLS certificate not found at: ${certPath}`);
    }
    if (!fs.existsSync(keyPath)) {
      throw new Error(`TLS key not found at: ${keyPath}`);
    }
    if (caPath && !fs.existsSync(caPath)) {
      throw new Error(`TLS CA certificate not found at: ${caPath}`);
    }

    logger.info({ certPath, keyPath, caPath: caPath || '(none)', requireClientCert }, 'TLS configuration loaded');
  }

  return {
    enabled,
    certPath,
    keyPath,
    caPath: caPath || undefined,
    requireClientCert,
  };
}
