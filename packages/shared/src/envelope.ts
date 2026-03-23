import crypto from 'node:crypto';

export interface EncryptedEnvelope {
  version: 1;
  tenantId: string;
  keyId: string;
  encryptedPayload: string; // hex
  iv: string;               // hex
  tag: string;              // hex
}

const ALGORITHM = 'aes-256-gcm';
const IV_LENGTH = 16;

/**
 * Derive a per-tenant data encryption key from the master key using HKDF.
 */
function deriveTenantDataKey(masterKey: Buffer, tenantId: string): Buffer {
  return Buffer.from(crypto.hkdfSync('sha256', masterKey, 'dac-envelope-v1', tenantId, 32));
}

/**
 * Encrypt a Kafka message payload for a specific tenant using
 * envelope encryption with a per-tenant derived key.
 */
export function sealEnvelope(
  tenantId: string,
  payload: unknown,
  masterKey: Buffer,
): EncryptedEnvelope {
  const tenantKey = deriveTenantDataKey(masterKey, tenantId);
  const iv = crypto.randomBytes(IV_LENGTH);
  const plaintext = JSON.stringify(payload);

  const cipher = crypto.createCipheriv(ALGORITHM, tenantKey, iv);
  let encrypted = cipher.update(plaintext, 'utf8', 'hex');
  encrypted += cipher.final('hex');
  const tag = cipher.getAuthTag();

  return {
    version: 1,
    tenantId,
    keyId: `tenant:${tenantId.substring(0, 8)}`,
    encryptedPayload: encrypted,
    iv: iv.toString('hex'),
    tag: tag.toString('hex'),
  };
}

/**
 * Decrypt a Kafka message envelope. Optionally verifies the tenantId
 * matches the expected tenant.
 */
export function openEnvelope(
  envelope: EncryptedEnvelope,
  masterKey: Buffer,
  expectedTenantId?: string,
): unknown {
  if (expectedTenantId && envelope.tenantId !== expectedTenantId) {
    throw new Error(`Envelope tenant mismatch: expected ${expectedTenantId}, got ${envelope.tenantId}`);
  }

  const tenantKey = deriveTenantDataKey(masterKey, envelope.tenantId);
  const iv = Buffer.from(envelope.iv, 'hex');
  const tag = Buffer.from(envelope.tag, 'hex');

  const decipher = crypto.createDecipheriv(ALGORITHM, tenantKey, iv);
  decipher.setAuthTag(tag);

  let decrypted = decipher.update(envelope.encryptedPayload, 'hex', 'utf8');
  decrypted += decipher.final('utf8');

  return JSON.parse(decrypted);
}
