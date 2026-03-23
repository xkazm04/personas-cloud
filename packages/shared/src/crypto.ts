import crypto from 'node:crypto';
import { promisify } from 'node:util';

const pbkdf2 = promisify(crypto.pbkdf2);

const ALGORITHM = 'aes-256-gcm';
const IV_LENGTH = 16;
const AUTH_TAG_LENGTH = 16;
const KEY_LENGTH = 32;
const PBKDF2_ITERATIONS = 100_000;
const SALT = 'dac-cloud-credentials-v1';

export interface EncryptedPayload {
  encrypted: string; // hex
  iv: string;        // hex
  tag: string;       // hex (GCM auth tag)
}

/**
 * Derive the master encryption key from a secret using PBKDF2.
 * Uses the libuv thread pool so the event loop stays responsive.
 */
export function deriveMasterKeyAsync(secret: string): Promise<Buffer> {
  return pbkdf2(secret, SALT, PBKDF2_ITERATIONS, KEY_LENGTH, 'sha256');
}

export function encrypt(plaintext: string, masterKey: Buffer): EncryptedPayload {
  const iv = crypto.randomBytes(IV_LENGTH);
  const cipher = crypto.createCipheriv(ALGORITHM, masterKey, iv, {
    authTagLength: AUTH_TAG_LENGTH,
  });

  let encrypted = cipher.update(plaintext, 'utf8', 'hex');
  encrypted += cipher.final('hex');
  const tag = cipher.getAuthTag().toString('hex');

  return {
    encrypted,
    iv: iv.toString('hex'),
    tag,
  };
}

/**
 * Thrown when AES-GCM decryption fails (bad key, tampered ciphertext, or corrupted data).
 * Carries identity context so callers can produce diagnostic logs and audit events.
 */
export class DecryptionError extends Error {
  constructor(
    public readonly cause: unknown,
    public readonly context: { credentialId?: string; tenantId?: string; resourceType?: string },
  ) {
    const parts: string[] = ['AES-GCM decryption failed'];
    if (context.tenantId) parts.push(`tenant=${context.tenantId}`);
    if (context.credentialId) parts.push(`credential=${context.credentialId}`);
    if (context.resourceType) parts.push(`resource=${context.resourceType}`);
    super(parts.join(' '));
    this.name = 'DecryptionError';
  }
}

/**
 * Convert a credential row (encryptedData/iv/tag) to the EncryptedPayload shape
 * expected by decrypt(). Bridges the DB column naming and the crypto interface.
 */
export function toEncryptedPayload(cred: { encryptedData: string; iv: string; tag: string }): EncryptedPayload {
  return { encrypted: cred.encryptedData, iv: cred.iv, tag: cred.tag };
}

export function decrypt(
  payload: EncryptedPayload,
  masterKey: Buffer,
  context?: { credentialId?: string; tenantId?: string; resourceType?: string },
): string {
  try {
    const iv = Buffer.from(payload.iv, 'hex');
    const tag = Buffer.from(payload.tag, 'hex');
    const decipher = crypto.createDecipheriv(ALGORITHM, masterKey, iv, {
      authTagLength: AUTH_TAG_LENGTH,
    });
    decipher.setAuthTag(tag);

    let decrypted = decipher.update(payload.encrypted, 'hex', 'utf8');
    decrypted += decipher.final('utf8');
    return decrypted;
  } catch (err) {
    throw new DecryptionError(err, context ?? {});
  }
}

export interface KeyDerivationResult {
  key: Buffer;
  elapsedMs: number;
}

const PBKDF2_SLOW_THRESHOLD_MS = 500;

/**
 * Derive a per-tenant encryption key from the master secret.
 * Uses a tenant-specific salt to ensure each tenant gets a unique key.
 * Offloads PBKDF2 to the libuv thread pool so the event loop stays responsive.
 * Returns timing info alongside the key so callers can log PBKDF2 slowdowns.
 */
export async function deriveTenantKeyAsync(masterSecret: string, tenantId: string): Promise<KeyDerivationResult> {
  const start = performance.now();
  const key = await pbkdf2(masterSecret, `dac-tenant-v1:${tenantId}`, PBKDF2_ITERATIONS, KEY_LENGTH, 'sha256');
  const elapsedMs = performance.now() - start;
  if (elapsedMs >= PBKDF2_SLOW_THRESHOLD_MS) {
    process.emitWarning(
      `PBKDF2 deriveTenantKeyAsync took ${Math.round(elapsedMs)}ms for tenant=${tenantId} (threshold=${PBKDF2_SLOW_THRESHOLD_MS}ms)`,
      'PerformanceWarning',
    );
  }
  return { key, elapsedMs };
}

export function generateApiKey(): string {
  return `dac_${crypto.randomBytes(24).toString('hex')}`;
}

export function hashApiKey(key: string): string {
  return crypto.createHash('sha256').update(key).digest('hex');
}
