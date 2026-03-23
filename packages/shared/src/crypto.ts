import crypto from 'node:crypto';
import { promisify } from 'node:util';

const pbkdf2 = promisify(crypto.pbkdf2);

const ALGORITHM = 'aes-256-gcm';
const IV_LENGTH = 16;
const AUTH_TAG_LENGTH = 16;
const KEY_LENGTH = 32;
const SALT_LENGTH = 32;
const PBKDF2_ITERATIONS = 600_000;

/** Previous iteration count before OWASP 2024 bump; used to decrypt legacy data. */
const LEGACY_PBKDF2_ITERATIONS = 310_000;

export interface EncryptedPayload {
  encrypted: string; // hex
  iv: string;        // hex
  tag: string;       // hex (GCM auth tag)
  salt?: string;     // hex — per-instance PBKDF2 salt (absent in legacy data)
  iter?: number;     // PBKDF2 iteration count used to derive the key (absent in legacy data)
}

/**
 * Derive an AES-256 master key from a secret using PBKDF2-SHA256.
 *
 * Uses 600,000 iterations per OWASP 2024 recommendations for PBKDF2-SHA256,
 * with a per-instance random salt. When decrypting data encrypted with an
 * older iteration count, pass `iterations` explicitly so the same key is
 * derived.
 */
export function deriveMasterKey(
  secret: string,
  salt?: Buffer,
  iterations?: number,
): { key: Buffer; salt: Buffer; iterations: number } {
  const usedSalt = salt ?? crypto.randomBytes(SALT_LENGTH);
  const usedIter = iterations ?? PBKDF2_ITERATIONS;
  const key = crypto.pbkdf2Sync(secret, usedSalt, usedIter, KEY_LENGTH, 'sha256');
  return { key, salt: usedSalt, iterations: usedIter };
}

/**
 * Derive the master encryption key from a secret using PBKDF2 (async).
 * Uses the libuv thread pool so the event loop stays responsive.
 */
export function deriveMasterKeyAsync(secret: string, salt?: Buffer, iterations?: number): Promise<Buffer> {
  const usedSalt = salt ?? crypto.randomBytes(SALT_LENGTH);
  const usedIter = iterations ?? PBKDF2_ITERATIONS;
  return pbkdf2(secret, usedSalt, usedIter, KEY_LENGTH, 'sha256');
}

export function encrypt(plaintext: string, masterKey: Buffer, salt?: Buffer, iterations?: number): EncryptedPayload {
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
    salt: salt?.toString('hex'),
    iter: iterations,
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

const API_KEY_SALT_LENGTH = 16;

/**
 * Hash an API key with a per-key random salt using SHA-256.
 * Returns `salt_hex:hash_hex`. When verifying, pass the stored hash as
 * `existingHash` so the same salt is reused for comparison.
 */
export function hashApiKey(key: string, existingHash?: string): string {
  let salt: Buffer;
  if (existingHash && existingHash.includes(':')) {
    salt = Buffer.from(existingHash.split(':')[0]!, 'hex');
  } else {
    salt = crypto.randomBytes(API_KEY_SALT_LENGTH);
  }
  const hash = crypto.createHash('sha256').update(Buffer.concat([salt, Buffer.from(key)])).digest('hex');
  return `${salt.toString('hex')}:${hash}`;
}

/**
 * Verify an API key against a stored salted hash (salt_hex:hash_hex format).
 * Also accepts legacy unsalted hashes for backward compatibility.
 */
export function verifyApiKey(key: string, storedHash: string): boolean {
  if (storedHash.includes(':')) {
    // Salted format — recompute with the stored salt
    const computed = hashApiKey(key, storedHash);
    const a = Buffer.from(computed);
    const b = Buffer.from(storedHash);
    if (a.length !== b.length) return false;
    return crypto.timingSafeEqual(a, b);
  }
  // Legacy unsalted format
  const legacy = crypto.createHash('sha256').update(key).digest('hex');
  const a = Buffer.from(legacy);
  const b = Buffer.from(storedHash);
  if (a.length !== b.length) return false;
  return crypto.timingSafeEqual(a, b);
}

// ---------------------------------------------------------------------------
// Kafka message HMAC signing / verification
// ---------------------------------------------------------------------------

export interface SignedMessage {
  payload: string;
  signature: string;
  timestamp: number;
  /** Unique message nonce for deduplication within the replay window. */
  nonce: string;
  /** Topic the message was produced to — binds the signature to a specific topic. */
  topic: string;
}

/** Maximum age (ms) for a signed message before it's rejected (5 minutes). */
const MAX_MESSAGE_AGE_MS = 5 * 60 * 1000;

/**
 * Sign a Kafka message payload with HMAC-SHA256.
 * Wraps the original payload in a SignedMessage envelope.
 *
 * The signature covers: nonce, timestamp, topic, and payload — binding the
 * message to a specific topic so it cannot be replayed across topics.
 */
export function signKafkaMessage(payload: string, signingKey: string, topic: string): string {
  const timestamp = Date.now();
  const nonce = crypto.randomBytes(16).toString('hex');
  const data = `${nonce}.${timestamp}.${topic}.${payload}`;
  const signature = crypto.createHmac('sha256', signingKey).update(data).digest('hex');
  const envelope: SignedMessage = { payload, signature, timestamp, nonce, topic };
  return JSON.stringify(envelope);
}

/**
 * Verify and unwrap a signed Kafka message.
 * Returns the original payload string on success, or null if verification fails.
 *
 * @param raw - The raw JSON envelope string
 * @param signingKey - The HMAC signing key
 * @param expectedTopic - The topic the message was consumed from (must match envelope)
 * @param seenNonces - Optional set of recently-seen nonces for deduplication
 */
export function verifyKafkaMessage(
  raw: string,
  signingKey: string,
  expectedTopic?: string,
  seenNonces?: Set<string>,
): string | null {
  let envelope: SignedMessage;
  try {
    envelope = JSON.parse(raw);
  } catch {
    return null;
  }

  if (!envelope.payload || !envelope.signature || !envelope.timestamp) {
    return null;
  }

  // Reject messages older than MAX_MESSAGE_AGE_MS to prevent replay attacks
  const age = Date.now() - envelope.timestamp;
  if (age > MAX_MESSAGE_AGE_MS || age < -MAX_MESSAGE_AGE_MS) {
    return null;
  }

  // If the envelope includes topic binding, verify it matches the expected topic
  if (expectedTopic && envelope.topic && envelope.topic !== expectedTopic) {
    return null;
  }

  // Build the signed data — use topic-bound format if nonce/topic are present (v2),
  // otherwise fall back to legacy format for backward compatibility
  const hasV2Fields = !!envelope.nonce && !!envelope.topic;
  const data = hasV2Fields
    ? `${envelope.nonce}.${envelope.timestamp}.${envelope.topic}.${envelope.payload}`
    : `${envelope.timestamp}.${envelope.payload}`;

  const expected = crypto.createHmac('sha256', signingKey).update(data).digest('hex');

  // Constant-time comparison to prevent timing attacks
  const sigBuf = Buffer.from(envelope.signature, 'hex');
  const expBuf = Buffer.from(expected, 'hex');
  if (sigBuf.length !== expBuf.length || !crypto.timingSafeEqual(sigBuf, expBuf)) {
    return null;
  }

  // Nonce deduplication: reject if we've seen this nonce before
  if (hasV2Fields && seenNonces) {
    if (seenNonces.has(envelope.nonce)) {
      return null; // duplicate message
    }
    seenNonces.add(envelope.nonce);
  }

  return envelope.payload;
}

// ---------------------------------------------------------------------------
// Worker ↔ Orchestrator WebSocket message HMAC signing / verification
// ---------------------------------------------------------------------------

/**
 * Compute HMAC-SHA256 signature over a serialised message body.
 * The signature covers the sequence number and the full JSON payload to
 * prevent both tampering and replay/reorder attacks.
 */
export function signWorkerPayload(payload: string, seq: number, key: string): string {
  const data = `${seq}.${payload}`;
  return crypto.createHmac('sha256', key).update(data).digest('hex');
}

/**
 * Verify the HMAC-SHA256 signature of a worker message.
 * Returns true if the signature is valid, false otherwise.
 */
export function verifyWorkerPayload(payload: string, seq: number, signature: string, key: string): boolean {
  const data = `${seq}.${payload}`;
  const expected = crypto.createHmac('sha256', key).update(data).digest('hex');

  const sigBuf = Buffer.from(signature, 'hex');
  const expBuf = Buffer.from(expected, 'hex');
  if (sigBuf.length !== expBuf.length) return false;
  return crypto.timingSafeEqual(sigBuf, expBuf);
}
