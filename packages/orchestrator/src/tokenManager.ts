import { deriveMasterKeyAsync, encrypt, decrypt, type EncryptedPayload } from '@dac-cloud/shared';
import type { Logger } from 'pino';

export interface TokenManagerHealth {
  hasToken: boolean;
  consecutiveDecryptionFailures: number;
}

export async function createTokenManager(masterKeyHex: string, logger: Logger) {
  const masterKey = await deriveMasterKeyAsync(masterKeyHex);
  let storedToken: EncryptedPayload | null = null;
  let decryptedCache: string | null = null;
  let consecutiveDecryptionFailures = 0;

  return {
    storeClaudeToken(plainToken: string): void {
      storedToken = encrypt(plainToken, masterKey);
      decryptedCache = null;
      consecutiveDecryptionFailures = 0;
      logger.info('Claude token stored (encrypted)');
    },

    getClaudeToken(): string | null {
      if (!storedToken) return null;
      if (decryptedCache !== null) return decryptedCache;
      try {
        decryptedCache = decrypt(storedToken, masterKey, { resourceType: 'claude-token' });
        consecutiveDecryptionFailures = 0;
        return decryptedCache;
      } catch (err) {
        consecutiveDecryptionFailures++;
        logger.error({ err, consecutiveDecryptionFailures }, 'Failed to decrypt Claude token');
        return null;
      }
    },

    hasToken(): boolean {
      return storedToken !== null;
    },

    clearToken(): void {
      storedToken = null;
      decryptedCache = null;
      consecutiveDecryptionFailures = 0;
      logger.info('Claude token cleared');
    },

    getHealth(): TokenManagerHealth {
      return {
        hasToken: storedToken !== null,
        consecutiveDecryptionFailures,
      };
    },
  };
}

export type TokenManager = Awaited<ReturnType<typeof createTokenManager>>;
