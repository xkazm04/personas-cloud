import { deriveMasterKey, encrypt, decrypt, type EncryptedPayload } from '@dac-cloud/shared';
import type { Logger } from 'pino';

export function createTokenManager(masterKeyHex: string, logger: Logger) {
  const masterKey = deriveMasterKey(masterKeyHex);
  let storedToken: EncryptedPayload | null = null;

  return {
    storeClaudeToken(plainToken: string): void {
      storedToken = encrypt(plainToken, masterKey);
      logger.info('Claude token stored (encrypted)');
    },

    getClaudeToken(): string | null {
      if (!storedToken) return null;
      try {
        return decrypt(storedToken, masterKey);
      } catch (err) {
        logger.error({ err }, 'Failed to decrypt Claude token');
        return null;
      }
    },

    hasToken(): boolean {
      return storedToken !== null;
    },

    clearToken(): void {
      storedToken = null;
      logger.info('Claude token cleared');
    },
  };
}

export type TokenManager = ReturnType<typeof createTokenManager>;
