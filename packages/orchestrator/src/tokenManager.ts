import { deriveMasterKey, encrypt, decrypt, type EncryptedPayload } from '@dac-cloud/shared';
import type { Logger } from 'pino';

export function createTokenManager(masterKeyHex: string, logger: Logger) {
  let storedToken: EncryptedPayload | null = null;

  return {
    storeClaudeToken(plainToken: string): void {
      const { key, salt, iterations } = deriveMasterKey(masterKeyHex);
      storedToken = encrypt(plainToken, key, salt, iterations);
      logger.info('Claude token stored (encrypted)');
    },

    getClaudeToken(): string | null {
      if (!storedToken) return null;
      try {
        const salt = storedToken.salt ? Buffer.from(storedToken.salt, 'hex') : undefined;
        const { key } = deriveMasterKey(masterKeyHex, salt, storedToken.iter);
        return decrypt(storedToken, key);
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
