import { hashApiKey } from '@dac-cloud/shared';
import type { IncomingMessage } from 'node:http';

export function createAuth(teamApiKey: string) {
  const expectedHash = hashApiKey(teamApiKey);

  return {
    validateRequest(req: IncomingMessage): boolean {
      const authHeader = req.headers['authorization'];
      if (!authHeader) return false;

      const parts = authHeader.split(' ');
      if (parts.length !== 2 || parts[0] !== 'Bearer') return false;

      const providedHash = hashApiKey(parts[1]!);
      return providedHash === expectedHash;
    },

    validateToken(token: string): boolean {
      return hashApiKey(token) === expectedHash;
    },

    validateWorkerToken(token: string, expectedWorkerToken: string): boolean {
      return token === expectedWorkerToken;
    },
  };
}

export type Auth = ReturnType<typeof createAuth>;
