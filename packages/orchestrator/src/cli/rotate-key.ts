#!/usr/bin/env node
/**
 * Rotate the master encryption key: re-encrypt all tenant credentials.
 *
 * Usage:
 *   MASTER_KEY=old_key NEW_MASTER_KEY=new_key npx tsx packages/orchestrator/src/cli/rotate-key.ts [dataDir]
 *
 * What it does:
 *   1. Derives old per-tenant keys from MASTER_KEY
 *   2. Derives new per-tenant keys from NEW_MASTER_KEY
 *   3. For each tenant DB, decrypts all credentials with old key, re-encrypts with new key
 *   4. Updates the encrypted_data/iv/tag in place
 *
 * After running, update MASTER_KEY in your environment to NEW_MASTER_KEY.
 */
import path from 'node:path';
import pino from 'pino';
import { deriveTenantKeyAsync, decrypt, encrypt, toEncryptedPayload } from '@dac-cloud/shared';
import { TenantDbManager } from '../tenantDbManager.js';

const logger = pino({ level: 'info' });

const oldMasterKey = process.env['MASTER_KEY'];
const newMasterKey = process.env['NEW_MASTER_KEY'];
const dataDir = process.argv[2] || path.join(process.cwd(), 'data');

if (!oldMasterKey || !newMasterKey) {
  console.error('Error: Both MASTER_KEY and NEW_MASTER_KEY environment variables are required.');
  process.exit(1);
}

if (oldMasterKey === newMasterKey) {
  console.error('Error: NEW_MASTER_KEY must differ from MASTER_KEY.');
  process.exit(1);
}

async function main() {
  logger.info({ dataDir }, 'Starting key rotation');

  const mgr = new TenantDbManager(dataDir, logger);
  const tenantIds = mgr.listTenantIds();

  logger.info({ tenantCount: tenantIds.length }, 'Found tenants');

  let totalRotated = 0;
  let totalFailed = 0;

  for (const tenantId of tenantIds) {
    const db = mgr.getTenantDb(tenantId);
    const { key: oldKey, elapsedMs: oldMs } = await deriveTenantKeyAsync(oldMasterKey!, tenantId);
    const { key: newKey, elapsedMs: newMs } = await deriveTenantKeyAsync(newMasterKey!, tenantId);
    logger.info({ tenantId, oldKeyMs: Math.round(oldMs), newKeyMs: Math.round(newMs) }, 'Derived tenant keys');

    const creds = db.prepare('SELECT id, name, encrypted_data AS encryptedData, iv, tag FROM persona_credentials').all() as Array<{
      id: string; name: string; encryptedData: string; iv: string; tag: string;
    }>;

    if (creds.length === 0) continue;

    logger.info({ tenantId, credentialCount: creds.length }, 'Rotating credentials');

    const updateStmt = db.prepare(
      'UPDATE persona_credentials SET encrypted_data = ?, iv = ?, tag = ?, updated_at = ? WHERE id = ?',
    );

    const rotateAll = db.transaction(() => {
      for (const cred of creds) {
        try {
          const plaintext = decrypt(toEncryptedPayload(cred), oldKey, { credentialId: cred.id, tenantId, resourceType: 'credential' });
          const newPayload = encrypt(plaintext, newKey);

          updateStmt.run(
            newPayload.encrypted,
            newPayload.iv,
            newPayload.tag,
            new Date().toISOString(),
            cred.id,
          );
          totalRotated++;
        } catch (err) {
          logger.error({ err, tenantId, credentialId: cred.id, credentialName: cred.name }, 'Failed to rotate credential');
          totalFailed++;
        }
      }
    });

    rotateAll();
  }

  console.log('\n=== Key Rotation Summary ===');
  console.log(`Tenants processed: ${tenantIds.length}`);
  console.log(`Credentials rotated: ${totalRotated}`);
  console.log(`Credentials failed: ${totalFailed}`);
  if (totalFailed === 0) {
    console.log('\nAll credentials rotated successfully.');
    console.log('Update MASTER_KEY in your environment to the new value.\n');
  } else {
    console.log('\nSome credentials failed to rotate. Investigate before updating MASTER_KEY.\n');
  }

  mgr.close();
}

main().catch((err) => {
  logger.fatal({ err }, 'Key rotation failed');
  process.exit(1);
});
