// ---------------------------------------------------------------------------
// Dispatch pipeline — composable stages for worker dispatch
//
// Each stage is a standalone async function with typed inputs/outputs,
// independently testable without mocking the entire Dispatcher.
//
// Pipeline: ResolveToken → ResolvePersona → AssemblePayload → BuildAssignment
// ---------------------------------------------------------------------------

import type { ExecAssign, ExecRequest, NetworkPolicy, Persona, PersonaToolDefinition, EncryptedPayload } from '@dac-cloud/shared';
import { assemblePrompt, resolveEnvOverrides, toEncryptedPayload, parseNetworkPolicy } from '@dac-cloud/shared';
import type Database from 'better-sqlite3';
import type { TokenManager } from './tokenManager.js';
import type { OAuthManager } from './oauth.js';
import type { Logger } from 'pino';
import * as db from './db/index.js';

// ========================== Stage result types =============================

export interface TokenResult {
  claudeToken: string;
}

export interface PersonaResolution {
  persona: Persona | null;
  tools: PersonaToolDefinition[];
  credentialHints: string[];
  credentialEnv: Record<string, string>;
  inferenceEnvSet: Record<string, string>;
  inferenceEnvRemove: string[];
}

export interface AssembledPayload {
  prompt: string;
  env: Record<string, string>;
  networkPolicy: NetworkPolicy | undefined;
  maxBudgetUsd: number | null;
  maxTurns: number | null;
}

// ========================= Dependency interfaces ===========================

/** Minimal decryption capability — abstracts master/tenant key resolution. */
export interface Decryptor {
  decrypt(
    payload: EncryptedPayload,
    tenantId: string,
    resourceId: string,
    logContext: Record<string, unknown>,
    logMessage: string,
  ): Promise<string | undefined>;
}

// =========================== Stage 1: Token ================================

export async function resolveToken(
  oauth: OAuthManager | null,
  tokenManager: TokenManager,
): Promise<TokenResult | null> {
  let claudeToken: string | null = null;

  if (oauth?.hasTokens()) {
    claudeToken = await oauth.getValidAccessToken();
    if (claudeToken) {
      tokenManager.storeClaudeToken(claudeToken);
    }
  }

  if (!claudeToken) {
    claudeToken = tokenManager.getClaudeToken();
  }

  return claudeToken ? { claudeToken } : null;
}

// ========================== Stage 2: Persona ===============================

export async function resolvePersona(
  dispatchDb: Database.Database | null,
  personaId: string,
  tenantId: string,
  decryptor: Decryptor,
  logger: Logger,
): Promise<PersonaResolution> {
  const empty: PersonaResolution = {
    persona: null,
    tools: [],
    credentialHints: [],
    credentialEnv: {},
    inferenceEnvSet: {},
    inferenceEnvRemove: [],
  };

  if (!dispatchDb) return empty;

  const persona = db.getPersona(dispatchDb, personaId);
  if (!persona) return empty;

  const tools = db.getToolsForPersona(dispatchDb, persona.id);
  const creds = db.listCredentialsForPersona(dispatchDb, persona.id);

  // Decrypt credentials
  const credentialHints: string[] = [];
  const credentialEnv: Record<string, string> = {};

  for (const cred of creds) {
    const envName = `CONNECTOR_${cred.name.toUpperCase().replace(/[^A-Z0-9]/g, '_')}`;
    credentialHints.push(envName);

    const decrypted = await decryptor.decrypt(
      toEncryptedPayload(cred),
      tenantId,
      cred.id,
      { credentialId: cred.id, credentialName: cred.name },
      'Failed to decrypt credential',
    );

    if (decrypted !== undefined) {
      try {
        const fields = JSON.parse(decrypted) as Record<string, string>;
        for (const [fieldKey, fieldValue] of Object.entries(fields)) {
          const fieldEnvName = `CONNECTOR_${cred.name.toUpperCase().replace(/[^A-Z0-9]/g, '_')}_${fieldKey.toUpperCase().replace(/[^A-Z0-9]/g, '_')}`;
          credentialEnv[fieldEnvName] = String(fieldValue);
        }
      } catch {
        credentialEnv[envName] = decrypted;
      }
    }
  }

  // Resolve inference profile env overrides
  let inferenceEnvSet: Record<string, string> = {};
  let inferenceEnvRemove: string[] = [];

  if (persona.inferenceProfileId) {
    const inferenceProfile = db.getInferenceProfile(dispatchDb, persona.inferenceProfileId);
    if (inferenceProfile) {
      let decryptedAuthToken: string | undefined;
      if (inferenceProfile.authTokenEncrypted && inferenceProfile.authTokenIv && inferenceProfile.authTokenTag) {
        decryptedAuthToken = await decryptor.decrypt(
          { encrypted: inferenceProfile.authTokenEncrypted, iv: inferenceProfile.authTokenIv, tag: inferenceProfile.authTokenTag },
          tenantId,
          `inference-profile:${inferenceProfile.id}`,
          { profileId: inferenceProfile.id },
          'Failed to decrypt inference profile auth token',
        );
        decryptedAuthToken = decryptedAuthToken ?? undefined;
      }
      const overrides = resolveEnvOverrides(inferenceProfile, decryptedAuthToken);
      inferenceEnvSet = overrides.set;
      inferenceEnvRemove = overrides.remove;
    }
  }

  logger.info({ personaId: persona.id, toolCount: tools.length, credCount: creds.length }, 'Assembled prompt from DB persona');

  return { persona, tools, credentialHints, credentialEnv, inferenceEnvSet, inferenceEnvRemove };
}

// ======================== Stage 3: Assemble ================================

export function assemblePayload(
  request: ExecRequest,
  token: TokenResult,
  resolution: PersonaResolution,
): AssembledPayload {
  // Build env map: API key + credential env + inference overrides
  const env: Record<string, string> = {
    ANTHROPIC_API_KEY: token.claudeToken,
    ...resolution.credentialEnv,
    ...resolution.inferenceEnvSet,
  };

  // Remove inference-specified keys
  for (const k of resolution.inferenceEnvRemove) {
    delete env[k];
  }

  // Build prompt
  let prompt: string;
  if (resolution.persona) {
    const inputData = request.inputData ? (request.inputData as Record<string, unknown>) : undefined;
    prompt = assemblePrompt(
      resolution.persona,
      resolution.tools,
      inputData,
      resolution.credentialHints.length > 0 ? resolution.credentialHints : undefined,
    );
  } else {
    prompt = request.prompt;
  }

  // Resolve network policy
  let networkPolicy: NetworkPolicy | undefined;
  if (resolution.persona?.networkPolicy) {
    networkPolicy = parseNetworkPolicy(resolution.persona.networkPolicy) ?? undefined;
  }

  return {
    prompt,
    env,
    networkPolicy,
    maxBudgetUsd: resolution.persona?.maxBudgetUsd ?? null,
    maxTurns: resolution.persona?.maxTurns ?? null,
  };
}

// =================== Stage 4: Build assignment =============================

export function buildAssignment(
  request: ExecRequest,
  payload: AssembledPayload,
): ExecAssign {
  return {
    type: 'assign',
    executionId: request.executionId,
    personaId: request.personaId,
    prompt: payload.prompt,
    env: payload.env,
    config: {
      timeoutMs: request.config.timeoutMs || 300_000,
      maxOutputBytes: 10 * 1024 * 1024,
      ...(payload.networkPolicy ? { networkPolicy: payload.networkPolicy } : {}),
      ...(payload.maxBudgetUsd ? { maxBudgetUsd: payload.maxBudgetUsd } : {}),
      ...(payload.maxTurns ? { maxTurns: payload.maxTurns } : {}),
    },
  };
}

// ============== ExecutionPreparer — stages 2-4 composed ====================

/** Resolves a DB handle for a given project/tenant ID. */
export type DbResolver = (projectId?: string) => Database.Database | null;

/**
 * Composes persona resolution, payload assembly, and assignment building
 * into a single prepare() call. Token acquisition and the actual send
 * remain in the Dispatcher.
 */
export class ExecutionPreparer {
  constructor(
    private resolveDb: DbResolver,
    private decryptor: Decryptor,
    private logger: Logger,
  ) {}

  /**
   * Prepare an ExecAssign from a request and a pre-resolved token.
   * Pure enrichment — no side effects beyond DB reads and decryption.
   */
  async prepare(request: ExecRequest, token: TokenResult): Promise<ExecAssign> {
    const tenantId = request.projectId || 'default';
    const dispatchDb = this.resolveDb(tenantId);

    const resolution = await resolvePersona(
      dispatchDb,
      request.personaId,
      tenantId,
      this.decryptor,
      this.logger,
    );

    const payload = assemblePayload(request, token, resolution);
    return buildAssignment(request, payload);
  }
}
