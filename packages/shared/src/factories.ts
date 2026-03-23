import { nanoid } from 'nanoid';
import type { Persona, PersonaToolDefinition } from './types.js';

/**
 * Input type for creating a persona — all fields optional except those
 * that have no sensible default (none currently).
 */
export type CreatePersonaInput = Partial<Persona> & {
  /** When provided by an authenticated user context, overrides body.projectId. */
  projectIdOverride?: string;
};

/**
 * Creates a fully-populated Persona by applying defaults to partial input.
 * This is the single source of truth for persona field defaults.
 */
export function createPersonaWithDefaults(input: CreatePersonaInput): Persona {
  const now = new Date().toISOString();
  return {
    id: input.id ?? nanoid(),
    projectId: input.projectIdOverride ?? input.projectId ?? 'default',
    name: input.name ?? 'Untitled',
    description: input.description ?? null,
    systemPrompt: input.systemPrompt ?? '',
    structuredPrompt: input.structuredPrompt ?? null,
    icon: input.icon ?? null,
    color: input.color ?? null,
    enabled: input.enabled ?? true,
    maxConcurrent: input.maxConcurrent ?? 1,
    timeoutMs: input.timeoutMs ?? 300_000,
    inferenceProfileId: input.inferenceProfileId ?? null,
    networkPolicy: typeof input.networkPolicy === 'object'
      ? JSON.stringify(input.networkPolicy)
      : (input.networkPolicy ?? null),
    maxBudgetUsd: input.maxBudgetUsd ?? null,
    maxTurns: input.maxTurns ?? null,
    designContext: input.designContext ?? null,
    groupId: input.groupId ?? null,
    createdAt: input.createdAt ?? now,
    updatedAt: now,
  };
}

/**
 * Creates a fully-populated PersonaToolDefinition by applying defaults to partial input.
 */
export function createToolDefinitionWithDefaults(input: Partial<PersonaToolDefinition>): PersonaToolDefinition {
  const now = new Date().toISOString();
  return {
    id: input.id ?? nanoid(),
    name: input.name ?? 'unnamed',
    category: input.category ?? 'general',
    description: input.description ?? '',
    scriptPath: input.scriptPath ?? '',
    inputSchema: input.inputSchema ?? null,
    outputSchema: input.outputSchema ?? null,
    requiresCredentialType: input.requiresCredentialType ?? null,
    implementationGuide: input.implementationGuide ?? null,
    isBuiltin: input.isBuiltin ?? false,
    createdAt: input.createdAt ?? now,
    updatedAt: now,
  };
}
