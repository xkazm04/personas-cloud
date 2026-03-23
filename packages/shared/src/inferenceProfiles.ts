// ---------------------------------------------------------------------------
// Inference profile presets, validation, and env resolution
// ---------------------------------------------------------------------------

import type { InferenceProfile, InferenceProvider, InferenceProviderEnvMapping } from './types.js';

export interface EnvOverrides {
  set: Record<string, string>;
  remove: string[];
}

const CLAUDE_REMOVALS = ['CLAUDECODE', 'CLAUDE_CODE'];
const NON_CLAUDE_REMOVALS = ['ANTHROPIC_API_KEY'];

type PresetDef = Pick<InferenceProfile, 'name' | 'provider' | 'envMappings' | 'removeEnvKeys' | 'isPreset'>;

export const PRESET_PROFILES: Record<string, PresetDef> = {
  'claude-default': {
    name: 'Claude (Default)',
    provider: 'claude',
    envMappings: [],
    removeEnvKeys: CLAUDE_REMOVALS,
    isPreset: true,
  },
  'ollama-local': {
    name: 'Ollama (Local)',
    provider: 'ollama',
    envMappings: [
      { envVar: 'OLLAMA_BASE_URL', source: 'baseUrl' },
      { envVar: 'OLLAMA_API_KEY', source: 'authToken' },
    ],
    removeEnvKeys: NON_CLAUDE_REMOVALS,
    isPreset: true,
  },
  'litellm-proxy': {
    name: 'LiteLLM Proxy',
    provider: 'litellm',
    envMappings: [
      { envVar: 'ANTHROPIC_BASE_URL', source: 'baseUrl' },
      { envVar: 'ANTHROPIC_AUTH_TOKEN', source: 'authToken' },
    ],
    removeEnvKeys: NON_CLAUDE_REMOVALS,
    isPreset: true,
  },
  'openai-compatible': {
    name: 'OpenAI-Compatible',
    provider: 'openai-compatible',
    envMappings: [
      { envVar: 'OPENAI_BASE_URL', source: 'baseUrl' },
      { envVar: 'OPENAI_API_KEY', source: 'authToken' },
    ],
    removeEnvKeys: NON_CLAUDE_REMOVALS,
    isPreset: true,
  },
};

const VALID_PROVIDERS: InferenceProvider[] = ['claude', 'ollama', 'litellm', 'openai-compatible', 'custom'];

export function validateProfile(profile: Partial<InferenceProfile>): string[] {
  const errors: string[] = [];
  if (!profile.name) errors.push('name is required');
  if (!profile.provider) {
    errors.push('provider is required');
  } else if (!VALID_PROVIDERS.includes(profile.provider)) {
    errors.push(`provider must be one of: ${VALID_PROVIDERS.join(', ')}`);
  }
  if (profile.provider !== 'claude' && !profile.baseUrl) {
    errors.push('baseUrl is required for non-claude providers');
  }
  return errors;
}

export function resolveEnvOverrides(profile: InferenceProfile, decryptedAuthToken?: string): EnvOverrides {
  const set: Record<string, string> = {};
  const remove = [...(profile.removeEnvKeys ?? [])];

  for (const mapping of profile.envMappings ?? []) {
    const value = resolveSource(profile, mapping.source, decryptedAuthToken);
    if (value) {
      set[mapping.envVar] = value;
    }
  }

  return { set, remove };
}

function resolveSource(profile: InferenceProfile, source: InferenceProviderEnvMapping['source'], decryptedAuthToken?: string): string | undefined {
  switch (source) {
    case 'baseUrl': return profile.baseUrl;
    case 'authToken': return decryptedAuthToken;
    case 'model': return profile.model;
  }
}

