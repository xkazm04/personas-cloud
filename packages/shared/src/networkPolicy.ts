// ---------------------------------------------------------------------------
// Network policy types, validation, and host matching
// ---------------------------------------------------------------------------

export interface NetworkPolicyRule {
  host: string;       // "api.anthropic.com" or "*.github.com"
  ports?: number[];   // [443] — empty/undefined = all ports
}

export interface NetworkPolicy {
  mode: 'deny-all' | 'allow-list';
  allowedHosts?: NetworkPolicyRule[];
}

export const DEFAULT_DENY_ALL: NetworkPolicy = { mode: 'deny-all' };

export const PRESET_POLICIES: Record<string, NetworkPolicy> = {
  'claude-only': {
    mode: 'allow-list',
    allowedHosts: [
      { host: 'api.anthropic.com', ports: [443] },
      { host: '*.anthropic.com', ports: [443] },
    ],
  },
  'claude-and-github': {
    mode: 'allow-list',
    allowedHosts: [
      { host: 'api.anthropic.com', ports: [443] },
      { host: '*.anthropic.com', ports: [443] },
      { host: 'github.com', ports: [443] },
      { host: 'api.github.com', ports: [443] },
    ],
  },
};

export function parseNetworkPolicy(json: string | null): NetworkPolicy | null {
  if (!json) return null;
  try {
    return JSON.parse(json) as NetworkPolicy;
  } catch {
    return null;
  }
}

export function validateNetworkPolicy(policy: NetworkPolicy): string[] {
  const errors: string[] = [];
  if (policy.mode !== 'deny-all' && policy.mode !== 'allow-list') {
    errors.push('mode must be "deny-all" or "allow-list"');
  }
  if (policy.mode === 'allow-list') {
    if (!policy.allowedHosts || policy.allowedHosts.length === 0) {
      errors.push('allow-list mode requires at least one allowedHosts entry');
    }
    for (const rule of policy.allowedHosts ?? []) {
      if (!rule.host || typeof rule.host !== 'string') {
        errors.push('each rule must have a non-empty host string');
      }
      if (rule.ports && !Array.isArray(rule.ports)) {
        errors.push('ports must be an array of numbers');
      }
    }
  }
  return errors;
}

/** Match a hostname against a pattern (supports leading wildcard *.domain.com). */
export function matchesHost(pattern: string, hostname: string): boolean {
  if (pattern === hostname) return true;
  if (pattern.startsWith('*.')) {
    const suffix = pattern.slice(1); // ".domain.com"
    return hostname.endsWith(suffix) || hostname === pattern.slice(2);
  }
  return false;
}

/** Check if a host:port is allowed by a network policy. */
export function isAllowed(policy: NetworkPolicy, host: string, port: number): boolean {
  if (policy.mode === 'deny-all') return false;
  if (!policy.allowedHosts) return false;
  for (const rule of policy.allowedHosts) {
    if (matchesHost(rule.host, host)) {
      if (!rule.ports || rule.ports.length === 0 || rule.ports.includes(port)) {
        return true;
      }
    }
  }
  return false;
}
