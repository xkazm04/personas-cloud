export interface ScrubResult {
  scrubbed: string;
  redactionCount: number;
  redactedTypes: string[];
}

export interface ScrubberConfig {
  enabled: boolean;
  customPatterns?: Array<{ name: string; pattern: RegExp }>;
}

const DEFAULT_PATTERNS: Array<{ name: string; pattern: RegExp }> = [
  { name: 'anthropic_key', pattern: /sk-ant-[a-zA-Z0-9_-]{20,}/g },
  { name: 'openai_key', pattern: /sk-[a-zA-Z0-9]{20,}/g },
  { name: 'github_pat', pattern: /ghp_[a-zA-Z0-9]{36}/g },
  { name: 'github_oauth', pattern: /gho_[a-zA-Z0-9]{36}/g },
  { name: 'github_app', pattern: /ghs_[a-zA-Z0-9]{36}/g },
  { name: 'aws_key_id', pattern: /AKIA[A-Z0-9]{16}/g },
  { name: 'aws_secret', pattern: /(?:aws_secret_access_key|AWS_SECRET_ACCESS_KEY)\s*[=:]\s*[A-Za-z0-9/+=]{40}/gi },
  { name: 'bearer_token', pattern: /Bearer\s+[a-zA-Z0-9._-]{20,}/g },
  { name: 'url_password', pattern: /:\/\/([^:]+):([^@]{8,})@/g },
  { name: 'slack_token', pattern: /xox[bpars]-[a-zA-Z0-9-]{10,}/g },
  { name: 'google_key', pattern: /AIza[a-zA-Z0-9_-]{35}/g },
  { name: 'stripe_key', pattern: /[rs]k_(?:live|test)_[a-zA-Z0-9]{20,}/g },
  { name: 'base64_secret', pattern: /(?:key|secret|password|token|credential)\s*[=:]\s*[A-Za-z0-9+/]{40,}={0,2}/gi },
];

/**
 * Scrub sensitive patterns from text output.
 * Returns the scrubbed text along with redaction statistics.
 */
export function scrub(text: string, config?: ScrubberConfig): ScrubResult {
  if (config?.enabled === false) {
    return { scrubbed: text, redactionCount: 0, redactedTypes: [] };
  }

  let result = text;
  let totalRedactions = 0;
  const typesFound = new Set<string>();

  const patterns = [...DEFAULT_PATTERNS, ...(config?.customPatterns ?? [])];

  for (const { name, pattern } of patterns) {
    // Reset regex lastIndex for global patterns
    pattern.lastIndex = 0;

    const matches = result.match(pattern);
    if (matches) {
      totalRedactions += matches.length;
      typesFound.add(name);

      if (name === 'url_password') {
        // For URL passwords, preserve the URL structure
        result = result.replace(pattern, '://$1:[REDACTED:url_password]@');
      } else {
        result = result.replace(pattern, `[REDACTED:${name}]`);
      }
    }
  }

  return {
    scrubbed: result,
    redactionCount: totalRedactions,
    redactedTypes: Array.from(typesFound),
  };
}
