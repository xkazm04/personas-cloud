import path from 'node:path';

// ---------------------------------------------------------------------------
// Environment variable sanitisation
// ---------------------------------------------------------------------------

/**
 * Prefixes / exact names that must never be set by user-supplied env maps.
 * These can hijack the subprocess runtime (RCE via NODE_OPTIONS, LD_PRELOAD,
 * DYLD_INSERT_LIBRARIES, etc.) or tamper with the execution sandbox.
 */
const BLOCKED_ENV_PREFIXES: readonly string[] = [
  'NODE_',        // NODE_OPTIONS, NODE_PATH, NODE_DEBUG, …
  'LD_',          // LD_PRELOAD, LD_LIBRARY_PATH, …
  'DYLD_',        // macOS dynamic linker
  'BASH_',        // BASH_ENV, BASH_FUNC_*
  'PYTHON',       // PYTHONPATH, PYTHONSTARTUP, …
  'PERL',         // PERL5OPT, PERL5LIB, …
  'RUBY',         // RUBYOPT, RUBYLIB, …
];

const BLOCKED_ENV_EXACT: ReadonlySet<string> = new Set([
  'PATH',
  'HOME',
  'USER',
  'SHELL',
  'TERM',
  'TMPDIR',
  'TMP',
  'TEMP',
  'USERPROFILE',
  'HOMEDRIVE',
  'HOMEPATH',
  'SYSTEMROOT',
  'COMSPEC',
  'IFS',
  'ENV',
  'CDPATH',
  'EDITOR',
  'VISUAL',
  'MAIL',
  'DISPLAY',
  'XAUTHORITY',
  'CLAUDECODE',
  // Process-level
  'LD_PRELOAD',
  'DYLD_INSERT_LIBRARIES',
]);

/**
 * Returns true when the given environment variable name is safe to pass
 * through to the child process.  All comparisons are case-insensitive.
 */
export function isAllowedEnvVar(name: string): boolean {
  const upper = name.toUpperCase();

  if (BLOCKED_ENV_EXACT.has(upper)) return false;

  for (const prefix of BLOCKED_ENV_PREFIXES) {
    if (upper.startsWith(prefix)) return false;
  }

  // Only allow printable ASCII keys (letters, digits, underscores)
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(name)) return false;

  return true;
}

/**
 * Filter an env record, returning only the entries whose keys are safe.
 * Returns a tuple of [safe env, rejected key names].
 */
export function sanitizeEnvVars(
  env: Record<string, string>,
): { safe: Record<string, string>; rejected: string[] } {
  const safe: Record<string, string> = {};
  const rejected: string[] = [];

  for (const [key, value] of Object.entries(env)) {
    if (isAllowedEnvVar(key)) {
      safe[key] = value;
    } else {
      rejected.push(key);
    }
  }

  return { safe, rejected };
}

// ---------------------------------------------------------------------------
// Execution ID validation
// ---------------------------------------------------------------------------

/**
 * nanoid default alphabet: A-Za-z0-9_-  with default length 21.
 * We accept 10–64 chars to be tolerant of custom sizes while still
 * rejecting path-traversal payloads and other garbage.
 */
const EXEC_ID_PATTERN = /^[A-Za-z0-9_-]{10,64}$/;

export function isValidExecutionId(id: string): boolean {
  return EXEC_ID_PATTERN.test(id);
}

// ---------------------------------------------------------------------------
// Path containment
// ---------------------------------------------------------------------------

/**
 * Resolve `child` and verify it starts with the resolved `parent` directory.
 * Returns the resolved child path, or throws if it escapes the parent.
 */
export function ensureContainedPath(parent: string, child: string): string {
  const resolvedParent = path.resolve(parent) + path.sep;
  const resolvedChild = path.resolve(child);

  // The child must either equal the parent dir or be strictly inside it
  if (resolvedChild !== resolvedParent.slice(0, -1) && !resolvedChild.startsWith(resolvedParent)) {
    throw new Error(
      `Path traversal detected: "${child}" resolves outside "${parent}"`,
    );
  }

  return resolvedChild;
}
