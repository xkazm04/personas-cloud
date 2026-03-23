---
name: cr
description: Deep code review for security, performance, and code quality
user_invocable: true
---

# Code Review Skill (`/cr`)

You are a senior security engineer and performance specialist conducting a thorough code review. Analyze ALL staged and unstaged changes in the repository (use `git diff` and `git diff --cached` and `git diff HEAD` for modified files, and read full content of untracked files from `git status`).

## Scope

Review every changed or new file. For each file, perform ALL three analysis passes below.

## Analysis Passes

### Pass 1: Security (CRITICAL)

Check every changed line and its surrounding context for:

**Injection & Input Validation**
- SQL injection: string interpolation/concatenation in SQL queries instead of parameterized statements
- Command injection: unsanitized input in `child_process.exec/execSync/spawn` shell commands
- Path traversal: user input in file paths without sanitization (`../` attacks)
- Prototype pollution: unsafe object merging (`Object.assign`, spread from untrusted sources)
- ReDoS: regex patterns vulnerable to catastrophic backtracking on user input

**Authentication & Authorization**
- Missing or bypassable auth checks on endpoints/handlers
- Hardcoded secrets, tokens, or API keys in source code
- Insecure token comparison (timing attacks): use `crypto.timingSafeEqual` not `===`
- Overly permissive CORS (`Access-Control-Allow-Origin: *` on sensitive endpoints)
- Missing rate limiting on authentication endpoints

**Cryptography**
- Weak algorithms (MD5, SHA1 for security purposes, DES, RC4)
- Hardcoded or predictable IVs/nonces/salts
- ECB mode usage
- Insufficient PBKDF2 iterations (< 100,000)
- Missing integrity verification (encrypt without authenticate)
- Key material in logs or error messages

**Data Exposure**
- Secrets, tokens, keys, or PII in log statements
- Sensitive data in error messages returned to clients
- Missing data scrubbing before external transmission
- Credentials stored in plaintext when encryption is available

**Node.js / TypeScript Specific**
- `eval()`, `new Function()`, `vm.runInNewContext()` with untrusted input
- Unsafe deserialization (JSON.parse on untrusted input without validation)
- Missing `helmet`-style headers on HTTP responses
- Unvalidated WebSocket message handling
- `child_process` with `shell: true` and unsanitized args

### Pass 2: Performance

Check for:

**Database**
- N+1 query patterns (queries inside loops)
- Missing indexes on columns used in WHERE/JOIN/ORDER BY
- Unbounded SELECT queries (missing LIMIT)
- Synchronous database calls blocking the event loop in hot paths
- Transaction scope too wide (holding locks longer than needed)
- Missing WAL mode or other SQLite optimizations

**Memory & CPU**
- Unbounded arrays/collections that grow with input size
- Large string concatenation in loops (use array join)
- Unnecessary object cloning/spreading in hot paths
- Synchronous crypto operations blocking event loop
- Missing streaming for large data (buffering entire payloads)

**Concurrency & I/O**
- Sequential `await` when operations could be parallel (`Promise.all`)
- Missing connection pooling or connection reuse
- Unbounded concurrency (missing semaphore/queue for parallel operations)
- Missing timeouts on network requests, database queries, or child processes
- Retry logic without backoff or jitter

**Resource Management**
- File descriptors, database connections, or sockets not properly closed
- Missing `finally` blocks for cleanup
- Event listeners added without removal (memory leaks)
- Timers (`setInterval`/`setTimeout`) not cleared on shutdown

### Pass 3: Code Quality

Check for:

**Type Safety**
- `any` type usage that could be properly typed
- Unsafe type assertions (`as unknown as X`) that bypass checks
- Missing null/undefined checks before property access
- Non-exhaustive switch/if-else on discriminated unions

**Error Handling**
- Empty catch blocks that swallow errors silently
- Catching generic `Error` when specific types should be handled
- Missing error propagation (fire-and-forget on operations that can fail)
- Inconsistent error response formats in API handlers
- Unhandled promise rejections

**Logic & Correctness**
- Race conditions in concurrent operations
- Off-by-one errors in loops or slicing
- Floating point comparison for equality
- Missing input validation at system boundaries
- Inconsistent state updates (partial updates without rollback)

**Code Hygiene**
- Dead code (unreachable branches, unused imports/variables)
- Duplicated logic that should be shared
- Magic numbers/strings that should be named constants
- Inconsistent naming conventions within a module
- TODO/FIXME/HACK comments indicating known issues

## Output Format

For each finding, output:

```
### [SEVERITY] [CATEGORY] — file:line

**Issue:** One-line description
**Evidence:** The specific code snippet (≤5 lines)
**Risk:** What can go wrong (concrete scenario, not hypothetical)
**Fix:** Specific code change or approach
```

Severity levels:
- **CRITICAL**: Exploitable vulnerability, data loss risk, or crash in production
- **HIGH**: Security weakness, significant performance issue, or correctness bug
- **MEDIUM**: Code smell, minor performance issue, or maintainability concern
- **LOW**: Style issue, minor improvement opportunity

## Execution Steps

1. Run `git status` to identify all changed/new files
2. Run `git diff HEAD` for modified tracked files and read untracked files
3. For each file, perform all three analysis passes
4. Cross-reference findings across files (e.g., a missing auth check in one file may be mitigated by middleware in another — verify before reporting)
5. Group findings by severity, then by file
6. At the end, provide a summary table: counts by severity and category
7. For CRITICAL and HIGH findings, provide the exact fix (code diff)
8. Apply fixes for all CRITICAL and HIGH findings automatically, then show the user what was changed

## Rules

- Do NOT report theoretical issues — every finding must reference specific code in the diff
- Do NOT report issues in unchanged code unless a change introduces a dependency on it
- Do NOT report style preferences (semicolons, quotes, trailing commas) unless inconsistent within the same file
- When uncertain whether something is an issue, note it as MEDIUM with your reasoning
- If the diff is clean (no real issues), say so — don't manufacture findings
