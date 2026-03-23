import { matchEvent, validatePayloadFilter } from './bus.js';
import type { PersonaEvent, PersonaEventSubscription } from './types.js';
import { strict as assert } from 'node:assert';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function makeEvent(overrides: Partial<PersonaEvent> = {}): PersonaEvent {
  return {
    id: 'evt_1',
    projectId: 'default',
    eventType: 'gitlab_push',
    sourceType: 'webhook',
    sourceId: 'gitlab',
    targetPersonaId: null,
    payload: null,
    status: 'pending',
    errorMessage: null,
    processedAt: null,
    useCaseId: null,
    retryCount: 0,
    nextRetryAt: null,
    createdAt: new Date().toISOString(),
    ...overrides,
  };
}

function makeSub(overrides: Partial<PersonaEventSubscription> = {}): PersonaEventSubscription {
  return {
    id: 'sub_1',
    personaId: 'persona_1',
    eventType: 'gitlab_push',
    sourceFilter: null,
    payloadFilter: null,
    enabled: true,
    maxRetries: 3,
    retryBackoffMs: 5000,
    useCaseId: null,
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    ...overrides,
  };
}

let passed = 0;
let failed = 0;

function test(name: string, fn: () => void) {
  try {
    fn();
    passed++;
    console.log(`  PASS: ${name}`);
  } catch (e) {
    failed++;
    console.error(`  FAIL: ${name}`);
    console.error(`    ${(e as Error).message}`);
  }
}

// ---------------------------------------------------------------------------
// Tests: matchEvent with payloadFilter
// ---------------------------------------------------------------------------

console.log('\n=== matchEvent payload filter tests ===\n');

test('no payload filter — matches as before', () => {
  const event = makeEvent({ payload: JSON.stringify({ ref: 'refs/heads/main' }) });
  const sub = makeSub();
  const matches = matchEvent(event, [sub]);
  assert.equal(matches.length, 1);
});

test('exact string match — matches', () => {
  const event = makeEvent({ payload: JSON.stringify({ ref: 'refs/heads/main' }) });
  const sub = makeSub({ payloadFilter: JSON.stringify({ ref: ['refs/heads/main'] }) });
  const matches = matchEvent(event, [sub]);
  assert.equal(matches.length, 1);
});

test('exact string match — no match', () => {
  const event = makeEvent({ payload: JSON.stringify({ ref: 'refs/heads/develop' }) });
  const sub = makeSub({ payloadFilter: JSON.stringify({ ref: ['refs/heads/main'] }) });
  const matches = matchEvent(event, [sub]);
  assert.equal(matches.length, 0);
});

test('OR within field — matches second value', () => {
  const event = makeEvent({ payload: JSON.stringify({ ref: 'refs/heads/develop' }) });
  const sub = makeSub({ payloadFilter: JSON.stringify({ ref: ['refs/heads/main', 'refs/heads/develop'] }) });
  const matches = matchEvent(event, [sub]);
  assert.equal(matches.length, 1);
});

test('AND across fields — both must match', () => {
  const event = makeEvent({ payload: JSON.stringify({ ref: 'refs/heads/main', action: 'push' }) });
  const sub = makeSub({ payloadFilter: JSON.stringify({ ref: ['refs/heads/main'], action: ['push'] }) });
  const matches = matchEvent(event, [sub]);
  assert.equal(matches.length, 1);
});

test('AND across fields — one field fails', () => {
  const event = makeEvent({ payload: JSON.stringify({ ref: 'refs/heads/main', action: 'merge' }) });
  const sub = makeSub({ payloadFilter: JSON.stringify({ ref: ['refs/heads/main'], action: ['push'] }) });
  const matches = matchEvent(event, [sub]);
  assert.equal(matches.length, 0);
});

test('prefix matcher — matches', () => {
  const event = makeEvent({ payload: JSON.stringify({ ref: 'refs/heads/feature/auth' }) });
  const sub = makeSub({ payloadFilter: JSON.stringify({ ref: [{ prefix: 'refs/heads/' }] }) });
  const matches = matchEvent(event, [sub]);
  assert.equal(matches.length, 1);
});

test('prefix matcher — no match', () => {
  const event = makeEvent({ payload: JSON.stringify({ ref: 'refs/tags/v1.0' }) });
  const sub = makeSub({ payloadFilter: JSON.stringify({ ref: [{ prefix: 'refs/heads/' }] }) });
  const matches = matchEvent(event, [sub]);
  assert.equal(matches.length, 0);
});

test('suffix matcher — matches', () => {
  const event = makeEvent({ payload: JSON.stringify({ filename: 'report.pdf' }) });
  const sub = makeSub({ payloadFilter: JSON.stringify({ filename: [{ suffix: '.pdf' }] }) });
  const matches = matchEvent(event, [sub]);
  assert.equal(matches.length, 1);
});

test('suffix matcher — no match', () => {
  const event = makeEvent({ payload: JSON.stringify({ filename: 'report.docx' }) });
  const sub = makeSub({ payloadFilter: JSON.stringify({ filename: [{ suffix: '.pdf' }] }) });
  const matches = matchEvent(event, [sub]);
  assert.equal(matches.length, 0);
});

test('anything-but matcher — matches (value not in exclusion list)', () => {
  const event = makeEvent({ payload: JSON.stringify({ status: 'success' }) });
  const sub = makeSub({ payloadFilter: JSON.stringify({ status: [{ 'anything-but': ['error', 'failed'] }] }) });
  const matches = matchEvent(event, [sub]);
  assert.equal(matches.length, 1);
});

test('anything-but matcher — no match (value in exclusion list)', () => {
  const event = makeEvent({ payload: JSON.stringify({ status: 'error' }) });
  const sub = makeSub({ payloadFilter: JSON.stringify({ status: [{ 'anything-but': ['error', 'failed'] }] }) });
  const matches = matchEvent(event, [sub]);
  assert.equal(matches.length, 0);
});

test('numeric >= matcher — matches', () => {
  const event = makeEvent({ payload: JSON.stringify({ commits: { count: 5 } }) });
  const sub = makeSub({ payloadFilter: JSON.stringify({ 'commits.count': [{ numeric: ['>=', 1] }] }) });
  const matches = matchEvent(event, [sub]);
  assert.equal(matches.length, 1);
});

test('numeric >= matcher — no match', () => {
  const event = makeEvent({ payload: JSON.stringify({ commits: { count: 0 } }) });
  const sub = makeSub({ payloadFilter: JSON.stringify({ 'commits.count': [{ numeric: ['>=', 1] }] }) });
  const matches = matchEvent(event, [sub]);
  assert.equal(matches.length, 0);
});

test('numeric range matcher — value in range', () => {
  const event = makeEvent({ payload: JSON.stringify({ score: 75 }) });
  const sub = makeSub({ payloadFilter: JSON.stringify({ score: [{ numeric: ['>=', 50, '<', 100] }] }) });
  const matches = matchEvent(event, [sub]);
  assert.equal(matches.length, 1);
});

test('numeric range matcher — value out of range', () => {
  const event = makeEvent({ payload: JSON.stringify({ score: 150 }) });
  const sub = makeSub({ payloadFilter: JSON.stringify({ score: [{ numeric: ['>=', 50, '<', 100] }] }) });
  const matches = matchEvent(event, [sub]);
  assert.equal(matches.length, 0);
});

test('exists: true — field exists', () => {
  const event = makeEvent({ payload: JSON.stringify({ author: { name: 'Alice' } }) });
  const sub = makeSub({ payloadFilter: JSON.stringify({ 'author.name': [{ exists: true }] }) });
  const matches = matchEvent(event, [sub]);
  assert.equal(matches.length, 1);
});

test('exists: true — field missing', () => {
  const event = makeEvent({ payload: JSON.stringify({ author: {} }) });
  const sub = makeSub({ payloadFilter: JSON.stringify({ 'author.name': [{ exists: true }] }) });
  const matches = matchEvent(event, [sub]);
  assert.equal(matches.length, 0);
});

test('exists: false — field missing (match)', () => {
  const event = makeEvent({ payload: JSON.stringify({ author: {} }) });
  const sub = makeSub({ payloadFilter: JSON.stringify({ 'author.email': [{ exists: false }] }) });
  const matches = matchEvent(event, [sub]);
  assert.equal(matches.length, 1);
});

test('exists: false — field exists (no match)', () => {
  const event = makeEvent({ payload: JSON.stringify({ author: { email: 'a@b.com' } }) });
  const sub = makeSub({ payloadFilter: JSON.stringify({ 'author.email': [{ exists: false }] }) });
  const matches = matchEvent(event, [sub]);
  assert.equal(matches.length, 0);
});

test('nested dot-notation path — matches', () => {
  const event = makeEvent({ payload: JSON.stringify({ commit: { author: { name: 'Alice' } } }) });
  const sub = makeSub({ payloadFilter: JSON.stringify({ 'commit.author.name': ['Alice'] }) });
  const matches = matchEvent(event, [sub]);
  assert.equal(matches.length, 1);
});

test('nested dot-notation path — no match', () => {
  const event = makeEvent({ payload: JSON.stringify({ commit: { author: { name: 'Bob' } } }) });
  const sub = makeSub({ payloadFilter: JSON.stringify({ 'commit.author.name': ['Alice'] }) });
  const matches = matchEvent(event, [sub]);
  assert.equal(matches.length, 0);
});

test('boolean exact match — matches true', () => {
  const event = makeEvent({ payload: JSON.stringify({ forced: true }) });
  const sub = makeSub({ payloadFilter: JSON.stringify({ forced: [true] }) });
  const matches = matchEvent(event, [sub]);
  assert.equal(matches.length, 1);
});

test('boolean exact match — no match', () => {
  const event = makeEvent({ payload: JSON.stringify({ forced: false }) });
  const sub = makeSub({ payloadFilter: JSON.stringify({ forced: [true] }) });
  const matches = matchEvent(event, [sub]);
  assert.equal(matches.length, 0);
});

test('null payload with filter — no match', () => {
  const event = makeEvent({ payload: null });
  const sub = makeSub({ payloadFilter: JSON.stringify({ ref: ['refs/heads/main'] }) });
  const matches = matchEvent(event, [sub]);
  assert.equal(matches.length, 0);
});

test('invalid payload JSON with filter — no match', () => {
  const event = makeEvent({ payload: 'not-json' });
  const sub = makeSub({ payloadFilter: JSON.stringify({ ref: ['refs/heads/main'] }) });
  const matches = matchEvent(event, [sub]);
  assert.equal(matches.length, 0);
});

test('invalid filter JSON — no match', () => {
  const event = makeEvent({ payload: JSON.stringify({ ref: 'refs/heads/main' }) });
  const sub = makeSub({ payloadFilter: 'not-json' });
  const matches = matchEvent(event, [sub]);
  assert.equal(matches.length, 0);
});

test('empty filter object — matches (no conditions)', () => {
  const event = makeEvent({ payload: JSON.stringify({ ref: 'refs/heads/main' }) });
  const sub = makeSub({ payloadFilter: '{}' });
  const matches = matchEvent(event, [sub]);
  assert.equal(matches.length, 1);
});

test('combined with sourceFilter — both must pass', () => {
  const event = makeEvent({
    sourceId: 'gitlab',
    payload: JSON.stringify({ ref: 'refs/heads/main' }),
  });
  const sub = makeSub({
    sourceFilter: 'gitlab',
    payloadFilter: JSON.stringify({ ref: ['refs/heads/main'] }),
  });
  const matches = matchEvent(event, [sub]);
  assert.equal(matches.length, 1);
});

test('combined with sourceFilter — source fails', () => {
  const event = makeEvent({
    sourceId: 'github',
    payload: JSON.stringify({ ref: 'refs/heads/main' }),
  });
  const sub = makeSub({
    sourceFilter: 'gitlab',
    payloadFilter: JSON.stringify({ ref: ['refs/heads/main'] }),
  });
  const matches = matchEvent(event, [sub]);
  assert.equal(matches.length, 0);
});

test('mixed matchers — prefix OR exact in one field', () => {
  const event = makeEvent({ payload: JSON.stringify({ branch: 'feature/cool-thing' }) });
  const sub = makeSub({ payloadFilter: JSON.stringify({ branch: ['main', { prefix: 'feature/' }] }) });
  const matches = matchEvent(event, [sub]);
  assert.equal(matches.length, 1);
});

test('multiple subscriptions — only matching ones returned', () => {
  const event = makeEvent({ payload: JSON.stringify({ ref: 'refs/heads/main', action: 'push' }) });
  const sub1 = makeSub({ id: 'sub_1', payloadFilter: JSON.stringify({ ref: ['refs/heads/main'] }) });
  const sub2 = makeSub({ id: 'sub_2', payloadFilter: JSON.stringify({ ref: ['refs/heads/develop'] }) });
  const sub3 = makeSub({ id: 'sub_3', payloadFilter: null }); // no filter = matches all
  const matches = matchEvent(event, [sub1, sub2, sub3]);
  assert.equal(matches.length, 2);
  assert.deepEqual(matches.map(m => m.subscriptionId).sort(), ['sub_1', 'sub_3']);
});

// ---------------------------------------------------------------------------
// Tests: validatePayloadFilter
// ---------------------------------------------------------------------------

console.log('\n=== validatePayloadFilter tests ===\n');

test('valid: simple exact match', () => {
  const result = validatePayloadFilter(JSON.stringify({ ref: ['refs/heads/main'] }));
  assert.equal(result, null);
});

test('valid: prefix matcher', () => {
  const result = validatePayloadFilter(JSON.stringify({ ref: [{ prefix: 'refs/' }] }));
  assert.equal(result, null);
});

test('valid: suffix matcher', () => {
  const result = validatePayloadFilter(JSON.stringify({ name: [{ suffix: '.ts' }] }));
  assert.equal(result, null);
});

test('valid: anything-but', () => {
  const result = validatePayloadFilter(JSON.stringify({ status: [{ 'anything-but': ['error'] }] }));
  assert.equal(result, null);
});

test('valid: numeric range', () => {
  const result = validatePayloadFilter(JSON.stringify({ score: [{ numeric: ['>=', 0, '<', 100] }] }));
  assert.equal(result, null);
});

test('valid: exists', () => {
  const result = validatePayloadFilter(JSON.stringify({ name: [{ exists: true }] }));
  assert.equal(result, null);
});

test('valid: mixed conditions', () => {
  const result = validatePayloadFilter(JSON.stringify({
    ref: ['main', { prefix: 'feature/' }],
    action: ['push'],
    'commits.count': [{ numeric: ['>=', 1] }],
  }));
  assert.equal(result, null);
});

test('invalid: not JSON', () => {
  const result = validatePayloadFilter('not-json');
  assert.equal(result, 'Invalid JSON');
});

test('invalid: not an object', () => {
  const result = validatePayloadFilter('"hello"');
  assert.equal(result, 'Filter must be a JSON object');
});

test('invalid: array instead of object', () => {
  const result = validatePayloadFilter('[]');
  assert.equal(result, 'Filter must be a JSON object');
});

test('invalid: value not array', () => {
  const result = validatePayloadFilter(JSON.stringify({ ref: 'main' }));
  assert.ok(result?.includes('must be an array'));
});

test('invalid: empty value array', () => {
  const result = validatePayloadFilter(JSON.stringify({ ref: [] }));
  assert.ok(result?.includes('must not be empty'));
});

test('invalid: unknown operator', () => {
  const result = validatePayloadFilter(JSON.stringify({ ref: [{ regex: '.*' }] }));
  assert.ok(result?.includes('Unknown operator'));
});

test('invalid: prefix with non-string', () => {
  const result = validatePayloadFilter(JSON.stringify({ ref: [{ prefix: 123 }] }));
  assert.ok(result?.includes('must be a string'));
});

test('invalid: numeric with odd-length array', () => {
  const result = validatePayloadFilter(JSON.stringify({ x: [{ numeric: ['>='] }] }));
  assert.ok(result?.includes('pairs'));
});

test('invalid: numeric with bad operator', () => {
  const result = validatePayloadFilter(JSON.stringify({ x: [{ numeric: ['!=', 5] }] }));
  assert.ok(result?.includes('Invalid numeric operator'));
});

test('invalid: exists with non-boolean', () => {
  const result = validatePayloadFilter(JSON.stringify({ x: [{ exists: 'yes' }] }));
  assert.ok(result?.includes('must be a boolean'));
});

// ---------------------------------------------------------------------------
// Summary
// ---------------------------------------------------------------------------

console.log(`\n=== Results: ${passed} passed, ${failed} failed ===\n`);
if (failed > 0) process.exit(1);
