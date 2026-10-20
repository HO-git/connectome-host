/**
 * An error record keeps the full request the first time its log sees that
 * request fail, whatever the error; a repeat (each retry of a
 * storm) records its hash and size instead, pointing at the copy written.
 */
import { test, expect, beforeEach, afterEach } from 'bun:test';
import { readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createHash } from 'node:crypto';
import { AnthropicAdapter, rateLimitError, serverError, invalidRequestError } from '@animalabs/membrane';
import { LoggingAnthropicAdapter } from '../src/logging-adapter.js';

const BIG = { model: 'claude-fable-5', messages: [{ role: 'user', content: 'x'.repeat(200_000) }] };
/** Multi-byte UTF-8 (CJK, accents, emoji): its byte size exceeds its string length. */
const NON_ASCII = { model: 'claude-fable-5', messages: [{ role: 'user', content: '日本語のテキスト café 🙂'.repeat(5_000) }] };
const proto = AnthropicAdapter.prototype as unknown as Record<'complete' | 'stream', unknown>;
const original = { complete: proto.complete, stream: proto.stream };
const logPath = join(tmpdir(), `llm-error-storm-${process.pid}.jsonl`);
const savedFullPayloads = process.env.LLM_CALLS_FULL_PAYLOADS;

// The adapter reads the flag at construction: clear it before each test so a
// run with LLM_CALLS_FULL_PAYLOADS=1 in the environment tests the default.
beforeEach(() => {
  delete process.env.LLM_CALLS_FULL_PAYLOADS;
});

afterEach(() => {
  proto.complete = original.complete;
  proto.stream = original.stream;
  if (savedFullPayloads === undefined) delete process.env.LLM_CALLS_FULL_PAYLOADS;
  else process.env.LLM_CALLS_FULL_PAYLOADS = savedFullPayloads;
  rmSync(logPath, { force: true });
});

/** Make the underlying adapter capture `raw` as its raw request, then fail. */
function failWith(err: unknown, raw: unknown = BIG): void {
  const fail = async (_req: unknown, a?: unknown, b?: unknown) => {
    const opts = (b ?? a) as { onRequest?: (r: unknown) => void } | undefined;
    opts?.onRequest?.(raw);
    throw err;
  };
  proto.complete = fail;
  proto.stream = fail;
}

/** Make the underlying adapter answer: a call that gets a response. */
function succeed(): void {
  const ok = async (_req: unknown, a?: unknown, b?: unknown) => {
    const opts = (b ?? a) as { onRequest?: (r: unknown) => void } | undefined;
    opts?.onRequest?.(BIG);
    return { content: [], stopReason: 'end_turn', usage: { inputTokens: 1, outputTokens: 1 }, raw: { stop_reason: 'end_turn' } };
  };
  proto.complete = ok;
  proto.stream = ok;
}

const sha = (raw: unknown) => createHash('sha256').update(JSON.stringify(raw)).digest('hex');
const newAdapter = () => new LoggingAnthropicAdapter({ apiKey: 'test-key' }, logPath, () => ({ enabled: false, budgetTokens: 0 }));

/** Call `n` times on one adapter; the error records it wrote, in order. */
async function errorRecords(kind: 'complete' | 'stream', n: number, adapter = newAdapter()): Promise<Record<string, unknown>[]> {
  const request = { model: 'claude-fable-5', messages: [] } as never;
  for (let i = 0; i < n; i++) {
    const call = kind === 'complete' ? adapter.complete(request) : adapter.stream(request, {} as never);
    await expect(call).rejects.toBeDefined();
  }
  return readFileSync(logPath, 'utf8').trim().split('\n').map((l) => JSON.parse(l)).filter((r) => r.type === 'error');
}

for (const kind of ['complete', 'stream'] as const) {
  test(`${kind}: a rate-limit storm keeps the request once, then its hash and size`, async () => {
    failWith(rateLimitError('429 too many requests'));
    const [first, ...repeats] = await errorRecords(kind, 3);
    expect(first.rawRequest).toEqual(BIG);
    expect(first.requestSha256).toBe(sha(BIG));
    expect(repeats).toHaveLength(2);
    for (const rec of repeats) {
      expect(rec.rawRequest).toBeUndefined();
      expect(rec.requestSha256).toBe(sha(BIG));
      expect(rec.rawRequestOmitted).toMatchObject({ bytes: JSON.stringify(BIG).length, reason: expect.stringContaining('LLM_CALLS_FULL_PAYLOADS=1') });
      expect(JSON.stringify(rec).length).toBeLessThan(10_000);
    }
  });
}

test('an overload, a bad request and a plain error are each kept the first time, and their repeats noted', async () => {
  for (const err of [serverError('529 overloaded', 529), invalidRequestError('400 roles must alternate'), new Error('something else')]) {
    rmSync(logPath, { force: true });
    failWith(err);
    const [first, second] = await errorRecords('stream', 2);
    expect(first.rawRequest).toEqual(BIG);
    expect(second.rawRequest).toBeUndefined();
    expect(second.rawRequestOmitted).toMatchObject({ bytes: expect.any(Number) });
  }
});

test('the note counts UTF-8 bytes, not string length', async () => {
  failWith(rateLimitError('429'), NON_ASCII);
  const bytes = Buffer.byteLength(JSON.stringify(NON_ASCII), 'utf8');
  expect(bytes).toBeGreaterThan(JSON.stringify(NON_ASCII).length);
  expect((await errorRecords('stream', 2))[1].rawRequestOmitted).toMatchObject({ bytes });
});

test('requests are told apart: A, B, A keeps A and B whole and notes the second A', async () => {
  const adapter = newAdapter();
  failWith(rateLimitError('429'));
  await errorRecords('stream', 1, adapter);
  failWith(rateLimitError('429'), NON_ASCII);
  await errorRecords('stream', 1, adapter);
  failWith(rateLimitError('429'));
  const [a, b, again] = await errorRecords('stream', 1, adapter);
  expect(a.rawRequest).toEqual(BIG);
  expect(b.rawRequest).toEqual(NON_ASCII);
  expect(again.rawRequest).toBeUndefined();
  expect(again.requestSha256).toBe(sha(BIG));
});

test("another caller's response doesn't make a storm write its request again", async () => {
  const adapter = newAdapter();
  failWith(rateLimitError('429'));
  await errorRecords('stream', 1, adapter);
  succeed();
  await adapter.stream({ model: 'claude-fable-5', messages: [] } as never, {} as never);
  failWith(rateLimitError('429'));
  const records = await errorRecords('stream', 1, adapter);
  expect(records).toHaveLength(2);
  expect(records[1].rawRequest).toBeUndefined();
  expect(records[1].rawRequestOmitted).toMatchObject({ bytes: JSON.stringify(BIG).length });
});

test('a request whose record the line bound cut is tried whole again, not noted as written', async () => {
  // Under 16 MiB itself, over it with the record's other fields.
  const near = { model: 'claude-fable-5', messages: [{ role: 'user', content: 'x'.repeat(16 * 1024 * 1024 - 150) }] };
  failWith(rateLimitError('429'), near);
  const records = await errorRecords('stream', 2);
  expect(records).toHaveLength(2);
  for (const rec of records) {
    expect(rec.rawRequestOmitted).toBeUndefined();
    expect(rec.truncated).toBe('raw bodies dropped (record exceeded size guard)');
  }
}, 30_000);

test('the memory keeps the latest 256 requests: the oldest is written whole again, the newest noted', async () => {
  const adapter = newAdapter();
  const req = (n: number) => ({ model: 'claude-fable-5', messages: [{ role: 'user', content: `request ${n}` }] });
  for (let n = 0; n <= 256; n++) {
    failWith(rateLimitError('429'), req(n));
    await errorRecords('stream', 1, adapter);
  }
  failWith(rateLimitError('429'), req(256));
  const newest = (await errorRecords('stream', 1, adapter)).at(-1)!;
  expect(newest.rawRequest).toBeUndefined();
  failWith(rateLimitError('429'), req(0));
  const oldest = (await errorRecords('stream', 1, adapter)).at(-1)!;
  expect(oldest.rawRequest).toEqual(req(0));
}, 30_000);

test('a request membrane rejects before onRequest is kept from the error', async () => {
  const err = Object.assign(new Error('prefill unsupported'), { type: 'unsupported', rawRequest: BIG });
  const fail = async () => { throw err; };
  proto.complete = fail;
  proto.stream = fail;
  expect((await errorRecords('complete', 1))[0].rawRequest).toEqual(BIG);
});

test('LLM_CALLS_FULL_PAYLOADS=1 keeps every copy', async () => {
  process.env.LLM_CALLS_FULL_PAYLOADS = '1';
  failWith(rateLimitError('429'));
  const records = await errorRecords('stream', 2);
  expect(records).toHaveLength(2);
  for (const rec of records) {
    expect(rec.rawRequest).toEqual(BIG);
    expect(rec.requestSha256).toBe(sha(BIG));
  }
});
