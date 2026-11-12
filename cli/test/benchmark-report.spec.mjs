import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readdir, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { KurrentDBClient } from '@kurrent/kurrentdb-client';

test('benchmark reports and progress do not expose connection credentials or query secrets', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'kcli-bench-'));
  const oldTarget = process.env.BENCH_TARGET_CONNECTION_STRING;
  const oldCompare = process.env.BENCH_COMPARE_CONNECTION_STRING;
  const originalConnectionString = KurrentDBClient.connectionString;
  const originalLog = console.log;
  const output = [];
  const appendedStreams = [];
  process.env.BENCH_TARGET_CONNECTION_STRING = 'kurrentdb://alice:password123@localhost:2113?token=querysecret';
  process.env.BENCH_COMPARE_CONNECTION_STRING = 'kurrentdb://bob:password456@localhost:2114?apiKey=anothersecret';
  KurrentDBClient.connectionString = () => ({
    appendToStream: async (stream) => { appendedStreams.push(stream); },
    readStream: async function* () { yield { event: {} }; },
    dispose: async () => {},
  });
  console.log = (...args) => output.push(args.join(' '));
  try {
    const { runBenchmarkReport } = await import('../dist/application/benchmark-report.js?credential-test');
    await runBenchmarkReport(dir, 'fast');
    const files = await readdir(dir);
    assert.equal(files.length, 2);
    const content = output.join('\n') + (await Promise.all(files.map((file) => readFile(join(dir, file), 'utf8')))).join('\n');
    for (const secret of ['alice', 'password123', 'querysecret', 'bob', 'password456', 'anothersecret']) {
      assert.equal(content.includes(secret), false, `leaked ${secret}`);
    }
    // Fast single-stream append scenario has ten untimed warmup operations.
    assert.equal(new Set(appendedStreams.slice(0, 10)).size, 1);
    assert.notEqual(appendedStreams[0], appendedStreams[10], 'warmup must not alter a measured stream');
  } finally {
    KurrentDBClient.connectionString = originalConnectionString;
    console.log = originalLog;
    if (oldTarget === undefined) delete process.env.BENCH_TARGET_CONNECTION_STRING; else process.env.BENCH_TARGET_CONNECTION_STRING = oldTarget;
    if (oldCompare === undefined) delete process.env.BENCH_COMPARE_CONNECTION_STRING; else process.env.BENCH_COMPARE_CONNECTION_STRING = oldCompare;
    await rm(dir, { recursive: true, force: true });
  }
});
