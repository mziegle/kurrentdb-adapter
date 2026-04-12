import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { CliError, runProgram } from '../dist/commands/program.js';

test('prints the CLI version for --version', async () => {
  const output = [];
  const originalLog = console.log;

  console.log = (...args) => {
    output.push(args.join(' '));
  };

  try {
    await runProgram(['--version']);
  } finally {
    console.log = originalLog;
  }

  assert.deepEqual(output, ['0.1.0']);
});

test('prints command-specific help for stream append', async () => {
  const output = [];
  const originalLog = console.log;

  console.log = (...args) => {
    output.push(args.join(' '));
  };

  try {
    await runProgram(['stream', 'append', '--help']);
  } finally {
    console.log = originalLog;
  }

  assert.match(output.join('\n'), /kcli stream append <stream>/);
  assert.match(output.join('\n'), /--data <json\|@file\|->/);
});

test('rejects unknown top-level flags with usage exit code', async () => {
  await assert.rejects(
    () => runProgram(['--bogus']),
    (error) =>
      error instanceof CliError &&
      error.exitCode === 2 &&
      /Unknown option '--bogus'/.test(error.message),
  );
});

test('reads append payloads from files before backend calls', async () => {
  const tempDir = await mkdtemp(join(tmpdir(), 'kcli-'));
  const eventPath = join(tempDir, 'event.json');

  await writeFile(eventPath, '{invalid json', 'utf8');

  await assert.rejects(
    () =>
      runProgram([
        'stream',
        'append',
        'my-stream',
        '--type',
        'user-created',
        '--data',
        `@${eventPath}`,
      ]),
    (error) =>
      error instanceof CliError &&
      error.exitCode === 2 &&
      /Invalid JSON input/.test(error.message) &&
      error.message.includes(`@${eventPath}`),
  );
});
