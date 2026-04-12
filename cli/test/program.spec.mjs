import test from 'node:test';
import assert from 'node:assert/strict';
import { runProgram } from '../dist/commands/program.js';

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
