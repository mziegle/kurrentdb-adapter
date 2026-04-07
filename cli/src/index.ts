#!/usr/bin/env node
import { runProgram } from './commands/program.js';

void runProgram(process.argv.slice(2)).catch((error: unknown) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
