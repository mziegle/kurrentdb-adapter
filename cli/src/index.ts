#!/usr/bin/env node
import { CliError, runProgram } from './commands/program.js';

void runProgram(process.argv.slice(2)).catch((error: unknown) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = error instanceof CliError ? error.exitCode : 1;
});
