import { access, readFile } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';
import { loadConfig, inspectConfig } from '../config/config.js';
import type { ExpectedRevision } from '../domain/types.js';
import { createBackendClient } from '../infrastructure/backends/factory.js';
import { printOutput } from '../infrastructure/output/format.js';
import {
  runAppendTest,
  runCompareTest,
  runReadTest,
  runSubscribeTest,
} from '../application/test-cases.js';
import { runBenchmarkReport } from '../application/benchmark-report.js';
import { runTraceProxy } from '../application/trace.js';
import { runTui } from '../tui/app.js';

type CommandKey =
  | 'bench report'
  | 'config show'
  | 'ping'
  | 'stream append'
  | 'stream read'
  | 'stream tail'
  | 'test append'
  | 'test compare'
  | 'test read'
  | 'test subscribe'
  | 'trace'
  | 'tui';

interface ParsedArgs {
  help: boolean;
  json: boolean;
  positionals: string[];
  version: boolean;
}

function parseArgs(argv: string[]): ParsedArgs {
  const positionals: string[] = [];
  const parsed: ParsedArgs = {
    help: false,
    json: false,
    positionals,
    version: false,
  };

  for (let index = 0; index < argv.length; index += 1) {
    const value = argv[index];

    if (value === '--help' || value === '-h') {
      parsed.help = true;
      continue;
    }

    if (value === '--json') {
      parsed.json = true;
      continue;
    }

    if (value === '--version' || value === '-v') {
      parsed.version = true;
      continue;
    }

    positionals.push(value);
  }

  return parsed;
}

function parseOption(positionals: string[], optionName: string): string | undefined {
  const optionIndex = positionals.findIndex((value) => value === optionName);
  if (optionIndex === -1) {
    return undefined;
  }

  return positionals[optionIndex + 1];
}

function requireOption(positionals: string[], optionName: string): string {
  const value = parseOption(positionals, optionName);

  if (!value) {
    throw new CliError(`Missing required option ${optionName}.`, 2);
  }

  return value;
}

function parseOptionalBigInt(value: string | undefined): bigint | undefined {
  if (value === undefined) {
    return undefined;
  }

  try {
    return BigInt(value);
  } catch {
    throw new CliError(`Invalid integer value '${value}'.`, 2);
  }
}

function parseExpectedRevision(value: string | undefined): ExpectedRevision | undefined {
  if (!value) {
    return undefined;
  }

  if (value === 'any' || value === 'no_stream' || value === 'stream_exists') {
    return value;
  }

  return parseOptionalBigInt(value);
}

function parseLimit(value: string | undefined): number {
  const raw = value ?? '100';
  const parsed = Number(raw);

  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new CliError(`Invalid --limit value '${raw}'. Expected a positive integer.`, 2);
  }

  return parsed;
}

function validateOptions(
  positionals: string[],
  commandKey: CommandKey,
  optionNamesWithValues: string[],
  flagNames: string[] = [],
): void {
  const allowedFlags = new Set(flagNames);
  const allowedOptions = new Set(optionNamesWithValues);

  for (let index = 0; index < positionals.length; index += 1) {
    const value = positionals[index];
    if (!value.startsWith('-')) {
      continue;
    }

    if (allowedFlags.has(value)) {
      continue;
    }

    if (allowedOptions.has(value)) {
      const next = positionals[index + 1];
      if (!next || next.startsWith('-')) {
        throw new CliError(
          `Option ${value} requires a value for '${commandKey}'.`,
          2,
        );
      }
      index += 1;
      continue;
    }

    throw new CliError(`Unknown option '${value}' for '${commandKey}'.`, 2);
  }
}

function printHelp(commandKey?: CommandKey): void {
  console.log(getHelpText(commandKey));
}

function getHelpText(commandKey?: CommandKey): string {
  const sharedConfigText = `Config:
  Default endpoint: kurrentdb://127.0.0.1:2113?tls=false
  Override with KDB_CONNECTION or create kcli.config.json with:
    { "connectionString": "kurrentdb://127.0.0.1:2113?tls=false" }
  For compare, also set KDB_COMPARE_CONNECTION or compareConnectionString.`;

  const helpByCommand: Partial<Record<CommandKey, string>> = {
    ping: `Usage:
  kcli ping [--json]

Checks connectivity to the configured KurrentDB endpoint.`,
    'bench report': `Usage:
  kcli bench report

Runs the benchmark report workflow and writes reports under benchmark/reports/.
Uses KDB_CONNECTION for the primary endpoint and KDB_COMPARE_CONNECTION for the comparison endpoint.`,
    trace: `Usage:
  kcli trace [--proxy-port <port>] [--proxy-host <host>] [--upstream-port <port>] [--upstream-host <host>] [--verbose <info|debug>] [--suppress-http-paths <paths>] [--suppress-http2-frame-types <types>] [--suppress-http1-headers] [--suppress-http1-bodies] [--no-default-suppressions]

Starts a trace proxy between a client and an upstream KurrentDB node.`,
    'stream read': `Usage:
  kcli stream read <stream> [--from <revision>] [--limit <count>] [--json]

Reads events from a stream.

Examples:
  kcli stream read my-stream
  kcli stream read my-stream --from 10 --limit 20 --json`,
    'stream append': `Usage:
  kcli stream append <stream> --type <event-type> --data <json|@file|-> [--metadata <json|@file|->] [--expected-revision <any|no_stream|stream_exists|revision>] [--json]

Appends a single event to a stream.

Examples:
  kcli stream append my-stream --type user-created --data '{"userId":"u-1"}'
  kcli stream append my-stream --type user-created --data @event.json
  Get-Content event.json | kcli stream append my-stream --type user-created --data -`,
    'stream tail': `Usage:
  kcli stream tail <stream> [--from <revision>] [--json]

Subscribes to a stream and prints arriving events.`,
    'test append': `Usage:
  kcli test append [--json]

Runs the append smoke test against the configured endpoint.`,
    'test read': `Usage:
  kcli test read [--json]

Runs the read smoke test against the configured endpoint.`,
    'test subscribe': `Usage:
  kcli test subscribe [--json]

Runs the subscribe smoke test against the configured endpoint.`,
    'test compare': `Usage:
  kcli test compare --stream <stream> [--json]

Compares the configured default endpoint with the compare endpoint for one stream.`,
    tui: `Usage:
  kcli tui --stream <stream>

Launches the interactive stream tail UI.`,
    'config show': `Usage:
  kcli config show [--json]

Prints the effective endpoint configuration and where each value came from.`,
  };

  if (commandKey) {
    return `kcli\n\n${helpByCommand[commandKey]}\n\n${sharedConfigText}`;
  }

  return `kcli

Usage:
  kcli ping [--json]
  kcli bench report
  kcli trace [--proxy-port <port>] [--proxy-host <host>] [--upstream-port <port>] [--upstream-host <host>] [--verbose <info|debug>] [--suppress-http-paths <paths>] [--suppress-http2-frame-types <types>] [--suppress-http1-headers] [--suppress-http1-bodies] [--no-default-suppressions]
  kcli stream read <stream> [--from <revision>] [--limit <count>] [--json]
  kcli stream append <stream> --type <event-type> --data <json|@file|-> [--metadata <json|@file|->] [--expected-revision <any|no_stream|stream_exists|revision>] [--json]
  kcli stream tail <stream> [--from <revision>] [--json]
  kcli test append [--json]
  kcli test read [--json]
  kcli test subscribe [--json]
  kcli test compare --stream <stream> [--json]
  kcli config show [--json]
  kcli tui --stream <stream>

Examples:
  kcli ping
  kcli stream read my-stream --limit 20
  kcli stream append my-stream --type user-created --data @event.json
  kcli config show --json

${sharedConfigText}`;
}

function resolveHelpCommand(positionals: string[]): CommandKey | undefined {
  const [group, action] = positionals;

  if (!group) {
    return undefined;
  }

  const command = [group, action].filter(Boolean).join(' ') as CommandKey;
  if (isCommandKey(command)) {
    return command;
  }

  if (isCommandKey(group as CommandKey)) {
    return group as CommandKey;
  }

  return undefined;
}

function isCommandKey(value: string): value is CommandKey {
  return new Set<string>([
    'bench report',
    'config show',
    'ping',
    'stream append',
    'stream read',
    'stream tail',
    'test append',
    'test compare',
    'test read',
    'test subscribe',
    'trace',
    'tui',
  ]).has(value);
}

async function parseJsonInput(
  value: string,
  readStdin: () => Promise<string>,
): Promise<unknown> {
  let content = value;

  if (value === '-') {
    content = await readStdin();
  } else if (value.startsWith('@')) {
    content = await readFile(value.slice(1), 'utf8');
  }

  try {
    return JSON.parse(content);
  } catch (error) {
    throw new CliError(
      `Invalid JSON input for '${value}': ${error instanceof Error ? error.message : String(error)}`,
      2,
    );
  }
}

export class CliError extends Error {
  constructor(
    message: string,
    public readonly exitCode = 1,
  ) {
    super(message);
  }
}

async function readCliVersion(): Promise<string> {
  for (const candidate of getPackageJsonCandidates()) {
    try {
      await access(candidate);
      const content = await readFile(candidate, 'utf8');
      const parsed = JSON.parse(content) as { version?: string };
      if (parsed.version) {
        return parsed.version;
      }
    } catch {
      continue;
    }
  }

  throw new CliError('Unable to determine CLI version.', 1);
}

function getPackageJsonCandidates(): string[] {
  const cwdCandidate = resolve(process.cwd(), 'package.json');
  const argvPath = process.argv[1];

  if (!argvPath) {
    return [cwdCandidate];
  }

  const scriptDir = dirname(resolve(argvPath));
  return [
    resolve(scriptDir, '..', 'package.json'),
    resolve(scriptDir, 'package.json'),
    cwdCandidate,
  ];
}

export async function runProgram(argv: string[]): Promise<void> {
  const parsed = parseArgs(argv);
  const [group, action, target] = parsed.positionals;
  const helpCommand = resolveHelpCommand(parsed.positionals);

  if (parsed.version) {
    console.log(await readCliVersion());
    return;
  }

  if (parsed.help) {
    printHelp(helpCommand);
    return;
  }

  if (group === 'help') {
    printHelp(resolveHelpCommand(parsed.positionals.slice(1)));
    return;
  }

  if (!group) {
    printHelp();
    return;
  }

  if (group.startsWith('-')) {
    throw new CliError(`Unknown option '${group}'. Use kcli --help.`, 2);
  }

  const config = await loadConfig();
  let stdinCache: string | undefined;

  const readStdinOnce = async (): Promise<string> => {
    if (stdinCache !== undefined) {
      return stdinCache;
    }

    const chunks: Buffer[] = [];
    for await (const chunk of process.stdin) {
      if (Buffer.isBuffer(chunk)) {
        chunks.push(chunk);
        continue;
      }

      chunks.push(Buffer.from(String(chunk)));
    }
    stdinCache = Buffer.concat(chunks).toString('utf8').trim();
    return stdinCache;
  };

  if (group === 'ping') {
    validateOptions(parsed.positionals.slice(1), 'ping', [], ['--json']);
    const backend = await createBackendClient(config, config.defaultBackend);
    try {
      printOutput(await backend.ping(), parsed.json);
      return;
    } finally {
      await backend.dispose();
    }
  }

  if (group === 'trace') {
    validateOptions(
      parsed.positionals.slice(1),
      'trace',
      [
        '--proxy-host',
        '--proxy-port',
        '--suppress-http-paths',
        '--suppress-http2-frame-types',
        '--upstream-host',
        '--upstream-port',
        '--verbose',
      ],
      [
        '--no-default-suppressions',
        '--suppress-http1-bodies',
        '--suppress-http1-headers',
      ],
    );
    await runTraceProxy({
      proxyHost: parseOption(parsed.positionals, '--proxy-host'),
      proxyPort: parseOption(parsed.positionals, '--proxy-port'),
      upstreamHost: parseOption(parsed.positionals, '--upstream-host'),
      upstreamPort: parseOption(parsed.positionals, '--upstream-port'),
      verbosity: parseOption(parsed.positionals, '--verbose'),
      suppressHttpPaths: parseOption(
        parsed.positionals,
        '--suppress-http-paths',
      ),
      suppressHttp2FrameTypes: parseOption(
        parsed.positionals,
        '--suppress-http2-frame-types',
      ),
      suppressHttp1Headers: parsed.positionals.includes(
        '--suppress-http1-headers',
      ),
      suppressHttp1Bodies: parsed.positionals.includes(
        '--suppress-http1-bodies',
      ),
      useDefaultSuppressions: parsed.positionals.includes(
        '--no-default-suppressions',
      )
        ? false
        : undefined,
    });
    return;
  }

  if (group === 'bench' && action === 'report') {
    validateOptions(parsed.positionals.slice(2), 'bench report', []);
    await runBenchmarkReport();
    return;
  }

  if (group === 'stream' && action === 'read' && target) {
    validateOptions(
      parsed.positionals.slice(3),
      'stream read',
      ['--from', '--limit'],
      ['--json'],
    );
    const backend = await createBackendClient(config, config.defaultBackend);
    try {
      const events = await backend.readStream(target, {
        fromRevision: parseOptionalBigInt(parseOption(parsed.positionals, '--from')),
        limit: parseLimit(parseOption(parsed.positionals, '--limit')),
      });
      printOutput(events, parsed.json);
      return;
    } finally {
      await backend.dispose();
    }
  }

  if (group === 'stream' && action === 'append' && target) {
    validateOptions(
      parsed.positionals.slice(3),
      'stream append',
      ['--data', '--expected-revision', '--metadata', '--type'],
      ['--json'],
    );
    const backend = await createBackendClient(config, config.defaultBackend);
    try {
      const metadataValue = parseOption(parsed.positionals, '--metadata');
      const result = await backend.appendToStream(
        target,
        [
          {
            eventType: requireOption(parsed.positionals, '--type'),
            data: await parseJsonInput(
              requireOption(parsed.positionals, '--data'),
              readStdinOnce,
            ),
            metadata: metadataValue
              ? await parseJsonInput(metadataValue, readStdinOnce)
              : undefined,
          },
        ],
        parseExpectedRevision(parseOption(parsed.positionals, '--expected-revision')),
      );

      printOutput(result, parsed.json);
      return;
    } finally {
      await backend.dispose();
    }
  }

  if (group === 'stream' && action === 'tail' && target) {
    validateOptions(
      parsed.positionals.slice(3),
      'stream tail',
      ['--from'],
      ['--json'],
    );
    const backend = await createBackendClient(config, config.defaultBackend);
    try {
      for await (const event of backend.subscribeToStream(
        target,
        parseOptionalBigInt(parseOption(parsed.positionals, '--from')),
      )) {
        printOutput(event, parsed.json);
      }
      return;
    } finally {
      await backend.dispose();
    }
  }

  if (group === 'test' && action === 'append') {
    validateOptions(parsed.positionals.slice(2), 'test append', [], ['--json']);
    const backend = await createBackendClient(config, config.defaultBackend);
    try {
      printOutput(await runAppendTest(backend), parsed.json);
      return;
    } finally {
      await backend.dispose();
    }
  }

  if (group === 'test' && action === 'read') {
    validateOptions(parsed.positionals.slice(2), 'test read', [], ['--json']);
    const backend = await createBackendClient(config, config.defaultBackend);
    try {
      printOutput(await runReadTest(backend), parsed.json);
      return;
    } finally {
      await backend.dispose();
    }
  }

  if (group === 'test' && action === 'subscribe') {
    validateOptions(parsed.positionals.slice(2), 'test subscribe', [], ['--json']);
    const backend = await createBackendClient(config, config.defaultBackend);
    try {
      printOutput(await runSubscribeTest(backend), parsed.json);
      return;
    } finally {
      await backend.dispose();
    }
  }

  if (group === 'test' && action === 'compare') {
    validateOptions(parsed.positionals.slice(2), 'test compare', ['--stream'], ['--json']);
    const stream = requireOption(parsed.positionals, '--stream');
    const reference = await createBackendClient(config, 'reference');
    const adapter = await createBackendClient(config, 'adapter');

    try {
      printOutput(await runCompareTest(reference, adapter, stream), parsed.json);
      return;
    } finally {
      await reference.dispose();
      await adapter.dispose();
    }
  }

  if (group === 'config' && action === 'show') {
    validateOptions(parsed.positionals.slice(2), 'config show', [], ['--json']);
    printOutput(await inspectConfig(), parsed.json);
    return;
  }

  if (group === 'tui') {
    validateOptions(parsed.positionals.slice(1), 'tui', ['--stream']);
    const stream = requireOption(parsed.positionals, '--stream');
    const backend = await createBackendClient(config, config.defaultBackend);
    await runTui(backend, stream);
    return;
  }

  throw new CliError(`Unsupported command: ${parsed.positionals.join(' ')}. Use kcli --help.`, 2);
}
