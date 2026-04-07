import type { CliConfig } from '../config/config.js';
import { loadConfig } from '../config/config.js';
import type { BackendName, ExpectedRevision } from '../domain/types.js';
import { createBackendClient } from '../infrastructure/backends/factory.js';
import { printOutput } from '../infrastructure/output/format.js';
import {
  runAppendTest,
  runCompareTest,
  runReadTest,
  runSubscribeTest,
} from '../application/test-cases.js';
import { runTui } from '../tui/app.js';

interface ParsedArgs {
  backend?: BackendName;
  json: boolean;
  positionals: string[];
}

function parseArgs(argv: string[]): ParsedArgs {
  const positionals: string[] = [];
  const parsed: ParsedArgs = { json: false, positionals };

  for (let index = 0; index < argv.length; index += 1) {
    const value = argv[index];

    if (value === '--json') {
      parsed.json = true;
      continue;
    }

    if (value === '--backend' || value === '-b') {
      parsed.backend = argv[index + 1] as BackendName;
      index += 1;
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
    throw new Error(`Missing required option ${optionName}`);
  }

  return value;
}

function parseOptionalBigInt(value: string | undefined): bigint | undefined {
  return value === undefined ? undefined : BigInt(value);
}

function parseExpectedRevision(value: string | undefined): ExpectedRevision | undefined {
  if (!value) {
    return undefined;
  }

  if (value === 'any' || value === 'no_stream' || value === 'stream_exists') {
    return value;
  }

  return BigInt(value);
}

function resolveBackend(parsed: ParsedArgs, config: CliConfig): BackendName {
  return parsed.backend ?? config.defaultBackend;
}

export async function runProgram(argv: string[]): Promise<void> {
  const parsed = parseArgs(argv);
  const [group, action, target] = parsed.positionals;

  if (!group) {
    throw new Error('No command provided. Try: ping, stream, test, tui');
  }

  const config = await loadConfig();

  if (group === 'ping') {
    const backend = await createBackendClient(config, resolveBackend(parsed, config));
    try {
      printOutput(await backend.ping(), parsed.json);
      return;
    } finally {
      await backend.dispose();
    }
  }

  if (group === 'stream' && action === 'read' && target) {
    const backend = await createBackendClient(config, resolveBackend(parsed, config));
    try {
      const events = await backend.readStream(target, {
        fromRevision: parseOptionalBigInt(parseOption(parsed.positionals, '--from')),
        limit: Number(parseOption(parsed.positionals, '--limit') ?? '100'),
      });
      printOutput(events, parsed.json);
      return;
    } finally {
      await backend.dispose();
    }
  }

  if (group === 'stream' && action === 'append' && target) {
    const backend = await createBackendClient(config, resolveBackend(parsed, config));
    try {
      const result = await backend.appendToStream(
        target,
        [
          {
            eventType: requireOption(parsed.positionals, '--type'),
            data: JSON.parse(requireOption(parsed.positionals, '--data')),
            metadata: parseOption(parsed.positionals, '--metadata')
              ? JSON.parse(requireOption(parsed.positionals, '--metadata'))
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
    const backend = await createBackendClient(config, resolveBackend(parsed, config));
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
    const backend = await createBackendClient(config, resolveBackend(parsed, config));
    try {
      printOutput(await runAppendTest(backend), parsed.json);
      return;
    } finally {
      await backend.dispose();
    }
  }

  if (group === 'test' && action === 'read') {
    const backend = await createBackendClient(config, resolveBackend(parsed, config));
    try {
      printOutput(await runReadTest(backend), parsed.json);
      return;
    } finally {
      await backend.dispose();
    }
  }

  if (group === 'test' && action === 'subscribe') {
    const backend = await createBackendClient(config, resolveBackend(parsed, config));
    try {
      printOutput(await runSubscribeTest(backend), parsed.json);
      return;
    } finally {
      await backend.dispose();
    }
  }

  if (group === 'test' && action === 'compare') {
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

  if (group === 'tui') {
    const stream = requireOption(parsed.positionals, '--stream');
    const backend = await createBackendClient(config, resolveBackend(parsed, config));
    await runTui(backend, stream);
    return;
  }

  throw new Error(`Unsupported command: ${parsed.positionals.join(' ')}`);
}
