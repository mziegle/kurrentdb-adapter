import { readFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import type { BackendName } from '../domain/types.js';

export interface BackendConfig {
  kind: 'kurrent';
  connectionString: string;
}

export interface CliConfig {
  defaultBackend: BackendName;
  backends: Record<BackendName, BackendConfig>;
}

const DEFAULT_CONFIG_FILE = 'kdb-cli.config.json';

function requireConnectionString(value: string | undefined, backend: BackendName): string {
  if (!value) {
    throw new Error(
      `Missing connection string for '${backend}'. Set env var KDB_${backend.toUpperCase()}_CONNECTION or add ${DEFAULT_CONFIG_FILE}.`,
    );
  }

  return value;
}

async function maybeLoadConfigFile(): Promise<Partial<CliConfig>> {
  const configuredPath = process.env.KDB_CLI_CONFIG_PATH;
  const targetPath = resolve(configuredPath ?? DEFAULT_CONFIG_FILE);

  try {
    const content = await readFile(targetPath, 'utf8');
    return JSON.parse(content) as Partial<CliConfig>;
  } catch {
    return {};
  }
}

export async function loadConfig(): Promise<CliConfig> {
  const fileConfig = await maybeLoadConfigFile();

  const referenceFromFile = fileConfig.backends?.reference?.connectionString;
  const adapterFromFile = fileConfig.backends?.adapter?.connectionString;

  const referenceConnectionString = requireConnectionString(
    process.env.KDB_REFERENCE_CONNECTION ?? referenceFromFile,
    'reference',
  );
  const adapterConnectionString = requireConnectionString(
    process.env.KDB_ADAPTER_CONNECTION ?? adapterFromFile,
    'adapter',
  );

  return {
    defaultBackend: fileConfig.defaultBackend ?? 'adapter',
    backends: {
      reference: {
        kind: 'kurrent',
        connectionString: referenceConnectionString,
      },
      adapter: {
        kind: 'kurrent',
        connectionString: adapterConnectionString,
      },
    },
  };
}
