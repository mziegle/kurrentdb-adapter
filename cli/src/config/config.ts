import { readFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import type { BackendName } from '../domain/types.js';

export interface BackendConfig {
  kind: 'kurrent';
  connectionString: string;
}

export interface CliConfig {
  defaultBackend: BackendName;
  backends: Partial<Record<BackendName, BackendConfig>>;
}

const DEFAULT_CONFIG_FILE = 'kcli.config.json';

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

function maybeBackendConfig(
  value: string | undefined,
): BackendConfig | undefined {
  if (!value) {
    return undefined;
  }

  return {
    kind: 'kurrent',
    connectionString: value,
  };
}

export async function loadConfig(): Promise<CliConfig> {
  const fileConfig = await maybeLoadConfigFile();

  const referenceFromFile = fileConfig.backends?.reference?.connectionString;
  const adapterFromFile = fileConfig.backends?.adapter?.connectionString;

  return {
    defaultBackend: fileConfig.defaultBackend ?? 'adapter',
    backends: {
      reference: maybeBackendConfig(
        process.env.KDB_REFERENCE_CONNECTION ?? referenceFromFile,
      ),
      adapter: maybeBackendConfig(
        process.env.KDB_ADAPTER_CONNECTION ?? adapterFromFile,
      ),
    },
  };
}

export function requireBackendConfig(
  config: CliConfig,
  backend: BackendName,
): BackendConfig {
  const backendConfig = config.backends[backend];

  if (!backendConfig) {
    throw new Error(
      `Missing connection string for '${backend}'. Set env var KDB_${backend.toUpperCase()}_CONNECTION or add ${DEFAULT_CONFIG_FILE}.`,
    );
  }

  return backendConfig;
}
