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

export interface ConfigValueInfo {
  connectionString: string;
  source: string;
}

export interface ConfigInspection {
  compareEndpoint?: ConfigValueInfo;
  configPath: string;
  defaultEndpoint: ConfigValueInfo;
}

const DEFAULT_CONFIG_FILE = 'kcli.config.json';
const DEFAULT_CONNECTION_STRING = 'kurrentdb://127.0.0.1:2113?tls=false';

type ConfigFileLoad = {
  config: Partial<CliConfig>;
  configPath: string;
};

function resolveConfigPath(): string {
  return resolve(process.env.KDB_CLI_CONFIG_PATH ?? DEFAULT_CONFIG_FILE);
}

async function maybeLoadConfigFile(): Promise<ConfigFileLoad> {
  const configPath = resolveConfigPath();

  try {
    const content = await readFile(configPath, 'utf8');
    return {
      config: JSON.parse(content) as Partial<CliConfig>,
      configPath,
    };
  } catch {
    return {
      config: {},
      configPath,
    };
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
  const { config: fileConfig } = await maybeLoadConfigFile();

  const defaultConnectionFromFile =
    (fileConfig as Partial<{ connectionString: string }>).connectionString;
  const compareConnectionFromFile =
    (fileConfig as Partial<{ compareConnectionString: string }>)
      .compareConnectionString;
  const referenceFromFile = fileConfig.backends?.reference?.connectionString;
  const adapterFromFile = fileConfig.backends?.adapter?.connectionString;

  return {
    defaultBackend: fileConfig.defaultBackend ?? 'adapter',
    backends: {
      reference: maybeBackendConfig(
        process.env.KDB_COMPARE_CONNECTION ??
          process.env.KDB_REFERENCE_CONNECTION ??
          compareConnectionFromFile ??
          referenceFromFile,
      ),
      adapter: maybeBackendConfig(
        process.env.KDB_CONNECTION ??
          process.env.KDB_ADAPTER_CONNECTION ??
          defaultConnectionFromFile ??
          adapterFromFile ??
          DEFAULT_CONNECTION_STRING,
      ),
    },
  };
}

export async function inspectConfig(): Promise<ConfigInspection> {
  const { config: fileConfig, configPath } = await maybeLoadConfigFile();
  const defaultConnectionFromFile =
    (fileConfig as Partial<{ connectionString: string }>).connectionString;
  const compareConnectionFromFile =
    (fileConfig as Partial<{ compareConnectionString: string }>)
      .compareConnectionString;
  const referenceFromFile = fileConfig.backends?.reference?.connectionString;
  const adapterFromFile = fileConfig.backends?.adapter?.connectionString;

  const defaultEndpoint = resolveValueInfo([
    ['env:KDB_CONNECTION', process.env.KDB_CONNECTION],
    ['env:KDB_ADAPTER_CONNECTION', process.env.KDB_ADAPTER_CONNECTION],
    ['file:connectionString', defaultConnectionFromFile],
    ['file:backends.adapter.connectionString', adapterFromFile],
    ['built-in default', DEFAULT_CONNECTION_STRING],
  ]);

  const compareEndpoint = resolveValueInfo([
    ['env:KDB_COMPARE_CONNECTION', process.env.KDB_COMPARE_CONNECTION],
    ['env:KDB_REFERENCE_CONNECTION', process.env.KDB_REFERENCE_CONNECTION],
    ['file:compareConnectionString', compareConnectionFromFile],
    ['file:backends.reference.connectionString', referenceFromFile],
  ]);

  return {
    compareEndpoint,
    configPath,
    defaultEndpoint: defaultEndpoint!,
  };
}

export function requireBackendConfig(
  config: CliConfig,
  backend: BackendName,
): BackendConfig {
  const backendConfig = config.backends[backend];

  if (!backendConfig) {
    const hint =
      backend === 'reference'
        ? `Set KDB_COMPARE_CONNECTION or add compareConnectionString to ${DEFAULT_CONFIG_FILE}.`
        : `Set KDB_CONNECTION or add connectionString to ${DEFAULT_CONFIG_FILE}.`;

    throw new Error(
      `Missing connection string for '${backend}'. ${hint}`,
    );
  }

  return backendConfig;
}

function resolveValueInfo(
  candidates: Array<[source: string, value: string | undefined]>,
): ConfigValueInfo | undefined {
  for (const [source, value] of candidates) {
    if (value) {
      return {
        connectionString: value,
        source,
      };
    }
  }

  return undefined;
}
