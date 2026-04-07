import type { CliConfig } from '../../config/config.js';
import type { BackendClient } from '../../domain/backend.js';
import type { BackendName } from '../../domain/types.js';

export async function createBackendClient(
  config: CliConfig,
  backend: BackendName,
): Promise<BackendClient> {
  const backendConfig = config.backends[backend];

  if (backendConfig.kind === 'kurrent') {
    try {
      const module = await import('./kurrent-backend-client.js');
      return new module.KurrentBackendClient(backend, backendConfig.connectionString);
    } catch (error) {
      throw new Error(
        `Failed to load Kurrent backend client dependency. Install '@kurrent/kurrentdb-client'. Cause: ${error instanceof Error ? error.message : String(error)}`,
      );
    }
  }

  throw new Error(`Unsupported backend kind for ${backend}.`);
}
