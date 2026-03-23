import { KurrentDBClient } from '@kurrent/kurrentdb-client';
import { GenericContainer, StartedTestContainer, Wait } from 'testcontainers';
import { StreamsContractBackend } from './streams-contract-suite';

const DEFAULT_KURRENTDB_GRPC_PORT = 2113;

function createClient(connectionString: string): KurrentDBClient {
  return KurrentDBClient.connectionString([
    connectionString,
  ] as unknown as TemplateStringsArray);
}

async function waitForWarmup(): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, 2_000));
}

export async function setupKurrentDbBackend(): Promise<StreamsContractBackend> {
  const configuredConnectionString =
    process.env.KURRENTDB_TEST_CONNECTION_STRING;

  if (configuredConnectionString) {
    const client = createClient(configuredConnectionString);

    return {
      getClient: () => client,
      supportsRestart: false,
      restart: () =>
        Promise.reject(
          new Error(
            'Restart is not supported when KURRENTDB_TEST_CONNECTION_STRING is used.',
          ),
        ),
      dispose: async () => {
        await client.dispose();
      },
    };
  }

  const image =
    process.env.KURRENTDB_TEST_IMAGE ??
    'docker.kurrent.io/kurrent-latest/kurrentdb:latest';

  let container: StartedTestContainer;
  let client: KurrentDBClient;

  async function startContainer(): Promise<void> {
    container = await new GenericContainer(image)
      .withExposedPorts(DEFAULT_KURRENTDB_GRPC_PORT)
      .withEnvironment({
        KURRENTDB_INSECURE: 'true',
      })
      .withWaitStrategy(Wait.forListeningPorts())
      .start();

    const port = container.getMappedPort(DEFAULT_KURRENTDB_GRPC_PORT);
    await waitForWarmup();
    client = KurrentDBClient.connectionString`kurrentdb://127.0.0.1:${port}?tls=false`;
  }

  await startContainer();

  return {
    getClient: () => client,
    supportsRestart: false,
    restart: async () => {
      const port = container.getMappedPort(DEFAULT_KURRENTDB_GRPC_PORT);
      await client.dispose();
      await container.restart();
      client = KurrentDBClient.connectionString`kurrentdb://127.0.0.1:${port}?tls=false`;
    },
    dispose: async () => {
      await client?.dispose();
      await container?.stop();
    },
  };
}
