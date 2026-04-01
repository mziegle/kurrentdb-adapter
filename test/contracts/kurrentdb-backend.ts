import { KurrentDBClient } from '@kurrent/kurrentdb-client';
import { GenericContainer, StartedTestContainer, Wait } from 'testcontainers';
import { wrapClientTimeouts } from '../util/wrap-client-timeouts';
import {
  createOperationsClient,
  createStartScavengeRequest,
  mapScavengeResponse,
  OperationsScavengeResponse,
  unaryCall,
} from './operations-client';
import { ScavengeCapableBackend } from './contract-test-context';

const DEFAULT_KURRENTDB_GRPC_PORT = 2113;

function createClient(connectionString: string): KurrentDBClient {
  return wrapClientTimeouts(
    KurrentDBClient.connectionString([
      connectionString,
    ] as unknown as TemplateStringsArray),
  );
}

function createContainerConnectionString(
  container: StartedTestContainer,
): string {
  return `kurrentdb://${container.getHost()}:${container.getMappedPort(
    DEFAULT_KURRENTDB_GRPC_PORT,
  )}?tls=false`;
}

async function waitForWarmup(): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, 2_000));
}

export async function setupKurrentDbBackend(): Promise<ScavengeCapableBackend> {
  const configuredConnectionString =
    process.env.KURRENTDB_TEST_CONNECTION_STRING;

  if (configuredConnectionString) {
    const client = createClient(configuredConnectionString);
    const operationsClient = createOperationsClient(configuredConnectionString);
    const parsed = new URL(
      configuredConnectionString.replace(/^kurrentdb:/, 'http:'),
    );

    return {
      getClient: () => client,
      getGrpcAddress: () =>
        `${parsed.hostname}:${parsed.port || DEFAULT_KURRENTDB_GRPC_PORT}`,
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
      startScavenge: async () => {
        const request = createStartScavengeRequest();
        const response = await unaryCall<OperationsScavengeResponse>(
          (callback) => {
            operationsClient.startScavenge(request, callback);
          },
        );
        return mapScavengeResponse(response);
      },
    };
  }

  const image =
    process.env.KURRENTDB_TEST_IMAGE ??
    'docker.kurrent.io/kurrent-latest/kurrentdb:latest';

  let container: StartedTestContainer;
  let client: KurrentDBClient;
  let operationsClient: ReturnType<typeof createOperationsClient>;

  async function startContainer(): Promise<void> {
    container = await new GenericContainer(image)
      .withExposedPorts(DEFAULT_KURRENTDB_GRPC_PORT)
      .withEnvironment({
        KURRENTDB_INSECURE: 'true',
      })
      .withWaitStrategy(Wait.forListeningPorts())
      .start();

    await waitForWarmup();
    const connectionString = createContainerConnectionString(container);
    const parsed = new URL(connectionString.replace(/^kurrentdb:/, 'http:'));
    client = createClient(connectionString);
    operationsClient = createOperationsClient(
      `${parsed.hostname}:${parsed.port || DEFAULT_KURRENTDB_GRPC_PORT}`,
    );
  }

  await startContainer();

  return {
    getClient: () => client,
    getGrpcAddress: () =>
      `${container.getHost()}:${container.getMappedPort(
        DEFAULT_KURRENTDB_GRPC_PORT,
      )}`,
    supportsRestart: true,
    restart: async () => {
      await client.dispose();
      await container.restart();
      await waitForWarmup();
      const connectionString = createContainerConnectionString(container);
      const parsed = new URL(connectionString.replace(/^kurrentdb:/, 'http:'));
      client = createClient(connectionString);
      operationsClient = createOperationsClient(
        `${parsed.hostname}:${parsed.port || DEFAULT_KURRENTDB_GRPC_PORT}`,
      );
    },
    dispose: async () => {
      await client?.dispose();
      await container?.stop();
    },
    startScavenge: async () => {
      const request = createStartScavengeRequest();
      const response = await unaryCall<OperationsScavengeResponse>(
        (callback) => {
          operationsClient.startScavenge(request, callback);
        },
      );
      return mapScavengeResponse(response);
    },
  };
}
