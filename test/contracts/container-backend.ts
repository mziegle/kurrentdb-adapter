import { KurrentDBClient } from '@kurrent/kurrentdb-client';
import { credentials } from '@grpc/grpc-js';
import {
  OperationsClient,
  IOperationsClient,
} from '@kurrent/kurrentdb-client/generated/kurrentdb/protocols/v1/operations_grpc_pb';
import {
  ScavengeResp,
  StartScavengeReq,
} from '@kurrent/kurrentdb-client/generated/kurrentdb/protocols/v1/operations_pb';
import {
  GenericContainer,
  Network,
  StartedNetwork,
  StartedTestContainer,
  Wait,
} from 'testcontainers';
import { wrapClientTimeouts } from '../util/wrap-client-timeouts';
import { ScavengeCapableBackend } from './contract-test-context';

const DEFAULT_POSTGRES_IMAGE = 'postgres:16-alpine';
const DEFAULT_ADAPTER_IMAGE = 'kurrentdb-adapter:local';
const DEFAULT_GRPC_PORT = 2113;
const NETWORK_ALIAS = 'postgres';

function createClient(grpcAddress: string): KurrentDBClient {
  return wrapClientTimeouts(
    KurrentDBClient.connectionString`kurrentdb://${grpcAddress}?tls=false`,
  );
}

function createOperationsClient(grpcAddress: string): IOperationsClient {
  return new OperationsClient(grpcAddress, credentials.createInsecure());
}

async function waitForWarmup(): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, 2_000));
}

export async function setupContainerBackend(): Promise<ScavengeCapableBackend> {
  const adapterImage = process.env.ADAPTER_TEST_IMAGE ?? DEFAULT_ADAPTER_IMAGE;
  const network: StartedNetwork = await new Network().start();
  let postgresContainer: StartedTestContainer;
  let adapterContainer: StartedTestContainer;
  let client: KurrentDBClient;
  let operationsClient: IOperationsClient;

  async function startPostgresContainer(): Promise<void> {
    postgresContainer = await new GenericContainer(DEFAULT_POSTGRES_IMAGE)
      .withNetwork(network)
      .withNetworkAliases(NETWORK_ALIAS)
      .withEnvironment({
        POSTGRES_DB: 'kurrentdb_adapter_test',
        POSTGRES_USER: 'postgres',
        POSTGRES_PASSWORD: 'postgres',
      })
      .withExposedPorts(5432)
      .withWaitStrategy(Wait.forListeningPorts())
      .start();
  }

  async function startAdapterContainer(): Promise<void> {
    adapterContainer = await new GenericContainer(adapterImage)
      .withNetwork(network)
      .withEnvironment({
        POSTGRES_URL:
          'postgres://postgres:postgres@postgres:5432/kurrentdb_adapter_test',
        GRPC_URL: `0.0.0.0:${DEFAULT_GRPC_PORT}`,
      })
      .withExposedPorts(DEFAULT_GRPC_PORT)
      .withWaitStrategy(Wait.forListeningPorts())
      .start();

    await waitForWarmup();

    const grpcAddress = `${adapterContainer.getHost()}:${adapterContainer.getMappedPort(
      DEFAULT_GRPC_PORT,
    )}`;
    client = createClient(grpcAddress);
    operationsClient = createOperationsClient(grpcAddress);
  }

  async function stopAdapterContainer(): Promise<void> {
    await adapterContainer?.stop();
  }

  async function stopPostgresContainer(): Promise<void> {
    await postgresContainer?.stop();
  }

  await startPostgresContainer();
  await startAdapterContainer();

  return {
    getClient: () => client,
    getGrpcAddress: () =>
      `${adapterContainer.getHost()}:${adapterContainer.getMappedPort(
        DEFAULT_GRPC_PORT,
      )}`,
    supportsRestart: true,
    restart: async () => {
      await client.dispose();
      await stopAdapterContainer();
      await startAdapterContainer();
    },
    dispose: async () => {
      await client?.dispose();
      await stopAdapterContainer();
      await stopPostgresContainer();
      await network?.stop();
    },
    startScavenge: async () => {
      const request = createStartScavengeRequest();
      const response = await unaryCall<ScavengeResp>((callback) => {
        operationsClient.startScavenge(request, callback);
      });

      return {
        scavengeId: response.getScavengeId(),
        scavengeResult: response.getScavengeResult(),
      };
    },
  };
}

function unaryCall<TResponse>(
  invoke: (
    callback: (error: Error | null, response: TResponse) => void,
  ) => void,
): Promise<TResponse> {
  return new Promise<TResponse>((resolve, reject) => {
    invoke((error, response) => {
      if (error) {
        reject(error);
        return;
      }

      resolve(response);
    });
  });
}

function createStartScavengeRequest(): StartScavengeReq {
  const options = new StartScavengeReq.Options();
  options.setThreadCount(1);
  options.setStartFromChunk(0);

  const request = new StartScavengeReq();
  request.setOptions(options);

  return request;
}
