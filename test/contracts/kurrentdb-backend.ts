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
import { GenericContainer, StartedTestContainer, Wait } from 'testcontainers';
import { wrapClientTimeouts } from '../util/wrap-client-timeouts';
import { ScavengeCapableBackend } from './contract-test-context';

const DEFAULT_KURRENTDB_GRPC_PORT = 2113;

function createClient(connectionString: string): KurrentDBClient {
  return wrapClientTimeouts(
    KurrentDBClient.connectionString([
      connectionString,
    ] as unknown as TemplateStringsArray),
  );
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

  const image =
    process.env.KURRENTDB_TEST_IMAGE ??
    'docker.kurrent.io/kurrent-latest/kurrentdb:latest';

  let container: StartedTestContainer;
  let client: KurrentDBClient;
  let operationsClient: IOperationsClient;

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
    client = wrapClientTimeouts(
      KurrentDBClient.connectionString`kurrentdb://127.0.0.1:${port}?tls=false`,
    );
    operationsClient = new OperationsClient(
      `127.0.0.1:${port}`,
      credentials.createInsecure(),
    );
  }

  await startContainer();

  return {
    getClient: () => client,
    supportsRestart: false,
    restart: async () => {
      const port = container.getMappedPort(DEFAULT_KURRENTDB_GRPC_PORT);
      await client.dispose();
      await container.restart();
      client = wrapClientTimeouts(
        KurrentDBClient.connectionString`kurrentdb://127.0.0.1:${port}?tls=false`,
      );
    },
    dispose: async () => {
      await client?.dispose();
      await container?.stop();
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

function createOperationsClient(connectionString: string): IOperationsClient {
  const parsed = new URL(connectionString.replace(/^kurrentdb:/, 'http:'));
  return new OperationsClient(
    `${parsed.hostname}:${parsed.port || DEFAULT_KURRENTDB_GRPC_PORT}`,
    credentials.createInsecure(),
  );
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
