import { INestMicroservice } from '@nestjs/common';
import { Test } from '@nestjs/testing';
import { Transport } from '@nestjs/microservices';
import { KurrentDBClient } from '@kurrent/kurrentdb-client';
import { GenericContainer, StartedTestContainer, Wait } from 'testcontainers';
import { join } from 'node:path';
import { AppModule } from '../../src/app.module';
import { getFreePort } from '../util/get-free-port';
import { wrapClientTimeouts } from '../util/wrap-client-timeouts';
import { ScavengeCapableBackend } from './contract-test-context';
import {
  createOperationsClient,
  createStartScavengeRequest,
  mapScavengeResponse,
  OperationsScavengeResponse,
  unaryCall,
} from './operations-client';

export async function setupMongoDBBackend(
  options: { unsafeScavenge?: boolean } = {},
): Promise<ScavengeCapableBackend> {
  let container: StartedTestContainer | undefined;
  let app: INestMicroservice | undefined;
  let client: KurrentDBClient | undefined;
  let operationsClient: ReturnType<typeof createOperationsClient> | undefined;
  let grpcPort = await getFreePort();
  const keys = [
    'EVENT_STORE_BACKEND',
    'MONGODB_URI',
    'MONGODB_DATABASE',
    'MONGODB_UNSAFE_ENABLE_SCAVENGE',
  ] as const;
  const previous = Object.fromEntries(
    keys.map((key) => [key, process.env[key]]),
  );

  async function cleanup(): Promise<void> {
    try {
      operationsClient?.close();
      await client?.dispose();
      await app?.close();
      await container?.stop();
    } finally {
      for (const key of keys) {
        if (previous[key] === undefined) delete process.env[key];
        else process.env[key] = previous[key];
      }
    }
  }

  async function startApp(): Promise<void> {
    const fixture = await Test.createTestingModule({
      imports: [AppModule],
    }).compile();
    app = fixture.createNestMicroservice({
      transport: Transport.GRPC,
      options: {
        package: [
          'event_store.client.operations',
          'event_store.client.persistent_subscriptions',
          'event_store.client.streams',
          'event_store.client.server_features',
        ],
        protoPath: [
          join(__dirname, '../../proto/Grpc/operations.proto'),
          join(__dirname, '../../proto/Grpc/persistent.proto'),
          join(__dirname, '../../proto/Grpc/streams.proto'),
          join(__dirname, '../../proto/Grpc/serverfeatures.proto'),
        ],
        loader: { includeDirs: [join(__dirname, '../../proto/Grpc')] },
        url: `127.0.0.1:${grpcPort}`,
      },
    });
    await app.listen();
    client = wrapClientTimeouts(
      KurrentDBClient.connectionString`kurrentdb://127.0.0.1:${grpcPort}?tls=false`,
    );
    operationsClient = createOperationsClient(`127.0.0.1:${grpcPort}`);
  }

  try {
    container = await new GenericContainer('mongo:7')
      .withCommand(['mongod', '--replSet', 'rs0', '--bind_ip_all'])
      .withExposedPorts(27017)
      .withWaitStrategy(Wait.forLogMessage('Waiting for connections'))
      .start();
    const init = await container.exec([
      'mongosh',
      '--quiet',
      '--eval',
      "rs.initiate({_id:'rs0',members:[{_id:0,host:'localhost:27017'}]})",
    ]);
    if (init.exitCode !== 0) throw new Error(init.output);
    const election = await container.exec([
      'mongosh',
      '--quiet',
      '--eval',
      'while (!db.hello().isWritablePrimary) { sleep(200); }',
    ]);
    if (election.exitCode !== 0) throw new Error(election.output);
    process.env.EVENT_STORE_BACKEND = 'mongodb';
    process.env.MONGODB_URI = `mongodb://${container.getHost()}:${container.getMappedPort(27017)}/?directConnection=true`;
    process.env.MONGODB_DATABASE = 'adapter_contract_test';
    if (options.unsafeScavenge)
      process.env.MONGODB_UNSAFE_ENABLE_SCAVENGE = 'true';
    else delete process.env.MONGODB_UNSAFE_ENABLE_SCAVENGE;
    await startApp();
  } catch (error) {
    await cleanup();
    throw error;
  }

  return {
    getClient: () => client!,
    getGrpcAddress: () => `127.0.0.1:${grpcPort}`,
    supportsRestart: true,
    restart: async () => {
      operationsClient?.close();
      await client?.dispose();
      await app?.close();
      grpcPort = await getFreePort();
      await startApp();
    },
    startScavenge: async () => {
      const response = await unaryCall<OperationsScavengeResponse>(
        (callback) => {
          operationsClient!.startScavenge(
            createStartScavengeRequest(),
            callback,
          );
        },
      );
      return mapScavengeResponse(response);
    },
    dispose: cleanup,
  };
}
