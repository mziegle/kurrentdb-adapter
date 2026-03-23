import { INestMicroservice } from '@nestjs/common';
import { Test, TestingModule } from '@nestjs/testing';
import { Transport } from '@nestjs/microservices';
import { KurrentDBClient } from '@kurrent/kurrentdb-client';
import {
  PostgreSqlContainer,
  StartedPostgreSqlContainer,
} from '@testcontainers/postgresql';
import { join } from 'node:path';
import { AppModule } from '../../src/app.module';
import { getFreePort } from '../util/get-free-port';
import { StreamsContractBackend } from './contract-test-context';

export async function setupAdapterBackend(): Promise<StreamsContractBackend> {
  let app: INestMicroservice;
  let client: KurrentDBClient;
  let grpcPort = await getFreePort();
  const originalPostgresUrl = process.env.POSTGRES_URL;
  const pgContainer: StartedPostgreSqlContainer = await new PostgreSqlContainer(
    'postgres:16-alpine',
  )
    .withDatabase('kurrentdb_adapter_test')
    .withUsername('postgres')
    .withPassword('postgres')
    .start();

  async function startApp(): Promise<void> {
    const moduleFixture: TestingModule = await Test.createTestingModule({
      imports: [AppModule],
    }).compile();

    app = moduleFixture.createNestMicroservice({
      transport: Transport.GRPC,
      options: {
        package: 'event_store.client.streams',
        protoPath: join(__dirname, '../../proto/streams.proto'),
        url: `127.0.0.1:${grpcPort}`,
      },
    });

    await app.listen();
    client = KurrentDBClient.connectionString`kurrentdb://127.0.0.1:${grpcPort}?tls=false`;
  }

  process.env.POSTGRES_URL = pgContainer.getConnectionUri();
  await startApp();

  return {
    getClient: () => client,
    supportsRestart: true,
    restart: async () => {
      await client.dispose();
      await app.close();
      grpcPort = await getFreePort();
      await startApp();
    },
    dispose: async () => {
      await client?.dispose();
      await app?.close();
      await pgContainer?.stop();

      if (originalPostgresUrl === undefined) {
        delete process.env.POSTGRES_URL;
      } else {
        process.env.POSTGRES_URL = originalPostgresUrl;
      }
    },
  };
}
