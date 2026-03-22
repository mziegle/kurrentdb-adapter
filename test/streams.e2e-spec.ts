import { INestMicroservice } from '@nestjs/common';
import { Test, TestingModule } from '@nestjs/testing';
import { Transport } from '@nestjs/microservices';
import {
  KurrentDBClient,
  FORWARDS,
  START,
  jsonEvent,
} from '@kurrent/kurrentdb-client';
import {
  PostgreSqlContainer,
  StartedPostgreSqlContainer,
} from '@testcontainers/postgresql';
import { join } from 'node:path';
import { AppModule } from '../src/app.module';
import { getFreePort } from './util/get-free-port';

describe('Streams', () => {
  jest.setTimeout(120_000);

  let app: INestMicroservice;
  let client: KurrentDBClient;
  let pgContainer: StartedPostgreSqlContainer;
  let grpcPort: number;
  let originalPostgresUrl: string | undefined;

  beforeAll(async () => {
    originalPostgresUrl = process.env.POSTGRES_URL;
    grpcPort = await getFreePort();

    pgContainer = await new PostgreSqlContainer('postgres:16-alpine')
      .withDatabase('kurrentdb_adapter_test')
      .withUsername('postgres')
      .withPassword('postgres')
      .start();

    process.env.POSTGRES_URL = pgContainer.getConnectionUri();

    const moduleFixture: TestingModule = await Test.createTestingModule({
      imports: [AppModule],
    }).compile();

    app = moduleFixture.createNestMicroservice({
      transport: Transport.GRPC,
      options: {
        package: 'event_store.client.streams',
        protoPath: join(__dirname, '../src/protos/streams.proto'),
        url: `127.0.0.1:${grpcPort}`,
      },
    });

    await app.listen();

    client = KurrentDBClient.connectionString`kurrentdb://127.0.0.1:${grpcPort}?tls=false`;
  });

  afterAll(async () => {
    await client?.dispose();
    await app?.close();
    await pgContainer?.stop();

    if (originalPostgresUrl === undefined) {
      delete process.env.POSTGRES_URL;
    } else {
      process.env.POSTGRES_URL = originalPostgresUrl;
    }
  });

  it('writes events and reads them back over the KurrentDB interface', async () => {
    const streamName = 'booking-abc123';

    const event = jsonEvent({
      type: 'booking-created',
      data: {
        foo: 'bar',
      },
    });

    const appendResult = await client.appendToStream(streamName, event);

    expect(appendResult).toMatchObject({
      success: true,
      nextExpectedRevision: 0n,
    });

    const readEvents = client.readStream(streamName, {
      fromRevision: START,
      direction: FORWARDS,
      maxCount: 10,
    });

    const received: Array<{ type: string; data: unknown }> = [];
    for await (const { event: readEvent } of readEvents) {
      if (!readEvent) {
        continue;
      }

      received.push({
        type: readEvent.type,
        data: readEvent.data,
      });
    }

    expect(received).toEqual([
      {
        type: 'booking-created',
        data: {
          foo: 'bar',
        },
      },
    ]);
  });
});
