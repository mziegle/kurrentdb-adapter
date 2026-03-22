import { INestMicroservice } from '@nestjs/common';
import { Test, TestingModule } from '@nestjs/testing';
import { Transport } from '@nestjs/microservices';
import {
  KurrentDBClient,
  BACKWARDS,
  END,
  Direction,
  FORWARDS,
  START,
  StreamDeletedError,
  StreamNotFoundError,
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
    await startApp();
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

  async function readStreamEvents(
    streamName: string,
    direction: Direction = FORWARDS,
    fromRevision: bigint | typeof START | typeof END = START,
    maxCount = 10,
  ): Promise<Array<{ type: string; data: unknown }>> {
    const readEvents = client.readStream(streamName, {
      fromRevision,
      direction,
      maxCount,
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

    return received;
  }

  async function startApp(): Promise<void> {
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
  }

  async function restartApp(): Promise<void> {
    await client.dispose();
    await app.close();
    await startApp();
  }

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

    expect(await readStreamEvents(streamName)).toEqual([
      {
        type: 'booking-created',
        data: {
          foo: 'bar',
        },
      },
    ]);
  });

  it('rejects stale expected revisions and keeps stream contents unchanged', async () => {
    const streamName = 'booking-concurrency';

    const firstAppend = await client.appendToStream(
      streamName,
      jsonEvent({
        type: 'booking-created',
        data: { step: 1 },
      }),
    );

    expect(firstAppend).toMatchObject({
      success: true,
      nextExpectedRevision: 0n,
    });

    const secondAppend = await client.appendToStream(
      streamName,
      jsonEvent({
        type: 'booking-confirmed',
        data: { step: 2 },
      }),
      {
        streamState: firstAppend.nextExpectedRevision,
      },
    );

    expect(secondAppend).toMatchObject({
      success: true,
      nextExpectedRevision: 1n,
    });

    let caughtError: unknown;
    try {
      await client.appendToStream(
        streamName,
        jsonEvent({
          type: 'booking-should-fail',
          data: { step: 999 },
        }),
        {
          streamState: firstAppend.nextExpectedRevision,
        },
      );
    } catch (error) {
      caughtError = error;
    }

    expect(caughtError).toMatchObject({
      type: 'wrong-expected-version',
      streamName,
      expectedState: 0n,
      actualState: 1n,
    });

    expect(await readStreamEvents(streamName)).toEqual([
      {
        type: 'booking-created',
        data: { step: 1 },
      },
      {
        type: 'booking-confirmed',
        data: { step: 2 },
      },
    ]);
  });

  it('returns stream not found when reading a missing stream', async () => {
    const streamName = 'missing-stream';

    let caughtError: unknown;
    try {
      await readStreamEvents(streamName);
    } catch (error) {
      caughtError = error;
    }

    expect(caughtError).toBeInstanceOf(StreamNotFoundError);
    expect(caughtError).toMatchObject({
      type: 'stream-not-found',
      streamName,
    });
  });

  it('preserves event order when appending multiple events in one call', async () => {
    const streamName = 'booking-batch';

    const appendResult = await client.appendToStream(streamName, [
      jsonEvent({
        type: 'booking-created',
        data: { step: 1 },
      }),
      jsonEvent({
        type: 'booking-confirmed',
        data: { step: 2 },
      }),
      jsonEvent({
        type: 'booking-completed',
        data: { step: 3 },
      }),
    ]);

    expect(appendResult).toMatchObject({
      success: true,
      nextExpectedRevision: 2n,
    });

    expect(await readStreamEvents(streamName)).toEqual([
      {
        type: 'booking-created',
        data: { step: 1 },
      },
      {
        type: 'booking-confirmed',
        data: { step: 2 },
      },
      {
        type: 'booking-completed',
        data: { step: 3 },
      },
    ]);
  });

  it('returns events newest-first when reading backwards', async () => {
    const streamName = 'booking-backwards';

    await client.appendToStream(streamName, [
      jsonEvent({
        type: 'booking-created',
        data: { step: 1 },
      }),
      jsonEvent({
        type: 'booking-confirmed',
        data: { step: 2 },
      }),
      jsonEvent({
        type: 'booking-completed',
        data: { step: 3 },
      }),
    ]);

    expect(await readStreamEvents(streamName, BACKWARDS, END)).toEqual([
      {
        type: 'booking-completed',
        data: { step: 3 },
      },
      {
        type: 'booking-confirmed',
        data: { step: 2 },
      },
      {
        type: 'booking-created',
        data: { step: 1 },
      },
    ]);
  });

  it('reads the correct slice when starting from a specific revision', async () => {
    const streamName = 'booking-revision-slice';

    await client.appendToStream(streamName, [
      jsonEvent({
        type: 'booking-created',
        data: { step: 1 },
      }),
      jsonEvent({
        type: 'booking-confirmed',
        data: { step: 2 },
      }),
      jsonEvent({
        type: 'booking-completed',
        data: { step: 3 },
      }),
    ]);

    expect(await readStreamEvents(streamName, FORWARDS, 1n)).toEqual([
      {
        type: 'booking-confirmed',
        data: { step: 2 },
      },
      {
        type: 'booking-completed',
        data: { step: 3 },
      },
    ]);

    expect(await readStreamEvents(streamName, BACKWARDS, 1n)).toEqual([
      {
        type: 'booking-confirmed',
        data: { step: 2 },
      },
      {
        type: 'booking-created',
        data: { step: 1 },
      },
    ]);
  });

  it('limits reads with maxCount in both directions', async () => {
    const streamName = 'booking-max-count';

    await client.appendToStream(streamName, [
      jsonEvent({
        type: 'booking-created',
        data: { step: 1 },
      }),
      jsonEvent({
        type: 'booking-confirmed',
        data: { step: 2 },
      }),
      jsonEvent({
        type: 'booking-completed',
        data: { step: 3 },
      }),
    ]);

    expect(await readStreamEvents(streamName, FORWARDS, START, 2)).toEqual([
      {
        type: 'booking-created',
        data: { step: 1 },
      },
      {
        type: 'booking-confirmed',
        data: { step: 2 },
      },
    ]);

    expect(await readStreamEvents(streamName, BACKWARDS, END, 2)).toEqual([
      {
        type: 'booking-completed',
        data: { step: 3 },
      },
      {
        type: 'booking-confirmed',
        data: { step: 2 },
      },
    ]);
  });

  it('keeps persisted events after restarting the app', async () => {
    const streamName = 'booking-restart-persistence';

    await client.appendToStream(streamName, [
      jsonEvent({
        type: 'booking-created',
        data: { step: 1 },
      }),
      jsonEvent({
        type: 'booking-confirmed',
        data: { step: 2 },
      }),
    ]);

    await restartApp();

    expect(await readStreamEvents(streamName)).toEqual([
      {
        type: 'booking-created',
        data: { step: 1 },
      },
      {
        type: 'booking-confirmed',
        data: { step: 2 },
      },
    ]);
  });

  it('deletes a stream and makes subsequent reads fail with stream not found', async () => {
    const streamName = 'booking-delete';

    await client.appendToStream(streamName, [
      jsonEvent({
        type: 'booking-created',
        data: { step: 1 },
      }),
      jsonEvent({
        type: 'booking-confirmed',
        data: { step: 2 },
      }),
    ]);

    const deleteResult = await client.deleteStream(streamName);

    expect(deleteResult).toHaveProperty('position');
    await expect(readStreamEvents(streamName)).rejects.toBeInstanceOf(
      StreamNotFoundError,
    );
  });

  it('rejects stale expected revisions when deleting and keeps the stream intact', async () => {
    const streamName = 'booking-delete-concurrency';

    await client.appendToStream(streamName, [
      jsonEvent({
        type: 'booking-created',
        data: { step: 1 },
      }),
      jsonEvent({
        type: 'booking-confirmed',
        data: { step: 2 },
      }),
    ]);

    let caughtError: unknown;
    try {
      await client.deleteStream(streamName, {
        expectedRevision: 0n,
      });
    } catch (error) {
      caughtError = error;
    }

    expect(caughtError).toMatchObject({
      type: 'unknown',
    });

    expect(await readStreamEvents(streamName)).toEqual([
      {
        type: 'booking-created',
        data: { step: 1 },
      },
      {
        type: 'booking-confirmed',
        data: { step: 2 },
      },
    ]);
  });

  it('tombstones a stream and rejects subsequent appends as stream deleted', async () => {
    const streamName = 'booking-tombstone';

    await client.appendToStream(streamName, jsonEvent({
      type: 'booking-created',
      data: { step: 1 },
    }));

    const tombstoneResult = await client.tombstoneStream(streamName);

    expect(tombstoneResult).toHaveProperty('position');

    await expect(
      client.appendToStream(
        streamName,
        jsonEvent({
          type: 'booking-should-fail',
          data: { step: 2 },
        }),
      ),
    ).rejects.toBeInstanceOf(StreamDeletedError);
  });

  it('rejects stale expected revisions when tombstoning and keeps the stream usable', async () => {
    const streamName = 'booking-tombstone-concurrency';

    await client.appendToStream(streamName, [
      jsonEvent({
        type: 'booking-created',
        data: { step: 1 },
      }),
      jsonEvent({
        type: 'booking-confirmed',
        data: { step: 2 },
      }),
    ]);

    let caughtError: unknown;
    try {
      await client.tombstoneStream(streamName, {
        expectedRevision: 0n,
      });
    } catch (error) {
      caughtError = error;
    }

    expect(caughtError).toMatchObject({
      type: 'unknown',
    });

    expect(await readStreamEvents(streamName)).toEqual([
      {
        type: 'booking-created',
        data: { step: 1 },
      },
      {
        type: 'booking-confirmed',
        data: { step: 2 },
      },
    ]);

    const appendResult = await client.appendToStream(
      streamName,
      jsonEvent({
        type: 'booking-completed',
        data: { step: 3 },
      }),
      {
        streamState: 1n,
      },
    );

    expect(appendResult).toMatchObject({
      success: true,
      nextExpectedRevision: 2n,
    });
  });

  it('rejects reads from a tombstoned stream as stream deleted', async () => {
    const streamName = 'booking-tombstone-read';

    await client.appendToStream(
      streamName,
      jsonEvent({
        type: 'booking-created',
        data: { step: 1 },
      }),
    );

    await client.tombstoneStream(streamName);

    await expect(readStreamEvents(streamName)).rejects.toMatchObject({
      name: 'UnknownError',
    });
  });
});
