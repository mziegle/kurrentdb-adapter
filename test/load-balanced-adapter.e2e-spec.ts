import { INestMicroservice } from '@nestjs/common';
import { Test, TestingModule } from '@nestjs/testing';
import { Transport } from '@nestjs/microservices';
import {
  jsonEvent,
  KurrentDBClient,
  START,
  WrongExpectedVersionError,
} from '@kurrent/kurrentdb-client';
import {
  PostgreSqlContainer,
  StartedPostgreSqlContainer,
} from '@testcontainers/postgresql';
import { randomUUID } from 'node:crypto';
import { join } from 'node:path';
import { AppModule } from '../src/app.module';
import { getFreePort } from './util/get-free-port';
import { wrapClientTimeouts } from './util/wrap-client-timeouts';

type RunningAdapterInstance = {
  app: INestMicroservice;
  client: KurrentDBClient;
};

async function startAdapterInstance(grpcPort: number): Promise<RunningAdapterInstance> {
  const moduleFixture: TestingModule = await Test.createTestingModule({
    imports: [AppModule],
  }).compile();

  const app = moduleFixture.createNestMicroservice({
    transport: Transport.GRPC,
    options: {
      package: [
        'event_store.client.operations',
        'event_store.client.streams',
        'event_store.client.server_features',
      ],
      protoPath: [
        join(__dirname, '../proto/Grpc/operations.proto'),
        join(__dirname, '../proto/Grpc/streams.proto'),
        join(__dirname, '../proto/Grpc/serverfeatures.proto'),
      ],
      loader: {
        includeDirs: [join(__dirname, '../proto/Grpc')],
      },
      url: `127.0.0.1:${grpcPort}`,
    },
  });

  await app.listen();

  const client = wrapClientTimeouts(
    KurrentDBClient.connectionString`kurrentdb://127.0.0.1:${grpcPort}?tls=false`,
  );

  return { app, client };
}

describe('Adapter load-balanced concurrency', () => {
  jest.setTimeout(120_000);

  const backend = process.env.E2E_BACKEND;
  const runAdapterOnly = backend !== 'kurrentdb';

  const maybeIt = runAdapterOnly ? it : it.skip;

  let originalPostgresUrl: string | undefined;
  let pgContainer: StartedPostgreSqlContainer;
  let firstInstance: RunningAdapterInstance;
  let secondInstance: RunningAdapterInstance;

  beforeAll(async () => {
    originalPostgresUrl = process.env.POSTGRES_URL;

    pgContainer = await new PostgreSqlContainer('postgres:16-alpine')
      .withDatabase('kurrentdb_adapter_test')
      .withUsername('postgres')
      .withPassword('postgres')
      .start();

    process.env.POSTGRES_URL = pgContainer.getConnectionUri();

    const [firstPort, secondPort] = await Promise.all([
      getFreePort(),
      getFreePort(),
    ]);

    [firstInstance, secondInstance] = await Promise.all([
      startAdapterInstance(firstPort),
      startAdapterInstance(secondPort),
    ]);
  });

  afterAll(async () => {
    await Promise.all([
      firstInstance?.client.dispose(),
      secondInstance?.client.dispose(),
      firstInstance?.app.close(),
      secondInstance?.app.close(),
    ]);

    await pgContainer?.stop();

    if (originalPostgresUrl === undefined) {
      delete process.env.POSTGRES_URL;
      return;
    }

    process.env.POSTGRES_URL = originalPostgresUrl;
  });

  async function readStreamPayloads<T>(streamName: string): Promise<T[]> {
    const events: T[] = [];
    for await (const resolved of firstInstance.client.readStream(streamName, {
      fromRevision: START,
      maxCount: 512,
    })) {
      if (!resolved.event) {
        continue;
      }

      events.push(resolved.event.data as T);
    }

    return events;
  }

  maybeIt('processes load-balanced concurrent writes without duplicate processing', async () => {
    const sharedStream = `lb-concurrent-${randomUUID()}`;

    const loadBalancedRequests = Array.from({ length: 60 }, (_, index) => {
      const client = index % 2 === 0 ? firstInstance.client : secondInstance.client;

      return client.appendToStream(
        sharedStream,
        jsonEvent({
          type: 'load-balanced-write',
          data: {
            requestId: index,
            instance: index % 2 === 0 ? 'first' : 'second',
          },
        }),
      );
    });

    await Promise.all(loadBalancedRequests);

    const writes = await readStreamPayloads<{
      requestId: number;
      instance: string;
    }>(sharedStream);

    expect(writes).toHaveLength(60);
    expect(new Set(writes.map(({ requestId }) => requestId)).size).toBe(60);
    expect(new Set(writes.map(({ instance }) => instance))).toEqual(
      new Set(['first', 'second']),
    );
  });

  maybeIt('enforces optimistic concurrency consistently across instances', async () => {
    const conflictStream = `lb-conflict-${randomUUID()}`;
    const seedResult = await firstInstance.client.appendToStream(
      conflictStream,
      jsonEvent({
        type: 'seed',
        data: { value: 'seed' },
      }),
    );

    let expectedRevision = seedResult.nextExpectedRevision;

    for (let round = 0; round < 20; round += 1) {
      const attemptFromFirst = firstInstance.client.appendToStream(
        conflictStream,
        jsonEvent({
          type: 'conflict-attempt',
          data: { round, instance: 'first' },
        }),
        { streamState: expectedRevision },
      );

      const attemptFromSecond = secondInstance.client.appendToStream(
        conflictStream,
        jsonEvent({
          type: 'conflict-attempt',
          data: { round, instance: 'second' },
        }),
        { streamState: expectedRevision },
      );

      const [firstResult, secondResult] = await Promise.allSettled([
        attemptFromFirst,
        attemptFromSecond,
      ]);

      const successfulWrites = [firstResult, secondResult].filter(
        (candidate): candidate is PromiseFulfilledResult<unknown> =>
          candidate.status === 'fulfilled',
      );

      const rejectedWrites = [firstResult, secondResult].filter(
        (candidate): candidate is PromiseRejectedResult =>
          candidate.status === 'rejected',
      );

      expect(successfulWrites).toHaveLength(1);
      expect(rejectedWrites).toHaveLength(1);
      expect(rejectedWrites[0].reason).toBeInstanceOf(WrongExpectedVersionError);

      expectedRevision = (successfulWrites[0].value as { nextExpectedRevision: bigint })
        .nextExpectedRevision;
    }

    const conflictEvents = await readStreamPayloads<{
      round?: number;
      instance?: string;
    }>(conflictStream);

    expect(conflictEvents).toHaveLength(21);

    const committedRounds = conflictEvents
      .filter(({ round }) => typeof round === 'number')
      .map(({ round }) => round as number);

    expect(committedRounds).toHaveLength(20);
    expect(new Set(committedRounds).size).toBe(20);
  });

  maybeIt('keeps both instances functional after concurrent traffic', async () => {
    const probeFromFirst = await firstInstance.client.appendToStream(
      `lb-probe-${randomUUID()}`,
      jsonEvent({
        type: 'probe',
        data: { via: 'first' },
      }),
    );

    const probeFromSecond = await secondInstance.client.appendToStream(
      `lb-probe-${randomUUID()}`,
      jsonEvent({
        type: 'probe',
        data: { via: 'second' },
      }),
    );

    expect(probeFromFirst).toMatchObject({ success: true });
    expect(probeFromSecond).toMatchObject({ success: true });
  });
});
