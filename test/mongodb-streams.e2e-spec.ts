import { INestMicroservice } from '@nestjs/common';
import { Test } from '@nestjs/testing';
import { Transport } from '@nestjs/microservices';
import {
  KurrentDBClient,
  BACKWARDS,
  END,
  START,
  jsonEvent,
  streamNameFilter,
  NO_STREAM,
  StreamDeletedError,
  WrongExpectedVersionError,
} from '@kurrent/kurrentdb-client';
import { GenericContainer, StartedTestContainer, Wait } from 'testcontainers';
import { join } from 'node:path';
import { once } from 'node:events';
import { Collection, Document, FindCursor, Long, MongoClient } from 'mongodb';
import { AppModule } from '../src/app.module';
import { MongoEventStoreService } from '../src/mongodb/mongodb-event-store.service';
import { StreamDeletedServiceError } from '../src/event-store/event-store.errors';

import { status } from '@grpc/grpc-js';
import {
  EVENT_STORE_BACKEND,
  EventStoreBackend,
} from '../src/event-store/event-store-backend';
import { getFreePort } from './util/get-free-port';

describe('MongoDB backend through the official client', () => {
  jest.setTimeout(120_000);
  let container: StartedTestContainer;
  let app: INestMicroservice;
  let client: KurrentDBClient;
  let backend: EventStoreBackend;
  const previous = { ...process.env };

  beforeAll(async () => {
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
    process.env.MONGODB_DATABASE = 'adapter_test';
    delete process.env.MONGODB_UNSAFE_ENABLE_SCAVENGE;
    const fixture = await Test.createTestingModule({
      imports: [AppModule],
    }).compile();
    backend = fixture.get<EventStoreBackend>(EVENT_STORE_BACKEND);
    try {
      backend.startScavenge({ options: undefined });
      throw new Error('expected scavenging to be disabled');
    } catch (error: unknown) {
      expect(error).toMatchObject({ code: status.UNIMPLEMENTED });
    }
    const port = await getFreePort();
    app = fixture.createNestMicroservice({
      transport: Transport.GRPC,
      options: {
        package: [
          'event_store.client.streams',
          'event_store.client.server_features',
        ],
        protoPath: [
          join(__dirname, '../proto/Grpc/streams.proto'),
          join(__dirname, '../proto/Grpc/serverfeatures.proto'),
        ],
        loader: { includeDirs: [join(__dirname, '../proto/Grpc')] },
        url: `127.0.0.1:${port}`,
      },
    });
    await app.listen();
    client = KurrentDBClient.connectionString`kurrentdb://127.0.0.1:${port}?tls=false`;
  });

  afterAll(async () => {
    try {
      await client?.dispose();
      await app?.close();
      await container?.stop();
    } finally {
      process.env = previous;
    }
  });

  it.each(['delete', 'tombstone'] as const)(
    'passes the official client %s deadline and cancellation to the backend',
    async (method) => {
      let observed!: (context: {
        signal: AbortSignal;
        deadline?: number;
      }) => void;
      const entered = new Promise<{ signal: AbortSignal; deadline?: number }>(
        (resolve) => {
          observed = resolve;
        },
      );
      let release!: () => void;
      const gate = new Promise<void>((resolve) => {
        release = resolve;
      });
      let finish!: () => void;
      let didEnter = false;
      const backendFinished = new Promise<void>((resolve) => {
        finish = resolve;
      });
      const original = backend[method].bind(
        backend,
      ) as EventStoreBackend['delete'];
      const spy = jest
        .spyOn(backend, method)
        .mockImplementation(async (request, context) => {
          if (!context) throw new Error('missing write context');
          didEnter = true;
          observed(context);
          await gate;
          try {
            return await original(request, context);
          } finally {
            finish();
          }
        });
      try {
        const result =
          method === 'delete'
            ? client.deleteStream('context-delete', { deadline: 1000 })
            : client.tombstoneStream('context-tombstone', { deadline: 1000 });
        const settled = result.then(
          () => false,
          () => true,
        );
        const context = await entered;
        expect(context.deadline).toBeGreaterThan(Date.now());
        if (!context.signal.aborted) {
          await new Promise<void>((resolve) =>
            context.signal.addEventListener('abort', () => resolve(), {
              once: true,
            }),
          );
        }
        expect(await settled).toBe(true);
      } finally {
        release();
        if (didEnter) await backendFinished;
        spy.mockRestore();
      }
    },
  );

  it('isolates a thrown stream-deleted error from another pending batch append', async () => {
    const original: EventStoreBackend['batchAppend'] = (...args) =>
      MongoEventStoreService.prototype.batchAppend.apply(
        backend as MongoEventStoreService,
        args,
      ) as ReturnType<EventStoreBackend['batchAppend']>;
    let release!: () => void;
    let entered!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const ready = new Promise<void>((resolve) => {
      entered = resolve;
    });
    const spy = jest
      .spyOn(backend, 'batchAppend')
      .mockImplementation(async (requests, context) => {
        const name = Buffer.from(
          requests[0].options!.streamIdentifier!.streamName,
        ).toString();
        if (name === 'isolated-healthy') {
          entered();
          await gate;
        }
        if (name === 'isolated-deleted')
          throw new StreamDeletedServiceError(name);
        return original(requests, context);
      });
    try {
      const healthy = client.appendToStream(
        'isolated-healthy',
        jsonEvent({ type: 'ok', data: {} }),
      );
      void healthy.catch(() => undefined);
      await ready;
      const deleted = client.appendToStream(
        'isolated-deleted',
        jsonEvent({ type: 'bad', data: {} }),
      );
      const deletedResult = await Promise.race([
        deleted.catch((error: unknown) => error),
        new Promise<never>((_, reject) =>
          setTimeout(
            () => reject(new Error('deleted response timed out')),
            3000,
          ).unref(),
        ),
      ]);
      release();
      const healthyResult = await healthy;
      expect(deletedResult).toBeInstanceOf(StreamDeletedError);
      expect(healthyResult.success).toBe(true);
      const eventTypes: string[] = [];
      for await (const event of client.readStream('isolated-healthy')) {
        if (event.event) eventTypes.push(event.event.type);
      }
      expect(eventTypes).toEqual(['ok']);
    } finally {
      release();
      spy.mockRestore();
    }
  });

  it('delivers historical and cross-instance live stream events in order', async () => {
    const fixture = await Test.createTestingModule({
      imports: [AppModule],
    }).compile();
    const port = await getFreePort();
    const other = fixture.createNestMicroservice({
      transport: Transport.GRPC,
      options: {
        package: [
          'event_store.client.streams',
          'event_store.client.server_features',
        ],
        protoPath: [
          join(__dirname, '../proto/Grpc/streams.proto'),
          join(__dirname, '../proto/Grpc/serverfeatures.proto'),
        ],
        loader: { includeDirs: [join(__dirname, '../proto/Grpc')] },
        url: `127.0.0.1:${port}`,
      },
    });
    await other.listen();
    const writer = KurrentDBClient.connectionString`kurrentdb://127.0.0.1:${port}?tls=false`;
    const stream = 'cross-instance-subscription';
    await writer.appendToStream(
      stream,
      jsonEvent({ type: 'history', data: {} }),
    );
    const subscription = client.subscribeToStream(stream, {
      fromRevision: START,
    });
    const resumed = client.subscribeToStream(stream, { fromRevision: 0n });
    const resumedCaughtUp = once(resumed, 'caughtUp');
    const caughtUp = once(subscription, 'caughtUp');
    try {
      const iterator = subscription[Symbol.asyncIterator]() as AsyncIterator<
        {
          event?: { type: string };
        },
        void
      >;
      const next = async () => {
        let timer: NodeJS.Timeout | undefined;
        try {
          return await Promise.race([
            iterator.next(),
            new Promise<never>((_, reject) => {
              timer = setTimeout(
                () => reject(new Error('subscription timeout')),
                5000,
              );
            }),
          ]);
        } finally {
          clearTimeout(timer);
        }
      };
      expect(await next()).toMatchObject({
        value: { event: { type: 'history' } },
      });
      await Promise.all([caughtUp, resumedCaughtUp]);
      const resumedNext = resumed[Symbol.asyncIterator]().next();
      await writer.appendToStream(stream, [
        jsonEvent({ type: 'live-one', data: {} }),
        jsonEvent({ type: 'live-two', data: {} }),
      ]);
      expect(await resumedNext).toMatchObject({
        value: { event: { type: 'live-one', revision: 1n } },
      });
      expect(await next()).toMatchObject({
        value: { event: { type: 'live-one' } },
      });
      expect(await next()).toMatchObject({
        value: { event: { type: 'live-two' } },
      });
    } finally {
      await subscription.unsubscribe();
      await resumed.unsubscribe();
      await writer.dispose();
      await other.close();
    }
  });

  it.each(['stream', 'all'] as const)(
    'delivers cross-instance writes committed after the %s END cursor snapshot',
    async (kind) => {
      const fixture = await Test.createTestingModule({
        imports: [AppModule],
      }).compile();
      const port = await getFreePort();
      const other = fixture.createNestMicroservice({
        transport: Transport.GRPC,
        options: {
          package: [
            'event_store.client.streams',
            'event_store.client.server_features',
          ],
          protoPath: [
            join(__dirname, '../proto/Grpc/streams.proto'),
            join(__dirname, '../proto/Grpc/serverfeatures.proto'),
          ],
          loader: { includeDirs: [join(__dirname, '../proto/Grpc')] },
          url: `127.0.0.1:${port}`,
        },
      });
      await other.listen();
      const writer = KurrentDBClient.connectionString`kurrentdb://127.0.0.1:${port}?tls=false`;
      const stream = `end-race-${kind}`;
      await writer.appendToStream(
        stream,
        jsonEvent({ type: 'before', data: {} }),
      );
      let release!: () => void;
      let observed!: () => void;
      const paused = new Promise<void>((resolve) => (observed = resolve));
      const resume = new Promise<void>((resolve) => (release = resolve));
      let intercepted = false;
      // eslint-disable-next-line @typescript-eslint/unbound-method
      const findOne = Collection.prototype.findOne;
      const stateSpy = jest
        .spyOn(Collection.prototype, 'findOne')
        .mockImplementation(async function (this: Collection, filter, options) {
          const result = (await findOne.call(
            this,
            filter,
            options,
          )) as Document | null;
          if (
            kind === 'stream' &&
            this.collectionName === 'streams' &&
            filter._id === stream &&
            !intercepted
          ) {
            expect(result?.revision).toBe(0);
            intercepted = true;
            observed();
            await resume;
          }
          return result;
        });
      // eslint-disable-next-line @typescript-eslint/unbound-method
      const cursorNext = FindCursor.prototype.next;
      const cursorSpy = jest
        .spyOn(FindCursor.prototype, 'next')
        .mockImplementation(async function (this: FindCursor) {
          const result = (await cursorNext.call(this)) as Document | null;
          if (kind === 'all' && !intercepted) {
            expect(result?.streamName).toBe(stream);
            intercepted = true;
            observed();
            await resume;
          }
          return result;
        });
      const subscription =
        kind === 'stream'
          ? client.subscribeToStream(stream, { fromRevision: END })
          : client.subscribeToAll({
              fromPosition: END,
              filter: streamNameFilter({ prefixes: [stream] }),
            });
      const caughtUp = once(subscription, 'caughtUp');
      try {
        let timer: NodeJS.Timeout | undefined;
        try {
          await Promise.race([
            paused,
            new Promise<never>((_, reject) => {
              timer = setTimeout(
                () => reject(new Error('END snapshot timeout')),
                10000,
              );
            }),
          ]);
        } finally {
          clearTimeout(timer);
        }
        await writer.appendToStream(
          stream,
          ['during-one', 'during-two'].map((type) =>
            jsonEvent({ type, data: {} }),
          ),
        );
        release();
        const iterator = subscription[Symbol.asyncIterator]() as AsyncIterator<
          { event?: { type: string; revision: bigint } },
          void
        >;
        expect((await iterator.next()).value?.event?.type).toBe('during-one');
        expect((await iterator.next()).value?.event?.type).toBe('during-two');
        await caughtUp;
        await writer.appendToStream(
          stream,
          jsonEvent({ type: 'after', data: {} }),
        );
        expect((await iterator.next()).value?.event?.type).toBe('after');
      } finally {
        release();
        stateSpy.mockRestore();
        cursorSpy.mockRestore();
        await subscription.unsubscribe();
        await writer.dispose();
        await other.close();
      }
    },
  );

  it('delivers $all catch-up and cross-instance live events in position order', async () => {
    const fixture = await Test.createTestingModule({
      imports: [AppModule],
    }).compile();
    const port = await getFreePort();
    const other = fixture.createNestMicroservice({
      transport: Transport.GRPC,
      options: {
        package: [
          'event_store.client.streams',
          'event_store.client.server_features',
        ],
        protoPath: [
          join(__dirname, '../proto/Grpc/streams.proto'),
          join(__dirname, '../proto/Grpc/serverfeatures.proto'),
        ],
        loader: { includeDirs: [join(__dirname, '../proto/Grpc')] },
        url: `127.0.0.1:${port}`,
      },
    });
    await other.listen();
    const writer = KurrentDBClient.connectionString`kurrentdb://127.0.0.1:${port}?tls=false`;
    await writer.appendToStream(
      'all-cross-a',
      jsonEvent({ type: 'all-history', data: {} }),
    );
    const subscription = client.subscribeToAll({ fromPosition: START });
    const caughtUp = once(subscription, 'caughtUp');
    try {
      const iterator = subscription[Symbol.asyncIterator]() as AsyncIterator<
        {
          event?: { type: string };
        },
        void
      >;
      const next = async () => {
        let timer: NodeJS.Timeout | undefined;
        try {
          return await Promise.race([
            iterator.next(),
            new Promise<never>((_, reject) => {
              timer = setTimeout(
                () => reject(new Error('subscription timeout')),
                5000,
              );
            }),
          ]);
        } finally {
          clearTimeout(timer);
        }
      };
      const seen: string[] = [];
      while (!seen.includes('all-history')) {
        const item = await next();
        seen.push(item.value?.event?.type ?? '');
      }
      await caughtUp;
      await writer.appendToStream('all-cross-b', [
        jsonEvent({ type: 'all-live-one', data: {} }),
        jsonEvent({ type: 'all-live-two', data: {} }),
      ]);
      expect((await next()).value?.event?.type).toBe('all-live-one');
      expect((await next()).value?.event?.type).toBe('all-live-two');
    } finally {
      await subscription.unsubscribe();
      await writer.dispose();
      await other.close();
    }
  });

  it('filters $all reads by stream prefix', async () => {
    await client.appendToStream(
      'filtered-match-a',
      jsonEvent({ type: 'wanted', data: {} }),
    );
    await client.appendToStream(
      'filtered-other',
      jsonEvent({ type: 'unwanted', data: {} }),
    );
    const types: string[] = [];
    for await (const { event } of client.readAll({
      fromPosition: START,
      filter: streamNameFilter({ prefixes: ['filtered-match-'] }),
    })) {
      if (event) types.push(event.type);
    }
    expect(types).toEqual(['wanted']);
  });

  it('filters $all subscription history and live events', async () => {
    await client.appendToStream(
      'sub-match-one',
      jsonEvent({ type: 'sub-history', data: {} }),
    );
    const subscription = client.subscribeToAll({
      fromPosition: START,
      filter: streamNameFilter({ prefixes: ['sub-match-'] }),
    });
    const caughtUp = once(subscription, 'caughtUp');
    try {
      const iterator = subscription[Symbol.asyncIterator]() as AsyncIterator<
        {
          event?: { type: string };
        },
        void
      >;
      expect((await iterator.next()).value?.event?.type).toBe('sub-history');
      await caughtUp;
      await client.appendToStream(
        'sub-ignore',
        jsonEvent({ type: 'ignored', data: {} }),
      );
      await client.appendToStream(
        'sub-match-two',
        jsonEvent({ type: 'sub-live', data: {} }),
      );
      expect((await iterator.next()).value?.event?.type).toBe('sub-live');
    } finally {
      await subscription.unsubscribe();
    }
  });

  describe('experimental scavenging on disposable data', () => {
    let backend: MongoEventStoreService;

    beforeEach(async () => {
      const previousOptIn = process.env.MONGODB_UNSAFE_ENABLE_SCAVENGE;
      try {
        process.env.MONGODB_UNSAFE_ENABLE_SCAVENGE = 'true';
        backend = new MongoEventStoreService();
      } finally {
        if (previousOptIn === undefined)
          delete process.env.MONGODB_UNSAFE_ENABLE_SCAVENGE;
        else process.env.MONGODB_UNSAFE_ENABLE_SCAVENGE = previousOptIn;
      }
      await backend.onModuleInit();
    });

    afterEach(async () => {
      await backend?.onModuleDestroy();
    });

    it('scavenges hidden history but preserves the last event and stream revision', async () => {
      const stream = 'mongo-scavenge-retention';
      await client.appendToStream(stream, [
        jsonEvent({ type: 'old', data: {} }),
        jsonEvent({ type: 'last', data: {} }),
      ]);
      await client.setStreamMetadata(stream, { truncateBefore: 5 });
      const readAll = async () => {
        const types: string[] = [];
        for await (const { event } of client.readAll({ fromPosition: START }))
          if (event?.streamId === stream) types.push(event.type);
        return types;
      };
      expect(await readAll()).toEqual(['old', 'last']);
      expect(backend.startScavenge().scavengeId).toBeTruthy();
      const deadline = Date.now() + 10000;
      while (Date.now() < deadline && (await readAll()).length !== 1)
        await new Promise((resolve) => setTimeout(resolve, 50));
      expect(await readAll()).toEqual(['last']);
      const result = await client.appendToStream(
        stream,
        jsonEvent({ type: 'next', data: {} }),
        { streamState: 1n },
      );
      expect(result.nextExpectedRevision).toBe(2n);
    });

    it('scavenges soft-deleted history after recreation but retains the new event', async () => {
      const stream = 'mongo-scavenge-recreated';
      await client.appendToStream(stream, [
        jsonEvent({ type: 'old-one', data: {} }),
        jsonEvent({ type: 'old-two', data: {} }),
      ]);
      await client.deleteStream(stream);
      await client.appendToStream(
        stream,
        jsonEvent({ type: 'new', data: {} }),
        {
          streamState: NO_STREAM,
        },
      );
      const readAll = async () => {
        const types: string[] = [];
        for await (const { event } of client.readAll({ fromPosition: START }))
          if (event?.streamId === stream) types.push(event.type);
        return types;
      };
      expect(await readAll()).toEqual(['old-one', 'old-two', 'new']);
      backend.startScavenge();
      const deadline = Date.now() + 10000;
      while (Date.now() < deadline && (await readAll()).length !== 1)
        await new Promise((resolve) => setTimeout(resolve, 50));
      expect(await readAll()).toEqual(['new']);
    });

    it('serializes a concurrent metadata update against a scavenge snapshot', async () => {
      const stream = 'mongo-scavenge-policy-race';
      await client.appendToStream(stream, [
        jsonEvent({ type: 'one', data: {} }),
        jsonEvent({ type: 'two', data: {} }),
      ]);
      let release!: () => void;
      let observed!: () => void;
      const paused = new Promise<void>((resolve) => (observed = resolve));
      const resume = new Promise<void>((resolve) => (release = resolve));
      // eslint-disable-next-line @typescript-eslint/unbound-method
      const original = Collection.prototype.findOne;
      let intercepted = false;
      const spy = jest
        .spyOn(Collection.prototype, 'findOne')
        .mockImplementation(async function (this: Collection, filter, options) {
          const result = (await original.call(
            this,
            filter,
            options,
          )) as Document | null;
          if (
            this.collectionName === 'retention' &&
            filter._id === stream &&
            !intercepted
          ) {
            intercepted = true;
            observed();
            await resume;
          }
          return result;
        });
      let timer: NodeJS.Timeout | undefined;
      try {
        backend.startScavenge();
        await Promise.race([
          paused,
          new Promise<never>((_, reject) => {
            timer = setTimeout(
              () => reject(new Error('scavenge timeout')),
              10000,
            );
          }),
        ]);
        let completed = false;
        const update = client
          .setStreamMetadata(stream, { maxCount: 1 })
          .then(() => {
            completed = true;
          });
        await new Promise((resolve) => setTimeout(resolve, 150));
        expect(completed).toBe(false);
        release();
        await update;
        const types: string[] = [];
        for await (const { event } of client.readAll({ fromPosition: START }))
          if (event?.streamId === stream) types.push(event.type);
        expect(types).toEqual(['one', 'two']);
      } finally {
        clearTimeout(timer);
        release();
        spy.mockRestore();
      }
    });
  });

  it('applies metadata maxCount without removing events from $all', async () => {
    const stream = 'mongo-retention-count';
    await client.appendToStream(stream, [
      jsonEvent({ type: 'count-one', data: {} }),
      jsonEvent({ type: 'count-two', data: {} }),
      jsonEvent({ type: 'count-three', data: {} }),
    ]);
    await client.setStreamMetadata(stream, { maxCount: 2 });
    expect((await client.getStreamMetadata(stream)).metadata?.maxCount).toBe(2);
    const visible: string[] = [];
    for await (const { event } of client.readStream(stream))
      if (event) visible.push(event.type);
    expect(visible).toEqual(['count-two', 'count-three']);
    const all: string[] = [];
    for await (const { event } of client.readAll({ fromPosition: START }))
      if (event?.streamId === stream) all.push(event.type);
    expect(all).toEqual(['count-one', 'count-two', 'count-three']);
  });

  it('applies and replaces truncateBefore and maxAge policies without changing $all', async () => {
    const stream = 'mongo-retention-policy';
    await client.appendToStream(stream, [
      jsonEvent({ type: 'old-one', data: {} }),
      jsonEvent({ type: 'old-two', data: {} }),
      jsonEvent({ type: 'old-three', data: {} }),
    ]);
    await client.setStreamMetadata(stream, { truncateBefore: 2 });
    const read = async () => {
      const types: string[] = [];
      for await (const { event } of client.readStream(stream))
        if (event) types.push(event.type);
      return types;
    };
    expect(await read()).toEqual(['old-three']);
    await client.setStreamMetadata(stream, { maxAge: 1 });
    expect(await read()).toEqual(['old-one', 'old-two', 'old-three']);
    await new Promise((resolve) => setTimeout(resolve, 1200));
    await client.appendToStream(stream, jsonEvent({ type: 'fresh', data: {} }));
    expect(await read()).toEqual(['fresh']);
    const all: string[] = [];
    for await (const { event } of client.readAll({ fromPosition: START }))
      if (event?.streamId === stream) all.push(event.type);
    expect(all).toEqual(['old-one', 'old-two', 'old-three', 'fresh']);
  });

  it('serializes competing metadata writes with the same expected revision', async () => {
    const stream = 'mongo-retention-race';
    await client.setStreamMetadata(stream, { maxCount: 1 });
    const results = await Promise.allSettled([
      client.appendToStream(
        `$$${stream}`,
        jsonEvent({ type: '$metadata', data: { $maxCount: 2 } }),
        { streamState: 0n },
      ),
      client.appendToStream(
        `$$${stream}`,
        jsonEvent({ type: '$metadata', data: { $maxCount: 3 } }),
        { streamState: 0n },
      ),
    ]);
    expect(
      results.filter((result) => result.status === 'fulfilled'),
    ).toHaveLength(1);
    expect(
      results.filter((result) => result.status === 'rejected'),
    ).toHaveLength(1);
    expect([2, 3]).toContain(
      (await client.getStreamMetadata(stream)).metadata?.maxCount,
    );
  });

  it('reads all events in global order across streams', async () => {
    const first = await client.appendToStream(
      'all-first',
      jsonEvent({ type: 'all-one', data: {} }),
    );
    await client.appendToStream(
      'all-second',
      jsonEvent({ type: 'all-two', data: {} }),
    );
    const forward: string[] = [];
    for await (const { event } of client.readAll({
      fromPosition: first.position!,
      maxCount: 2,
    })) {
      if (event) forward.push(event.type);
    }
    expect(forward).toEqual(['all-one', 'all-two']);
    const backward: string[] = [];
    for await (const { event } of client.readAll({
      fromPosition: END,
      direction: BACKWARDS,
      maxCount: 2,
    })) {
      if (event) backward.push(event.type);
    }
    expect(backward).toEqual(['all-two', 'all-one']);
  });

  it('does not emit a tombstone as a normal event during a subscription race', async () => {
    const stream = 'mongo-subscribe-tombstone-race';
    await client.appendToStream(stream, jsonEvent({ type: 'old', data: {} }));
    let release!: () => void;
    let observed!: () => void;
    const paused = new Promise<void>((resolve) => (observed = resolve));
    const resume = new Promise<void>((resolve) => (release = resolve));
    // Preserve the receiver when invoking the intercepted MongoDB method.
    // eslint-disable-next-line @typescript-eslint/unbound-method
    const original = Collection.prototype.findOne;
    let intercepted = false;
    const spy = jest
      .spyOn(Collection.prototype, 'findOne')
      .mockImplementation(async function (this: Collection, filter, options) {
        const result = (await original.call(
          this,
          filter,
          options,
        )) as Document | null;
        if (
          this.collectionName === 'streams' &&
          filter._id === stream &&
          !intercepted
        ) {
          intercepted = true;
          observed();
          await resume;
        }
        return result;
      });
    const subscription = client.subscribeToStream(stream, {
      fromRevision: START,
    });
    try {
      const iterator = subscription[Symbol.asyncIterator]() as AsyncIterator<
        {
          event?: { type: string };
        },
        void
      >;
      const first = iterator.next();
      await paused;
      await client.tombstoneStream(stream);
      release();
      expect((await first).value).toMatchObject({ event: { type: 'old' } });
      await expect(iterator.next()).rejects.toBeInstanceOf(StreamDeletedError);
    } finally {
      release();
      spy.mockRestore();
      await subscription.unsubscribe();
    }
  });

  it('reads one snapshot across a concurrent delete and recreation', async () => {
    const stream = 'mongo-read-delete-snapshot';
    await client.appendToStream(stream, jsonEvent({ type: 'old', data: {} }));
    let release!: () => void;
    let observed!: () => void;
    const paused = new Promise<void>((resolve) => (observed = resolve));
    const resume = new Promise<void>((resolve) => (release = resolve));
    // Preserve the receiver when invoking the intercepted MongoDB method.
    // eslint-disable-next-line @typescript-eslint/unbound-method
    const original = Collection.prototype.findOne;
    let intercepted = false;
    const spy = jest
      .spyOn(Collection.prototype, 'findOne')
      .mockImplementation(async function (this: Collection, filter, options) {
        const result = (await original.call(
          this,
          filter,
          options,
        )) as Document | null;
        if (
          this.collectionName === 'streams' &&
          filter._id === stream &&
          !intercepted
        ) {
          intercepted = true;
          observed();
          await resume;
        }
        return result;
      });
    try {
      const reading = (async () => {
        const types: string[] = [];
        for await (const { event } of client.readStream(stream))
          if (event) types.push(event.type);
        return types;
      })();
      await paused;
      await client.deleteStream(stream);
      await client.appendToStream(stream, jsonEvent({ type: 'new', data: {} }));
      release();
      expect(await reading).toEqual(['old']);
    } finally {
      release();
      spy.mockRestore();
    }
  });

  it('soft deletes and recreates a stream without exposing old events', async () => {
    const stream = 'mongo-soft-delete';
    await client.appendToStream(stream, jsonEvent({ type: 'old', data: {} }));
    await client.deleteStream(stream);
    await client.appendToStream(stream, jsonEvent({ type: 'new', data: {} }), {
      streamState: NO_STREAM,
    });
    const types: string[] = [];
    for await (const { event } of client.readStream(stream)) {
      if (event) types.push(event.type);
    }
    expect(types).toEqual(['new']);
  });

  it('does not replay deleted events when subscribing before recreation', async () => {
    const stream = 'mongo-subscribe-while-deleted';
    await client.appendToStream(stream, jsonEvent({ type: 'old', data: {} }));
    await client.deleteStream(stream);
    const subscription = client.subscribeToStream(stream, {
      fromRevision: START,
    });
    try {
      const caughtUp = once(subscription, 'caughtUp');
      const iterator = subscription[Symbol.asyncIterator]() as AsyncIterator<
        {
          event?: { type: string };
        },
        void
      >;
      const next = iterator.next();
      await caughtUp;
      await client.appendToStream(stream, jsonEvent({ type: 'new', data: {} }));
      expect((await next).value).toMatchObject({ event: { type: 'new' } });
    } finally {
      await subscription.unsubscribe();
    }
  });

  it('starts a recreated stream subscription at its visible events', async () => {
    const stream = 'mongo-recreated-subscription';
    await client.appendToStream(stream, jsonEvent({ type: 'old', data: {} }));
    await client.deleteStream(stream);
    await client.appendToStream(stream, jsonEvent({ type: 'new', data: {} }));
    const subscription = client.subscribeToStream(stream, {
      fromRevision: START,
    });
    try {
      const result = await subscription[Symbol.asyncIterator]().next();
      expect(result.value).toMatchObject({ event: { type: 'new' } });
    } finally {
      await subscription.unsubscribe();
    }
  });

  it('tombstones a stream and rejects subsequent appends', async () => {
    const stream = 'mongo-tombstone';
    await client.appendToStream(
      stream,
      jsonEvent({ type: 'before', data: {} }),
    );
    await client.tombstoneStream(stream);
    await expect(
      client.appendToStream(stream, jsonEvent({ type: 'after', data: {} })),
    ).rejects.toBeInstanceOf(StreamDeletedError);
    const types: string[] = [];
    for await (const { event } of client.readAll({ fromPosition: START })) {
      if (event?.streamId === stream) types.push(event.type);
    }
    expect(types).toEqual(['before', '$streamDeleted']);
  });

  it.each(['delete', 'tombstone'] as const)(
    'rejects stale expected revisions on %s without changing the stream',
    async (operation) => {
      const stream = `mongo-stale-${operation}`;
      await client.appendToStream(stream, [
        jsonEvent({ type: 'one', data: {} }),
        jsonEvent({ type: 'two', data: {} }),
      ]);
      await expect(
        operation === 'delete'
          ? client.deleteStream(stream, { expectedRevision: 0n })
          : client.tombstoneStream(stream, { expectedRevision: 0n }),
      ).rejects.toBeInstanceOf(WrongExpectedVersionError);
      const types: string[] = [];
      for await (const { event } of client.readStream(stream))
        if (event) types.push(event.type);
      expect(types).toEqual(['one', 'two']);
    },
  );

  it.each(['delete', 'tombstone'] as const)(
    'serializes %s against an append with the same expected revision',
    async (operation) => {
      for (let n = 0; n < 4; n++) {
        const stream = `mongo-race-${operation}-${n}`;
        await client.appendToStream(
          stream,
          jsonEvent({ type: 'initial', data: {} }),
        );
        const results = await Promise.allSettled([
          operation === 'delete'
            ? client.deleteStream(stream, { expectedRevision: 0n })
            : client.tombstoneStream(stream, { expectedRevision: 0n }),
          client.appendToStream(
            stream,
            jsonEvent({ type: 'racing', data: {} }),
            { streamState: 0n },
          ),
        ]);
        expect(
          results.filter((result) => result.status === 'fulfilled'),
        ).toHaveLength(1);
        expect(
          results.filter((result) => result.status === 'rejected'),
        ).toHaveLength(1);
      }
    },
  );

  it('allows exactly one concurrent NO_STREAM writer', async () => {
    const results = await Promise.allSettled(
      Array.from({ length: 8 }, (_, n) =>
        client.appendToStream(
          'mongo-race',
          jsonEvent({ type: 'race', data: { n } }),
          { streamState: NO_STREAM },
        ),
      ),
    );
    expect(
      results.filter((result) => result.status === 'fulfilled'),
    ).toHaveLength(1);
    expect(
      results.filter((result) => result.status === 'rejected'),
    ).toHaveLength(7);
    const events: unknown[] = [];
    for await (const event of client.readStream('mongo-race'))
      events.push(event);
    expect(events).toHaveLength(1);
  });

  it('rolls back a tombstone when the global counter exceeds safe precision', async () => {
    const mongo = new MongoClient(process.env.MONGODB_URI!);
    await mongo.connect();
    const counters = mongo
      .db(process.env.MONGODB_DATABASE)
      .collection('counters');
    const original = await counters.findOne({ _id: 'global' } as Document);
    try {
      await counters.updateOne(
        { _id: 'global' } as Document,
        { $set: { value: Long.fromString('9007199254740991') } },
        { upsert: true },
      );
      await expect(
        client.tombstoneStream('mongo-counter-overflow', { deadline: 2000 }),
      ).rejects.toThrow(/safe integer/i);
      const counter = await counters.findOne({ _id: 'global' } as Document);
      expect(String(counter?.value)).toBe('9007199254740991');
    } finally {
      if (original)
        await counters.replaceOne({ _id: 'global' } as Document, original);
      else await counters.deleteOne({ _id: 'global' } as Document);
      await mongo.close();
    }
  });

  it('rejects an unsafe prepare position in an $all read', async () => {
    const read = async () => {
      for await (const _event of client.readAll({
        fromPosition: { commit: 1n, prepare: 9007199254740993n },
      })) {
        void _event;
      }
    };
    await expect(read()).rejects.toThrow(/safe integer/i);
  });

  it('rejects an unsafe stream read revision rather than rounding it', async () => {
    const stream = 'mongo-unsafe-read';
    await client.appendToStream(stream, jsonEvent({ type: 'one', data: {} }));
    const read = async () => {
      for await (const _event of client.readStream(stream, {
        fromRevision: 9007199254740993n,
      })) {
        void _event;
      }
    };
    await expect(read()).rejects.toThrow(/safe integer/i);
  });

  it('matches KurrentDB when retrying an exact NO_STREAM append', async () => {
    const reference = await new GenericContainer(
      'docker.kurrent.io/kurrent-latest/kurrentdb:latest',
    )
      .withEnvironment({ KURRENTDB_INSECURE: 'true' })
      .withExposedPorts(2113)
      .withWaitStrategy(Wait.forListeningPorts())
      .start();
    const referenceClient = KurrentDBClient.connectionString`kurrentdb://${reference.getHost()}:${reference.getMappedPort(2113)}?tls=false`;
    try {
      const event = jsonEvent({ type: 'retry', data: { n: 1 } });
      for (const target of [referenceClient, client]) {
        const stream = 'exact-no-stream-retry';
        const first = await target.appendToStream(stream, event, {
          streamState: NO_STREAM,
          deadline: 10000,
        });
        const retry = await target.appendToStream(stream, event, {
          streamState: NO_STREAM,
          deadline: 10000,
        });
        expect(retry.nextExpectedRevision).toBe(first.nextExpectedRevision);
        const events: string[] = [];
        for await (const { event: stored } of target.readStream(stream))
          if (stored) events.push(stored.id);
        expect(events).toEqual([event.id]);
        await target.appendToStream('same-id-another-stream', event, {
          streamState: NO_STREAM,
          deadline: 3000,
        });
        const otherIds: string[] = [];
        for await (const { event: stored } of target.readStream(
          'same-id-another-stream',
        ))
          if (stored) otherIds.push(stored.id);
        expect(otherIds).toEqual([event.id]);
      }
    } finally {
      await referenceClient.dispose();
      await reference.stop();
    }
  });

  it('preserves positions across a wrong revision and an exact retry', async () => {
    const stream = 'mongo-append-position-regression';
    const event = jsonEvent({ type: 'first', data: {} });
    const first = await client.appendToStream(stream, event, {
      streamState: NO_STREAM,
    });
    await expect(
      client.appendToStream(
        stream,
        [
          jsonEvent({ type: 'rejected-one', data: {} }),
          jsonEvent({ type: 'rejected-two', data: {} }),
        ],
        { streamState: NO_STREAM },
      ),
    ).rejects.toBeInstanceOf(WrongExpectedVersionError);
    const retry = await client.appendToStream(stream, event, {
      streamState: NO_STREAM,
    });
    expect(retry.position).toEqual(first.position);
    expect(retry.nextExpectedRevision).toBe(first.nextExpectedRevision);
    const next = await client.appendToStream(
      stream,
      jsonEvent({ type: 'next', data: {} }),
      { streamState: 0n },
    );
    expect(next.position!.commit).toBe(first.position!.commit + 4n);
    expect(next.position!.prepare).toBe(next.position!.commit);
    const types: string[] = [];
    for await (const { event: stored } of client.readStream(stream))
      if (stored) types.push(stored.type);
    expect(types).toEqual(['first', 'next']);
  });

  it('does not mistake separately appended events for one retried batch', async () => {
    const first = jsonEvent({ type: 'one', data: {} });
    const second = jsonEvent({ type: 'two', data: {} });
    const stream = 'retry-distinct-batches';
    await client.appendToStream(stream, first, { streamState: NO_STREAM });
    await client.appendToStream(stream, second);
    await expect(
      client.appendToStream(stream, [first, second], {
        streamState: NO_STREAM,
      }),
    ).rejects.toBeInstanceOf(WrongExpectedVersionError);
  });

  it('rejects a retry that reuses an event ID with different content', async () => {
    const original = jsonEvent({ type: 'original', data: { n: 1 } });
    await client.appendToStream('retry-changed-content', original, {
      streamState: NO_STREAM,
    });
    const changed = jsonEvent({
      id: original.id,
      type: 'original',
      data: { n: 2 },
    });
    await expect(
      client.appendToStream('retry-changed-content', changed, {
        streamState: NO_STREAM,
      }),
    ).rejects.toBeInstanceOf(WrongExpectedVersionError);
  });

  it('atomically appends, reads and rejects a stale expected revision', async () => {
    const stream = 'mongo-test';
    await client.appendToStream(
      stream,
      [
        jsonEvent({ type: 'one', data: { n: 1 } }),
        jsonEvent({ type: 'two', data: { n: 2 } }),
      ],
      { streamState: NO_STREAM },
    );
    const types: string[] = [];
    for await (const { event } of client.readStream(stream)) {
      if (event) types.push(event.type);
    }
    expect(types).toEqual(['one', 'two']);
    await expect(
      client.appendToStream(stream, jsonEvent({ type: 'stale', data: {} }), {
        streamState: NO_STREAM,
      }),
    ).rejects.toBeInstanceOf(WrongExpectedVersionError);
  });
});
