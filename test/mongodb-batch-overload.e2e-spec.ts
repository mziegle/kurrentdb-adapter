import { INestMicroservice } from '@nestjs/common';
import { Transport } from '@nestjs/microservices';
import { Test } from '@nestjs/testing';
import {
  KurrentDBClient,
  UnknownError,
  NO_STREAM,
  jsonEvent,
} from '@kurrent/kurrentdb-client';
import { GenericContainer, StartedTestContainer, Wait } from 'testcontainers';
import { join } from 'node:path';
import { AppModule } from '../src/app.module';
import { EVENT_STORE_BACKEND } from '../src/event-store/event-store-backend';
import { MongoEventStoreService } from '../src/mongodb/mongodb-event-store.service';
import { BatchAppendSession } from '../src/streams/batch-append-session';
import * as requestSizing from '../src/mongodb/request-size';
import { getFreePort } from './util/get-free-port';

describe('Mongo AppModule BatchAppend overload', () => {
  jest.setTimeout(120000);
  let container: StartedTestContainer;
  let app: INestMicroservice;
  let client: KurrentDBClient;
  let peer: KurrentDBClient;
  let port: number;
  let backend: MongoEventStoreService;
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
      "rs.initiate({_id:'rs0',members:[{_id:0,host:'localhost:27017'}]}); while (!db.hello().isWritablePrimary) sleep(200)",
    ]);
    if (init.exitCode !== 0) throw new Error(init.output);
    process.env.EVENT_STORE_BACKEND = 'mongodb';
    process.env.MONGODB_URI = `mongodb://${container.getHost()}:${container.getMappedPort(27017)}/?directConnection=true`;
    process.env.MONGODB_DATABASE = 'batch_overload';
    const fixture = await Test.createTestingModule({
      imports: [AppModule],
    }).compile();
    backend = fixture.get<MongoEventStoreService>(EVENT_STORE_BACKEND);
    port = await getFreePort();
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
    peer = KurrentDBClient.connectionString`kurrentdb://127.0.0.1:${port}?tls=false`;
  });

  afterAll(async () => {
    await client?.dispose();
    await peer?.dispose();
    await app?.close();
    await container?.stop();
    process.env = previous;
  });

  it('enforces the process budget across live clients and recovers after work settles', async () => {
    const clients: KurrentDBClient[] = [];
    const pending: Promise<unknown>[] = [];
    const committed: Array<{ stream: string; id: string }> = [];
    let release!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    let notify!: () => void;
    let entered = 0;
    const original = backend.append.bind(
      backend,
    ) as MongoEventStoreService['append'];
    const spy = jest
      .spyOn(backend, 'append')
      .mockImplementation(async (...args) => {
        const result = await original(...args);
        entered++;
        notify?.();
        await gate;
        return result;
      });
    const sizes = jest.spyOn(requestSizing, 'requestSize');
    const onData = jest.spyOn(BatchAppendSession.prototype, 'onData');
    const payload = 'x'.repeat(3 * 1024 * 1024);
    let rejected = false;
    let rejection: unknown;
    try {
      for (let i = 0; i < 21; i++) {
        const session = KurrentDBClient.connectionString`kurrentdb://127.0.0.1:${port}?tls=false`;
        clients.push(session);
        const event = jsonEvent({ type: 'retained', data: { payload, i } });
        const stream = `process-budget-${i}`;
        const reached = new Promise<'committed'>((resolve) => {
          notify = () => resolve('committed');
        });
        const outcome = session
          .appendToStream(stream, event, {
            streamState: NO_STREAM,
            deadline: 60000,
          })
          .then(
            (result) =>
              result.success ? ('success' as const) : ('error' as const),
            (error: unknown) => {
              rejection = error;
              return 'error' as const;
            },
          );
        pending.push(outcome);
        const state = await Promise.race([
          reached,
          outcome,
          new Promise<'timeout'>((resolve) =>
            setTimeout(() => resolve('timeout'), 10000).unref(),
          ),
        ]);
        if (state === 'error') {
          rejected = true;
          break;
        }
        expect(state).toBe('committed');
        committed.push({ stream, id: event.id });
      }
      expect(committed).toHaveLength(21);
      const overloaded = KurrentDBClient.connectionString`kurrentdb://127.0.0.1:${port}?tls=false`;
      clients.push(overloaded);
      const fragmentStart = onData.mock.calls.length;
      const sizingStart = sizes.mock.calls.length;
      const target = overloaded
        .appendToStream(
          'process-fragmented-overflow',
          [
            jsonEvent({ type: 'first', data: { payload } }),
            jsonEvent({ type: 'final', data: { value: 'small' } }),
          ],
          {
            streamState: NO_STREAM,
            batchAppendSize: 2 * 1024 * 1024,
            deadline: 5000,
          },
        )
        .then(
          () => 'success' as const,
          (error: unknown) => {
            rejection = error;
            return 'error' as const;
          },
        );
      pending.push(target);
      const targetResult = await Promise.race([
        target,
        new Promise<'timeout'>((resolve) =>
          setTimeout(() => resolve('timeout'), 10000).unref(),
        ),
      ]);
      rejected = targetResult === 'error';
      const fragments = onData.mock.calls
        .slice(fragmentStart)
        .map(([message]) => message);
      expect(fragments).toHaveLength(3);
      expect(fragments.map((part) => Boolean(part.isFinal))).toEqual([
        false,
        false,
        true,
      ]);
      expect(fragments.map((part) => part.proposedMessages?.length)).toEqual([
        undefined,
        1,
        1,
      ]);
      expect(fragments[0].correlationId).toEqual(fragments[1].correlationId);
      expect(fragments[0].correlationId).toEqual(fragments[2].correlationId);
      const targetSizes = sizes.mock.calls.slice(sizingStart);
      const targetResults = sizes.mock.results.slice(sizingStart);
      expect(targetSizes.slice(0, 4).map((call) => call[0])).toEqual([
        fragments[0],
        fragments[0],
        fragments[1],
        fragments[1],
      ]);
      expect(targetResults.slice(0, 4).map((result) => result.type)).toEqual([
        'return',
        'return',
        'return',
        'throw',
      ]);
      expect(targetSizes.slice(4)).toHaveLength(2);
      expect(targetResults.slice(4).map((result) => result.type)).toEqual([
        'return',
        'return',
      ]);
      expect(rejected).toBe(true);
      expect(rejection).toBeInstanceOf(UnknownError);
      expect(rejection).toMatchObject({ code: undefined });
      expect(entered).toBeGreaterThan(4);
      const processBudgets = sizes.mock.calls
        .filter((_, index) => index % 2 === 1)
        .map((call) => call[1]);
      expect(processBudgets[0]).toBe(64 * 1024 * 1024);
      expect(processBudgets[processBudgets.length - 1]).toBeLessThan(
        3 * 1024 * 1024,
      );
      // Rejection does not release committed work still held by the backend.
      // A fresh session must also be rejected.
      const retry = KurrentDBClient.connectionString`kurrentdb://127.0.0.1:${port}?tls=false`;
      clients.push(retry);
      const stillFull = await Promise.race([
        retry
          .appendToStream(
            'process-still-full',
            jsonEvent({ type: 'probe', data: { payload } }),
            { streamState: NO_STREAM, deadline: 5000 },
          )
          .then(
            () => 'success',
            () => 'error',
          ),
        new Promise<'timeout'>((resolve) =>
          setTimeout(() => resolve('timeout'), 10000).unref(),
        ),
      ]);
      expect(stillFull).toBe('error');
      expect(entered).toBe(committed.length);
    } finally {
      release();
      spy.mockRestore();
      sizes.mockRestore();
      onData.mockRestore();
      const settled = await Promise.all(pending);
      await Promise.all(clients.map((session) => session.dispose()));
      expect(settled).toEqual([...committed.map(() => 'success'), 'error']);
    }
    const later = jsonEvent({ type: 'recovered', data: { payload } });
    expect(
      (
        await peer.appendToStream('process-recovered', later, {
          streamState: NO_STREAM,
          deadline: 5000,
        })
      ).success,
    ).toBe(true);
    for (const { stream, id } of committed) {
      const ids: string[] = [];
      for await (const { event } of peer.readStream(stream))
        if (event) ids.push(event.id);
      expect(ids).toEqual([id]);
    }
    const ids: string[] = [];
    for await (const { event } of peer.readStream('process-recovered'))
      if (event) ids.push(event.id);
    expect(ids).toEqual([later.id]);
  });

  it('writes concurrent normal traffic through separate official clients', async () => {
    const clients = Array.from(
      { length: 8 },
      () =>
        KurrentDBClient.connectionString`kurrentdb://127.0.0.1:${port}?tls=false`,
    );
    const events = clients.map((_, i) =>
      jsonEvent({ type: 'normal', data: { i } }),
    );
    try {
      const results = await Promise.all(
        clients.map((session, i) =>
          session.appendToStream(`normal-load-${i}`, events[i], {
            streamState: NO_STREAM,
            deadline: 10000,
          }),
        ),
      );
      expect(results.every((result) => result.success)).toBe(true);
      for (let i = 0; i < clients.length; i++) {
        const ids: string[] = [];
        for await (const { event } of peer.readStream(`normal-load-${i}`))
          if (event) ids.push(event.id);
        expect(ids).toEqual([events[i].id]);
      }
    } finally {
      await Promise.all(clients.map((session) => session.dispose()));
    }
  });

  it('settles an over-limit fragmented append and leaves another client usable', async () => {
    const onData = jest.spyOn(BatchAppendSession.prototype, 'onData');
    const size = jest.spyOn(requestSizing, 'requestSize');
    try {
      const events = Array.from({ length: 10001 }, (_, index) =>
        jsonEvent({ type: 'overload', data: { index } }),
      );
      const settled = client
        .appendToStream('over-limit', events, {
          streamState: NO_STREAM,
          batchAppendSize: 700,
          deadline: 2000,
        })
        .then(
          () => 'success',
          (error: unknown) => error,
        );
      const outcome = await Promise.race([
        settled,
        new Promise<'unsettled'>((resolve) =>
          setTimeout(() => resolve('unsettled'), 5000).unref(),
        ),
      ]);
      const received = onData.mock.calls.reduce(
        (sum, [message]) => sum + (message.proposedMessages?.length ?? 0),
        0,
      );
      expect(received).toBeGreaterThan(0);
      expect(onData.mock.calls.length).toBeGreaterThan(1);
      expect(size.mock.calls[0][1]).toBe(16 * 1024 * 1024);
      expect(size.mock.calls[0][2]).toBe(true);
      expect(received).toBeGreaterThan(10000);
      expect(onData.mock.calls[onData.mock.calls.length - 1][0].isFinal).toBe(
        true,
      );
      const event = jsonEvent({ type: 'peer', data: {} });
      expect(
        (
          await peer.appendToStream('healthy-peer', event, {
            streamState: NO_STREAM,
            deadline: 3000,
          })
        ).success,
      ).toBe(true);
      const ids: string[] = [];
      for await (const { event: read } of peer.readStream('healthy-peer'))
        if (read) ids.push(read.id);
      expect(ids).toEqual([event.id]);
      expect(outcome).toBeInstanceOf(UnknownError);
    } finally {
      onData.mockRestore();
      size.mockRestore();
    }
  });
});
