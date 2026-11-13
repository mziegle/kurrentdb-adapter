import { INestMicroservice } from '@nestjs/common';
import { Test } from '@nestjs/testing';
import { Transport } from '@nestjs/microservices';
import {
  ANY,
  NO_STREAM,
  KurrentDBClient,
  UnknownError,
  jsonEvent,
} from '@kurrent/kurrentdb-client';
import { AbstractCursor, MongoClient } from 'mongodb';
import { GenericContainer, StartedTestContainer, Wait } from 'testcontainers';
import { join } from 'node:path';
import { AppModule } from '../src/app.module';
import { EVENT_STORE_BACKEND } from '../src/event-store/event-store-backend';
import { MongoEventStoreService } from '../src/mongodb/mongodb-event-store.service';
import { getFreePort } from './util/get-free-port';

describe('Mongo shared admission through AppModule', () => {
  jest.setTimeout(120000);
  let container: StartedTestContainer;
  let uri: string;
  const previous = { ...process.env };
  let savedExitCode: typeof process.exitCode;
  beforeEach(() => {
    savedExitCode = process.exitCode;
  });
  afterEach(() => {
    process.exitCode = savedExitCode;
  });

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
    uri = `mongodb://${container.getHost()}:${container.getMappedPort(27017)}/?directConnection=true`;
  });
  afterAll(async () => {
    process.env = previous;
    await container?.stop();
  });

  async function start(database: string): Promise<{
    app: INestMicroservice;
    client: KurrentDBClient;
    backend: MongoEventStoreService;
    mongo: MongoClient;
  }> {
    process.env.EVENT_STORE_BACKEND = 'mongodb';
    process.env.MONGODB_URI = uri;
    process.env.MONGODB_DATABASE = database;
    process.env.MONGODB_EXPERIMENTAL_GROUP_COMMIT = 'true';
    process.env.MONGODB_GROUP_COMMIT_ACKNOWLEDGE_SINGLE_ADAPTER = 'true';
    process.env.MONGODB_UNSAFE_ENABLE_SCAVENGE = 'true';
    const fixture = await Test.createTestingModule({
      imports: [AppModule],
    }).compile();
    const backend = fixture.get<MongoEventStoreService>(EVENT_STORE_BACKEND);
    const mongo = backend['infrastructure'].getClient();
    const port = await getFreePort();
    const app = fixture.createNestMicroservice({
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
    return {
      app,
      client: KurrentDBClient.connectionString`kurrentdb://127.0.0.1:${port}?tls=false`,
      backend,
      mongo,
    };
  }

  it('isolates a committed ordinary append whose session cleanup fails', async () => {
    const { app, client, backend, mongo } = await start('cleanup_correlated');
    Object.defineProperty(backend, 'admission', { value: undefined });
    const original = mongo.startSession.bind(
      mongo,
    ) as MongoClient['startSession'];
    let enter!: () => void;
    let release!: () => void;
    const entered = new Promise<void>((resolve) => {
      enter = resolve;
    });
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    let peerEnter!: () => void;
    let peerRelease!: () => void;
    const peerEntered = new Promise<void>((resolve) => {
      peerEnter = resolve;
    });
    const peerGate = new Promise<void>((resolve) => {
      peerRelease = resolve;
    });
    let driverCleaned = false;
    const spy = jest
      .spyOn(mongo, 'startSession')
      .mockImplementation((options) => {
        const session = original(options);
        spy.mockRestore();
        const end = session.endSession.bind(session) as () => Promise<void>;
        jest.spyOn(session, 'endSession').mockImplementation(async () => {
          await end();
          driverCleaned = true;
          enter();
          await gate;
          throw new Error('injected cleanup failure');
        });
        return session;
      });
    const failedEvent = jsonEvent({ type: 'committed', data: {} });
    const peerEvent = jsonEvent({ type: 'peer', data: {} });
    try {
      const failed = client
        .appendToStream('cleanup-target', failedEvent, {
          streamState: NO_STREAM,
          deadline: 5000,
        })
        .then(
          () => null,
          (error: unknown) => error,
        );
      await entered;
      const peerSpy = jest
        .spyOn(mongo, 'startSession')
        .mockImplementationOnce((options) => {
          const session = original(options);
          peerSpy.mockRestore();
          const end = session.endSession.bind(session) as () => Promise<void>;
          jest.spyOn(session, 'endSession').mockImplementation(async () => {
            await end();
            peerEnter();
            await peerGate;
          });
          return session;
        });
      const peer = client.appendToStream('cleanup-peer', peerEvent, {
        streamState: NO_STREAM,
        deadline: 5000,
      });
      await peerEntered;
      release();
      expect(await failed).toBeInstanceOf(UnknownError);
      peerRelease();
      expect((await peer).success).toBe(true);
      const later = jsonEvent({ type: 'later', data: {} });
      expect(
        (
          await client.appendToStream('cleanup-later', later, {
            streamState: NO_STREAM,
            deadline: 5000,
          })
        ).success,
      ).toBe(true);
      const ids: string[] = [];
      for await (const { event } of client.readAll())
        if (event) ids.push(event.id);
      expect(ids.filter((id) => id === failedEvent.id)).toHaveLength(1);
      expect(ids.filter((id) => id === peerEvent.id)).toHaveLength(1);
      expect(ids.filter((id) => id === later.id)).toHaveLength(1);
    } finally {
      release();
      peerRelease();
      spy.mockRestore();
      await client.dispose();
      await app.close();
      if (driverCleaned) await mongo.close();
    }
  });

  it('releases admission when ordinary session creation throws', async () => {
    const { app, client, mongo } = await start('admission_session_throw');
    const spy = jest.spyOn(mongo, 'startSession').mockImplementationOnce(() => {
      throw new Error('injected session start failure');
    });
    try {
      await expect(
        client.deleteStream('failed', { deadline: 2000 }),
      ).rejects.toThrow();
      expect(spy).toHaveBeenCalledTimes(1);
      spy.mockRestore();
      const event = jsonEvent({ type: 'peer', data: {} });
      const result = await client.appendToStream('session-peer', event, {
        streamState: NO_STREAM,
        deadline: 3000,
      });
      expect(result.success).toBe(true);
      const ids: string[] = [];
      for await (const { event: read } of client.readStream('session-peer'))
        if (read) ids.push(read.id);
      expect(ids).toEqual([event.id]);
    } finally {
      spy.mockRestore();
      await client.dispose();
      await app.close();
    }
  });

  it('rejects an over-capacity write without affecting a later peer', async () => {
    const { app, client, backend } = await start('admission_capacity_peer');
    const release = await backend['admission']!.acquire(16 * 1024 * 1024 - 1);
    try {
      const rejectedEvent = jsonEvent({ type: 'capacity', data: {} });
      await expect(
        client.appendToStream('capacity-rejected', rejectedEvent, {
          streamState: NO_STREAM,
          deadline: 2000,
        }),
      ).rejects.toThrow();
      release();
      const peer = jsonEvent({ type: 'peer', data: {} });
      expect(
        (
          await client.appendToStream('capacity-peer', peer, {
            streamState: NO_STREAM,
            deadline: 3000,
          })
        ).success,
      ).toBe(true);
      const ids: string[] = [];
      for await (const { event } of client.readAll())
        if (event) ids.push(event.id);
      expect(ids.filter((id) => id === rejectedEvent.id)).toHaveLength(0);
      expect(ids.filter((id) => id === peer.id)).toHaveLength(1);
    } finally {
      release();
      await client.dispose();
      await app.close();
    }
  });

  it('removes an expired queued write without affecting its healthy peer', async () => {
    const { app, client, backend } = await start('admission_cancel_peer');
    const release = await backend['admission']!.acquire(1);
    const acquire = jest.spyOn(backend['admission']!, 'acquire');
    try {
      const expiredEvent = jsonEvent({ type: 'expired', data: {} });
      const expired = client
        .appendToStream('expired', expiredEvent, {
          streamState: NO_STREAM,
          deadline: 1500,
        })
        .then(
          () => 'success',
          () => 'error',
        );
      const peerEvent = jsonEvent({ type: 'peer', data: {} });
      const peer = client.appendToStream('cancel-peer', peerEvent, {
        streamState: NO_STREAM,
        deadline: 5000,
      });
      for (let i = 0; i < 120 && acquire.mock.calls.length < 2; i++)
        await new Promise((resolve) => setTimeout(resolve, 10));
      expect(acquire).toHaveBeenCalledTimes(2);
      expect(await expired).toBe('error');
      release();
      expect((await peer).success).toBe(true);
      const ids: string[] = [];
      for await (const { event } of client.readAll())
        if (event) ids.push(event.id);
      expect(ids.filter((id) => id === expiredEvent.id)).toHaveLength(0);
      expect(ids.filter((id) => id === peerEvent.id)).toHaveLength(1);
    } finally {
      acquire.mockRestore();
      release();
      await client.dispose();
      await app.close();
    }
  });

  it('stops a scavenge waiting for admission and leaves a peer write usable', async () => {
    const { app, client, backend } = await start('admission_scavenge_stop');
    await client.appendToStream('seed', jsonEvent({ type: 'seed', data: {} }), {
      streamState: NO_STREAM,
    });
    const release = await backend['admission']!.acquire(1);
    try {
      const acquire = jest.spyOn(backend['admission']!, 'acquire');
      const close = jest.spyOn(AbstractCursor.prototype, 'close');
      const { scavengeId } = backend.startScavenge();
      const task = backend['scavenges'].get(scavengeId)!.task!;
      for (let i = 0; i < 200 && acquire.mock.calls.length === 0; i++)
        await new Promise((resolve) => setTimeout(resolve, 10));
      expect(acquire).toHaveBeenCalled();
      backend.stopScavenge({ options: { scavengeId } });
      await Promise.race([
        task,
        new Promise<never>((_, reject) =>
          setTimeout(
            () => reject(new Error('scavenge did not stop')),
            2000,
          ).unref(),
        ),
      ]);
      expect(backend['scavenges'].has(scavengeId)).toBe(false);
      expect(close).toHaveBeenCalled();
      close.mockRestore();
      acquire.mockRestore();
      release();
      const peer = jsonEvent({ type: 'peer', data: {} });
      expect(
        (
          await client.appendToStream('scavenge-peer', peer, {
            streamState: ANY,
            deadline: 3000,
          })
        ).success,
      ).toBe(true);
      const ids: string[] = [];
      for await (const { event } of client.readStream('scavenge-peer'))
        if (event) ids.push(event.id);
      expect(ids).toEqual([peer.id]);
    } finally {
      jest.restoreAllMocks();
      release();
      await client.dispose();
      await app.close();
    }
  });
});
