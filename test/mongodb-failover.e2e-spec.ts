import { INestMicroservice } from '@nestjs/common';
import { Test } from '@nestjs/testing';
import { Transport } from '@nestjs/microservices';
import {
  KurrentDBClient,
  ANY,
  jsonEvent,
  NO_STREAM,
  WrongExpectedVersionError,
  UnknownError,
} from '@kurrent/kurrentdb-client';
import { GenericContainer, StartedTestContainer, Wait } from 'testcontainers';
import { ClientSession, MongoClient } from 'mongodb';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { join } from 'node:path';
import { AppModule } from '../src/app.module';
import { EVENT_STORE_BACKEND } from '../src/event-store/event-store-backend';
import { MongoEventStoreService } from '../src/mongodb/mongodb-event-store.service';
import { getFreePort } from './util/get-free-port';

// Host networking makes the same replica-set addresses reachable from mongod and
// the adapter. Requires a local Linux Docker daemon with host networking.
describe('MongoDB replica-set election through the official client', () => {
  jest.setTimeout(180_000);
  const containers: StartedTestContainer[] = [];
  const ports: number[] = [];
  const previous = { ...process.env };
  let app: INestMicroservice;
  let client: KurrentDBClient;
  let mongo: MongoClient;
  const docker = promisify(execFile);

  const hello = async (port: number): Promise<boolean> => {
    const probe = new MongoClient(
      `mongodb://127.0.0.1:${port}/?directConnection=true&serverSelectionTimeoutMS=1000&connectTimeoutMS=1000`,
    );
    try {
      const result = await probe.db('admin').command({ hello: 1 });
      return result.isWritablePrimary === true;
    } catch {
      return false;
    } finally {
      await probe.close();
    }
  };

  const awaitPrimary = async (excluded?: number): Promise<number> => {
    const deadline = Date.now() + 60_000;
    while (Date.now() < deadline) {
      for (let i = 0; i < ports.length; i++) {
        if (i !== excluded && (await hello(ports[i]))) return i;
      }
      await new Promise((resolve) => setTimeout(resolve, 250));
    }
    throw new Error('Timed out waiting for a replica-set primary');
  };

  beforeAll(async () => {
    for (let i = 0; i < 3; i++) {
      let port = await getFreePort();
      while (ports.includes(port)) port = await getFreePort();
      ports.push(port);
      containers.push(
        await new GenericContainer('mongo:7')
          .withNetworkMode('host')
          .withCommand([
            'mongod',
            '--replSet',
            'rs0',
            '--bind_ip_all',
            '--port',
            String(port),
          ])
          .withWaitStrategy(Wait.forLogMessage('Waiting for connections'))
          .start(),
      );
    }
    const members = ports.map((port, i) => ({
      _id: i,
      host: `127.0.0.1:${port}`,
    }));
    const init = await containers[0].exec([
      'mongosh',
      '--quiet',
      '--port',
      String(ports[0]),
      '--eval',
      `const r = rs.initiate(${JSON.stringify({ _id: 'rs0', members })}); if (!r.ok) throw new Error(JSON.stringify(r));`,
    ]);
    if (init.exitCode !== 0) throw new Error(init.output);
    await awaitPrimary();
    process.env.EVENT_STORE_BACKEND = 'mongodb';
    process.env.MONGODB_EXPERIMENTAL_GROUP_COMMIT = 'true';
    process.env.MONGODB_GROUP_COMMIT_ACKNOWLEDGE_SINGLE_ADAPTER = 'true';
    process.env.MONGODB_URI = `mongodb://${ports.map((p) => `127.0.0.1:${p}`).join(',')}/?replicaSet=rs0&serverSelectionTimeoutMS=30000&heartbeatFrequencyMS=500`;
    process.env.MONGODB_DATABASE = 'adapter_failover_test';
    const fixture = await Test.createTestingModule({
      imports: [AppModule],
    }).compile();
    const backend = fixture.get<MongoEventStoreService>(EVENT_STORE_BACKEND);
    mongo = backend['infrastructure'].getClient();
    Object.assign(mongo.options, { monitorCommands: true });
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
    await client?.dispose();
    await app?.close();
    await Promise.all(containers.map((container) => container.stop()));
    process.env = previous;
  });

  it('preserves acknowledged grouped writes across election and continues positions', async () => {
    const commits: Record<string, unknown>[] = [];
    const listener = (event: {
      commandName: string;
      command: Record<string, unknown>;
    }) => {
      if (event.commandName === 'commitTransaction')
        commits.push(event.command);
    };
    mongo.on('commandStarted', listener);
    const before = ['group-election-a', 'group-election-b'].map((stream) => ({
      stream,
      event: jsonEvent({ type: 'group-before', data: { stream } }),
    }));
    try {
      const acknowledged = await Promise.all(
        before.map(({ stream, event }) =>
          client.appendToStream(stream, event, {
            streamState: ANY,
            deadline: 10000,
          }),
        ),
      );
      expect(commits).toHaveLength(1);
      expect(commits[0].writeConcern).toMatchObject({ w: 'majority', j: true });
      const oldPrimary = await awaitPrimary();
      await docker('docker', ['pause', containers[oldPrimary].getId()]);
      try {
        expect(await awaitPrimary(oldPrimary)).not.toBe(oldPrimary);
        for (const { stream, event } of before) {
          const found: string[] = [];
          for await (const item of client.readStream(stream))
            if (item.event) found.push(item.event.id);
          expect(found).toEqual([event.id]);
        }
        commits.length = 0;
        const after = ['group-election-c', 'group-election-d'].map(
          (stream) => ({
            stream,
            event: jsonEvent({ type: 'group-after', data: { stream } }),
          }),
        );
        const later = await Promise.all(
          after.map(({ stream, event }) =>
            client.appendToStream(stream, event, {
              streamState: ANY,
              deadline: 30000,
            }),
          ),
        );
        expect(commits).toHaveLength(1);
        expect(commits[0].writeConcern).toMatchObject({
          w: 'majority',
          j: true,
        });
        const positions = [...acknowledged, ...later]
          .map((result) => result.position!.commit)
          .sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
        expect(positions).toEqual(
          positions.map((_, i) => positions[0] + BigInt(i)),
        );
        const records: Array<{ id: string; position: bigint }> = [];
        for await (const { event } of client.readAll()) {
          if (
            event &&
            [...before, ...after].some((item) => item.event.id === event.id)
          )
            records.push({ id: event.id, position: event.position.commit });
        }
        expect(records.map((record) => record.id).sort()).toEqual(
          [...before, ...after].map((item) => item.event.id).sort(),
        );
        expect(
          records
            .map((record) => record.position)
            .sort((a, b) => (a < b ? -1 : a > b ? 1 : 0)),
        ).toEqual(positions);
      } finally {
        await docker('docker', ['unpause', containers[oldPrimary].getId()]);
      }
    } finally {
      mongo.off('commandStarted', listener);
    }
  });

  it('reports a simulated lost grouped acknowledgement after election as unknown without replay', async () => {
    const oldPrimary = await awaitPrimary();
    let release!: () => void;
    let entered!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const committed = new Promise<void>((resolve) => {
      entered = resolve;
    });
    // eslint-disable-next-line @typescript-eslint/unbound-method
    const original = ClientSession.prototype.commitTransaction;
    const spy = jest
      .spyOn(ClientSession.prototype, 'commitTransaction')
      .mockImplementation(async function (this: ClientSession) {
        await original.call(this);
        if ((this as unknown as { client: MongoClient }).client === mongo) {
          entered();
          await gate;
          throw new Error('simulated lost acknowledgement');
        }
      });
    const items = ['group-uncertain-a', 'group-uncertain-b'].map((stream) => ({
      stream,
      event: jsonEvent({ type: 'uncertain-election', data: { stream } }),
    }));
    let paused = false;
    try {
      const results = Promise.allSettled(
        items.map(({ stream, event }) =>
          client.appendToStream(stream, event, {
            streamState: ANY,
            deadline: 90000,
          }),
        ),
      );
      await Promise.race([
        committed,
        new Promise<never>((_, reject) => {
          const timer = setTimeout(
            () => reject(new Error('commit was not reached')),
            20000,
          );
          void committed.then(() => clearTimeout(timer));
        }),
      ]);
      await docker('docker', ['pause', containers[oldPrimary].getId()]);
      paused = true;
      expect(await awaitPrimary(oldPrimary)).not.toBe(oldPrimary);
      release();
      for (const result of await results) {
        expect(result.status).toBe('rejected');
        if (result.status !== 'rejected') throw new Error('expected unknown');
        expect(result.reason).toBeInstanceOf(UnknownError);
      }
      expect(spy).toHaveBeenCalledTimes(1);
      spy.mockRestore();
      for (const { stream, event } of items) {
        const ids: string[] = [];
        for await (const item of client.readStream(stream))
          if (item.event) ids.push(item.event.id);
        expect(ids).toEqual([event.id]);
      }
    } finally {
      release();
      spy.mockRestore();
      if (paused)
        await docker('docker', ['unpause', containers[oldPrimary].getId()]);
    }
  });

  it('preserves acknowledged events and reconnects to the elected primary', async () => {
    const stream = 'election-survival';
    const first = jsonEvent({ type: 'before-election', data: { value: 1 } });
    const second = jsonEvent({ type: 'after-election', data: { value: 2 } });
    const ack = await client.appendToStream(stream, first, {
      streamState: NO_STREAM,
    });
    expect(ack.nextExpectedRevision).toBe(0n);
    const oldPrimary = await awaitPrimary();
    await docker('docker', ['pause', containers[oldPrimary].getId()]);
    try {
      const newPrimary = await awaitPrimary(oldPrimary);
      expect(newPrimary).not.toBe(oldPrimary);
      const events: unknown[] = [];
      for await (const { event } of client.readStream(stream))
        events.push(event);
      expect(events).toHaveLength(1);
      expect(events[0]).toMatchObject({
        id: first.id,
        type: first.type,
        revision: 0,
        data: { value: 1 },
      });
      const next = await client.appendToStream(stream, second, {
        streamState: 0n,
      });
      expect(next.nextExpectedRevision).toBe(1n);
      await expect(
        client.appendToStream(stream, jsonEvent({ type: 'stale', data: {} }), {
          streamState: 0n,
        }),
      ).rejects.toBeInstanceOf(WrongExpectedVersionError);
      const after: Array<{ id: string; revision: bigint } | undefined> = [];
      for await (const { event } of client.readStream(stream))
        after.push(event);
      expect(after.map((event) => event?.id)).toEqual([first.id, second.id]);
      expect(after.map((event) => Number(event?.revision))).toEqual([0, 1]);
    } finally {
      await docker('docker', ['unpause', containers[oldPrimary].getId()]);
    }
  });
});
