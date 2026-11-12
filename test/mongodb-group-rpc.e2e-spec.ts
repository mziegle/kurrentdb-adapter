import { INestMicroservice } from '@nestjs/common';
import { Test } from '@nestjs/testing';
import { Transport } from '@nestjs/microservices';
import {
  ANY,
  KurrentDBClient,
  jsonEvent,
  StreamDeletedError,
  UnknownError,
  DeadlineExceededError,
} from '@kurrent/kurrentdb-client';
import { BSON, ClientSession, MongoClient, MongoServerError } from 'mongodb';
import { GenericContainer, StartedTestContainer, Wait } from 'testcontainers';
import { join } from 'node:path';
import { AppModule } from '../src/app.module';
import { getFreePort } from './util/get-free-port';
import { EVENT_STORE_BACKEND } from '../src/event-store/event-store-backend';
import { MongoEventStoreService } from '../src/mongodb/mongodb-event-store.service';
import { AppendReq } from '../src/interfaces/streams';
import { GroupDispatcher } from '../src/mongodb/group-dispatcher';
import { runMongoGroup } from '../src/mongodb/group-runner';

type Request = {
  stream: string;
  options: NonNullable<AppendReq['options']>;
  events: NonNullable<AppendReq['proposedMessage']>[];
};
type Result = Awaited<ReturnType<MongoEventStoreService['appendGroupMember']>>;

describe('test-only Mongo group RPC bridge', () => {
  jest.setTimeout(120000);
  let container: StartedTestContainer;
  let app: INestMicroservice;
  let app2: INestMicroservice;
  let client2: KurrentDBClient;
  let mongo2: MongoClient;
  let client: KurrentDBClient;
  let mongo: MongoClient;
  let dispatcher: GroupDispatcher<Request, Result>;
  let backend: MongoEventStoreService;
  const observedOutcomes: string[] = [];
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
    const uri = `mongodb://${container.getHost()}:${container.getMappedPort(27017)}/?directConnection=true`;
    process.env.EVENT_STORE_BACKEND = 'mongodb';
    process.env.MONGODB_URI = uri;
    process.env.MONGODB_DATABASE = 'group_rpc';
    mongo = new MongoClient(uri, { monitorCommands: true });
    await mongo.connect();
    const fixture = await Test.createTestingModule({
      imports: [AppModule],
    }).compile();
    backend = fixture.get<MongoEventStoreService>(EVENT_STORE_BACKEND);
    const original = backend.append.bind(
      backend,
    ) as MongoEventStoreService['append'];
    dispatcher = new GroupDispatcher<Request, Result>(
      { maxRequests: 2, maxEvents: 2, maxBytes: 1024 * 1024 },
      100,
      (value) => value.stream,
      (batch) =>
        runMongoGroup(
          mongo,
          batch,
          (value, session) =>
            backend['appendGroupMember'](
              session,
              mongo.db('group_rpc'),
              value.options,
              value.stream,
              value.events,
            ),
          { maxAttempts: 1, maxCommitAttempts: 1 },
        ).finally(() => {
          observedOutcomes.push(
            ...batch.members.map(
              (member) => member.outcome?.kind ?? 'unsettled',
            ),
          );
        }),
    );
    jest
      .spyOn(backend, 'append')
      .mockImplementation(async (messages, context) => {
        const options = messages.find((message) => message.options)?.options;
        const events = messages.flatMap((message) =>
          message.proposedMessage ? [message.proposedMessage] : [],
        );
        const stream =
          options?.streamIdentifier?.streamName &&
          Buffer.from(options.streamIdentifier.streamName).toString('utf8');
        if (
          !options ||
          !stream ||
          stream.startsWith('$') ||
          options.any === undefined ||
          options.revision !== undefined ||
          options.noStream !== undefined ||
          options.streamExists !== undefined ||
          events.length !== 1
        )
          return original(messages, context);
        const result = await dispatcher.enqueue(
          { stream, options, events },
          1,
          Buffer.byteLength(JSON.stringify(messages)),
          context,
        );
        if (result.kind === 'error') throw result.error;
        return result.value;
      });
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
    const fixture2 = await Test.createTestingModule({
      imports: [AppModule],
    }).compile();
    const backend2 = fixture2.get<MongoEventStoreService>(EVENT_STORE_BACKEND);
    mongo2 = backend2['infrastructure'].getClient();
    Object.assign(mongo2.options, { monitorCommands: true });
    const port2 = await getFreePort();
    app2 = fixture2.createNestMicroservice({
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
        url: `127.0.0.1:${port2}`,
      },
    });
    await app2.listen();
    client2 = KurrentDBClient.connectionString`kurrentdb://127.0.0.1:${port2}?tls=false`;
  });
  afterAll(async () => {
    await client2?.dispose();
    await app2?.close();
    await client?.dispose();
    await dispatcher?.shutdown(1000);
    await app?.close();
    await mongo?.close();
    await container?.stop();
    process.env = previous;
  });
  // A started event alone only proves a driver attempted to send. Require either
  // a server reply or a matching in-progress operation on the replica-set primary.
  function counterCommandGate(source: MongoClient) {
    let observed:
      | { requestId: number; lsid: unknown; txnNumber: unknown }
      | undefined;
    let replied = false;
    const started = (event: {
      commandName: string;
      requestId: number;
      command: Record<string, unknown>;
    }) => {
      if (
        !observed &&
        event.commandName === 'findAndModify' &&
        event.command.findAndModify === 'counters'
      )
        observed = {
          requestId: event.requestId,
          lsid: event.command.lsid,
          txnNumber: event.command.txnNumber,
        };
    };
    const finished = (event: { requestId: number }) => {
      if (event.requestId === observed?.requestId) replied = true;
    };
    const failed = (event: { requestId: number; failure: Error }) => {
      if (event.failure instanceof MongoServerError) finished(event);
    };
    source.on('commandStarted', started);
    source.on('commandSucceeded', finished);
    source.on('commandFailed', failed);
    return {
      async reachedServer() {
        const until = Date.now() + 10000;
        while (Date.now() < until) {
          if (observed) {
            if (replied) return;
            const operations = await mongo
              .db('admin')
              .aggregate<{ command: { lsid: unknown; txnNumber: unknown } }>([
                { $currentOp: { allUsers: true, localOps: true } },
                { $match: { 'command.findAndModify': 'counters' } },
              ])
              .toArray();
            if (
              operations.some(
                (operation) =>
                  BSON.EJSON.stringify(operation.command?.lsid) ===
                    BSON.EJSON.stringify(observed?.lsid) &&
                  BSON.EJSON.stringify(operation.command?.txnNumber) ===
                    BSON.EJSON.stringify(observed?.txnNumber),
              )
            )
              return;
          }
          await new Promise((resolve) => setTimeout(resolve, 10));
        }
        throw new Error('competing counter command did not reach MongoDB');
      },
      close() {
        source.off('commandStarted', started);
        source.off('commandSucceeded', finished);
        source.off('commandFailed', failed);
      },
    };
  }

  it('fences a group racing an uncommitted tombstone on another adapter', async () => {
    let release!: () => void;
    let entered!: () => void;
    const gate = new Promise<void>((resolve) => (release = resolve));
    const atCommit = new Promise<void>((resolve) => (entered = resolve));
    // eslint-disable-next-line @typescript-eslint/unbound-method
    const original = ClientSession.prototype.commitTransaction;
    const commit = jest
      .spyOn(ClientSession.prototype, 'commitTransaction')
      .mockImplementation(async function (this: ClientSession) {
        if ((this as unknown as { client: MongoClient }).client === mongo2) {
          entered();
          await gate;
        }
        await original.call(this);
      });
    const competing = counterCommandGate(mongo);
    const commits: Record<string, unknown>[] = [];
    const listener = (event: {
      commandName: string;
      command: Record<string, unknown>;
    }) => {
      if (event.commandName === 'commitTransaction')
        commits.push(event.command);
    };
    mongo2.on('commandStarted', listener);
    mongo.on('commandStarted', listener);
    try {
      const deletion = client2.tombstoneStream('race-first');
      await atCommit;
      const writes = Promise.allSettled([
        client.appendToStream(
          'race-first',
          jsonEvent({ type: 'forbidden', data: {} }),
          { streamState: ANY, deadline: 3000 },
        ),
        client.appendToStream(
          'race-first-peer',
          jsonEvent({ type: 'peer', data: {} }),
          { streamState: ANY, deadline: 3000 },
        ),
      ]);
      await competing.reachedServer();
      release();
      const deleted = await deletion;
      let timeout: ReturnType<typeof setTimeout> | undefined;
      const outcomes = await Promise.race([
        writes,
        new Promise<null>((resolve) => {
          timeout = setTimeout(() => resolve(null), 5000);
        }),
      ]).finally(() => clearTimeout(timeout));
      expect(outcomes).not.toBeNull();
      if (!outcomes) throw new Error('competing appends did not settle');
      expect(outcomes[0].status).toBe('rejected');
      if (outcomes[0].status === 'rejected')
        expect(
          [UnknownError, StreamDeletedError].some(
            (type) =>
              outcomes[0].status === 'rejected' &&
              outcomes[0].reason instanceof type,
          ),
        ).toBe(true);
      for (const command of commits)
        expect(command.writeConcern).toMatchObject({ w: 'majority', j: true });
      expect(commits.length).toBeGreaterThan(0);
      commit.mockRestore();
      const after = await client2.appendToStream(
        'race-first-after',
        jsonEvent({ type: 'after', data: {} }),
        { streamState: ANY },
      );
      expect(after.success).toBe(true);
      expect(after.position!.commit).toBeGreaterThan(deleted.position!.commit);
      if (outcomes[1].status === 'fulfilled') {
        expect(outcomes[1].value.position!.commit).toBeGreaterThan(
          deleted.position!.commit,
        );
        expect(after.position!.commit).toBeGreaterThan(
          outcomes[1].value.position!.commit,
        );
        const types: string[] = [];
        for await (const { event } of client2.readStream('race-first-peer'))
          if (event) types.push(event.type);
        expect(types).toEqual(['peer']);
      }
      const firstTypes: string[] = [];
      for await (const { event } of client2.readAll())
        if (event && ['forbidden', 'peer'].includes(event.type))
          firstTypes.push(event.type);
      expect(firstTypes).toEqual(
        outcomes[1].status === 'fulfilled' ? ['peer'] : [],
      );
      expect(after.position!.commit).toBe(
        deleted.position!.commit +
          (outcomes[1].status === 'fulfilled' ? 2n : 1n),
      );
      await expect(
        client2.appendToStream(
          'race-first',
          jsonEvent({ type: 'no', data: {} }),
          { streamState: ANY },
        ),
      ).rejects.toBeInstanceOf(StreamDeletedError);
    } finally {
      release();
      commit.mockRestore();
      competing.close();
      mongo.off('commandStarted', listener);
      mongo2.off('commandStarted', listener);
    }
  });

  it('retains the fence after a tombstoned member reverses its reservation', async () => {
    await client2.tombstoneStream('race-reversed-deleted');
    let release!: () => void;
    let entered!: () => void;
    const gate = new Promise<void>((resolve) => (release = resolve));
    const atCommit = new Promise<void>((resolve) => (entered = resolve));
    // eslint-disable-next-line @typescript-eslint/unbound-method
    const original = ClientSession.prototype.commitTransaction;
    const commit = jest
      .spyOn(ClientSession.prototype, 'commitTransaction')
      .mockImplementation(async function (this: ClientSession) {
        if ((this as unknown as { client: MongoClient }).client === mongo) {
          entered();
          await gate;
        }
        await original.call(this);
      });
    const competing = counterCommandGate(mongo2);
    const commits: Record<string, unknown>[] = [];
    const listener = (event: {
      commandName: string;
      command: Record<string, unknown>;
    }) => {
      if (event.commandName === 'commitTransaction')
        commits.push(event.command);
    };
    mongo.on('commandStarted', listener);
    mongo2.on('commandStarted', listener);
    const reversed: number[] = [];
    const successful = new Set<number>();
    const reversalStarted = (event: {
      requestId: number;
      commandName: string;
      command: {
        update?: unknown;
        updates?: { u?: { $inc?: { value?: number } } }[];
      };
    }) => {
      if (
        event.commandName === 'update' &&
        event.command.update === 'counters' &&
        event.command.updates?.[0]?.u?.$inc?.value === -1
      )
        reversed.push(event.requestId);
    };
    const reversalSucceeded = (event: { requestId: number }) =>
      successful.add(event.requestId);
    mongo.on('commandStarted', reversalStarted);
    mongo.on('commandSucceeded', reversalSucceeded);
    let queued!: () => void;
    const firstQueued = new Promise<void>((resolve) => (queued = resolve));
    const enqueue = jest.spyOn(dispatcher, 'enqueue');
    try {
      // Enqueue the deleted stream first so its reservation is reversed before
      // the healthy member reserves its position in the same transaction.
      const first = client.appendToStream(
        'race-reversed-deleted',
        jsonEvent({ type: 'forbidden', data: {} }),
        { streamState: ANY },
      );
      const poll = setInterval(() => {
        if (
          enqueue.mock.calls.some(
            ([value]) => value.stream === 'race-reversed-deleted',
          )
        )
          queued();
      }, 1);
      try {
        await firstQueued;
      } finally {
        clearInterval(poll);
      }
      const second = client.appendToStream(
        'race-reversed-live',
        jsonEvent({ type: 'live', data: {} }),
        { streamState: ANY },
      );
      await atCommit;
      expect(reversed).toHaveLength(1);
      expect(successful.has(reversed[0])).toBe(true);
      expect(
        enqueue.mock.calls.slice(0, 2).map(([value]) => value.stream),
      ).toEqual(['race-reversed-deleted', 'race-reversed-live']);
      const deletion = client2.tombstoneStream('race-reversed-live');
      await competing.reachedServer();
      release();
      const [rejected, written, deleted] = await Promise.all([
        first.then(
          () => null,
          (error: unknown) => error,
        ),
        second,
        deletion,
      ]);
      expect(rejected).toBeInstanceOf(StreamDeletedError);
      expect(written.success).toBe(true);
      expect(deleted.position!.commit).toBe(written.position!.commit + 1n);
      commit.mockRestore();
      const after = await client2.appendToStream(
        'race-reversed-after',
        jsonEvent({ type: 'after', data: {} }),
        { streamState: ANY },
      );
      expect(after.position!.commit).toBe(deleted.position!.commit + 1n);
      expect(commits.length).toBeGreaterThanOrEqual(2);
      for (const command of commits)
        expect(command.writeConcern).toMatchObject({ w: 'majority', j: true });
      await expect(
        client2.appendToStream(
          'race-reversed-live',
          jsonEvent({ type: 'no', data: {} }),
          { streamState: ANY },
        ),
      ).rejects.toBeInstanceOf(StreamDeletedError);
      const all: string[] = [];
      for await (const { event } of client2.readAll())
        if (event?.type === 'live') all.push(event.type);
      expect(all).toEqual(['live']);
    } finally {
      release();
      commit.mockRestore();
      competing.close();
      mongo.off('commandStarted', listener);
      mongo2.off('commandStarted', listener);
      enqueue.mockRestore();
      mongo.off('commandStarted', reversalStarted);
      mongo.off('commandSucceeded', reversalSucceeded);
    }
  });
  it('reports a tombstone independently while committing its healthy peer', async () => {
    const tombstone = await client.tombstoneStream('rpc-deleted');
    const commits: string[] = [];
    const listener = (event: { commandName: string }) => {
      if (event.commandName === 'commitTransaction')
        commits.push(event.commandName);
    };
    mongo.on('commandStarted', listener);
    try {
      let markQueued!: () => void;
      const queued = new Promise<void>((resolve) => (markQueued = resolve));
      const enqueue = jest.spyOn(dispatcher, 'enqueue');
      const deletedAppend = client.appendToStream(
        'rpc-deleted',
        jsonEvent({ type: 'deleted', data: {} }),
        { streamState: ANY },
      );
      const poll = setInterval(() => {
        if (enqueue.mock.calls.length) markQueued();
      }, 1);
      await queued;
      clearInterval(poll);
      enqueue.mockRestore();
      const [deleted, healthy] = await Promise.allSettled([
        deletedAppend,
        client.appendToStream(
          'rpc-healthy',
          jsonEvent({ type: 'healthy', data: {} }),
          { streamState: ANY },
        ),
      ]);
      expect(deleted.status).toBe('rejected');
      if (deleted.status !== 'rejected') throw new Error('expected deletion');
      expect(deleted.reason).toBeInstanceOf(StreamDeletedError);
      expect(healthy).toMatchObject({
        status: 'fulfilled',
        value: { success: true },
      });
      expect(commits).toHaveLength(1);
      if (
        healthy.status !== 'fulfilled' ||
        !healthy.value.position ||
        !tombstone.position
      )
        throw new Error('expected positions');
      expect(healthy.value.position.commit).toBe(
        tombstone.position.commit + 1n,
      );
      const types: string[] = [];
      for await (const { event } of client.readStream('rpc-healthy'))
        if (event) types.push(event.type);
      expect(types).toEqual(['healthy']);
    } finally {
      mongo.off('commandStarted', listener);
    }
  });
  it('orders a concurrent tombstone after a group holding the global fence', async () => {
    let release!: () => void;
    let entered!: () => void;
    const gate = new Promise<void>((resolve) => (release = resolve));
    const atCommit = new Promise<void>((resolve) => (entered = resolve));
    let deletionEntered!: () => void;
    const atDeletion = new Promise<void>(
      (resolve) => (deletionEntered = resolve),
    );
    const originalTombstone = backend.tombstone.bind(
      backend,
    ) as MongoEventStoreService['tombstone'];
    const tombstoneSpy = jest
      .spyOn(backend, 'tombstone')
      .mockImplementation((request) => {
        deletionEntered();
        return originalTombstone(request);
      });
    // eslint-disable-next-line @typescript-eslint/unbound-method
    const originalCommit = ClientSession.prototype.commitTransaction;
    const commit = jest
      .spyOn(ClientSession.prototype, 'commitTransaction')
      .mockImplementation(async function (this: ClientSession) {
        commit.mockRestore();
        entered();
        await gate;
        await originalCommit.call(this);
      });
    try {
      const append = client.appendToStream(
        'rpc-race',
        jsonEvent({ type: 'before', data: {} }),
        { streamState: ANY },
      );
      const peer = client.appendToStream(
        'rpc-race-peer',
        jsonEvent({ type: 'peer', data: {} }),
        { streamState: ANY },
      );
      await atCommit;
      const deletion = client.tombstoneStream('rpc-race');
      await atDeletion;
      release();
      const [written, peerResult, deleted] = await Promise.all([
        append,
        peer,
        deletion,
      ]);
      expect(peerResult.success).toBe(true);
      expect(written.position).toBeDefined();
      expect(deleted.position).toBeDefined();
      expect(deleted.position!.commit).toBeGreaterThan(
        written.position!.commit,
      );
      await expect(
        client.appendToStream(
          'rpc-race',
          jsonEvent({ type: 'after', data: {} }),
          { streamState: ANY },
        ),
      ).rejects.toBeInstanceOf(StreamDeletedError);
    } finally {
      release();
      commit.mockRestore();
      tombstoneSpy.mockRestore();
    }
  });
  it('coalesces two official-client ANY appends into one majority journal commit', async () => {
    const commits: Record<string, unknown>[] = [];
    const listener = (event: {
      commandName: string;
      command: Record<string, unknown>;
    }) => {
      if (event.commandName === 'commitTransaction')
        commits.push(event.command);
    };
    mongo.on('commandStarted', listener);
    try {
      const results = await Promise.all(
        ['rpc-a', 'rpc-b'].map((stream) =>
          client.appendToStream(
            stream,
            jsonEvent({ type: 'event', data: { stream } }),
            { streamState: ANY },
          ),
        ),
      );
      expect(results.every((result) => result.success)).toBe(true);
      expect(commits).toHaveLength(1);
      expect(commits[0].writeConcern).toMatchObject({ w: 'majority', j: true });
    } finally {
      mongo.off('commandStarted', listener);
    }
  });
  it('isolates capacity rejection from a concurrent append on the same client', async () => {
    let timeout: ReturnType<typeof setTimeout> | undefined;
    const results = await Promise.race([
      Promise.allSettled([
        client.appendToStream(
          'rpc-capacity',
          jsonEvent({ type: 'large', data: { payload: 'x'.repeat(1100000) } }),
          { streamState: ANY, deadline: 3000 },
        ),
        client.appendToStream(
          'rpc-capacity-peer',
          jsonEvent({ type: 'peer', data: {} }),
          { streamState: ANY, deadline: 3000 },
        ),
      ]),
      new Promise<never>(
        (_, reject) =>
          (timeout = setTimeout(
            () => reject(new Error('appends did not settle')),
            5000,
          )),
      ),
    ]).finally(() => clearTimeout(timeout));
    expect(results[0].status).toBe('rejected');
    if (results[0].status !== 'rejected') throw new Error('expected rejection');
    expect(results[0].reason).toBeInstanceOf(UnknownError);
    expect(results[1]).toMatchObject({
      status: 'fulfilled',
      value: { success: true },
    });
    const types: string[] = [];
    for await (const { event } of client.readAll())
      if (event?.type === 'large') types.push(event.type);
    expect(types).toEqual([]);
  });
  it('isolates a queued deadline from a concurrent append on the same client', async () => {
    let release!: () => void;
    let entered!: () => void;
    const gate = new Promise<void>((resolve) => (release = resolve));
    const atCommit = new Promise<void>((resolve) => (entered = resolve));
    // eslint-disable-next-line @typescript-eslint/unbound-method
    const originalCommit = ClientSession.prototype.commitTransaction;
    const commit = jest
      .spyOn(ClientSession.prototype, 'commitTransaction')
      .mockImplementation(async function (this: ClientSession) {
        commit.mockRestore();
        entered();
        await gate;
        await originalCommit.call(this);
      });
    try {
      const active = Promise.all(
        ['rpc-active-a', 'rpc-active-b'].map((stream) =>
          client.appendToStream(
            stream,
            jsonEvent({ type: 'active', data: {} }),
            { streamState: ANY, deadline: 5000 },
          ),
        ),
      );
      await atCommit;
      const expired = client.appendToStream(
        'rpc-queued-deadline',
        jsonEvent({ type: 'expired', data: {} }),
        { streamState: ANY, deadline: 100 },
      );
      const result = expired.then(
        () => null,
        (error: unknown) => error,
      );
      const peer = client.appendToStream(
        'rpc-queued-peer',
        jsonEvent({ type: 'peer', data: {} }),
        { streamState: ANY, deadline: 5000 },
      );
      let timeout: ReturnType<typeof setTimeout> | undefined;
      const error = await Promise.race([
        result,
        new Promise<never>(
          (_, reject) =>
            (timeout = setTimeout(
              () => reject(new Error('deadline did not settle')),
              2000,
            )),
        ),
      ]).finally(() => clearTimeout(timeout));
      expect(error).toBeInstanceOf(DeadlineExceededError);
      release();
      expect((await active).every((value) => value.success)).toBe(true);
      expect(await peer).toMatchObject({ success: true });
      const events: string[] = [];
      for await (const { event } of client.readAll())
        if (event?.type === 'expired') events.push(event.type);
      expect(events).toEqual([]);
    } finally {
      release();
      commit.mockRestore();
    }
  });
  it('settles a sealed expired caller unknown while its peer and late write commit', async () => {
    let release!: () => void;
    let entered!: () => void;
    const gate = new Promise<void>((resolve) => (release = resolve));
    const atCommit = new Promise<void>((resolve) => (entered = resolve));
    // eslint-disable-next-line @typescript-eslint/unbound-method
    const originalCommit = ClientSession.prototype.commitTransaction;
    const commit = jest
      .spyOn(ClientSession.prototype, 'commitTransaction')
      .mockImplementation(async function (this: ClientSession) {
        entered();
        await gate;
        await originalCommit.call(this);
      });
    const enqueue = jest.spyOn(dispatcher, 'enqueue');
    let settlements = 0;
    try {
      const started = Date.now();
      const expired = client
        .appendToStream('rpc-deadline', jsonEvent({ type: 'late', data: {} }), {
          streamState: ANY,
          deadline: 600,
        })
        .then(
          (value) => {
            settlements++;
            return value;
          },
          (error: unknown) => {
            settlements++;
            throw error;
          },
        );
      const result = expired.then(
        () => null,
        (error: unknown) => error,
      );
      const peer = client.appendToStream(
        'rpc-deadline-peer',
        jsonEvent({ type: 'peer', data: {} }),
        { streamState: ANY, deadline: 5000 },
      );
      await atCommit;
      expect(enqueue.mock.calls[0][3]?.deadline).toEqual(expect.any(Number));
      const error = await Promise.race([
        result,
        new Promise<never>((_, reject) =>
          setTimeout(() => reject(new Error('deadline did not settle')), 2000),
        ),
      ]);
      expect(error).toBeInstanceOf(UnknownError);
      expect(Date.now() - started).toBeLessThan(2000);
      release();
      expect(await peer).toMatchObject({ success: true });
      commit.mockRestore();
      const types: string[] = [];
      for await (const { event } of client.readStream('rpc-deadline'))
        if (event) types.push(event.type);
      expect(types).toEqual(['late']);
      expect(settlements).toBe(1);
    } finally {
      release();
      commit.mockRestore();
      enqueue.mockRestore();
    }
  });
  it('rejects each uncertain correlation without retrying committed writes', async () => {
    observedOutcomes.length = 0;
    // eslint-disable-next-line @typescript-eslint/unbound-method
    const originalCommit = ClientSession.prototype.commitTransaction;
    const spy = jest
      .spyOn(ClientSession.prototype, 'commitTransaction')
      .mockImplementation(async function (this: ClientSession) {
        await originalCommit.call(this);
        throw new Error('injected lost commit acknowledgement');
      });
    const commits: string[] = [];
    const listener = (event: { commandName: string }) => {
      if (event.commandName === 'commitTransaction')
        commits.push(event.commandName);
    };
    mongo.on('commandStarted', listener);
    try {
      const appends = ['rpc-unknown-a', 'rpc-unknown-b'].map((stream) =>
        client.appendToStream(
          stream,
          jsonEvent({ type: 'unknown', data: {} }),
          {
            streamState: ANY,
            deadline: 500,
          },
        ),
      );
      let timeout: ReturnType<typeof setTimeout> | undefined;
      const results = await Promise.race([
        Promise.allSettled(appends),
        new Promise<never>((_, reject) => {
          timeout = setTimeout(
            () => reject(new Error('append promises remained pending')),
            2000,
          );
        }),
      ]).finally(() => clearTimeout(timeout));
      expect(observedOutcomes).toEqual(['unknown', 'unknown']);
      for (const result of results) {
        expect(result.status).toBe('rejected');
        if (result.status !== 'rejected')
          throw new Error('expected uncertainty');
        expect(result.reason).toBeInstanceOf(UnknownError);
      }
      expect(commits).toHaveLength(1);
      // Reads can use transactions too; inject only into the target commit.
      spy.mockRestore();
      expect(
        await client.appendToStream(
          'rpc-after-unknown',
          jsonEvent({ type: 'after', data: {} }),
          { streamState: ANY, deadline: 2000 },
        ),
      ).toMatchObject({ success: true });
      for (const stream of ['rpc-unknown-a', 'rpc-unknown-b']) {
        const types: string[] = [];
        for await (const { event } of client.readStream(stream))
          if (event) types.push(event.type);
        expect(types).toEqual(['unknown']);
      }
    } finally {
      spy.mockRestore();
      mongo.off('commandStarted', listener);
    }
  });
});
