import { randomUUID } from 'node:crypto';
import { ClientSession, Db, MongoClient } from 'mongodb';
import { GenericContainer, StartedTestContainer, Wait } from 'testcontainers';
import { Batch } from '../src/mongodb/group-commit';
import { CooperativeDispatcher } from '../src/mongodb/cooperative-dispatcher';
import { TransactionAdmission } from '../src/mongodb/transaction-admission';
import { runMongoGroup } from '../src/mongodb/group-runner';
import { MongoEventStoreService } from '../src/mongodb/mongodb-event-store.service';
import { MongoInfrastructureService } from '../src/mongodb/mongodb-infrastructure.service';
import { AppendReq } from '../src/interfaces/streams';

type Request = {
  stream: string;
  options: NonNullable<AppendReq['options']>;
  events: NonNullable<AppendReq['proposedMessage']>[];
};
type Result = Awaited<ReturnType<MongoEventStoreService['appendGroupMember']>>;
function request(stream: string): Request {
  return {
    stream,
    options: {
      streamIdentifier: { streamName: Buffer.from(stream) },
      noStream: {},
    },
    events: [
      {
        id: { string: randomUUID() },
        metadata: { type: stream, 'content-type': 'application/json' },
        customMetadata: Buffer.alloc(0),
        data: Buffer.from('{}'),
      },
    ],
  };
}
function gate() {
  let release!: () => void;
  const promise = new Promise<void>((resolve) => {
    release = resolve;
  });
  return { promise, release };
}

async function runBorrowed(
  client: MongoClient,
  batch: Batch<Request, Result>,
  apply: (value: Request, session: ClientSession) => Promise<Result>,
): Promise<void> {
  // The dispatcher owns the real hold. The coordinator's second hold is
  // replaced only in this test harness; its release is a no-op.
  const borrowed = new Proxy(batch, {
    get(target, property) {
      if (property === 'retain') return () => () => undefined;
      const value: unknown = Reflect.get(target, property, target);
      return (
        typeof value === 'function' ? value.bind(target) : value
      ) as unknown;
    },
  });
  let cleaned = false;
  const borrowedClient = new Proxy(client, {
    get(target, property) {
      if (property === 'startSession')
        return () => {
          const session = target.startSession();
          const end = session.endSession.bind(session) as () => Promise<void>;
          session.endSession = async () => {
            await end();
            cleaned = true;
          };
          return session;
        };
      const value: unknown = Reflect.get(target, property, target);
      return (
        typeof value === 'function' ? value.bind(target) : value
      ) as unknown;
    },
  });
  try {
    await runMongoGroup(borrowedClient, borrowed, apply, {
      maxAttempts: 1,
      maxCommitAttempts: 1,
    });
  } catch (error) {
    if (!cleaned) throw error;
  }
  if (!cleaned) throw new Error('session cleanup unconfirmed');
}

describe('cooperative dispatcher Mongo integration', () => {
  jest.setTimeout(120000);
  let container: StartedTestContainer;
  let infrastructure: MongoInfrastructureService;
  let client: MongoClient;
  let db: Db;
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
    infrastructure = new MongoInfrastructureService(
      uri,
      'cooperative_integration',
    );
    await infrastructure.connect();
    client = new MongoClient(uri);
    await client.connect();
    db = client.db('cooperative_integration');
  });
  beforeEach(async () => {
    for (const name of ['stream_events', 'streams', 'counters'])
      await db.collection(name).deleteMany({});
  });
  afterAll(async () => {
    await client?.close();
    await infrastructure?.close();
    await container?.stop();
  });
  it('holds real admission through session cleanup and excludes pre-start cancellation', async () => {
    const mutex = new TransactionAdmission(3, 3);
    const blocker = await mutex.acquire(1);
    const entered = gate();
    const cleaning = gate();
    const finishCleanup = gate();
    const adapter = new MongoEventStoreService();
    const gatedClient = new Proxy(client, {
      get(target, property) {
        if (property === 'startSession')
          return () => {
            const session = target.startSession();
            const end = session.endSession.bind(session) as () => Promise<void>;
            session.endSession = async () => {
              cleaning.release();
              await finishCleanup.promise;
              await end();
            };
            return session;
          };
        const value: unknown = Reflect.get(target, property, target);
        return typeof value === 'function'
          ? (value.bind(target) as unknown)
          : value;
      },
    });
    const dispatcher = new CooperativeDispatcher<Request, Result>(
      { maxRequests: 2, maxEvents: 2, maxBytes: 2 },
      0,
      (value) => value.stream,
      async (finalize) => {
        entered.release();
        const release = await mutex.acquire(1);
        const batch = finalize();
        if (!batch) {
          release();
          return;
        }
        await runBorrowed(gatedClient, batch, (value, session) =>
          adapter['appendGroupMember'](
            session,
            db,
            value.options,
            value.stream,
            value.events,
          ),
        );
        release();
      },
    );
    const controller = new AbortController();
    const cancelled = dispatcher.enqueue(request('mutex-cancelled'), 1, 1, {
      signal: controller.signal,
    });
    const rejection = expect(cancelled).rejects.toMatchObject({
      code: 'cancelled',
    });
    await entered.promise;
    const peer = dispatcher.enqueue(request('mutex-peer'), 1, 1);
    controller.abort();
    await rejection;
    blocker();
    try {
      await cleaning.promise;
      let acquired = false;
      const contender = mutex.acquire(1).then((release) => {
        acquired = true;
        return release;
      });
      await Promise.resolve();
      expect(acquired).toBe(false);
      finishCleanup.release();
      (await contender)();
      await peer;
    } finally {
      finishCleanup.release();
    }
    const rows = await db
      .collection<{ streamName: string }>('stream_events')
      .find()
      .toArray();
    expect(rows.map((row) => row.streamName)).toEqual(['mutex-peer']);
    await dispatcher.shutdown(1000);
    await mutex.shutdown();
  });
  it('excludes individually cancelled members while admission waits and commits peers at contiguous positions', async () => {
    const admission = gate();
    const entered = gate();
    const adapter = new MongoEventStoreService();
    const dispatcher = new CooperativeDispatcher<Request, Result>(
      { maxRequests: 4, maxEvents: 4, maxBytes: 4 },
      0,
      (value) => value.stream,
      async (finalize) => {
        entered.release();
        await admission.promise;
        const batch = finalize();
        if (!batch) return;
        await runBorrowed(client, batch, (value, session) =>
          adapter['appendGroupMember'](
            session,
            db,
            value.options,
            value.stream,
            value.events,
          ),
        );
      },
    );
    const firstController = new AbortController();
    const secondController = new AbortController();
    const first = dispatcher.enqueue(request('cancel-a'), 1, 1, {
      signal: firstController.signal,
    });
    const firstRejected = expect(first).rejects.toMatchObject({
      code: 'cancelled',
    });
    await entered.promise;
    const a = dispatcher.enqueue(request('peer-a'), 1, 1);
    const second = dispatcher.enqueue(request('cancel-b'), 1, 1, {
      signal: secondController.signal,
    });
    const secondRejected = expect(second).rejects.toMatchObject({
      code: 'cancelled',
    });
    const b = dispatcher.enqueue(request('peer-b'), 1, 1);
    firstController.abort();
    await firstRejected;
    secondController.abort();
    await secondRejected;
    admission.release();
    const results = await Promise.all([a, b]);
    expect(
      results.map((r) =>
        r.kind === 'result'
          ? r.value.success?.position?.commitPosition
          : undefined,
      ),
    ).toEqual([1, 2]);
    const rows = await db
      .collection<{ streamName: string; position: number }>('stream_events')
      .find()
      .sort({ position: 1 })
      .toArray();
    expect(rows.map((r) => [r.streamName, r.position])).toEqual([
      ['peer-a', 1],
      ['peer-b', 2],
    ]);
    await dispatcher.shutdown(1000);
  });
  it('keeps a post-start cancellation unknown while a peer commits and the session is cleaned', async () => {
    const started = gate();
    const work = gate();
    const adapter = new MongoEventStoreService();
    const controller = new AbortController();
    let ended = false;
    const dispatcher = new CooperativeDispatcher<Request, Result>(
      { maxRequests: 2, maxEvents: 2, maxBytes: 2 },
      0,
      (value) => value.stream,
      async (finalize) => {
        const batch = finalize();
        if (!batch) return;
        await runBorrowed(client, batch, async (value, session) => {
          session.once('ended', () => {
            ended = true;
          });
          started.release();
          await work.promise;
          return adapter['appendGroupMember'](
            session,
            db,
            value.options,
            value.stream,
            value.events,
          );
        });
      },
    );
    const cancelled = dispatcher.enqueue(request('started'), 1, 1, {
      signal: controller.signal,
    });
    const rejection = expect(cancelled).rejects.toMatchObject({
      code: 'unknown',
    });
    const peer = dispatcher.enqueue(request('after-start'), 1, 1);
    try {
      await Promise.race([
        started.promise,
        peer.then(() => {
          throw new Error('peer settled before transaction');
        }),
      ]);
      controller.abort();
      await rejection;
    } finally {
      work.release();
    }
    expect(await peer).toMatchObject({
      kind: 'result',
      value: { success: { position: { commitPosition: 2 } } },
    });
    expect(ended).toBe(true);
    const rows = await db
      .collection<{ streamName: string; position: number }>('stream_events')
      .find()
      .sort({ position: 1 })
      .toArray();
    expect(rows.map((r) => [r.streamName, r.position])).toEqual([
      ['started', 1],
      ['after-start', 2],
    ]);
    await dispatcher.shutdown(1000);
  });
});
