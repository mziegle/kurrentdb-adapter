import { randomUUID } from 'node:crypto';
import { ClientSession, Db, MongoClient } from 'mongodb';
import { GenericContainer, StartedTestContainer, Wait } from 'testcontainers';
import { AppendReq } from '../src/interfaces/streams';
import { MongoEventStoreService } from '../src/mongodb/mongodb-event-store.service';
import { MongoInfrastructureService } from '../src/mongodb/mongodb-infrastructure.service';
import { GroupDispatcher } from '../src/mongodb/group-dispatcher';
import { runMongoGroup } from '../src/mongodb/group-runner';
import { StreamDeletedServiceError } from '../src/event-store/event-store.errors';

type Request = {
  stream: string;
  options: NonNullable<AppendReq['options']>;
  events: NonNullable<AppendReq['proposedMessage']>[];
};
type Result = Awaited<ReturnType<MongoEventStoreService['appendGroupMember']>>;
function request(stream: string, type: string): Request {
  return {
    stream,
    options: {
      streamIdentifier: { streamName: Buffer.from(stream) },
      noStream: {},
    },
    events: [
      {
        id: { string: randomUUID() },
        metadata: { type, 'content-type': 'application/json' },
        customMetadata: Buffer.alloc(0),
        data: Buffer.from('{}'),
      },
    ],
  };
}
function harness(
  client: MongoClient,
  db: Db,
  applyHook?: (value: Request, session: ClientSession) => Promise<void>,
): GroupDispatcher<Request, Result> {
  const adapter = new MongoEventStoreService();
  return new GroupDispatcher(
    { maxRequests: 2, maxEvents: 10, maxBytes: 1000 },
    100,
    (value) => value.stream,
    (batch) =>
      runMongoGroup(
        client,
        batch,
        async (value, session) => {
          await applyHook?.(value, session);
          return adapter['appendGroupMember'](
            session,
            db,
            value.options,
            value.stream,
            value.events,
          );
        },
        { maxAttempts: 1, maxCommitAttempts: 1 },
      ),
  );
}

describe('dispatcher with real Mongo group runner and adapter', () => {
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
    infrastructure = new MongoInfrastructureService(uri, 'group_dispatcher');
    await infrastructure.connect();
    client = new MongoClient(uri, { monitorCommands: true });
    await client.connect();
    db = client.db('group_dispatcher');
  });
  afterAll(async () => {
    await client?.close();
    await infrastructure?.close();
    await container?.stop();
  });
  beforeEach(async () => {
    for (const name of ['stream_events', 'streams', 'counters'])
      await db.collection(name).deleteMany({});
  });

  it('coalesces concurrent appends in one ordered majority journal transaction', async () => {
    const sessions: ClientSession[] = [];
    const commits: Record<string, unknown>[] = [];
    const listener = (event: {
      commandName: string;
      command: Record<string, unknown>;
    }) => {
      if (event.commandName === 'commitTransaction')
        commits.push(event.command);
    };
    client.on('commandStarted', listener);
    const dispatcher = harness(client, db, (_value, session) => {
      sessions.push(session);
      return Promise.resolve();
    });
    const [a, b] = await Promise.all([
      dispatcher.enqueue(request('a', 'a'), 1, 1),
      dispatcher.enqueue(request('b', 'b'), 1, 1),
    ]);
    expect(a).toMatchObject({
      kind: 'result',
      value: {
        success: { currentRevision: 0, position: { commitPosition: 1 } },
      },
    });
    expect(b).toMatchObject({
      kind: 'result',
      value: {
        success: { currentRevision: 0, position: { commitPosition: 2 } },
      },
    });
    expect(sessions).toHaveLength(2);
    expect(sessions[0]).toBe(sessions[1]);
    expect(commits).toHaveLength(1);
    expect(commits[0].writeConcern).toMatchObject({ w: 'majority', j: true });
    client.off('commandStarted', listener);
    const rows = await db
      .collection<{ streamName: string; position: number }>('stream_events')
      .find()
      .sort({ position: 1 })
      .toArray();
    expect(rows.map((r) => [r.streamName, r.position])).toEqual([
      ['a', 1],
      ['b', 2],
    ]);
    await dispatcher.shutdown(1000);
  });

  it('returns wrong expected version independently of a healthy member', async () => {
    const seed = harness(client, db);
    await seed.enqueue(request('existing', 'seed'), 1, 1);
    await seed.shutdown(1000);
    const dispatcher = harness(client, db);
    const [stale, healthy] = await Promise.all([
      dispatcher.enqueue(request('existing', 'stale'), 1, 1),
      dispatcher.enqueue(request('fresh', 'fresh'), 1, 1),
    ]);
    expect(stale).toMatchObject({
      kind: 'result',
      value: { wrongExpectedVersion: { currentRevision: '0' } },
    });
    expect(healthy).toMatchObject({
      kind: 'result',
      value: { success: { currentRevision: 0 } },
    });
    expect(
      await db
        .collection('stream_events')
        .countDocuments({ streamName: 'existing' }),
    ).toBe(1);
    await dispatcher.shutdown(1000);
  });

  it('commits healthy member while reporting a tombstone', async () => {
    await db
      .collection('streams')
      .insertOne({ _id: 'deleted' as never, revision: 0, tombstoned: true });
    const dispatcher = harness(client, db);
    const [deleted, healthy] = await Promise.all([
      dispatcher.enqueue(request('deleted', 'd'), 1, 1),
      dispatcher.enqueue(request('healthy', 'h'), 1, 1),
    ]);
    expect(deleted).toMatchObject({
      kind: 'error',
      error: expect.any(StreamDeletedServiceError) as StreamDeletedServiceError,
    });
    expect(healthy).toMatchObject({
      kind: 'result',
      value: { success: { currentRevision: 0 } },
    });
    expect(
      await db
        .collection('stream_events')
        .countDocuments({ streamName: 'deleted' }),
    ).toBe(0);
    expect(
      await db
        .collection('stream_events')
        .countDocuments({ streamName: 'healthy' }),
    ).toBe(1);
    await dispatcher.shutdown(1000);
  });

  it('cancels queued and deadline-expired work, then shuts down without dispatching', async () => {
    const dispatcher = harness(client, db);
    const controller = new AbortController();
    const cancelled = dispatcher.enqueue(request('cancelled', 'c'), 1, 1, {
      signal: controller.signal,
    });
    controller.abort();
    await expect(cancelled).rejects.toMatchObject({ code: 'cancelled' });
    const expired = dispatcher.enqueue(request('expired', 'e'), 1, 1, {
      signal: AbortSignal.timeout(5),
    });
    await expect(expired).rejects.toMatchObject({ code: 'cancelled' });
    await dispatcher.shutdown(1000);
    await expect(
      dispatcher.enqueue(request('closed', 'x'), 1, 1),
    ).rejects.toMatchObject({ code: 'closed' });
    expect(await db.collection('stream_events').countDocuments()).toBe(0);
  });

  it('settles cancellation after sealing as unknown and drains on shutdown', async () => {
    let enter!: () => void;
    let release!: () => void;
    const entered = new Promise<void>((resolve) => {
      enter = resolve;
    });
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const dispatcher = harness(client, db, async () => {
      enter();
      await gate;
    });
    const controller = new AbortController();
    const first = dispatcher.enqueue(request('first', 'a'), 1, 1, {
      signal: controller.signal,
    });
    const second = dispatcher.enqueue(request('second', 'b'), 1, 1);
    await entered;
    const firstRejected = expect(first).rejects.toMatchObject({
      code: 'unknown',
    });
    controller.abort();
    await firstRejected;
    const closing = dispatcher.shutdown(5000);
    release();
    const b = await second;
    await closing;
    expect(b).toMatchObject({
      kind: 'result',
      value: { success: { currentRevision: 0 } },
    });
    expect(await db.collection('stream_events').countDocuments()).toBe(2);
  });
});
