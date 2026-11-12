import { randomUUID } from 'node:crypto';
import { ClientSession, Db, MongoClient } from 'mongodb';
import { GenericContainer, StartedTestContainer, Wait } from 'testcontainers';
import { AppendReq, AppendResp } from '../src/interfaces/streams';
import { MongoEventStoreService } from '../src/mongodb/mongodb-event-store.service';
import { MongoInfrastructureService } from '../src/mongodb/mongodb-infrastructure.service';
import { GroupQueue } from '../src/mongodb/group-commit';
import { runMongoGroup } from '../src/mongodb/group-runner';
import { StreamDeletedServiceError } from '../src/event-store/event-store.errors';

type Request = {
  stream: string;
  options: NonNullable<AppendReq['options']>;
  events: NonNullable<AppendReq['proposedMessage']>[];
};

function request(
  stream: string,
  expected: 'new' | number,
  types: string[],
): Request {
  return {
    stream,
    options: {
      streamIdentifier: { streamName: Buffer.from(stream) },
      ...(expected === 'new' ? { noStream: {} } : { revision: expected }),
    },
    events: types.map((type) => ({
      id: { string: randomUUID() },
      metadata: { type, 'content-type': 'application/json' },
      customMetadata: Buffer.alloc(0),
      data: Buffer.from(JSON.stringify({ type })),
    })),
  };
}

describe('isolated group runner using the adapter append helper', () => {
  jest.setTimeout(120000);
  let container: StartedTestContainer;
  let infrastructure: MongoInfrastructureService;
  let client: MongoClient;
  let db: Db;
  const adapter = new MongoEventStoreService();
  beforeAll(async () => {
    container = await new GenericContainer('mongo:7')
      .withCommand([
        'mongod',
        '--replSet',
        'rs0',
        '--bind_ip_all',
        '--setParameter',
        'enableTestCommands=1',
      ])
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
    infrastructure = new MongoInfrastructureService(uri, 'group_adapter');
    await infrastructure.connect();
    client = infrastructure.getClient();
    db = client.db('group_adapter');
  });
  afterAll(async () => {
    await infrastructure?.close();
    await container?.stop();
  });
  beforeEach(async () => {
    for (const name of ['stream_events', 'streams', 'counters'])
      await db.collection(name).deleteMany({});
  });
  const apply = (value: Request, session: ClientSession) =>
    adapter['appendInTransaction'](
      session,
      db,
      value.options,
      value.stream,
      value.events,
      null,
    );
  const queue = () =>
    new GroupQueue<Request, AppendResp>({
      maxRequests: 2,
      maxEvents: 4,
      maxBytes: 1000,
    });

  it('commits two ordered independent requests with distinct expected versions in one transaction', async () => {
    const seed = request('existing', 'new', ['seed']);
    const session = client.startSession();
    try {
      await session.withTransaction(() => apply(seed, session));
    } finally {
      await session.endSession();
    }
    const q = queue();
    const a = q.enqueue(request('existing', 0, ['a1', 'a2']), 2, 1)!;
    const b = q.enqueue(request('new', 'new', ['b']), 1, 1)!;
    const sessions: ClientSession[] = [];
    let commit: jest.SpyInstance | undefined;
    await runMongoGroup(
      client,
      q.seal(2),
      async (value, shared) => {
        sessions.push(shared);
        if (!commit) commit = jest.spyOn(shared, 'commitTransaction');
        expect(a.outcome).toBeUndefined();
        expect(b.outcome).toBeUndefined();
        return apply(value, shared);
      },
      { maxAttempts: 1, maxCommitAttempts: 1 },
    );
    expect(sessions).toHaveLength(2);
    expect(sessions[0]).toBe(sessions[1]);
    expect(commit).toHaveBeenCalledTimes(1);
    expect(a.outcome).toMatchObject({
      kind: 'committed',
      value: {
        success: { currentRevision: 2, position: { commitPosition: 3 } },
      },
    });
    expect(b.outcome).toMatchObject({
      kind: 'committed',
      value: {
        success: { currentRevision: 0, position: { commitPosition: 4 } },
      },
    });
    const rows = await db
      .collection<{
        streamName: string;
        revision: number;
        position: number;
        metadata: { type: string };
      }>('stream_events')
      .find({ streamName: { $in: ['existing', 'new'] } })
      .sort({ position: 1 })
      .toArray();
    expect(
      rows.map((r) => [r.streamName, r.revision, r.position, r.metadata.type]),
    ).toEqual([
      ['existing', 0, 1, 'seed'],
      ['existing', 1, 2, 'a1'],
      ['existing', 2, 3, 'a2'],
      ['new', 0, 4, 'b'],
    ]);
  });

  it('applies expected version independently when one member is stale', async () => {
    const seed = request('existing', 'new', ['seed', 'seed2', 'seed3']);
    const session = client.startSession();
    try {
      await session.withTransaction(() => apply(seed, session));
    } finally {
      await session.endSession();
    }
    const q = queue();
    const a = q.enqueue(request('existing', 0, ['stale']), 1, 1)!;
    const b = q.enqueue(request('fresh', 'new', ['fresh']), 1, 1)!;
    await runMongoGroup(client, q.seal(2), apply, {
      maxAttempts: 1,
      maxCommitAttempts: 1,
    });
    expect(a.outcome).toMatchObject({
      kind: 'committed',
      value: {
        wrongExpectedVersion: { currentRevision: '2', expectedRevision: '0' },
      },
    });
    expect(b.outcome).toMatchObject({
      kind: 'committed',
      value: { success: { currentRevision: 0 } },
    });
    expect(
      await db
        .collection('stream_events')
        .countDocuments({ 'metadata.type': 'stale' }),
    ).toBe(0);
    expect(
      await db
        .collection('stream_events')
        .countDocuments({ streamName: 'fresh' }),
    ).toBe(1);
  });

  it('isolates a tombstoned member while committing a healthy peer', async () => {
    await db.collection('streams').insertOne({
      _id: 'deleted' as never,
      revision: 0,
      tombstoned: true,
    });
    type Result =
      | { kind: 'result'; value: AppendResp }
      | { kind: 'error'; error: StreamDeletedServiceError };
    const q = new GroupQueue<Request, Result>({
      maxRequests: 2,
      maxEvents: 4,
      maxBytes: 1000,
    });
    const healthy = q.enqueue(request('healthy', 'new', ['healthy']), 1, 1)!;
    const deleted = q.enqueue(request('deleted', 'new', ['deleted']), 1, 1)!;
    const groupApply = (value: Request, session: ClientSession) =>
      adapter['appendGroupMember'](
        session,
        db,
        value.options,
        value.stream,
        value.events,
      );
    await runMongoGroup(
      client,
      q.seal(2),
      async (value, session) => {
        expect(healthy.outcome).toBeUndefined();
        expect(deleted.outcome).toBeUndefined();
        return groupApply(value, session);
      },
      { maxAttempts: 1, maxCommitAttempts: 1 },
    );
    expect(healthy.outcome).toMatchObject({
      kind: 'committed',
      value: { kind: 'result', value: { success: { currentRevision: 0 } } },
    });
    expect(deleted.outcome).toMatchObject({
      kind: 'committed',
      value: { kind: 'error' },
    });
    if (
      deleted.outcome?.kind !== 'committed' ||
      deleted.outcome.value.kind !== 'error'
    )
      throw new Error('expected tombstone result');
    expect(deleted.outcome.value.error).toBeInstanceOf(
      StreamDeletedServiceError,
    );
    expect(await db.collection('stream_events').countDocuments()).toBe(1);
    expect(
      await db.collection('streams').findOne({ _id: 'healthy' } as never),
    ).not.toBeNull();
    expect(q.activeGroups).toBe(0);
    expect(q.retainedBytes).toBe(0);
  });

  it.each(['abort', 'unknown'] as const)(
    'does not publish staged tombstone or success on %s',
    async (failure) => {
      await db.collection('streams').insertOne({
        _id: 'deleted' as never,
        revision: 0,
        tombstoned: true,
      });
      type Result = Awaited<ReturnType<(typeof adapter)['appendGroupMember']>>;
      const q = new GroupQueue<Request, Result>({
        maxRequests: 2,
        maxEvents: 2,
        maxBytes: 2,
      });
      const deleted = q.enqueue(request('deleted', 'new', ['d']), 1, 1)!;
      const healthy = q.enqueue(request('healthy', 'new', ['h']), 1, 1)!;
      await expect(
        runMongoGroup(
          client,
          q.seal(2),
          async (value, session) => {
            const result = await adapter['appendGroupMember'](
              session,
              db,
              value.options,
              value.stream,
              value.events,
            );
            expect(deleted.outcome).toBeUndefined();
            expect(healthy.outcome).toBeUndefined();
            if (value.stream === 'healthy') {
              if (failure === 'abort') throw new Error('injected work failure');
              const commit = session.commitTransaction.bind(
                session,
              ) as () => Promise<void>;
              jest
                .spyOn(session, 'commitTransaction')
                .mockImplementation(async () => {
                  await commit();
                  throw new Error('injected lost acknowledgement');
                });
            }
            return result;
          },
          { maxAttempts: 1, maxCommitAttempts: 1 },
        ),
      ).rejects.toMatchObject({
        outcome: failure === 'abort' ? 'aborted' : 'unknown',
      });
      const outcome = { kind: failure === 'abort' ? 'aborted' : 'unknown' };
      expect(deleted.outcome).toEqual(outcome);
      expect(healthy.outcome).toEqual(outcome);
      expect(
        await db
          .collection('stream_events')
          .countDocuments({ streamName: 'healthy' }),
      ).toBe(failure === 'abort' ? 0 : 1);
    },
  );

  it('does not swallow a real Mongo write failure after a tombstone', async () => {
    await db
      .collection('streams')
      .insertOne({ _id: 'deleted' as never, revision: 0, tombstoned: true });
    type Result = Awaited<ReturnType<(typeof adapter)['appendGroupMember']>>;
    const q = new GroupQueue<Request, Result>({
      maxRequests: 2,
      maxEvents: 2,
      maxBytes: 2,
    });
    const deleted = q.enqueue(request('deleted', 'new', ['d']), 1, 1)!;
    const healthy = q.enqueue(request('healthy', 'new', ['h']), 1, 1)!;
    await db.admin().command({
      configureFailPoint: 'failCommand',
      mode: { times: 1 },
      data: {
        failCommands: ['insert'],
        errorCode: 112,
        errorLabels: ['TransientTransactionError'],
      },
    });
    try {
      await expect(
        runMongoGroup(
          client,
          q.seal(2),
          (value, session) =>
            adapter['appendGroupMember'](
              session,
              db,
              value.options,
              value.stream,
              value.events,
            ),
          { maxAttempts: 1, maxCommitAttempts: 1 },
        ),
      ).rejects.toMatchObject({ outcome: 'aborted' });
      expect(deleted.outcome).toEqual({ kind: 'aborted' });
      expect(healthy.outcome).toEqual({ kind: 'aborted' });
      expect(await db.collection('stream_events').countDocuments()).toBe(0);
    } finally {
      await db
        .admin()
        .command({ configureFailPoint: 'failCommand', mode: 'off' });
    }
  });

  it('rolls back staged adapter writes when a later member throws', async () => {
    const before = await db
      .collection('counters')
      .findOne({ _id: 'global' } as never);
    const q = queue();
    const a = q.enqueue(request('rollback-a', 'new', ['a']), 1, 1)!;
    const b = q.enqueue(request('rollback-b', 'new', ['b']), 1, 1)!;
    await expect(
      runMongoGroup(
        client,
        q.seal(2),
        async (value, session) => {
          const result = await apply(value, session);
          if (value.stream === 'rollback-b')
            throw new Error('injected after both appends');
          return result;
        },
        { maxAttempts: 1, maxCommitAttempts: 1 },
      ),
    ).rejects.toMatchObject({ outcome: 'aborted' });
    expect(a.outcome).toEqual({ kind: 'aborted' });
    expect(b.outcome).toEqual({ kind: 'aborted' });
    expect(
      await db
        .collection('stream_events')
        .countDocuments({ streamName: { $in: ['rollback-a', 'rollback-b'] } }),
    ).toBe(0);
    expect(
      await db.collection('streams').countDocuments({
        _id: { $in: ['rollback-a', 'rollback-b'] },
      } as never),
    ).toBe(0);
    expect(
      await db.collection('counters').findOne({ _id: 'global' } as never),
    ).toEqual(before);
  });
});
