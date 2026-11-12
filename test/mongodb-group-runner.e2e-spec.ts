import { EventEmitter } from 'events';
import { GenericContainer, StartedTestContainer, Wait } from 'testcontainers';
import { ClientSession, Db, MongoClient, MongoError } from 'mongodb';
import { GroupQueue } from '../src/mongodb/group-commit';
import {
  isDefinitiveGroupAbort,
  runMongoGroup,
} from '../src/mongodb/group-runner';

describe('isolated Mongo group runner', () => {
  jest.setTimeout(120000);
  let container: StartedTestContainer;
  let client: MongoClient;
  let db: Db;
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
    client = new MongoClient(
      `mongodb://${container.getHost()}:${container.getMappedPort(27017)}/?directConnection=true`,
      { monitorCommands: true },
    );
    await client.connect();
    db = client.db('runner');
  });
  afterAll(async () => {
    await client?.close();
    await container?.stop();
  });
  it('ignores forged abort events with mismatched connection identity', async () => {
    const queue = new GroupQueue<string, string>({
      maxRequests: 1,
      maxEvents: 1,
      maxBytes: 1,
    });
    queue.enqueue('forged', 1, 1);
    const batch = queue.seal(1);
    const error: unknown = await runMongoGroup(
      client,
      batch,
      async (value, session) => {
        await db.collection('forged_abort').insertOne({ value }, { session });
        const abort = session.abortTransaction.bind(
          session,
        ) as () => Promise<void>;
        jest.spyOn(session, 'abortTransaction').mockImplementation(async () => {
          EventEmitter.prototype.emit.call(client, 'commandStarted', {
            commandName: 'abortTransaction',
            address: 'fake',
            connectionId: 1,
            requestId: 123,
            command: {
              lsid: session.id,
              txnNumber: session.serverSession.txnNumber,
            },
          });
          EventEmitter.prototype.emit.call(client, 'commandSucceeded', {
            commandName: 'abortTransaction',
            address: 'fake',
            connectionId: 2,
            requestId: 123,
            reply: { ok: 1 },
          });
          await abort();
        });
        throw new Error('work failed');
      },
      { maxAttempts: 1, maxCommitAttempts: 1 },
    ).catch((e: unknown) => e);
    expect(isDefinitiveGroupAbort(error, batch)).toBe(false);
    expect(await db.collection('forged_abort').countDocuments()).toBe(0);
    expect(queue.activeGroups).toBe(0);
  });
  it('does not certify a driver-swallowed abort failure', async () => {
    const queue = new GroupQueue<string, string>({
      maxRequests: 1,
      maxEvents: 1,
      maxBytes: 1,
    });
    queue.enqueue('rollback-failed', 1, 1);
    const batch = queue.seal(1);
    await db.admin().command({
      configureFailPoint: 'failCommand',
      mode: { times: 1 },
      data: { failCommands: ['abortTransaction'], errorCode: 2 },
    });
    try {
      const error: unknown = await runMongoGroup(
        client,
        batch,
        async (value, session) => {
          await db
            .collection('abort_failure')
            .insertOne({ value }, { session });
          throw new Error('work failed');
        },
        { maxAttempts: 1, maxCommitAttempts: 1 },
      ).catch((e: unknown) => e);
      expect(isDefinitiveGroupAbort(error, batch)).toBe(false);
    } finally {
      await db
        .admin()
        .command({ configureFailPoint: 'failCommand', mode: 'off' });
    }
  });
  it('does not certify an acknowledged abort when shutdown times out before cleanup', async () => {
    const queue = new GroupQueue<string, string>({
      maxRequests: 1,
      maxEvents: 1,
      maxBytes: 1,
    });
    queue.enqueue('shutdown-abort', 1, 1);
    const batch = queue.seal(1);
    let enter!: () => void;
    let release!: () => void;
    const ready = new Promise<void>((resolve) => {
      enter = resolve;
    });
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const running = runMongoGroup(
      client,
      batch,
      async (value, session) => {
        const abort = session.abortTransaction.bind(
          session,
        ) as () => Promise<void>;
        jest.spyOn(session, 'abortTransaction').mockImplementation(async () => {
          await abort();
          enter();
          await gate;
        });
        await db.collection('abort_shutdown').insertOne({ value }, { session });
        throw new Error('work failed');
      },
      { maxAttempts: 1, maxCommitAttempts: 1 },
    ).catch((e: unknown) => e);
    try {
      await ready;
      expect(queue.activeGroups).toBe(1);
      expect(queue.retainedBytes).toBe(1);
      await queue.shutdown(0);
      expect(queue.activeGroups).toBe(1);
      expect(queue.retainedBytes).toBe(1);
    } finally {
      release();
    }
    const error = await running;
    expect(isDefinitiveGroupAbort(error, batch)).toBe(false);
    expect(queue.activeGroups).toBe(0);
  });
  it('does not authorize replay even after a real acknowledged abort', async () => {
    const queue = new GroupQueue<string, string>({
      maxRequests: 1,
      maxEvents: 1,
      maxBytes: 1,
    });
    queue.enqueue('rollback', 1, 1);
    const batch = queue.seal(1);
    const commands: string[] = [];
    const listener = (event: { commandName: string }) => {
      commands.push(event.commandName);
    };
    client.on('commandSucceeded', listener);
    const collection = db.collection('definitive_abort');
    try {
      const error: unknown = await runMongoGroup(
        client,
        batch,
        async (value, session) => {
          await collection.insertOne({ value }, { session });
          throw new Error('work failed');
        },
        { maxAttempts: 1, maxCommitAttempts: 1 },
      ).catch((e: unknown) => e);
      expect(isDefinitiveGroupAbort(error, batch)).toBe(false);
      expect(isDefinitiveGroupAbort(error, {})).toBe(false);
      expect(isDefinitiveGroupAbort({ outcome: 'aborted' }, batch)).toBe(false);
      expect(commands).toContain('abortTransaction');
      expect(commands).not.toContain('commitTransaction');
      expect(await collection.countDocuments()).toBe(0);
      expect(queue.activeGroups).toBe(0);
    } finally {
      client.off('commandSucceeded', listener);
    }
  });
  it('does not replay when the server rejects retry abort', async () => {
    const queue = new GroupQueue<string, string>({
      maxRequests: 2,
      maxEvents: 2,
      maxBytes: 2,
    });
    const a = queue.enqueue('abort-a', 1, 1)!;
    const b = queue.enqueue('abort-b', 1, 1)!;
    const collection = db.collection('retry_abort_rejected');
    let calls = 0;
    let abortFailures = 0;
    const failed = (event: { commandName: string }) => {
      if (event.commandName === 'abortTransaction') abortFailures++;
    };
    client.on('commandFailed', failed);
    try {
      const result: unknown = await runMongoGroup(
        client,
        queue.seal(2),
        async (value, session) => {
          calls++;
          if (calls === 2) {
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
              await collection.insertOne({ value }, { session });
            } catch (error) {
              await db.admin().command({
                configureFailPoint: 'failCommand',
                mode: { times: 1 },
                data: { failCommands: ['abortTransaction'], errorCode: 2 },
              });
              throw error;
            }
          } else await collection.insertOne({ value }, { session });
          return value;
        },
        { maxAttempts: 2, maxCommitAttempts: 1 },
      ).catch((error: unknown) => error);
      expect(abortFailures).toBe(1);
      expect(result).toMatchObject({ outcome: 'aborted' });
      expect(calls).toBe(2);
      expect(a.outcome).toEqual({ kind: 'aborted' });
      expect(b.outcome).toEqual({ kind: 'aborted' });
      expect(await collection.countDocuments()).toBe(0);
    } finally {
      client.off('commandFailed', failed);
      await db
        .admin()
        .command({ configureFailPoint: 'failCommand', mode: 'off' });
    }
  });
  it('does not restage after a transient write even when cleanup can succeed', async () => {
    const queue = new GroupQueue<string, string>({
      maxRequests: 2,
      maxEvents: 2,
      maxBytes: 2,
    });
    const a = queue.enqueue('retry-a', 1, 1)!;
    const b = queue.enqueue('retry-b', 1, 1)!;
    const collection = db.collection('restage_events');
    const commands: { lsid: unknown; txnNumber: { toString(): string } }[] = [];
    const monitor = (event: {
      commandName: string;
      command: { lsid: unknown; txnNumber: { toString(): string } };
    }) => {
      if (event.commandName === 'insert')
        commands.push({
          lsid: event.command.lsid,
          txnNumber: event.command.txnNumber,
        });
    };
    client.on('commandStarted', monitor);
    let calls = 0;
    try {
      await expect(
        runMongoGroup(
          client,
          queue.seal(2),
          async (value, session) => {
            calls++;
            if (calls === 2)
              await db.admin().command({
                configureFailPoint: 'failCommand',
                mode: { times: 1 },
                data: {
                  failCommands: ['insert'],
                  errorCode: 112,
                  errorLabels: ['TransientTransactionError'],
                },
              });
            await collection.insertOne({ value }, { session });
            return `${value}-${calls}`;
          },
          { maxAttempts: 2, maxCommitAttempts: 1 },
        ),
      ).rejects.toMatchObject({ outcome: 'aborted' });
      expect(calls).toBe(2);
      expect(commands).toHaveLength(2);
      expect(commands.map((command) => command.lsid)).toEqual(
        Array(2).fill(commands[0].lsid),
      );
      expect(commands[0].txnNumber.toString()).toBe(
        commands[1].txnNumber.toString(),
      );
      expect(a.outcome).toEqual({ kind: 'aborted' });
      expect(b.outcome).toEqual({ kind: 'aborted' });
      expect(
        await collection
          .find({}, { projection: { _id: 0 } })
          .sort({ value: 1 })
          .toArray(),
      ).toEqual([]);
    } finally {
      client.off('commandStarted', monitor);
      await db
        .admin()
        .command({ configureFailPoint: 'failCommand', mode: 'off' });
    }
  });
  it('does not replay after a labelled transient write failure with one attempt', async () => {
    const queue = new GroupQueue<string, number>({
      maxRequests: 2,
      maxEvents: 2,
      maxBytes: 2,
    });
    const a = queue.enqueue('retry-a', 1, 1)!;
    const b = queue.enqueue('retry-b', 1, 1)!;
    let calls = 0;
    await db.admin().command({
      configureFailPoint: 'failCommand',
      mode: { times: 1 },
      data: {
        failCommands: ['insert'],
        errorCode: 112,
        errorLabels: ['TransientTransactionError'],
      },
    });
    await expect(
      runMongoGroup(
        client,
        queue.seal(2),
        async (value, session) => {
          calls++;
          await db.collection('retry_events').insertOne({ value }, { session });
          return calls;
        },
        { maxAttempts: 1, maxCommitAttempts: 2 },
      ),
    ).rejects.toMatchObject({ outcome: 'aborted' });
    expect(calls).toBe(1);
    expect(a.outcome).toEqual({ kind: 'aborted' });
    expect(b.outcome).toEqual({ kind: 'aborted' });
    expect(await db.collection('retry_events').countDocuments()).toBe(0);
  });

  it('marks exhausted unknown commit results uncertain without replaying writes', async () => {
    const queue = new GroupQueue<string, string>({
      maxRequests: 1,
      maxEvents: 1,
      maxBytes: 1,
    });
    const member = queue.enqueue('unknown', 1, 1)!;
    let calls = 0;
    await db.admin().command({
      configureFailPoint: 'failCommand',
      mode: { times: 10 },
      data: {
        failCommands: ['commitTransaction'],
        errorCode: 91,
        errorLabels: ['UnknownTransactionCommitResult'],
      },
    });
    try {
      await expect(
        runMongoGroup(
          client,
          queue.seal(1),
          async (value, session) => {
            calls++;
            await db
              .collection('uncertain_events')
              .insertOne({ value }, { session });
            return value;
          },
          { maxAttempts: 2, maxCommitAttempts: 1 },
        ),
      ).rejects.toMatchObject({ outcome: 'unknown' });
      expect(member.outcome).toEqual({ kind: 'unknown' });
      expect(calls).toBe(1);
    } finally {
      await db
        .admin()
        .command({ configureFailPoint: 'failCommand', mode: 'off' });
    }
  });

  it.each([false, true])(
    'reconciles a real committed transaction with lost replies (exhausted=%s)',
    async (exhausted) => {
      const queue = new GroupQueue<string, string>({
        maxRequests: 2,
        maxEvents: 2,
        maxBytes: 2,
      });
      const a = queue.enqueue('lost-a', 1, 1)!;
      const b = queue.enqueue('lost-b', 1, 1)!;
      const collection = db.collection(`lost_reply_${exhausted}`);
      const commands: { lsid: unknown; txnNumber: unknown }[] = [];
      const monitor = (event: {
        commandName: string;
        command: { lsid?: unknown; txnNumber?: unknown };
      }) => {
        if (event.commandName === 'commitTransaction')
          commands.push({
            lsid: event.command.lsid,
            txnNumber: event.command.txnNumber,
          });
      };
      client.on('commandStarted', monitor);
      let calls = 0;
      let commitCalls = 0;
      let wrapped = false;
      try {
        const result = runMongoGroup(
          client,
          queue.seal(2),
          async (value, session: ClientSession) => {
            calls++;
            if (!wrapped) {
              wrapped = true;
              const commit = session.commitTransaction.bind(
                session,
              ) as () => Promise<void>;
              jest
                .spyOn(session, 'commitTransaction')
                .mockImplementation(async () => {
                  commitCalls++;
                  await commit(); // MongoDB has actually acknowledged this commit.
                  if (exhausted || commitCalls === 1) {
                    const error = new MongoError('injected lost commit reply');
                    error.addErrorLabel('UnknownTransactionCommitResult');
                    throw error;
                  }
                });
            }
            await collection.insertOne({ value }, { session });
            return value;
          },
          { maxAttempts: 2, maxCommitAttempts: 2 },
        );
        if (exhausted)
          await expect(result).rejects.toMatchObject({ outcome: 'unknown' });
        else await expect(result).resolves.toBeUndefined();
        expect(calls).toBe(2);
        expect(commitCalls).toBe(2);
        expect(commands).toHaveLength(2);
        expect(commands[1]).toEqual(commands[0]);
        expect(a.outcome).toEqual(
          exhausted
            ? { kind: 'unknown' }
            : { kind: 'committed', value: 'lost-a' },
        );
        expect(b.outcome).toEqual(
          exhausted
            ? { kind: 'unknown' }
            : { kind: 'committed', value: 'lost-b' },
        );
        expect(
          await collection
            .find({}, { projection: { _id: 0 } })
            .sort({ value: 1 })
            .toArray(),
        ).toEqual([{ value: 'lost-a' }, { value: 'lost-b' }]);
      } finally {
        client.off('commandStarted', monitor);
      }
    },
  );

  it('times out shutdown during a live transaction without claiming rollback', async () => {
    const queue = new GroupQueue<string, string>({
      maxRequests: 2,
      maxEvents: 2,
      maxBytes: 2,
    });
    const member = queue.enqueue('in-flight', 1, 1)!;
    const batch = queue.seal(1);
    const queued = queue.enqueue('queued', 1, 1)!;
    let release!: () => void;
    let entered!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const ready = new Promise<void>((resolve) => {
      entered = resolve;
    });
    const collection = db.collection('shutdown_live');
    const execution = runMongoGroup(
      client,
      batch,
      async (value, session) => {
        const commit = session.commitTransaction.bind(
          session,
        ) as () => Promise<void>;
        jest
          .spyOn(session, 'commitTransaction')
          .mockImplementation(async () => {
            entered();
            await gate;
            await commit();
          });
        await collection.insertOne({ value }, { session });
        return value;
      },
      { maxAttempts: 1, maxCommitAttempts: 1 },
    );
    const settled = execution.then(
      () => 'resolved',
      () => 'rejected',
    );
    try {
      await ready;
      expect(await collection.countDocuments()).toBe(0);
      await queue.shutdown(10);
      expect(member.outcome).toEqual({ kind: 'unknown' });
      expect(queued.state).toBe('cancelled');
      expect(queue.enqueue('later', 1, 1)).toBeUndefined();
      expect(queue.activeGroups).toBe(1);
      expect(queue.retainedBytes).toBe(1);
    } finally {
      release();
    }
    expect(await settled).toBe('rejected');
    expect(member.outcome).toEqual({ kind: 'unknown' });
    expect(
      await collection.find({}, { projection: { _id: 0 } }).toArray(),
    ).toEqual([{ value: 'in-flight' }]);
    expect(queue.activeGroups).toBe(0);
    expect(queue.retainedBytes).toBe(0);
  });

  it('retains a real acknowledged commit if session cleanup then throws', async () => {
    const queue = new GroupQueue<string, string>({
      maxRequests: 1,
      maxEvents: 1,
      maxBytes: 1,
    });
    const member = queue.enqueue('cleanup', 1, 1)!;
    let endCalls = 0;
    await runMongoGroup(
      client,
      queue.seal(1),
      async (value, session) => {
        const end = session.endSession.bind(session) as () => Promise<void>;
        jest.spyOn(session, 'endSession').mockImplementation(async () => {
          endCalls++;
          await end();
          throw new Error('injected cleanup failure');
        });
        await db.collection('cleanup_events').insertOne({ value }, { session });
        return value;
      },
      { maxAttempts: 1, maxCommitAttempts: 1 },
    );
    expect(endCalls).toBe(1);
    expect(member.outcome).toEqual({ kind: 'committed', value: 'cleanup' });
    expect(
      await db
        .collection('cleanup_events')
        .countDocuments({ value: 'cleanup' }),
    ).toBe(1);
  });

  it('commits serial members with majority journal and cleans the session', async () => {
    const queue = new GroupQueue<string, number>({
      maxRequests: 2,
      maxEvents: 2,
      maxBytes: 2,
    });
    const a = queue.enqueue('a', 1, 1)!;
    const b = queue.enqueue('b', 1, 1)!;
    const commits: Record<string, unknown>[] = [];
    client.on('commandStarted', (event) => {
      if (event.commandName === 'commitTransaction')
        commits.push(event.command);
    });
    let ended = false;
    await runMongoGroup(
      client,
      queue.seal(2),
      async (value, session) => {
        session.once('ended', () => {
          ended = true;
        });
        const counter = await db
          .collection<{ _id: string; value: number }>('counter')
          .findOneAndUpdate(
            { _id: 'global' },
            { $inc: { value: 1 } },
            { session, upsert: true, returnDocument: 'after' },
          );
        await db
          .collection('events')
          .insertOne({ value, position: counter!.value }, { session });
        expect(a.outcome).toBeUndefined();
        return counter!.value;
      },
      { maxAttempts: 3, maxCommitAttempts: 3 },
    );
    expect(a.outcome).toEqual({ kind: 'committed', value: 1 });
    expect(b.outcome).toEqual({ kind: 'committed', value: 2 });
    expect(ended).toBe(true);
    expect(commits[0].writeConcern).toMatchObject({ w: 'majority', j: true });
    expect(
      await db
        .collection('events')
        .find({}, { projection: { _id: 0 } })
        .sort({ position: 1 })
        .toArray(),
    ).toEqual([
      { value: 'a', position: 1 },
      { value: 'b', position: 2 },
    ]);
  });
});
