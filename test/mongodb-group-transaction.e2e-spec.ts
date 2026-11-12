import { GenericContainer, StartedTestContainer, Wait } from 'testcontainers';
import { Db, MongoClient } from 'mongodb';
import { GroupQueue } from '../src/mongodb/group-commit';
import { executeGroup } from '../src/mongodb/group-coordinator';

// Test-only tracer, not an implementation of the adapter's append contract.
async function writeGroup(client: MongoClient, db: Db): Promise<void> {
  const session = client.startSession();
  try {
    await session.withTransaction(
      async () => {
        const counter = await db
          .collection<{ _id: string; value: number }>('counters')
          .findOneAndUpdate(
            { _id: 'global' },
            { $inc: { value: 2 } },
            { session, upsert: true, returnDocument: 'after' },
          );
        if (!counter) throw new Error('counter update returned no document');
        const events = db.collection('events');
        await events.insertOne(
          { stream: 'a', position: counter.value - 1 },
          { session },
        );
        await events.insertOne(
          { stream: 'b', position: counter.value },
          { session },
        );
      },
      { writeConcern: { w: 'majority', j: true } },
    );
  } finally {
    await session.endSession();
  }
}

type Request = {
  stream: string;
  expected: 'any' | 'no_stream' | 'exists' | number;
};
type Result = { ok: boolean; revision?: number; position?: number };

async function writeValidatedGroup(
  client: MongoClient,
  db: Db,
  requests: Request[],
): Promise<Result[]> {
  const session = client.startSession();
  try {
    const result = await session.withTransaction(
      async () => {
        const counters = db.collection<{ _id: string; value: number }>(
          'validated_counters',
        );
        const events = db.collection<{
          stream: string;
          revision: number;
          position: number;
        }>('validated_events');
        const counter = await counters.findOneAndUpdate(
          { _id: 'global' },
          { $inc: { value: 0 } },
          { session, upsert: true, returnDocument: 'after' },
        );
        if (!counter) throw new Error('counter update returned no document');
        let position = counter.value;
        const results: Result[] = [];
        for (const request of requests) {
          const last = await events.findOne(
            { stream: request.stream },
            { session, sort: { revision: -1 } },
          );
          const revision = last?.revision;
          const valid =
            request.expected === 'any' ||
            (request.expected === 'no_stream' && revision === undefined) ||
            (request.expected === 'exists' && revision !== undefined) ||
            (typeof request.expected === 'number' &&
              request.expected === revision);
          if (!valid) {
            results.push({ ok: false });
            continue;
          }
          const next = (revision ?? -1) + 1;
          position++;
          await events.insertOne(
            { stream: request.stream, revision: next, position },
            { session },
          );
          results.push({ ok: true, revision: next, position });
        }
        await counters.updateOne(
          { _id: 'global' },
          { $set: { value: position } },
          { session },
        );
        return results;
      },
      { writeConcern: { w: 'majority', j: true } },
    );
    if (!result) throw new Error('transaction returned no results');
    return result;
  } finally {
    await session.endSession();
  }
}

describe('MongoDB test-only group transaction tracer', () => {
  jest.setTimeout(120_000);
  let container: StartedTestContainer;
  let client: MongoClient;
  let db: Db;
  const commands: { name: string; command: Record<string, unknown> }[] = [];

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
    client = new MongoClient(
      `mongodb://${container.getHost()}:${container.getMappedPort(27017)}/?directConnection=true`,
      { monitorCommands: true },
    );
    client.on('commandStarted', (event) => {
      if (
        [
          'findAndModify',
          'insert',
          'update',
          'commitTransaction',
          'abortTransaction',
        ].includes(event.commandName)
      ) {
        const { lsid, txnNumber, autocommit, writeConcern } = event.command;
        commands.push({
          name: event.commandName,
          command: { lsid, txnNumber, autocommit, writeConcern },
        });
      }
    });
    await client.connect();
    db = client.db('group_transaction_tracer');
    await db.collection('events').createIndex({ stream: 1 }, { unique: true });
  });

  afterAll(async () => {
    await client?.close();
    await container?.stop();
  });

  it('runs the isolated coordinator on one real majority journaled transaction', async () => {
    const queue = new GroupQueue<string, string>({
      maxRequests: 2,
      maxEvents: 2,
      maxBytes: 2,
    });
    const a = queue.enqueue('coordinator-a', 1, 1)!;
    const b = queue.enqueue('coordinator-b', 1, 1)!;
    const session = client.startSession();
    commands.length = 0;
    try {
      await executeGroup(
        queue.seal(2),
        async (work) => {
          await session.withTransaction(work, {
            writeConcern: { w: 'majority', j: true },
          });
        },
        async (stream) => {
          await db
            .collection('coordinator_events')
            .insertOne({ stream }, { session });
          expect(a.outcome).toBeUndefined();
          expect(b.outcome).toBeUndefined();
          return stream;
        },
      );
      expect(a.outcome).toEqual({ kind: 'committed', value: 'coordinator-a' });
      expect(b.outcome).toEqual({ kind: 'committed', value: 'coordinator-b' });
      expect(await db.collection('coordinator_events').countDocuments()).toBe(
        2,
      );
      const commits = commands.filter(
        ({ name }) => name === 'commitTransaction',
      );
      const writes = commands.filter(({ name }) => name === 'insert');
      expect(commits).toHaveLength(1);
      expect(writes).toHaveLength(2);
      expect(commits[0].command.writeConcern).toMatchObject({
        w: 'majority',
        j: true,
      });
      for (const { command } of writes) {
        expect(command.lsid).toEqual(commits[0].command.lsid);
        expect(command.txnNumber).toEqual(commits[0].command.txnNumber);
      }
    } finally {
      await session.endSession();
      await queue.shutdown(1000);
    }
  });

  it('isolates a stale revision from a successful different stream', async () => {
    await db.collection('validated_events').deleteMany({});
    await db.collection('validated_counters').deleteMany({});
    const results = await writeValidatedGroup(client, db, [
      { stream: 'a', expected: 0 },
      { stream: 'b', expected: 'no_stream' },
    ]);
    expect(results).toEqual([
      { ok: false },
      { ok: true, revision: 0, position: 1 },
    ]);
    expect(await db.collection('validated_events').countDocuments()).toBe(1);
  });

  it('evaluates same-stream requests in order without poisoning peers', async () => {
    await db.collection('validated_events').deleteMany({});
    await db.collection('validated_counters').deleteMany({});
    commands.length = 0;
    const results = await writeValidatedGroup(client, db, [
      { stream: 'a', expected: 'no_stream' },
      { stream: 'a', expected: 'no_stream' },
      { stream: 'a', expected: 0 },
      { stream: 'b', expected: 'exists' },
      { stream: 'b', expected: 'any' },
    ]);
    expect(results).toEqual([
      { ok: true, revision: 0, position: 1 },
      { ok: false },
      { ok: true, revision: 1, position: 2 },
      { ok: false },
      { ok: true, revision: 0, position: 3 },
    ]);
    const commits = commands.filter(({ name }) => name === 'commitTransaction');
    expect(commits).toHaveLength(1);
    expect(commits[0].command.writeConcern).toMatchObject({
      w: 'majority',
      j: true,
    });
    const writes = commands.filter(({ name }) =>
      ['findAndModify', 'insert', 'update'].includes(name),
    );
    for (const { command } of writes) {
      expect(command.lsid).toEqual(commits[0].command.lsid);
      expect(command.txnNumber).toEqual(commits[0].command.txnNumber);
    }
  });

  it('serializes conflicting groups from separate clients by committed position', async () => {
    await db.collection('validated_events').deleteMany({});
    await db.collection('validated_counters').deleteMany({});
    await db
      .collection('validated_counters')
      .insertOne({ _id: 'global' as never, value: 0 });
    const other = new MongoClient(
      `mongodb://${container.getHost()}:${container.getMappedPort(27017)}/?directConnection=true`,
    );
    await other.connect();
    try {
      const results = await Promise.all([
        writeValidatedGroup(client, db, [
          { stream: 'race', expected: 'no_stream' },
          { stream: 'a', expected: 'any' },
        ]),
        writeValidatedGroup(other, other.db(db.databaseName), [
          { stream: 'race', expected: 'no_stream' },
          { stream: 'b', expected: 'any' },
        ]),
      ]);
      expect(results.filter((group) => group[0].ok)).toHaveLength(1);
      expect(results.every((group) => group[1].ok)).toBe(true);
      const events = await db
        .collection<{ stream: string; position: number }>('validated_events')
        .find({}, { projection: { _id: 0 } })
        .sort({ position: 1 })
        .toArray();
      expect(events.map((event) => event.position)).toEqual([1, 2, 3]);
      expect(events.filter((event) => event.stream === 'race')).toHaveLength(1);
      const winning = results.find((group) => group[0].ok)!;
      expect(winning[1].position).toBe(winning[0].position! + 1);
    } finally {
      await other.close();
    }
  });

  it('commits two independent streams and their increasing positions in one majority journaled transaction', async () => {
    commands.length = 0;
    await writeGroup(client, db);
    const events = await db
      .collection('events')
      .find({}, { projection: { _id: 0 } })
      .sort({ position: 1 })
      .toArray();
    expect(events).toEqual([
      { stream: 'a', position: 1 },
      { stream: 'b', position: 2 },
    ]);
    expect(
      await db.collection('counters').findOne({ _id: 'global' } as never),
    ).toMatchObject({ value: 2 });
    const writes = commands.filter(({ name }) =>
      ['findAndModify', 'insert'].includes(name),
    );
    const commits = commands.filter(({ name }) => name === 'commitTransaction');
    expect(writes).toHaveLength(3);
    expect(commits).toHaveLength(1);
    expect(commits[0].command).toMatchObject({
      writeConcern: { w: 'majority', j: true },
    });
    for (const { command } of writes) {
      expect(command.lsid).toEqual(commits[0].command.lsid);
      expect(command.txnNumber).toEqual(commits[0].command.txnNumber);
      expect(command.autocommit).toBe(false);
    }
  });

  it('rolls back the first write and counter when the second write violates a unique index', async () => {
    await db.collection('events').deleteMany({});
    await db.collection('counters').deleteMany({});
    await db
      .collection('counters')
      .insertOne({ _id: 'global' as never, value: 10 });
    await db.collection('events').insertOne({ stream: 'b', position: 99 });
    commands.length = 0;

    await expect(writeGroup(client, db)).rejects.toMatchObject({ code: 11000 });
    expect(
      await db
        .collection('events')
        .find({}, { projection: { _id: 0 } })
        .toArray(),
    ).toEqual([{ stream: 'b', position: 99 }]);
    expect(
      await db.collection('counters').findOne({ _id: 'global' } as never),
    ).toMatchObject({ value: 10 });
    expect(
      commands.filter(({ name }) => name === 'abortTransaction'),
    ).toHaveLength(1);
    expect(
      commands.filter(({ name }) => name === 'commitTransaction'),
    ).toHaveLength(0);

    await db.collection('events').deleteMany({});
    await writeGroup(client, db);
    expect(
      await db
        .collection('events')
        .find({}, { projection: { _id: 0 } })
        .sort({ position: 1 })
        .toArray(),
    ).toEqual([
      { stream: 'a', position: 11 },
      { stream: 'b', position: 12 },
    ]);
  });
});
