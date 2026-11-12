import { GenericContainer, StartedTestContainer, Wait } from 'testcontainers';
import { MongoClient } from 'mongodb';
import { acquire, protectedWrite } from './mongodb-fenced-tracer';
import { SerialAdmission } from './serial-admission';

describe('test-only durable fence tracer', () => {
  jest.setTimeout(120_000);
  let container: StartedTestContainer;
  let a: MongoClient;
  let b: MongoClient;
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
    if (init.exitCode) throw new Error(init.output);
    const election = await container.exec([
      'mongosh',
      '--quiet',
      '--eval',
      'while (!db.hello().isWritablePrimary) { sleep(200); }',
    ]);
    if (election.exitCode) throw new Error(election.output);
    const uri = `mongodb://${container.getHost()}:${container.getMappedPort(27017)}/?directConnection=true`;
    a = new MongoClient(uri);
    b = new MongoClient(uri);
    await Promise.all([a.connect(), b.connect()]);
    const db = a.db('fence');
    await db
      .collection<{
        _id: string;
        owner: string | null;
        epoch: number;
        writes: number;
      }>('fence')
      .insertOne({ _id: 'writer', owner: null, epoch: 0, writes: 0 });
    await db
      .collection('events')
      .createIndex({ position: 1 }, { unique: true });
  });
  beforeEach(async () => {
    await a.db('fence').collection('events').deleteMany({});
    await a
      .db('fence')
      .collection('fence')
      .updateOne({}, { $set: { owner: null, epoch: 0, writes: 0 } });
  });
  afterAll(async () => {
    await a?.close();
    await b?.close();
    await container?.stop();
  });
  it('combines independent admissions with explicit takeover and fenced writes', async () => {
    const db = a.db('fence');
    const old = await acquire(db, null, 0, 'old-queue');
    const oldQueue = new SerialAdmission(3);
    const newQueue = new SerialAdmission(3);
    let release!: () => void;
    let entered!: () => void;
    const gate = new Promise<void>((resolve) => (release = resolve));
    const started = new Promise<void>((resolve) => (entered = resolve));
    const first = oldQueue.run(() =>
      protectedWrite(a, db, old, 'old-running', async () => {
        entered();
        await gate;
      }),
    );
    const firstResult = first.then(
      () => 'committed',
      (error: Error) => error.message,
    );
    const cancelled = new AbortController();
    const skipped = oldQueue.run(
      () => protectedWrite(a, db, old, 'cancelled'),
      cancelled.signal,
    );
    const skippedResult = skipped.then(
      () => 'committed',
      (error: Error) => error.message,
    );
    const stale = oldQueue.run(() => protectedWrite(a, db, old, 'old-queued'));
    const staleResult = stale.then(
      () => 'committed',
      (error: Error) => error.message,
    );
    await expect(
      oldQueue.run(() => protectedWrite(a, db, old, 'overflow')),
    ).rejects.toThrow('admission full');
    cancelled.abort();
    await started;
    try {
      const next = await acquire(
        b.db('fence'),
        old.owner,
        old.epoch,
        'new-queue',
      );
      await Promise.all([
        newQueue.run(() => protectedWrite(b, b.db('fence'), next, 'new-1')),
        newQueue.run(() => protectedWrite(b, b.db('fence'), next, 'new-2')),
      ]);
    } finally {
      release();
    }
    expect(await firstResult).toBe('fence rejected');
    expect(await skippedResult).toBe('admission cancelled');
    expect(await staleResult).toBe('fence rejected');
    const events = await db
      .collection<{ name: string; position: number }>('events')
      .find({}, { readConcern: { level: 'majority' } })
      .sort({ position: 1 })
      .toArray();
    expect(events.map(({ name, position }) => ({ name, position }))).toEqual([
      { name: 'new-1', position: 1 },
      { name: 'new-2', position: 2 },
    ]);
  });
  it('measures competing independent serial queues without replay', async () => {
    const token = await acquire(a.db('fence'), null, 0, 'shared-token');
    const queues = [new SerialAdmission(20), new SerialAdmission(20)];
    const clients = [a, b];
    const start = performance.now();
    const results = await Promise.allSettled(
      clients.flatMap((client, owner) =>
        Array.from({ length: 20 }, (_, index) =>
          queues[owner].run(() =>
            protectedWrite(
              client,
              client.db('fence'),
              token,
              `${owner}-${index}`,
            ),
          ),
        ),
      ),
    );
    const elapsedMs = performance.now() - start;
    const successes = results.filter(
      (result) => result.status === 'fulfilled',
    ).length;
    const failures = results.filter((result) => result.status === 'rejected');
    const events = await a
      .db('fence')
      .collection<{ name: string; position: number }>('events')
      .find({}, { readConcern: { level: 'majority' } })
      .sort({ position: 1 })
      .toArray();
    expect(events).toHaveLength(successes);
    expect(new Set(events.map((event) => event.name)).size).toBe(successes);
    expect(events.map((event) => event.position)).toEqual(
      events.map((_, i) => i + 1),
    );
    expect(successes).toBeGreaterThan(0);
    console.log('fenced independent-queue contention', {
      attempted: results.length,
      successes,
      failures: failures.length,
      elapsedMs: Math.round(elapsedMs),
    });
  });
  it('rejects a paused stale transaction after takeover', async () => {
    const db = a.db('fence');
    const old = await acquire(db, null, 0, 'paused');
    let resume!: () => void;
    let started!: () => void;
    const entered = new Promise<void>((resolve) => {
      started = resolve;
    });
    const gate = new Promise<void>((resolve) => {
      resume = resolve;
    });
    const pending = protectedWrite(a, db, old, 'paused-event', async () => {
      started();
      await gate;
    });
    await Promise.race([
      entered,
      new Promise<never>((_, reject) =>
        setTimeout(() => reject(new Error('gate not reached')), 1000),
      ),
    ]);
    const next = await acquire(b.db('fence'), old.owner, old.epoch, 'takeover');
    resume();
    await expect(pending).rejects.toThrow();
    await protectedWrite(b, b.db('fence'), next, 'takeover-event');
    expect(
      await db.collection('events').countDocuments({ name: 'paused-event' }),
    ).toBe(0);
  });
  it('rejects a transaction with an old server snapshot after takeover', async () => {
    const db = a.db('fence');
    const old = await acquire(db, null, 0, 'snapshot-old');
    const session = a.startSession();
    try {
      session.startTransaction({ writeConcern: { w: 'majority', j: true } });
      expect(
        await db.collection('fence').findOne({ owner: old.owner }, { session }),
      ).not.toBeNull();
      await acquire(b.db('fence'), old.owner, old.epoch, 'snapshot-new');
      await expect(
        db
          .collection('fence')
          .updateOne(
            { owner: old.owner, epoch: old.epoch },
            { $inc: { writes: 1 } },
            { session },
          ),
      ).rejects.toThrow();
      await expect(session.commitTransaction()).rejects.toThrow();
      expect(await db.collection('events').countDocuments()).toBe(0);
    } finally {
      await session.endSession();
    }
  });
  it('allows only one concurrent acquisition and never duplicates positions', async () => {
    const outcomes = await Promise.allSettled([
      acquire(a.db('fence'), null, 0, 'a'),
      acquire(b.db('fence'), null, 0, 'b'),
    ]);
    const winners = outcomes.filter(
      (
        outcome,
      ): outcome is PromiseFulfilledResult<{ owner: string; epoch: number }> =>
        outcome.status === 'fulfilled',
    );
    expect(winners).toHaveLength(1);
    const token = winners[0].value;
    const writes = await Promise.allSettled(
      Array.from({ length: 12 }, (_, index) =>
        protectedWrite(
          index % 2 ? a : b,
          (index % 2 ? a : b).db('fence'),
          token,
          `event-${index}`,
        ),
      ),
    );
    const stored = await a
      .db('fence')
      .collection<{ position: number }>('events')
      .find()
      .sort({ position: 1 })
      .toArray();
    expect(stored.length).toBe(
      writes.filter((item) => item.status === 'fulfilled').length,
    );
    expect(stored.length).toBeGreaterThan(0);
    expect(stored.map((event) => event.position)).toEqual(
      stored.map((_, index) => index + 1),
    );
  });
  it('invalidates an old owner after a conditional durable takeover', async () => {
    const first = await acquire(a.db('fence'), null, 0, 'old');
    expect(first).toEqual({ owner: 'old', epoch: 1 });
    const next = await acquire(b.db('fence'), first.owner, first.epoch, 'new');
    expect(next).toEqual({ owner: 'new', epoch: 2 });
    await expect(
      protectedWrite(a, a.db('fence'), first, 'stale'),
    ).rejects.toThrow();
    await protectedWrite(b, b.db('fence'), next, 'fresh');
    expect(await a.db('fence').collection('events').find().toArray()).toEqual([
      expect.objectContaining({ name: 'fresh', position: 1 }),
    ]);
  });
});
