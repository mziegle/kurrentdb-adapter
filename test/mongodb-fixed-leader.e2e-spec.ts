import { GenericContainer, StartedTestContainer, Wait } from 'testcontainers';
import { MongoClient } from 'mongodb';
import { acquire } from './mongodb-fenced-tracer';
import * as routing from './mongodb-fixed-leader';

describe('test-only fixed leader routing', () => {
  jest.setTimeout(120000);
  it('routes independent clients through one bounded fenced writer', async () => {
    let container: StartedTestContainer | undefined;
    let mongo: MongoClient | undefined;
    try {
      container = await new GenericContainer('mongo:7')
        .withCommand(['mongod', '--replSet', 'rs0', '--bind_ip_all'])
        .withExposedPorts(27017)
        .withWaitStrategy(Wait.forLogMessage('Waiting for connections'))
        .start();
      await container.exec([
        'mongosh',
        '--quiet',
        '--eval',
        "rs.initiate({_id:'rs0',members:[{_id:0,host:'localhost:27017'}]})",
      ]);
      await container.exec([
        'mongosh',
        '--quiet',
        '--eval',
        'while (!db.hello().isWritablePrimary) { sleep(200); }',
      ]);
      mongo = new MongoClient(
        `mongodb://${container.getHost()}:${container.getMappedPort(27017)}/?directConnection=true`,
      );
      await mongo.connect();
      const db = mongo.db('fixed');
      await db.collection('fence').insertOne({
        _id: 'writer',
        owner: null,
        epoch: 0,
        writes: 0,
      } as never);
      await db.collection('events').createIndex({ name: 1 }, { unique: true });
      await db
        .collection('events')
        .createIndex({ position: 1 }, { unique: true });
      const token = await acquire(db, null, 0, 'designated');
      const leader = await routing.startLeader(mongo, db, token, 128);
      const start = performance.now();
      const results = await Promise.all(
        Array.from({ length: 100 }, (_, i) =>
          routing.submit(leader.port, `client-${i % 2}-${i}`, 10000),
        ),
      );
      const elapsed = performance.now() - start;
      expect(results.every((r) => r === 'committed')).toBe(true);
      expect(leader.maxActive()).toBe(1);
      const events = await db
        .collection<{ name: string; position: number }>('events')
        .find({}, { readConcern: { level: 'majority' } })
        .sort({ position: 1 })
        .toArray();
      expect(events).toHaveLength(100);
      expect(new Set(events.map((e) => e.name)).size).toBe(100);
      expect(events.map((e) => e.position)).toEqual(
        events.map((_, i) => i + 1),
      );
      console.log('fixed leader', {
        attempted: 100,
        committed: results.filter((r) => r === 'committed').length,
        unknown: results.filter((r) => r === 'unknown').length,
        elapsedMs: Math.round(elapsed),
        writesPerSecond: Math.round(100000 / elapsed),
        maxActive: leader.maxActive(),
      });
      await leader.close();
      expect(await routing.submit(leader.port, 'offline', 1000)).toBe(
        'rejected',
      );
      let entered!: () => void;
      let release!: () => void;
      const started = new Promise<void>((resolve) => {
        entered = resolve;
      });
      const gate = new Promise<void>((resolve) => {
        release = resolve;
      });
      const gated = await routing.startLeader(mongo, db, token, 1, async () => {
        entered();
        await gate;
      });
      try {
        const first = routing.submit(gated.port, 'timed-out', 500);
        await Promise.race([
          started,
          new Promise<never>((_, reject) =>
            setTimeout(
              () => reject(new Error('writer gate not entered')),
              2000,
            ).unref(),
          ),
        ]);
        expect(await routing.submit(gated.port, 'overflow', 1000)).toBe(
          'rejected',
        );
        expect(await first).toBe('unknown');
      } finally {
        release();
        await gated.close();
      }
      // Close waits for accepted writes even after the caller has timed out.
      expect(
        await db.collection('events').countDocuments({ name: 'timed-out' }),
      ).toBe(1);
      expect(
        await db.collection('events').countDocuments({ name: 'overflow' }),
      ).toBe(0);
      await acquire(db, token.owner, token.epoch, 'revoked');
      const stale = await routing.startLeader(mongo, db, token, 1);
      expect(await routing.submit(stale.port, 'stale', 1000)).toBe('unknown');
      await stale.close();
      expect(
        await db.collection('events').countDocuments({ name: 'stale' }),
      ).toBe(0);
    } finally {
      await mongo?.close();
      await container?.stop();
    }
  });
});
