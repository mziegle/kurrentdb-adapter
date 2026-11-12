import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { MongoClient } from 'mongodb';
import { GenericContainer, StartedTestContainer, Wait } from 'testcontainers';
import { acquire, protectedWrite } from './mongodb-fenced-tracer';
import { getFreePort } from './util/get-free-port';

describe('test-only fence across a three-node election', () => {
  jest.setTimeout(180_000);
  const containers: StartedTestContainer[] = [];
  const ports: number[] = [];
  const docker = promisify(execFile);
  let oldClient: MongoClient;
  let newClient: MongoClient;

  const primary = async (excluded = -1): Promise<number> => {
    const deadline = Date.now() + 60_000;
    while (Date.now() < deadline) {
      for (let i = 0; i < ports.length; i++) {
        if (i === excluded) continue;
        const probe = new MongoClient(
          `mongodb://127.0.0.1:${ports[i]}/?directConnection=true&serverSelectionTimeoutMS=1000&connectTimeoutMS=1000`,
        );
        try {
          if ((await probe.db('admin').command({ hello: 1 })).isWritablePrimary)
            return i;
        } catch {
          // A stopped or electing node is not a primary.
        } finally {
          await probe.close();
        }
      }
      await new Promise((resolve) => setTimeout(resolve, 250));
    }
    throw new Error('Timed out waiting for primary');
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
    await primary();
    const uri = `mongodb://${ports.map((port) => `127.0.0.1:${port}`).join(',')}/?replicaSet=rs0&serverSelectionTimeoutMS=30000&heartbeatFrequencyMS=500`;
    oldClient = new MongoClient(uri);
    newClient = new MongoClient(uri);
    await Promise.all([oldClient.connect(), newClient.connect()]);
    const db = oldClient.db('fence_election');
    await db
      .collection<{
        _id: string;
        owner: string | null;
        epoch: number;
        writes: number;
      }>('fence')
      .insertOne(
        { _id: 'writer', owner: null, epoch: 0, writes: 0 },
        { writeConcern: { w: 'majority', j: true } },
      );
    await db
      .collection('events')
      .createIndex({ position: 1 }, { unique: true });
  });

  afterAll(async () => {
    await oldClient?.close();
    await newClient?.close();
    await Promise.all(containers.map((container) => container.stop()));
  });

  it('fences a paused owner after a majority election and preserves acknowledged positions', async () => {
    const oldDb = oldClient.db('fence_election');
    const newDb = newClient.db('fence_election');
    const old = await acquire(oldDb, null, 0, 'old');
    await protectedWrite(oldClient, oldDb, old, 'ack-before');
    const oldPrimary = await primary();
    let release!: () => void;
    let entered!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const started = new Promise<void>((resolve) => {
      entered = resolve;
    });
    const pending = protectedWrite(oldClient, oldDb, old, 'stale', async () => {
      entered();
      await gate;
    });
    // Attach the rejection handler immediately, even if election setup fails.
    const stale = pending.then(
      () => ({ committed: true as const }),
      (error: unknown) => ({ committed: false as const, error }),
    );
    let paused = false;
    try {
      await started;
      await docker('docker', ['pause', containers[oldPrimary].getId()]);
      paused = true;
      expect(await primary(oldPrimary)).not.toBe(oldPrimary);
      const next = await acquire(newDb, old.owner, old.epoch, 'new');
      expect(next).toEqual({ owner: 'new', epoch: old.epoch + 1 });
      await protectedWrite(newClient, newDb, next, 'ack-after');
      release();
      const result = await stale;
      expect(result.committed).toBe(false);
      if (result.committed) throw new Error('stale owner committed');
      expect(result.error).toBeInstanceOf(Error);
      expect((result.error as Error).message).toBe('fence rejected');
      const events = await newDb
        .collection<{ name: string; position: number }>('events')
        .find({}, { readConcern: { level: 'majority' } })
        .sort({ position: 1 })
        .toArray();
      expect(events.map(({ name, position }) => ({ name, position }))).toEqual([
        { name: 'ack-before', position: 1 },
        { name: 'ack-after', position: 2 },
      ]);
      expect(new Set(events.map((event) => event.position)).size).toBe(
        events.length,
      );
    } finally {
      release();
      if (paused)
        await docker('docker', ['unpause', containers[oldPrimary].getId()]);
      await stale;
    }
  });
});
