import { fork, ChildProcess } from 'node:child_process';
import { GenericContainer, StartedTestContainer, Wait } from 'testcontainers';
import { MongoClient } from 'mongodb';
import { acquire } from './mongodb-fenced-tracer';

function spawn(role: string, args: string[]): ChildProcess {
  return fork(require.resolve('./mongodb-process-worker'), [role, ...args], {
    execArgv: ['-r', 'ts-node/register/transpile-only'],
    stdio: ['ignore', 'inherit', 'inherit', 'ipc'],
  });
}

type WorkerMessage = {
  kind: string;
  port?: number;
  epoch?: number;
  outcome?: string;
};

async function bounded<T>(promise: Promise<T>, label: string): Promise<T> {
  let timer: NodeJS.Timeout | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<never>((_, reject) => {
        timer = setTimeout(
          () => reject(new Error(`${label} timed out`)),
          10000,
        );
        timer.unref();
      }),
    ]);
  } finally {
    if (timer) clearTimeout(timer);
  }
}

function message(child: ChildProcess, kind: string): Promise<WorkerMessage> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(
      () => finish(new Error(`timed out waiting for ${kind}`)),
      15000,
    );
    const onMessage = (value: WorkerMessage) => {
      if (value?.kind === kind) finish(undefined, value);
    };
    const onExit = () => finish(new Error(`worker exited before ${kind}`));
    function finish(error?: Error, value?: WorkerMessage) {
      clearTimeout(timer);
      child.off('message', onMessage);
      child.off('exit', onExit);
      if (error) reject(error);
      else if (value) resolve(value);
    }
    child.on('message', onMessage);
    child.on('exit', onExit);
  });
}

async function stop(child: ChildProcess) {
  if (child.exitCode !== null || child.signalCode !== null) return;
  const exited = new Promise<void>((resolve) =>
    child.once('exit', () => resolve()),
  );
  child.kill('SIGKILL');
  await Promise.race([
    exited,
    new Promise<never>((_, reject) =>
      setTimeout(() => reject(new Error('worker did not exit')), 5000).unref(),
    ),
  ]);
}

describe('fixed leader process crash', () => {
  jest.setTimeout(120000);
  it.each([
    ['before commit', 'leader', 'staged', false],
    [
      'after commit acknowledgement',
      'leader-committed',
      'committed-gate',
      true,
    ],
  ])(
    'returns unknown after SIGKILL %s and manually resumes',
    async (_label, role, gate, committed) => {
      let container: StartedTestContainer | undefined;
      let mongo: MongoClient | undefined;
      const children: ChildProcess[] = [];
      try {
        container = await new GenericContainer('mongo:7')
          .withCommand([
            'mongod',
            '--replSet',
            'rs0',
            '--bind_ip_all',
            '--setParameter',
            'transactionLifetimeLimitSeconds=5',
          ])
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
        const uri = `mongodb://${container.getHost()}:${container.getMappedPort(27017)}/?directConnection=true`;
        mongo = new MongoClient(uri);
        await mongo.connect();
        const db = mongo.db('crash');
        await db.collection('fence').insertOne({
          _id: 'writer',
          owner: null,
          epoch: 0,
          writes: 0,
        } as never);
        await db
          .collection('events')
          .createIndex({ name: 1 }, { unique: true });
        await db
          .collection('events')
          .createIndex({ position: 1 }, { unique: true });
        const old = await acquire(db, null, 0, 'old');
        const leader = spawn(role, [uri, JSON.stringify(old)]);
        children.push(leader);
        const ready = await message(leader, 'ready');
        const staged = message(leader, gate);
        const client = spawn('client', [String(ready.port)]);
        children.push(client);
        const result = message(client, 'result');
        await staged;
        const atGate = await db
          .collection<{ name: string; position: number }>('events')
          .find({}, { readConcern: { level: 'majority' } })
          .toArray();
        expect(
          atGate.map(({ name, position }) => ({ name, position })),
        ).toEqual(committed ? [{ name: 'staged-old', position: 1 }] : []);
        await stop(leader);
        expect(leader.signalCode).toBe('SIGKILL');
        expect((await result).outcome).toBe('unknown');
        const takeover = spawn('takeover', [uri, JSON.stringify(old)]);
        children.push(takeover);
        const completed = await message(takeover, 'completed');
        expect(completed.epoch).toEqual(old.epoch + 1);
        const events = await db
          .collection<{ name: string; position: number }>('events')
          .find({}, { readConcern: { level: 'majority' } })
          .sort({ position: 1 })
          .toArray();
        expect(
          events.map(({ name, position }) => ({ name, position })),
        ).toEqual([
          ...(committed ? [{ name: 'staged-old', position: 1 }] : []),
          { name: 'new-1', position: committed ? 2 : 1 },
          { name: 'new-2', position: committed ? 3 : 2 },
        ]);
      } finally {
        try {
          await Promise.all(children.map(stop));
        } finally {
          try {
            if (mongo) await bounded(mongo.close(), 'MongoDB close');
          } finally {
            if (container) await bounded(container.stop(), 'container stop');
          }
        }
      }
    },
  );
});
