import { GroupQueue } from './group-commit';
import { executeGroup } from './group-coordinator';

function setup() {
  const queue = new GroupQueue<string, string>({
    maxRequests: 2,
    maxEvents: 2,
    maxBytes: 2,
  });
  const a = queue.enqueue('a', 1, 1)!;
  const b = queue.enqueue('b', 1, 1)!;
  return { queue, a, b, batch: queue.seal(2) };
}

test('publishes staged results only after the transaction acknowledges commit', async () => {
  const { a, b, batch } = setup();
  let release!: () => void;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  const running = executeGroup(
    batch,
    async (work) => {
      await work();
      await gate;
    },
    (value) => Promise.resolve(value.toUpperCase()),
  );
  await new Promise((resolve) => setImmediate(resolve));
  expect(a.outcome).toBeUndefined();
  expect(b.outcome).toBeUndefined();
  release();
  await running;
  expect(a.outcome).toEqual({ kind: 'committed', value: 'A' });
  expect(b.outcome).toEqual({ kind: 'committed', value: 'B' });
});

test('rebuilds results on a transaction callback retry', async () => {
  const { a, b, batch } = setup();
  let generation = 0;
  await executeGroup(
    batch,
    async (work) => {
      generation++;
      await work();
      generation++;
      await work();
    },
    (value) => Promise.resolve(`${value}${generation}`),
  );
  expect(a.outcome).toEqual({ kind: 'committed', value: 'a2' });
  expect(b.outcome).toEqual({ kind: 'committed', value: 'b2' });
});

test('settles every member unknown when transaction outcome is uncertain', async () => {
  const { a, b, batch } = setup();
  const failure = new Error('lost commit reply');
  await expect(
    executeGroup(
      batch,
      async (work) => {
        await work();
        throw failure;
      },
      (value) => Promise.resolve(value),
    ),
  ).rejects.toBe(failure);
  expect(a.outcome).toEqual({ kind: 'unknown' });
  expect(b.outcome).toEqual({ kind: 'unknown' });
});

test('settles all members aborted only when the runner proves abort', async () => {
  const { a, b, batch } = setup();
  const failure = new Error('write failed, transaction aborted');
  await expect(
    executeGroup(
      batch,
      async (work) => {
        await work();
      },
      (value) =>
        value === 'b' ? Promise.reject(failure) : Promise.resolve(value),
      () => 'aborted',
    ),
  ).rejects.toBe(failure);
  expect(a.outcome).toEqual({ kind: 'aborted' });
  expect(b.outcome).toEqual({ kind: 'aborted' });
});

test('does not overwrite shutdown uncertainty when a late commit resolves', async () => {
  const { queue, a, batch } = setup();
  let release!: () => void;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  const running = executeGroup(
    batch,
    async (work) => {
      await work();
      await gate;
    },
    (value) => Promise.resolve(value),
  );
  await new Promise((resolve) => setImmediate(resolve));
  await queue.shutdown(0);
  release();
  await expect(running).rejects.toThrow();
  expect(a.outcome).toEqual({ kind: 'unknown' });
});
