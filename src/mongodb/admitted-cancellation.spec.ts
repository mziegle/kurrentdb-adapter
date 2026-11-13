import { MongoEventStoreService } from './mongodb-event-store.service';

test('ordinary work without admission settles unknown on deadline and observes late rejection', async () => {
  const service = new MongoEventStoreService();
  let fail!: (reason: Error) => void;
  const gate = new Promise<never>((_, reject) => (fail = reject));
  const task = service['admitted'](
    0,
    { signal: new AbortController().signal, deadline: Date.now() + 30 },
    () => gate,
  );
  await expect(task).rejects.toMatchObject({ code: 'unknown' });
  fail(new Error('late failure'));
  await new Promise((resolve) => setImmediate(resolve));
});

test('ordinary work without admission settles unknown on cancellation while cleanup continues', async () => {
  const service = new MongoEventStoreService();
  const abort = new AbortController();
  let finish!: () => void;
  const gate = new Promise<void>((resolve) => (finish = resolve));
  let cleaned = false;
  const task = service['admitted'](0, { signal: abort.signal }, async () => {
    await gate;
    cleaned = true;
    return 1;
  });
  await Promise.resolve();
  abort.abort();
  const result = await Promise.race([
    task.then(
      () => 'success',
      (error: { code: string }) => error.code,
    ),
    new Promise<string>((resolve) => setTimeout(() => resolve('hung'), 50)),
  ]);
  expect(result).toBe('unknown');
  expect(cleaned).toBe(false);
  finish();
  await gate;
  await Promise.resolve();
  expect(cleaned).toBe(true);
});

test('started ordinary cancellation reports unknown and holds admission through cleanup', async () => {
  const previous = { ...process.env };
  process.env.MONGODB_EXPERIMENTAL_GROUP_COMMIT = 'true';
  process.env.MONGODB_GROUP_COMMIT_ACKNOWLEDGE_SINGLE_ADAPTER = 'true';
  const service = new MongoEventStoreService();
  process.env = previous;
  const abort = new AbortController();
  let finish!: () => void;
  const gate = new Promise<void>((resolve) => (finish = resolve));
  let entered!: () => void;
  const started = new Promise<void>((resolve) => (entered = resolve));
  const first = service['admitted'](
    1,
    { signal: abort.signal },
    async (cleanup) => {
      cleanup.cleanup = 'unconfirmed';
      entered();
      await gate;
      cleanup.cleanup = 'complete';
      return 1;
    },
  );
  await started;
  abort.abort();
  await expect(first).rejects.toMatchObject({ code: 'unknown' });
  let secondStarted = false;
  const second = service['admitted'](1, undefined, () => {
    secondStarted = true;
    return Promise.resolve(2);
  });
  await new Promise((resolve) => setTimeout(resolve, 20));
  expect(secondStarted).toBe(false);
  finish();
  await expect(second).resolves.toBe(2);
});
