import { CooperativeDispatcher } from './cooperative-dispatcher';

test('a cancelled member is removed while the runner awaits admission', async () => {
  let release!: () => void;
  let entered!: () => void;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  const ready = new Promise<void>((resolve) => {
    entered = resolve;
  });
  const seen: string[] = [];
  const dispatcher = new CooperativeDispatcher<string, string>(
    { maxRequests: 2, maxEvents: 2, maxBytes: 2 },
    100,
    (value) => value,
    async (finalize) => {
      entered();
      await gate;
      const batch = finalize();
      if (!batch) return;
      const attempt = batch.beginAttempt();
      for (const member of batch.members) {
        seen.push(member.value);
        batch.stage(attempt, member, member.value);
      }
      batch.commit(attempt);
    },
  );
  const controller = new AbortController();
  const a = dispatcher.enqueue('a', 1, 1, { signal: controller.signal });
  const rejected = expect(a).rejects.toMatchObject({ code: 'cancelled' });
  const b = dispatcher.enqueue('b', 1, 1);
  await ready;
  controller.abort();
  await rejected;
  release();
  await expect(b).resolves.toBe('b');
  expect(seen).toEqual(['b']);
  await dispatcher.shutdown(0);
});

test('finalize removes deadlines elapsed while timer delivery is delayed', async () => {
  jest.useFakeTimers();
  try {
    let resume!: () => void;
    let ready!: () => void;
    const gate = new Promise<void>((r) => {
      resume = r;
    });
    const entered = new Promise<void>((r) => {
      ready = r;
    });
    const dispatcher = new CooperativeDispatcher<string, string>(
      { maxRequests: 2, maxEvents: 2, maxBytes: 2 },
      10,
      (v) => v,
      async (finalize) => {
        ready();
        await gate;
        const batch = finalize()!;
        const attempt = batch.beginAttempt();
        for (const member of batch.members)
          batch.stage(attempt, member, member.value);
        batch.commit(attempt);
      },
    );
    const a = dispatcher.enqueue('a', 1, 1, { deadline: Date.now() + 50 });
    const rejected = expect(a).rejects.toMatchObject({ code: 'cancelled' });
    const b = dispatcher.enqueue('b', 1, 1);
    await entered;
    jest.setSystemTime(Date.now() + 51);
    resume();
    await rejected;
    await expect(b).resolves.toBe('b');
    await dispatcher.shutdown(0);
  } finally {
    jest.useRealTimers();
  }
});

test('shutdown before finalize prevents session work', async () => {
  let resume!: () => void;
  let ready!: () => void;
  const gate = new Promise<void>((r) => {
    resume = r;
  });
  const entered = new Promise<void>((r) => {
    ready = r;
  });
  let started = false;
  const dispatcher = new CooperativeDispatcher<string, string>(
    { maxRequests: 1, maxEvents: 1, maxBytes: 1 },
    0,
    (v) => v,
    async (finalize) => {
      ready();
      await gate;
      started = !!finalize();
    },
  );
  const request = dispatcher.enqueue('a', 1, 1);
  const rejected = expect(request).rejects.toMatchObject({ code: 'closed' });
  await entered;
  await dispatcher.shutdown(0);
  await rejected;
  resume();
  await new Promise<void>((r) => setImmediate(r));
  expect(started).toBe(false);
});

test('cancellation after finalize is unknown and retains capacity until cleanup', async () => {
  let resume!: () => void;
  let ready!: () => void;
  const gate = new Promise<void>((r) => {
    resume = r;
  });
  const entered = new Promise<void>((r) => {
    ready = r;
  });
  const dispatcher = new CooperativeDispatcher<string, string>(
    { maxRequests: 1, maxEvents: 1, maxBytes: 1, maxRetainedBytes: 1 },
    0,
    (v) => v,
    async (finalize) => {
      const batch = finalize()!;
      ready();
      await gate;
      const attempt = batch.beginAttempt();
      for (const member of batch.members)
        batch.stage(attempt, member, member.value);
      batch.commit(attempt);
    },
  );
  const controller = new AbortController();
  const request = dispatcher.enqueue('a', 1, 1, { signal: controller.signal });
  const rejected = expect(request).rejects.toMatchObject({ code: 'unknown' });
  await entered;
  controller.abort();
  await rejected;
  await expect(dispatcher.enqueue('b', 1, 1)).rejects.toMatchObject({
    code: 'capacity',
  });
  resume();
  await new Promise<void>((r) => setImmediate(r));
  await expect(dispatcher.enqueue('c', 1, 1)).resolves.toBe('c');
  await dispatcher.shutdown(0);
});

test('shutdown times out a finalized runner without releasing its ownership', async () => {
  jest.useFakeTimers();
  try {
    let ready!: () => void;
    let resume!: () => void;
    const entered = new Promise<void>((r) => {
      ready = r;
    });
    const gate = new Promise<void>((r) => {
      resume = r;
    });
    let batchWasSettled = false;
    const dispatcher = new CooperativeDispatcher<string, string>(
      { maxRequests: 1, maxEvents: 1, maxBytes: 1 },
      0,
      (v) => v,
      async (finalize) => {
        const batch = finalize()!;
        ready();
        await gate;
        batchWasSettled = batch.settled;
      },
    );
    const request = dispatcher.enqueue('a', 1, 1);
    const rejected = expect(request).rejects.toMatchObject({ code: 'unknown' });
    await entered;
    const shutdown = dispatcher.shutdown(5);
    jest.advanceTimersByTime(5);
    await shutdown;
    await rejected;
    resume();
    await Promise.resolve();
    expect(batchWasSettled).toBe(true);
  } finally {
    jest.useRealTimers();
  }
});

test('a runner that fails before finalize does not spin or strand callers', async () => {
  const runner = jest.fn(() => Promise.reject(new Error('admission failed')));
  const dispatcher = new CooperativeDispatcher<string, string>(
    { maxRequests: 1, maxEvents: 1, maxBytes: 1 },
    0,
    (v) => v,
    runner,
  );
  await expect(dispatcher.enqueue('a', 1, 1)).rejects.toMatchObject({
    code: 'aborted',
  });
  expect(runner).toHaveBeenCalledTimes(1);
  await dispatcher.shutdown(0);
});
