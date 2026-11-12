import { Batch } from './group-commit';
import { GroupDispatcher } from './group-dispatcher';

const commit = (batch: Batch<string, string>) => {
  const attempt = batch.beginAttempt();
  for (const member of batch.members)
    batch.stage(attempt, member, member.value);
  batch.commit(attempt);
  return Promise.resolve();
};

test('waits for the time window and separates duplicate stream keys', async () => {
  jest.useFakeTimers();
  try {
    const runner = jest.fn(commit);
    const dispatcher = new GroupDispatcher(
      { maxRequests: 3, maxEvents: 3, maxBytes: 3 },
      20,
      (value: string) => value,
      runner,
    );
    const a = dispatcher.enqueue('a', 1, 1);
    const b = dispatcher.enqueue('a', 1, 1);
    jest.advanceTimersByTime(19);
    expect(runner).not.toHaveBeenCalled();
    jest.advanceTimersByTime(1);
    await expect(Promise.all([a, b])).resolves.toEqual(['a', 'a']);
    expect(runner.mock.calls.map(([batch]) => batch.members.length)).toEqual([
      1, 1,
    ]);
    await dispatcher.shutdown(0);
    expect(jest.getTimerCount()).toBe(0);
  } finally {
    jest.useRealTimers();
  }
});

test('cancels queued requests and removes their timers and listeners', async () => {
  jest.useFakeTimers();
  try {
    const controller = new AbortController();
    const remove = jest.spyOn(controller.signal, 'removeEventListener');
    const runner = jest.fn(commit);
    const dispatcher = new GroupDispatcher(
      { maxRequests: 3, maxEvents: 3, maxBytes: 3 },
      20,
      (value: string) => value,
      runner,
    );
    const result = dispatcher.enqueue('a', 1, 1, { signal: controller.signal });
    controller.abort();
    await expect(result).rejects.toMatchObject({ code: 'cancelled' });
    expect(remove).toHaveBeenCalled();
    expect(jest.getTimerCount()).toBe(0);
    expect(runner).not.toHaveBeenCalled();
    await dispatcher.shutdown(0);
  } finally {
    jest.useRealTimers();
  }
});

test('shutdown times out an in-flight group with unknown outcome', async () => {
  jest.useFakeTimers();
  try {
    const runner = jest.fn(() => new Promise<void>(() => undefined));
    const dispatcher = new GroupDispatcher(
      { maxRequests: 1, maxEvents: 1, maxBytes: 1 },
      20,
      (value: string) => value,
      runner,
    );
    const result = dispatcher.enqueue('a', 1, 1);
    const rejected = expect(result).rejects.toMatchObject({ code: 'unknown' });
    await Promise.resolve();
    const shutdown = dispatcher.shutdown(5);
    jest.advanceTimersByTime(5);
    await shutdown;
    await rejected;
    await expect(dispatcher.enqueue('b', 1, 1)).rejects.toMatchObject({
      code: 'closed',
    });
    expect(jest.getTimerCount()).toBe(0);
  } finally {
    jest.useRealTimers();
  }
});

test('rejects waiting requests if a completed runner retains group capacity', async () => {
  const runner = jest.fn((batch: Batch<string, string>) => {
    batch.retain();
    return commit(batch);
  });
  const dispatcher = new GroupDispatcher(
    {
      maxRequests: 2,
      maxEvents: 2,
      maxBytes: 2,
      maxActiveGroups: 1,
      maxRetainedBytes: 4,
    },
    5,
    (value: string) => value,
    runner,
  );
  const a = dispatcher.enqueue('a', 1, 1);
  const b = dispatcher.enqueue('a', 1, 1);
  const rejected = expect(b).rejects.toMatchObject({ code: 'capacity' });
  await expect(a).resolves.toBe('a');
  await rejected;
  await dispatcher.shutdown(0);
});

test('an invalid shutdown timeout does not close or strand the dispatcher', async () => {
  const dispatcher = new GroupDispatcher(
    { maxRequests: 1, maxEvents: 1, maxBytes: 1 },
    10,
    (value: string) => value,
    commit,
  );
  expect(() => dispatcher.shutdown(-1)).toThrow('invalid shutdown timeout');
  await expect(dispatcher.enqueue('a', 1, 1)).resolves.toBe('a');
  await dispatcher.shutdown(0);
});

test('dispatches immediately at the event size limit', async () => {
  const runner = jest.fn((batch: Batch<string, string>) => {
    const attempt = batch.beginAttempt();
    for (const member of batch.members)
      batch.stage(attempt, member, member.value);
    batch.commit(attempt);
    return Promise.resolve();
  });
  const dispatcher = new GroupDispatcher<string, string>(
    { maxRequests: 3, maxEvents: 2, maxBytes: 10 },
    10000,
    (value) => value,
    runner,
  );
  const request = dispatcher.enqueue('a', 2, 1);
  await Promise.resolve();
  expect(runner).toHaveBeenCalledTimes(1);
  await expect(request).resolves.toBe('a');
  await dispatcher.shutdown(0);
});

test('coalesces concurrent distinct streams and drains on shutdown', async () => {
  jest.useFakeTimers();
  try {
    const runner = jest.fn((batch: Batch<string, string>) => {
      const attempt = batch.beginAttempt();
      for (const member of batch.members)
        batch.stage(attempt, member, member.value.toUpperCase());
      batch.commit(attempt);
      return Promise.resolve();
    });
    const dispatcher = new GroupDispatcher<string, string>(
      { maxRequests: 2, maxEvents: 2, maxBytes: 2 },
      10,
      (value) => value,
      runner,
    );
    const a = dispatcher.enqueue('a', 1, 1);
    const b = dispatcher.enqueue('b', 1, 1);
    await expect(Promise.all([a, b])).resolves.toEqual(['A', 'B']);
    expect(runner).toHaveBeenCalledTimes(1);
    expect(runner.mock.calls[0][0].members).toHaveLength(2);
    await dispatcher.shutdown(100);
    expect(jest.getTimerCount()).toBe(0);
  } finally {
    jest.useRealTimers();
  }
});

test('post-seal cancellation settles only that caller unknown while the runner continues', async () => {
  let release!: () => void;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  let batch!: Batch<string, string>;
  const runner = jest.fn(async (value: Batch<string, string>) => {
    batch = value;
    await gate;
    await commit(value);
  });
  const dispatcher = new GroupDispatcher(
    { maxRequests: 2, maxEvents: 2, maxBytes: 2, maxRetainedBytes: 2 },
    100,
    (value: string) => value,
    runner,
  );
  const controller = new AbortController();
  const a = dispatcher.enqueue('a', 1, 1, { signal: controller.signal });
  const b = dispatcher.enqueue('b', 1, 1);
  const aResult = expect(a).rejects.toMatchObject({ code: 'unknown' });
  await Promise.resolve();
  controller.abort();
  await aResult;
  expect(batch.settled).toBe(false);
  await expect(dispatcher.enqueue('c', 1, 1)).rejects.toMatchObject({
    code: 'capacity',
  });
  release();
  await expect(b).resolves.toBe('b');
  expect(batch.members[0].outcome).toEqual({ kind: 'committed', value: 'a' });
  await dispatcher.shutdown(0);
});

test('deadline bounds a sealed caller even while transaction cleanup hangs', async () => {
  jest.useFakeTimers();
  try {
    const dispatcher = new GroupDispatcher(
      { maxRequests: 1, maxEvents: 1, maxBytes: 1 },
      10,
      (value: string) => value,
      async (batch: Batch<string, string>) => {
        batch.retain();
        await commit(batch);
        await new Promise<void>(() => undefined);
      },
    );
    const options = {
      signal: new AbortController().signal,
      deadline: Date.now() + 20,
    };
    const result = dispatcher.enqueue('a', 1, 1, options);
    const rejected = expect(result).rejects.toMatchObject({ code: 'unknown' });
    await Promise.resolve();
    jest.advanceTimersByTime(20);
    await rejected;
    await expect(dispatcher.enqueue('b', 1, 1)).rejects.toMatchObject({
      code: 'capacity',
    });
    const shutdown = dispatcher.shutdown(5);
    jest.advanceTimersByTime(5);
    await shutdown;
  } finally {
    jest.useRealTimers();
  }
});

test('an expired queued deadline is removed before a size-triggered seal', async () => {
  jest.useFakeTimers();
  try {
    const runner = jest.fn(commit);
    const dispatcher = new GroupDispatcher(
      { maxRequests: 2, maxEvents: 2, maxBytes: 2 },
      100,
      (value: string) => value,
      runner,
    );
    const a = dispatcher.enqueue('a', 1, 1, { deadline: Date.now() + 10 });
    const rejected = expect(a).rejects.toMatchObject({ code: 'cancelled' });
    jest.setSystemTime(Date.now() + 11);
    const b = dispatcher.enqueue('b', 1, 1);
    jest.advanceTimersByTime(100);
    await rejected;
    await expect(b).resolves.toBe('b');
    expect(
      runner.mock.calls.flatMap(([batch]) => batch.members.map((m) => m.value)),
    ).toEqual(['b']);
    await dispatcher.shutdown(0);
  } finally {
    jest.useRealTimers();
  }
});

test('a deadline elapsed before runner settlement wins even if its timer is delayed', async () => {
  jest.useFakeTimers();
  try {
    let release!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const dispatcher = new GroupDispatcher(
      { maxRequests: 1, maxEvents: 1, maxBytes: 1 },
      10,
      (value: string) => value,
      async (batch: Batch<string, string>) => {
        await gate;
        await commit(batch);
      },
    );
    const result = dispatcher.enqueue('a', 1, 1, { deadline: Date.now() + 10 });
    const rejected = expect(result).rejects.toMatchObject({ code: 'unknown' });
    await Promise.resolve();
    jest.setSystemTime(Date.now() + 11);
    release();
    await rejected;
    await dispatcher.shutdown(0);
    expect(jest.getTimerCount()).toBe(0);
  } finally {
    jest.useRealTimers();
  }
});
