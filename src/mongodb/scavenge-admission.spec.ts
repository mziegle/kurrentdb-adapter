import { MongoEventStoreService } from './mongodb-event-store.service';
import { TransactionAdmission } from './transaction-admission';

let savedExitCode: typeof process.exitCode;
beforeEach(() => {
  savedExitCode = process.exitCode;
});
afterEach(() => {
  process.exitCode = savedExitCode;
});

test('shutdown does not close Mongo when admitted work has not drained', async () => {
  jest.useFakeTimers();
  try {
    const service = new MongoEventStoreService();
    const admission = new TransactionAdmission(2, 100);
    Object.defineProperty(service, 'admission', { value: admission });
    const release = await admission.acquire(1);
    const close = jest
      .spyOn(service['infrastructure'], 'close')
      .mockResolvedValue();
    const shutdown = service.onModuleDestroy();
    await jest.advanceTimersByTimeAsync(2500);
    await shutdown;
    expect(close).not.toHaveBeenCalled();
    release();
  } finally {
    jest.useRealTimers();
  }
});

test('shutdown leaves Mongo open while a cancelled scavenge transaction or cleanup is live', async () => {
  jest.useFakeTimers();
  try {
    const service = new MongoEventStoreService();
    Object.defineProperty(service, 'scavengeEnabled', { value: true });
    const admission = new TransactionAdmission(2, 100);
    jest.spyOn(admission, 'shutdown').mockResolvedValue();
    Object.defineProperty(service, 'admission', { value: admission });
    let finishTransaction!: () => void;
    let finishCleanup!: () => void;
    const transaction = new Promise<void>((resolve) => {
      finishTransaction = resolve;
    });
    const cleanup = new Promise<void>((resolve) => {
      finishCleanup = resolve;
    });
    const session = {
      withTransaction: () => transaction,
      endSession: () => cleanup,
    };
    const cursor = {
      close: jest.fn().mockResolvedValue(undefined),
      async *[Symbol.asyncIterator]() {
        await Promise.resolve();
        yield { _id: 's' };
      },
    };
    jest.spyOn(service['infrastructure'], 'getClient').mockReturnValue({
      db: () => ({ collection: () => ({ find: () => cursor }) }),
      startSession: () => session,
    } as never);
    const close = jest
      .spyOn(service['infrastructure'], 'close')
      .mockResolvedValue();
    service.startScavenge();
    await jest.advanceTimersByTimeAsync(0);
    const shutdown = service.onModuleDestroy();
    await jest.advanceTimersByTimeAsync(2500);
    await shutdown;
    expect(close).not.toHaveBeenCalled();
    finishTransaction();
    await jest.advanceTimersByTimeAsync(0);
    expect(close).not.toHaveBeenCalled();
    finishCleanup();
    await jest.advanceTimersByTimeAsync(0);
  } finally {
    jest.useRealTimers();
  }
});

test('shutdown is bounded when a scavenge cursor stalls and does not close live resources', async () => {
  jest.useFakeTimers();
  try {
    const service = new MongoEventStoreService();
    Object.defineProperty(service, 'scavengeEnabled', { value: true });
    const cursor = {
      close: jest.fn().mockResolvedValue(undefined),
      [Symbol.asyncIterator]() {
        return { next: () => new Promise(() => {}) };
      },
    };
    jest.spyOn(service['infrastructure'], 'getClient').mockReturnValue({
      db: () => ({ collection: () => ({ find: () => cursor }) }),
    } as never);
    const close = jest
      .spyOn(service['infrastructure'], 'close')
      .mockResolvedValue();
    service.startScavenge();
    let finished = false;
    const shutdown = service.onModuleDestroy().then(() => {
      finished = true;
    });
    await jest.advanceTimersByTimeAsync(1500);
    expect(finished).toBe(true);
    expect(close).not.toHaveBeenCalled();
    await shutdown;
  } finally {
    jest.useRealTimers();
  }
});

test('failed scavenge cursor cleanup never authorizes client closure', async () => {
  jest.useFakeTimers();
  const service = new MongoEventStoreService();
  Object.defineProperty(service, 'scavengeEnabled', { value: true });
  const diagnostic = jest.spyOn(console, 'error').mockImplementation();
  const close = jest
    .spyOn(service['infrastructure'], 'close')
    .mockResolvedValue();
  jest.spyOn(service['infrastructure'], 'getClient').mockReturnValue({
    db: () => ({
      collection: () => ({
        find: () => ({
          async *[Symbol.asyncIterator]() {
            await Promise.resolve();
            yield { _id: 's' };
          },
          close: () => Promise.reject(new Error('cursor close unconfirmed')),
        }),
      }),
    }),
  } as never);
  try {
    service.startScavenge();
    const shutdown = expect(service.onModuleDestroy()).rejects.toThrow(
      'Scavenge cleanup unconfirmed',
    );
    await jest.advanceTimersByTimeAsync(2000);
    await shutdown;
    expect(close).not.toHaveBeenCalled();
  } finally {
    diagnostic.mockRestore();
    jest.useRealTimers();
  }
});

test('stopping a scavenge aborts the cursor operation', async () => {
  const service = new MongoEventStoreService();
  Object.defineProperty(service, 'scavengeEnabled', { value: true });
  let signal: AbortSignal | undefined;
  const cursor = {
    close: jest.fn().mockResolvedValue(undefined),
    // The blocked iterator deliberately produces no documents.
    // eslint-disable-next-line require-yield
    async *[Symbol.asyncIterator]() {
      await new Promise<void>((resolve) => {
        signal?.addEventListener('abort', () => resolve(), { once: true });
      });
    },
  };
  jest.spyOn(service['infrastructure'], 'getClient').mockReturnValue({
    db: () => ({
      collection: () => ({
        find: (_query: unknown, options: { signal?: AbortSignal }) => {
          signal = options.signal;
          return cursor;
        },
      }),
    }),
  } as never);
  const result = service.startScavenge();
  const task = service['scavenges'].get(result.scavengeId)!.task!;
  service.stopScavenge({ options: { scavengeId: result.scavengeId } });
  expect(signal?.aborted).toBe(true);
  await task;
  expect(cursor.close).toHaveBeenCalled();
});

test('stopping a scavenge removes its admission waiter and closes its cursor', async () => {
  const service = new MongoEventStoreService();
  const admission = new TransactionAdmission(2, 100);
  Object.defineProperty(service, 'admission', { value: admission });
  Object.defineProperty(service, 'scavengeEnabled', { value: true });
  const release = await admission.acquire(1);
  const close = jest.fn().mockResolvedValue(undefined);
  const cursor = {
    close,
    async *[Symbol.asyncIterator]() {
      await Promise.resolve();
      yield { _id: 's' };
    },
  };
  jest.spyOn(service['infrastructure'], 'getClient').mockReturnValue({
    db: () => ({ collection: () => ({ find: () => cursor }) }),
  } as never);
  const result = service.startScavenge();
  await new Promise((resolve) => setImmediate(resolve));
  const task = service['scavenges'].get(result.scavengeId)!.task!;
  service.stopScavenge({ options: { scavengeId: result.scavengeId } });
  const settled = await Promise.race([
    task.then(() => true),
    new Promise<boolean>((resolve) => setTimeout(() => resolve(false), 100)),
  ]);
  expect(settled).toBe(true);
  expect(close).toHaveBeenCalled();
  release();
});
