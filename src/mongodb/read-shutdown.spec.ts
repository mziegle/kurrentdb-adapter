import { MongoEventStoreService } from './mongodb-event-store.service';
import { ReadReq } from '../interfaces/streams';

let savedExitCode: typeof process.exitCode;
beforeEach(() => {
  savedExitCode = process.exitCode;
  process.exitCode = 0;
});
afterEach(() => {
  process.exitCode = savedExitCode;
});

test.each([
  [999, 0],
  [1000, 1],
])('drain completion at %i ms leaves exit code %i', async (delay, code) => {
  jest.useFakeTimers();
  const service = new MongoEventStoreService();
  let release!: () => void;
  const work = new Promise<void>((resolve) => {
    release = resolve;
  });
  service['liveReads'].add(work);
  const close = jest
    .spyOn(service['infrastructure'], 'close')
    .mockResolvedValue();
  const diagnostic = jest.spyOn(console, 'error').mockImplementation();
  try {
    const shutdown = service.onModuleDestroy();
    setTimeout(release, delay);
    await jest.advanceTimersByTimeAsync(1000);
    await shutdown;
    await jest.advanceTimersByTimeAsync(0);
    expect(close).toHaveBeenCalledTimes(1);
    expect(process.exitCode).toBe(code);
  } finally {
    release();
    diagnostic.mockRestore();
    jest.useRealTimers();
  }
});

test('shutdown rejects when client close definitely rejects', async () => {
  const service = new MongoEventStoreService();
  jest
    .spyOn(service['infrastructure'], 'close')
    .mockRejectedValue(new Error('close failed'));
  await expect(service.onModuleDestroy()).rejects.toThrow('close failed');
});

test('late client close rejection marks shutdown failure after bounded timeout', async () => {
  jest.useFakeTimers();
  const originalExitCode = process.exitCode;
  const diagnostic = jest.spyOn(console, 'error').mockImplementation();
  const service = new MongoEventStoreService();
  let rejectClose!: (error: Error) => void;
  jest.spyOn(service['infrastructure'], 'close').mockReturnValue(
    new Promise<void>((_, reject) => {
      rejectClose = reject;
    }),
  );
  try {
    const shutdown = service.onModuleDestroy();
    await jest.advanceTimersByTimeAsync(1001);
    await shutdown;
    expect(process.exitCode).toBe(1);
    rejectClose(new Error('late close failure'));
    await jest.advanceTimersByTimeAsync(0);
    expect(process.exitCode).toBe(1);
  } finally {
    process.exitCode = originalExitCode;
    diagnostic.mockRestore();
    jest.useRealTimers();
  }
});

test('shutdown waits for a live read session to finish cleanup before closing', async () => {
  jest.useFakeTimers();
  const service = new MongoEventStoreService();
  let release!: () => void;
  let entered!: () => void;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  const started = new Promise<void>((resolve) => {
    entered = resolve;
  });
  const session = {
    withTransaction: async () => {
      entered();
      await gate;
      return [];
    },
    endSession: async () => {},
  };
  jest.spyOn(service['infrastructure'], 'getClient').mockReturnValue({
    db: () => ({}),
    startSession: () => session,
  } as never);
  const close = jest
    .spyOn(service['infrastructure'], 'close')
    .mockResolvedValue();
  const diagnostic = jest.spyOn(console, 'error').mockImplementation();
  try {
    const read = service.read({
      options: {
        stream: { streamIdentifier: { streamName: Buffer.from('s') } },
      },
    } as unknown as ReadReq);
    await started;
    const shutdown = service.onModuleDestroy();
    await jest.advanceTimersByTimeAsync(1100);
    await shutdown;
    expect(close).not.toHaveBeenCalled();
    release();
    await read;
    await jest.advanceTimersByTimeAsync(0);
    expect(close).toHaveBeenCalledTimes(1);
  } finally {
    release();
    diagnostic.mockRestore();
    jest.useRealTimers();
  }
});

test('hung session cleanup defers close until cleanup finishes', async () => {
  jest.useFakeTimers();
  const service = new MongoEventStoreService();
  let finish!: () => void;
  let entered!: () => void;
  const started = new Promise<void>((resolve) => {
    entered = resolve;
  });
  const gate = new Promise<void>((resolve) => {
    finish = resolve;
  });
  jest.spyOn(service['infrastructure'], 'getClient').mockReturnValue({
    db: () => ({}),
    startSession: () => ({
      withTransaction: () => Promise.resolve([]),
      endSession: async () => {
        entered();
        await gate;
      },
    }),
  } as never);
  const close = jest
    .spyOn(service['infrastructure'], 'close')
    .mockResolvedValue();
  const diagnostic = jest.spyOn(console, 'error').mockImplementation();
  try {
    const read = service.read({
      options: {
        stream: { streamIdentifier: { streamName: Buffer.from('s') } },
      },
    } as unknown as ReadReq);
    await started;
    const shutdown = service.onModuleDestroy();
    await jest.advanceTimersByTimeAsync(1100);
    await shutdown;
    expect(close).not.toHaveBeenCalled();
    finish();
    await read;
    await jest.advanceTimersByTimeAsync(0);
    expect(close).toHaveBeenCalledTimes(1);
  } finally {
    finish();
    diagnostic.mockRestore();
    jest.useRealTimers();
  }
});

test('failed session cleanup keeps the client open after bounded shutdown', async () => {
  jest.useFakeTimers();
  const service = new MongoEventStoreService();
  jest.spyOn(service['infrastructure'], 'getClient').mockReturnValue({
    db: () => ({}),
    startSession: () => ({
      withTransaction: () => Promise.resolve([]),
      endSession: () => Promise.reject(new Error('cleanup failed')),
    }),
  } as never);
  const close = jest
    .spyOn(service['infrastructure'], 'close')
    .mockResolvedValue();
  const diagnostic = jest.spyOn(console, 'error').mockImplementation();
  try {
    await expect(
      service.read({
        options: {
          stream: { streamIdentifier: { streamName: Buffer.from('s') } },
        },
      } as unknown as ReadReq),
    ).rejects.toThrow('cleanup failed');
    const shutdown = service.onModuleDestroy();
    await jest.advanceTimersByTimeAsync(1100);
    await shutdown;
    expect(close).not.toHaveBeenCalled();
  } finally {
    diagnostic.mockRestore();
    jest.useRealTimers();
  }
});
