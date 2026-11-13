import { MongoEventStoreService } from './mongodb-event-store.service';
import { DeleteReq } from '../interfaces/streams';

let savedExitCode: typeof process.exitCode;
beforeEach(() => {
  savedExitCode = process.exitCode;
});
afterEach(() => {
  process.exitCode = savedExitCode;
});

test('diagnostic failure cannot change an unconfirmed write outcome', async () => {
  const service = new MongoEventStoreService();
  const diagnostic = jest.spyOn(console, 'error').mockImplementation(() => {
    throw new Error('logger');
  });
  jest.spyOn(service['infrastructure'], 'getClient').mockReturnValue({
    db: () => ({}),
    startSession: () => ({
      withTransaction: () => Promise.resolve({ noPosition: {} }),
      endSession: () => Promise.reject(new Error('cleanup')),
    }),
  } as never);
  try {
    await expect(
      service.delete({
        options: {
          streamIdentifier: { streamName: Buffer.from('s') },
          any: {},
        },
      } as DeleteReq),
    ).rejects.toMatchObject({ code: 'unknown' });
  } finally {
    diagnostic.mockRestore();
  }
});

test('failed cleanup after a completed transaction reports unknown and retains ownership', async () => {
  const service = new MongoEventStoreService();
  const error = new Error('cleanup failed');
  const diagnostic = jest.spyOn(console, 'error').mockImplementation();
  const transaction = jest.fn().mockResolvedValue({ noPosition: {} });
  jest.spyOn(service['infrastructure'], 'getClient').mockReturnValue({
    db: () => ({}),
    startSession: () => ({
      withTransaction: transaction,
      endSession: () => Promise.reject(error),
    }),
  } as never);
  const outcome = { cleanup: 'complete' as 'complete' | 'unconfirmed' };
  const request = {
    options: { streamIdentifier: { streamName: Buffer.from('s') }, any: {} },
  } as DeleteReq;
  await expect(
    service.delete(request, undefined, outcome),
  ).rejects.toMatchObject({
    code: 'unknown',
    cause: error,
  });
  expect(outcome.cleanup).toBe('unconfirmed');
  expect(transaction).toHaveBeenCalledTimes(1);
  expect(service['liveOrdinary'].size).toBe(1);
  expect(diagnostic).toHaveBeenCalledWith(
    expect.stringContaining('cleanup'),
    error,
  );
  diagnostic.mockRestore();
});

test('shutdown bounds a hung client close and retains the pending close', async () => {
  jest.useFakeTimers();
  const service = new MongoEventStoreService();
  let finish!: () => void;
  const close = jest
    .spyOn(service['infrastructure'], 'close')
    .mockImplementation(
      () =>
        new Promise<void>((resolve) => {
          finish = resolve;
        }),
    );
  const diagnostic = jest.spyOn(console, 'error').mockImplementation();
  try {
    const shutdown = service.onModuleDestroy();
    await jest.advanceTimersByTimeAsync(1100);
    await expect(shutdown).resolves.toBeUndefined();
    expect(close).toHaveBeenCalledTimes(1);
    expect(diagnostic).toHaveBeenCalledWith(expect.stringContaining('close'));
    const pendingClose: unknown = Reflect.get(service, 'closingClient');
    expect(pendingClose).toBeInstanceOf(Promise);
    finish();
    await pendingClose;
  } finally {
    jest.useRealTimers();
    diagnostic.mockRestore();
    close.mockRestore();
  }
});

test('failed session creation releases admission for the next delete', async () => {
  const service = new MongoEventStoreService();
  const { TransactionAdmission } = await import('./transaction-admission');
  Object.defineProperty(service, 'admission', {
    value: new TransactionAdmission(1, 1024 * 1024),
  });
  const startSession = jest
    .fn()
    .mockImplementationOnce(() => {
      throw new Error('session failed');
    })
    .mockReturnValue({
      withTransaction: () => Promise.resolve({ noPosition: {} }),
      endSession: () => Promise.resolve(),
    });
  jest
    .spyOn(service['infrastructure'], 'getClient')
    .mockReturnValue({ db: () => ({}), startSession } as never);
  const request = {
    options: { streamIdentifier: { streamName: Buffer.from('s') }, any: {} },
  } as DeleteReq;
  await expect(service.delete(request)).rejects.toThrow('session failed');
  await expect(service.delete(request)).resolves.toEqual({ noPosition: {} });
});

test('disabled admission does not serialize write requests', async () => {
  const service = new MongoEventStoreService();
  const session = {
    withTransaction: () => Promise.resolve({ noPosition: {} }),
    endSession: () => Promise.resolve(),
  };
  jest
    .spyOn(service['infrastructure'], 'getClient')
    .mockReturnValue({ db: () => ({}), startSession: () => session } as never);
  const request = {
    options: { streamIdentifier: { streamName: Buffer.from('s') }, any: {} },
    toJSON: () => {
      throw new Error('serialized');
    },
  } as unknown as DeleteReq;
  await expect(service.delete(request)).resolves.toEqual({ noPosition: {} });
});

test('oversized admitted write fails before serializing payload', async () => {
  const service = new MongoEventStoreService();
  const { TransactionAdmission } = await import('./transaction-admission');
  Object.defineProperty(service, 'admission', {
    value: new TransactionAdmission(1, 16 * 1024 * 1024),
  });
  const serialize = jest.fn(() => {
    throw new Error('serialized');
  });
  const payload = Buffer.alloc(17 * 1024 * 1024);
  payload.toJSON = serialize;
  const request = {
    options: { streamIdentifier: { streamName: payload }, any: {} },
  } as DeleteReq;
  await expect(service.delete(request)).rejects.toMatchObject({
    code: 'capacity',
  });
  expect(serialize).not.toHaveBeenCalled();
});

test('ordinary queued writes charge distinct full backing allocations', async () => {
  const service = new MongoEventStoreService();
  const { TransactionAdmission } = await import('./transaction-admission');
  Object.defineProperty(service, 'admission', {
    value: new TransactionAdmission(4, 12 * 1024 * 1024),
  });
  let finish!: () => void;
  const gate = new Promise<void>((resolve) => {
    finish = resolve;
  });
  jest.spyOn(service['infrastructure'], 'getClient').mockReturnValue({
    db: () => ({}),
    startSession: () => ({
      withTransaction: async () => {
        await gate;
        return { noPosition: {} };
      },
      endSession: async () => {},
    }),
  } as never);
  const request = () =>
    ({
      options: {
        streamIdentifier: {
          streamName: new Uint8Array(new ArrayBuffer(8 * 1024 * 1024), 0, 1),
        },
        any: {},
      },
    }) as DeleteReq;
  const first = service.delete(request());
  await expect(service.delete(request())).rejects.toMatchObject({
    code: 'capacity',
  });
  finish();
  await first;
});

test.each([false, true])(
  'shutdown leaves client open for cancelled live ordinary write (admission=%s)',
  async (enabled) => {
    const service = new MongoEventStoreService();
    if (enabled) {
      const { TransactionAdmission } = await import('./transaction-admission');
      Object.defineProperty(service, 'admission', {
        value: new TransactionAdmission(4, 100000),
      });
    }
    let finish!: () => void;
    let started!: () => void;
    const gate = new Promise<void>((resolve) => {
      finish = resolve;
    });
    const running = new Promise<void>((resolve) => {
      started = resolve;
    });
    jest.spyOn(service['infrastructure'], 'getClient').mockReturnValue({
      db: () => ({}),
      startSession: () => ({
        withTransaction: async () => {
          started();
          await gate;
          return { noPosition: {} };
        },
        endSession: async () => {},
      }),
    } as never);
    const close = jest
      .spyOn(service['infrastructure'], 'close')
      .mockResolvedValue();
    const controller = new AbortController();
    const request = {
      options: { streamIdentifier: { streamName: Buffer.from('s') }, any: {} },
    } as DeleteReq;
    const write = service.delete(request, { signal: controller.signal });
    await running;
    controller.abort();
    await expect(write).rejects.toMatchObject({ code: 'unknown' });
    await service.onModuleDestroy();
    expect(close).not.toHaveBeenCalled();
    await expect(service.delete(request)).rejects.toMatchObject({
      code: 'closed',
    });
    finish();
  },
);

test.each([false, true])(
  'shutdown leaves client open while ordinary session cleanup is gated (admission=%s)',
  async (enabled) => {
    const service = new MongoEventStoreService();
    if (enabled) {
      const { TransactionAdmission } = await import('./transaction-admission');
      Object.defineProperty(service, 'admission', {
        value: new TransactionAdmission(4, 1024 * 1024),
      });
    }
    let finish!: () => void;
    let started!: () => void;
    const gate = new Promise<void>((resolve) => {
      finish = resolve;
    });
    const cleaning = new Promise<void>((resolve) => {
      started = resolve;
    });
    jest.spyOn(service['infrastructure'], 'getClient').mockReturnValue({
      db: () => ({}),
      startSession: () => ({
        withTransaction: () => Promise.resolve({ noPosition: {} }),
        endSession: async () => {
          started();
          await gate;
        },
      }),
    } as never);
    const close = jest
      .spyOn(service['infrastructure'], 'close')
      .mockResolvedValue();
    const controller = new AbortController();
    const request = {
      options: { streamIdentifier: { streamName: Buffer.from('s') }, any: {} },
    } as DeleteReq;
    const write = service.delete(request, { signal: controller.signal });
    await cleaning;
    controller.abort();
    await expect(write).rejects.toMatchObject({ code: 'unknown' });
    await service.onModuleDestroy();
    expect(close).not.toHaveBeenCalled();
    finish();
  },
);

test('unconfirmed cleanup never permits a late client close', async () => {
  jest.useFakeTimers();
  const service = new MongoEventStoreService();
  jest.spyOn(service['infrastructure'], 'getClient').mockReturnValue({
    db: () => ({}),
    startSession: () => ({
      withTransaction: () => Promise.resolve({ noPosition: {} }),
      endSession: () => Promise.reject(new Error('unconfirmed')),
    }),
  } as never);
  const close = jest
    .spyOn(service['infrastructure'], 'close')
    .mockResolvedValue();
  const diagnostic = jest.spyOn(console, 'error').mockImplementation();
  try {
    await expect(
      service.delete({
        options: {
          streamIdentifier: { streamName: Buffer.from('s') },
          any: {},
        },
      } as DeleteReq),
    ).rejects.toMatchObject({ code: 'unknown' });
    const shutdown = service.onModuleDestroy();
    await jest.advanceTimersByTimeAsync(5000);
    await shutdown;
    expect(close).not.toHaveBeenCalled();
  } finally {
    diagnostic.mockRestore();
    jest.useRealTimers();
  }
});

test('late confirmed cleanup closes the Mongo client exactly once', async () => {
  jest.useFakeTimers();
  const service = new MongoEventStoreService();
  let finish!: () => void;
  let started!: () => void;
  const gate = new Promise<void>((resolve) => {
    finish = resolve;
  });
  const cleaning = new Promise<void>((resolve) => {
    started = resolve;
  });
  jest.spyOn(service['infrastructure'], 'getClient').mockReturnValue({
    db: () => ({}),
    startSession: () => ({
      withTransaction: () => Promise.resolve({ noPosition: {} }),
      endSession: async () => {
        started();
        await gate;
      },
    }),
  } as never);
  const close = jest
    .spyOn(service['infrastructure'], 'close')
    .mockResolvedValue();
  const diagnostic = jest.spyOn(console, 'error').mockImplementation();
  try {
    const write = service.delete({
      options: { streamIdentifier: { streamName: Buffer.from('s') }, any: {} },
    } as DeleteReq);
    await cleaning;
    const shutdown = service.onModuleDestroy();
    await jest.advanceTimersByTimeAsync(1100);
    await shutdown;
    expect(close).not.toHaveBeenCalled();
    finish();
    await write;
    await jest.advanceTimersByTimeAsync(0);
    expect(close).toHaveBeenCalledTimes(1);
  } finally {
    diagnostic.mockRestore();
    jest.useRealTimers();
  }
});

test('ordinary delete exposes successful session cleanup', async () => {
  const service = new MongoEventStoreService();
  const session = {
    withTransaction: () => Promise.resolve({ noPosition: {} }),
    endSession: () => Promise.resolve(),
  };
  jest
    .spyOn(service['infrastructure'], 'getClient')
    .mockReturnValue({ db: () => ({}), startSession: () => session } as never);
  const outcome = { cleanup: 'unconfirmed' as 'complete' | 'unconfirmed' };
  const request = {
    options: { streamIdentifier: { streamName: Buffer.from('s') }, any: {} },
  } as DeleteReq;
  await service.delete(request, undefined, outcome);
  expect(outcome.cleanup).toBe('complete');
});
