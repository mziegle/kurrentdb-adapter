import { MongoEventStoreService } from './mongodb-event-store.service';
import { ReadReq } from '../interfaces/streams';

test('shutdown leaves Mongo open while a subscription transaction is live', async () => {
  jest.useFakeTimers();
  const originalExitCode = process.exitCode;
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
      return { rows: [], visibleRevision: -1 };
    },
    endSession: async () => {},
  };
  jest.spyOn(service['infrastructure'], 'getClient').mockReturnValue({
    db: () => ({ collection: () => ({}) }),
    startSession: () => session,
  } as never);
  const close = jest
    .spyOn(service['infrastructure'], 'close')
    .mockResolvedValue();
  const diagnostic = jest.spyOn(console, 'error').mockImplementation();
  const request = {
    options: { stream: { streamIdentifier: { streamName: Buffer.from('s') } } },
  } as unknown as ReadReq;
  const iterator = service.subscribeToStream(request, () => false);
  try {
    await iterator.next();
    const pending = iterator.next();
    await started;
    const shutdown = service.onModuleDestroy();
    await jest.advanceTimersByTimeAsync(1100);
    await shutdown;
    expect(process.exitCode).toBe(1);
    expect(close).not.toHaveBeenCalled();
    release();
    await pending;
    await iterator.return(undefined);
    await jest.advanceTimersByTimeAsync(0);
    expect(close).toHaveBeenCalledTimes(1);
  } finally {
    release();
    process.exitCode = originalExitCode;
    diagnostic.mockRestore();
    jest.useRealTimers();
  }
});
