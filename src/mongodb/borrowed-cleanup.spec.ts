import { ClientSession, MongoClient } from 'mongodb';
import { CooperativeDispatcher } from './cooperative-dispatcher';
import { runMongoGroup } from './group-runner';
import { TransactionAdmission } from './transaction-admission';

test('shutdown during borrowed cleanup returns unknown, not the committed result', async () => {
  let entered!: () => void;
  let finish!: () => void;
  const ready = new Promise<void>((resolve) => {
    entered = resolve;
  });
  const gate = new Promise<void>((resolve) => {
    finish = resolve;
  });
  const session = {
    startTransaction: () => undefined,
    commitTransaction: () => Promise.resolve(),
    endSession: () => {
      entered();
      return gate;
    },
  } as unknown as ClientSession;
  const client = { startSession: () => session } as MongoClient;
  const dispatcher = new CooperativeDispatcher<string, string>(
    { maxRequests: 1, maxEvents: 1, maxBytes: 1 },
    0,
    (v) => v,
    (finalize, ownership) => {
      const batch = finalize()!;
      return runMongoGroup(
        client,
        batch,
        (v) => Promise.resolve(v),
        { maxAttempts: 1, maxCommitAttempts: 1 },
        ownership(),
      );
    },
  );
  const result = dispatcher.enqueue('a', 1, 1);
  const rejected = expect(result).rejects.toMatchObject({ code: 'unknown' });
  try {
    await ready;
    await dispatcher.shutdown(0);
    await rejected;
  } finally {
    finish();
  }
});

test.each(['failure', 'hung'] as const)(
  'borrowed %s cleanup retains admission and never reports abort',
  async (mode) => {
    let entered!: () => void;
    const ready = new Promise<void>((resolve) => {
      entered = resolve;
    });
    let finish!: () => void;
    const gate = new Promise<void>((resolve) => {
      finish = resolve;
    });
    const session = {
      startTransaction: () => undefined,
      inTransaction: () => true,
      abortTransaction: () => Promise.resolve(),
      endSession: () => {
        entered();
        return mode === 'failure' ? Promise.reject(new Error('cleanup')) : gate;
      },
    } as unknown as ClientSession;
    const client = { startSession: () => session } as MongoClient;
    const admission = new TransactionAdmission(2, 2);
    const report = jest
      .spyOn(console, 'error')
      .mockImplementation(() => undefined);
    const dispatcher = new CooperativeDispatcher<string, string>(
      { maxRequests: 1, maxEvents: 1, maxBytes: 1 },
      0,
      (v) => v,
      async (finalize, ownership) => {
        const release = await admission.acquire(1);
        const batch = finalize();
        if (!batch) {
          release();
          return;
        }
        const outcome = await runMongoGroup(
          client,
          batch,
          () => Promise.reject(new Error('work')),
          { maxAttempts: 1, maxCommitAttempts: 1 },
          ownership(),
        );
        if (outcome.cleanup === 'complete') release();
        return outcome;
      },
    );
    try {
      const result = dispatcher.enqueue('a', 1, 1);
      const rejected = expect(result).rejects.toMatchObject({
        code: 'unknown',
      });
      await ready;
      const controller = new AbortController();
      let acquired = false;
      const contender = admission
        .acquire(1, { signal: controller.signal })
        .then(
          (release) => {
            acquired = true;
            release();
          },
          () => undefined,
        );
      await new Promise<void>((resolve) => setImmediate(resolve));
      expect(acquired).toBe(false);
      await dispatcher.shutdown(0);
      await rejected;
      expect(acquired).toBe(false);
      controller.abort();
      await contender;
    } finally {
      finish();
      report.mockRestore();
    }
  },
);
