import { ClientSession, MongoClient, MongoError } from 'mongodb';
import { GroupQueue } from './group-commit';
import { runMongoGroup } from './group-runner';
import * as runner from './group-runner';

test.each(['abort', 'session', 'inactive', 'commit'] as const)(
  'does not certify %s failure or uncertainty',
  async (phase) => {
    const queue = new GroupQueue<string, string>({
      maxRequests: 1,
      maxEvents: 1,
      maxBytes: 1,
    });
    queue.enqueue('a', 1, 1);
    const batch = queue.seal(1);
    const session = {
      startTransaction: jest.fn(),
      inTransaction: () => phase !== 'inactive',
      abortTransaction:
        phase === 'abort'
          ? jest.fn().mockRejectedValue(new Error('abort'))
          : jest.fn().mockResolvedValue(undefined),
      endSession:
        phase === 'session'
          ? jest.fn().mockRejectedValue(new Error('session'))
          : jest.fn().mockResolvedValue(undefined),
      commitTransaction: jest.fn().mockRejectedValue(new Error('commit')),
    } as unknown as ClientSession;
    const report = jest
      .spyOn(console, 'error')
      .mockImplementation(() => undefined);
    try {
      const error: unknown = await runMongoGroup(
        { startSession: () => session } as MongoClient,
        batch,
        (value) =>
          phase === 'commit'
            ? Promise.resolve(value)
            : Promise.reject(new Error('work')),
        { maxAttempts: 1, maxCommitAttempts: 1 },
      ).catch((e: unknown) => e);
      expect(runner.isDefinitiveGroupAbort(error, batch)).toBe(false);
    } finally {
      report.mockRestore();
    }
  },
);

test('does not expose evidence while abort or session cleanup is pending', async () => {
  const queue = new GroupQueue<string, string>({
    maxRequests: 1,
    maxEvents: 1,
    maxBytes: 1,
  });
  const member = queue.enqueue('a', 1, 1)!;
  const batch = queue.seal(1);
  let releaseAbort!: () => void;
  let releaseSession!: () => void;
  const abortGate = new Promise<void>((resolve) => {
    releaseAbort = resolve;
  });
  const sessionGate = new Promise<void>((resolve) => {
    releaseSession = resolve;
  });
  const session = {
    startTransaction: jest.fn(),
    inTransaction: () => true,
    abortTransaction: jest.fn(() => abortGate),
    endSession: jest.fn(() => sessionGate),
  } as unknown as ClientSession;
  let finished = false;
  const result = runMongoGroup(
    { startSession: () => session } as MongoClient,
    batch,
    () => Promise.reject(new Error('work')),
    { maxAttempts: 1, maxCommitAttempts: 1 },
  ).catch((e: unknown) => {
    finished = true;
    return e;
  });
  await new Promise((resolve) => setImmediate(resolve));
  expect(member.outcome).toEqual({ kind: 'aborted' });
  expect(finished).toBe(false);
  expect(queue.activeGroups).toBe(1);
  releaseAbort();
  await new Promise((resolve) => setImmediate(resolve));
  expect(finished).toBe(false);
  expect(queue.activeGroups).toBe(1);
  releaseSession();
  const error = await result;
  expect(runner.isDefinitiveGroupAbort(error, batch)).toBe(false);
});

test('shutdown timeout during abort prevents definitive evidence', async () => {
  const queue = new GroupQueue<string, string>({
    maxRequests: 1,
    maxEvents: 1,
    maxBytes: 1,
  });
  queue.enqueue('a', 1, 1);
  const batch = queue.seal(1);
  let release!: () => void;
  let entered!: () => void;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  const ready = new Promise<void>((resolve) => {
    entered = resolve;
  });
  const session = {
    startTransaction: jest.fn(),
    inTransaction: () => true,
    abortTransaction: jest.fn(() => {
      entered();
      return gate;
    }),
    endSession: jest.fn().mockResolvedValue(undefined),
  } as unknown as ClientSession;
  const result = runMongoGroup(
    { startSession: () => session } as MongoClient,
    batch,
    () => Promise.reject(new Error('work')),
    { maxAttempts: 1, maxCommitAttempts: 1 },
  ).catch((e: unknown) => e);
  await ready;
  await queue.shutdown(0);
  release();
  const error = await result;
  expect(runner.isDefinitiveGroupAbort(error, batch)).toBe(false);
});

test('does not certify an unmonitored mock abort or forged error', async () => {
  const queue = new GroupQueue<string, string>({
    maxRequests: 1,
    maxEvents: 1,
    maxBytes: 1,
  });
  const member = queue.enqueue('a', 1, 1)!;
  const batch = queue.seal(1);
  const session = {
    startTransaction: jest.fn(),
    inTransaction: () => true,
    abortTransaction: jest.fn().mockResolvedValue(undefined),
    endSession: jest.fn().mockResolvedValue(undefined),
  } as unknown as ClientSession;
  const error: unknown = await runMongoGroup(
    { startSession: () => session } as MongoClient,
    batch,
    () => Promise.reject(new Error('work')),
    { maxAttempts: 1, maxCommitAttempts: 1 },
  ).catch((e: unknown) => e);
  const predicate = Reflect.get(runner, 'isDefinitiveGroupAbort') as
    | ((error: unknown, batch: unknown) => boolean)
    | undefined;
  expect(predicate).toBeDefined();
  expect(predicate!(error, batch)).toBe(false);
  expect(predicate!({ outcome: 'aborted' }, batch)).toBe(false);
  expect(predicate!(error, {})).toBe(false);
  expect(member.outcome).toEqual({ kind: 'aborted' });
  expect(queue.activeGroups).toBe(0);
});

test('preserves acknowledged commit when session cleanup fails', async () => {
  const queue = new GroupQueue<string, string>({
    maxRequests: 1,
    maxEvents: 1,
    maxBytes: 1,
  });
  const member = queue.enqueue('a', 1, 1)!;
  const endSession = jest.fn().mockRejectedValue(new Error('cleanup failed'));
  const session = {
    startTransaction: jest.fn(),
    commitTransaction: jest.fn().mockResolvedValue(undefined),
    endSession,
  } as unknown as ClientSession;
  const client = { startSession: () => session } as MongoClient;
  await runMongoGroup(
    client,
    queue.seal(1),
    (value) => Promise.resolve(value),
    {
      maxAttempts: 1,
      maxCommitAttempts: 1,
    },
  );
  expect(member.outcome).toEqual({ kind: 'committed', value: 'a' });
  expect(endSession).toHaveBeenCalledTimes(1);
});

test('invalid limits abort the sealed batch and release capacity', async () => {
  const queue = new GroupQueue<string, string>({
    maxRequests: 1,
    maxEvents: 1,
    maxBytes: 1,
  });
  const member = queue.enqueue('a', 1, 1)!;
  const startSession = jest.fn();
  const client = { startSession } as unknown as MongoClient;
  await expect(
    runMongoGroup(client, queue.seal(1), (value) => Promise.resolve(value), {
      maxAttempts: 0,
      maxCommitAttempts: 1,
    }),
  ).rejects.toMatchObject({
    outcome: 'aborted',
    cause: { message: 'invalid retry limit' },
  });
  expect(member.outcome).toEqual({ kind: 'aborted' });
  expect(startSession).not.toHaveBeenCalled();
  expect(queue.enqueue('b', 1, 1)).toBeDefined();
});

test.each(['work', 'commit'] as const)(
  'preserves %s failure and classification when cleanup fails',
  async (phase) => {
    const queue = new GroupQueue<string, string>({
      maxRequests: 1,
      maxEvents: 1,
      maxBytes: 1,
    });
    const member = queue.enqueue('a', 1, 1)!;
    const primary = new Error('primary');
    const endSession = jest.fn().mockRejectedValue(new Error('cleanup'));
    const abortTransaction = jest
      .fn()
      .mockRejectedValue(new Error('abort cleanup'));
    const session = {
      startTransaction: jest.fn(),
      inTransaction: () => true,
      abortTransaction,
      commitTransaction: jest.fn().mockRejectedValue(primary),
      endSession,
    } as unknown as ClientSession;
    const client = { startSession: () => session } as MongoClient;
    await expect(
      runMongoGroup(
        client,
        queue.seal(1),
        (value) => {
          if (phase === 'work') return Promise.reject(primary);
          return Promise.resolve(value);
        },
        { maxAttempts: 1, maxCommitAttempts: 1 },
      ),
    ).rejects.toMatchObject({
      outcome: phase === 'work' ? 'aborted' : 'unknown',
      cause: primary,
    });
    expect(member.outcome).toEqual({
      kind: phase === 'work' ? 'aborted' : 'unknown',
    });
    expect(abortTransaction).toHaveBeenCalledTimes(phase === 'work' ? 1 : 0);
    expect(endSession).toHaveBeenCalledTimes(1);
  },
);

test('does not replay transient work when abort acknowledgement is unverified', async () => {
  const queue = new GroupQueue<string, string>({
    maxRequests: 2,
    maxEvents: 2,
    maxBytes: 2,
  });
  const a = queue.enqueue('a', 1, 1)!;
  const b = queue.enqueue('b', 1, 1)!;
  const transient = new MongoError('write conflict');
  transient.addErrorLabel('TransientTransactionError');
  const startTransaction = jest.fn();
  const abortTransaction = jest.fn().mockResolvedValue(undefined);
  const commitTransaction = jest.fn().mockResolvedValue(undefined);
  const session = {
    startTransaction,
    inTransaction: () => true,
    abortTransaction,
    commitTransaction,
    endSession: jest.fn().mockResolvedValue(undefined),
  } as unknown as ClientSession;
  const startSession = jest.fn(() => session);
  let calls = 0;
  const apply = (value: string, received: ClientSession): Promise<string> => {
    expect(received).toBe(session);
    calls++;
    if (calls === 2) return Promise.reject(transient);
    return Promise.resolve(`${value}-${calls}`);
  };
  await expect(
    runMongoGroup(
      { startSession } as unknown as MongoClient,
      queue.seal(2),
      apply,
      { maxAttempts: 2, maxCommitAttempts: 1 },
    ),
  ).rejects.toMatchObject({ outcome: 'aborted', cause: transient });
  expect(startSession).toHaveBeenCalledTimes(1);
  expect(startTransaction).toHaveBeenCalledTimes(1);
  expect(abortTransaction).toHaveBeenCalledTimes(1);
  expect(commitTransaction).not.toHaveBeenCalled();
  expect(calls).toBe(2);
  expect(a.outcome).toEqual({ kind: 'aborted' });
  expect(b.outcome).toEqual({ kind: 'aborted' });
});

test('retries uncertain commit without replay and closes once', async () => {
  const queue = new GroupQueue<string, string>({
    maxRequests: 1,
    maxEvents: 1,
    maxBytes: 1,
  });
  const member = queue.enqueue('a', 1, 1)!;
  const unknown = new MongoError('lost reply');
  unknown.addErrorLabel('UnknownTransactionCommitResult');
  const commitTransaction = jest
    .fn()
    .mockRejectedValueOnce(unknown)
    .mockResolvedValue(undefined);
  const endSession = jest.fn().mockRejectedValue(new Error('cleanup'));
  const session = {
    startTransaction: jest.fn(),
    commitTransaction,
    endSession,
  } as unknown as ClientSession;
  const client = { startSession: () => session } as MongoClient;
  const apply = jest.fn((value: string) => Promise.resolve(value));
  await runMongoGroup(client, queue.seal(1), apply, {
    maxAttempts: 2,
    maxCommitAttempts: 2,
  });
  expect(member.outcome).toEqual({ kind: 'committed', value: 'a' });
  expect(apply).toHaveBeenCalledTimes(1);
  expect(commitTransaction).toHaveBeenCalledTimes(2);
  expect(endSession).toHaveBeenCalledTimes(1);
});

test.each([
  'exhausted',
  'non-transient',
  'commit-transient',
  'abort-fails',
] as const)('bounds retries and cleans up on %s', async (scenario) => {
  const queue = new GroupQueue<string, string>({
    maxRequests: 1,
    maxEvents: 1,
    maxBytes: 1,
  });
  const member = queue.enqueue('a', 1, 1)!;
  const transient = new MongoError('transient');
  transient.addErrorLabel('TransientTransactionError');
  const error = scenario === 'non-transient' ? new Error('work') : transient;
  const abortTransaction =
    scenario === 'abort-fails'
      ? jest.fn().mockRejectedValue(new Error('abort'))
      : jest.fn().mockResolvedValue(undefined);
  const startTransaction = jest.fn();
  const commitTransaction = jest.fn().mockRejectedValue(error);
  const endSession = jest.fn().mockResolvedValue(undefined);
  const session = {
    startTransaction,
    inTransaction: () => true,
    abortTransaction,
    commitTransaction,
    endSession,
  } as unknown as ClientSession;
  const apply = jest.fn((value: string) =>
    scenario === 'commit-transient'
      ? Promise.resolve(value)
      : Promise.reject(error),
  );
  const report = jest
    .spyOn(console, 'error')
    .mockImplementation(() => undefined);
  try {
    await expect(
      runMongoGroup(
        { startSession: () => session } as MongoClient,
        queue.seal(1),
        apply,
        { maxAttempts: 2, maxCommitAttempts: 2 },
      ),
    ).rejects.toMatchObject({
      outcome: scenario === 'commit-transient' ? 'unknown' : 'aborted',
      cause: error,
    });
    expect(apply).toHaveBeenCalledTimes(1);
    expect(startTransaction).toHaveBeenCalledTimes(1);
    expect(commitTransaction).toHaveBeenCalledTimes(
      scenario === 'commit-transient' ? 1 : 0,
    );
    expect(endSession).toHaveBeenCalledTimes(1);
    expect(member.outcome).toEqual({
      kind: scenario === 'commit-transient' ? 'unknown' : 'aborted',
    });
    expect(queue.activeGroups).toBe(0);
  } finally {
    report.mockRestore();
  }
});

test('shutdown while abort cleanup is pending prevents another transaction', async () => {
  const queue = new GroupQueue<string, string>({
    maxRequests: 1,
    maxEvents: 1,
    maxBytes: 1,
  });
  const member = queue.enqueue('a', 1, 1)!;
  const transient = new MongoError('conflict');
  transient.addErrorLabel('TransientTransactionError');
  let release!: () => void;
  let entered!: () => void;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  const ready = new Promise<void>((resolve) => {
    entered = resolve;
  });
  const startTransaction = jest.fn();
  const endSession = jest.fn().mockResolvedValue(undefined);
  const session = {
    startTransaction,
    inTransaction: () => true,
    abortTransaction: jest.fn(() => {
      entered();
      return gate;
    }),
    endSession,
  } as unknown as ClientSession;
  const running = runMongoGroup(
    { startSession: () => session } as MongoClient,
    queue.seal(1),
    () => Promise.reject(transient),
    { maxAttempts: 2, maxCommitAttempts: 1 },
  ).catch(() => undefined);
  await ready;
  await queue.shutdown(0);
  release();
  await running;
  expect(startTransaction).toHaveBeenCalledTimes(1);
  expect(endSession).toHaveBeenCalledTimes(1);
  expect(member.outcome).toEqual({ kind: 'aborted' });
  expect(queue.activeGroups).toBe(0);
});

test('publishes commit before hanging cleanup while retaining capacity', async () => {
  const queue = new GroupQueue<string, string>({
    maxRequests: 1,
    maxEvents: 1,
    maxBytes: 1,
  });
  const member = queue.enqueue('a', 1, 1)!;
  let release!: () => void;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  const session = {
    startTransaction: jest.fn(),
    commitTransaction: jest.fn().mockResolvedValue(undefined),
    endSession: jest.fn(() => gate),
  } as unknown as ClientSession;
  const running = runMongoGroup(
    { startSession: () => session } as MongoClient,
    queue.seal(1),
    (value) => Promise.resolve(value),
    { maxAttempts: 1, maxCommitAttempts: 1 },
  );
  await new Promise((resolve) => setImmediate(resolve));
  expect(member.outcome).toEqual({ kind: 'committed', value: 'a' });
  expect(queue.enqueue('blocked', 1, 1)).toBeUndefined();
  release();
  await running;
  expect(queue.enqueue('next', 1, 1)).toBeDefined();
});

test('reports cleanup failures without changing committed result even if reporter throws', async () => {
  const queue = new GroupQueue<string, string>({
    maxRequests: 1,
    maxEvents: 1,
    maxBytes: 1,
  });
  const member = queue.enqueue('a', 1, 1)!;
  const failure = new Error('end failed');
  const report = jest.spyOn(console, 'error').mockImplementation(() => {
    throw new Error('report failed');
  });
  try {
    const session = {
      startTransaction: jest.fn(),
      commitTransaction: jest.fn().mockResolvedValue(undefined),
      endSession: jest.fn().mockRejectedValue(failure),
    } as unknown as ClientSession;
    await runMongoGroup(
      { startSession: () => session } as MongoClient,
      queue.seal(1),
      (value) => Promise.resolve(value),
      { maxAttempts: 1, maxCommitAttempts: 1 },
    );
    expect(report).toHaveBeenCalledWith('group session cleanup failed');
    expect(member.outcome).toEqual({ kind: 'committed', value: 'a' });
  } finally {
    report.mockRestore();
  }
});

test('retains capacity if session cleanup fails', async () => {
  const queue = new GroupQueue<string, string>({
    maxRequests: 1,
    maxEvents: 1,
    maxBytes: 1,
  });
  queue.enqueue('a', 1, 1);
  const report = jest
    .spyOn(console, 'error')
    .mockImplementation(() => undefined);
  try {
    const session = {
      startTransaction: jest.fn(),
      commitTransaction: jest.fn().mockResolvedValue(undefined),
      endSession: jest.fn().mockRejectedValue(new Error('failed')),
    } as unknown as ClientSession;
    await runMongoGroup(
      { startSession: () => session } as MongoClient,
      queue.seal(1),
      (value) => Promise.resolve(value),
      { maxAttempts: 1, maxCommitAttempts: 1 },
    );
    expect(queue.enqueue('blocked', 1, 1)).toBeUndefined();
  } finally {
    report.mockRestore();
  }
});

test('shutdown settles unknown without releasing a live transaction budget', async () => {
  const queue = new GroupQueue<string, string>({
    maxRequests: 1,
    maxEvents: 1,
    maxBytes: 1,
  });
  const member = queue.enqueue('a', 1, 1)!;
  let release!: () => void;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  const session = {
    startTransaction: jest.fn(),
    commitTransaction: jest.fn(() => gate),
    endSession: jest.fn().mockResolvedValue(undefined),
  } as unknown as ClientSession;
  const running = runMongoGroup(
    { startSession: () => session } as MongoClient,
    queue.seal(1),
    (value) => Promise.resolve(value),
    { maxAttempts: 1, maxCommitAttempts: 1 },
  );
  const result = running.catch(() => undefined);
  await new Promise((resolve) => setImmediate(resolve));
  await queue.shutdown(0);
  expect(member.outcome).toEqual({ kind: 'unknown' });
  expect(Reflect.get(queue, 'activeGroups')).toBe(1);
  expect(Reflect.get(queue, 'retainedBytes')).toBe(1);
  release();
  await result;
  expect(member.outcome).toEqual({ kind: 'unknown' });
  expect(Reflect.get(queue, 'activeGroups')).toBe(0);
  expect(Reflect.get(queue, 'retainedBytes')).toBe(0);
});

test('publishes work failure before hanging abort cleanup', async () => {
  const queue = new GroupQueue<string, string>({
    maxRequests: 1,
    maxEvents: 1,
    maxBytes: 1,
  });
  const member = queue.enqueue('a', 1, 1)!;
  let release!: () => void;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  const session = {
    startTransaction: jest.fn(),
    inTransaction: () => true,
    abortTransaction: jest.fn(() => gate),
    endSession: jest.fn().mockResolvedValue(undefined),
  } as unknown as ClientSession;
  const running = runMongoGroup(
    { startSession: () => session } as MongoClient,
    queue.seal(1),
    () => Promise.reject(new Error('work')),
    { maxAttempts: 1, maxCommitAttempts: 1 },
  );
  const result = running.catch(() => undefined);
  await new Promise((resolve) => setImmediate(resolve));
  expect(member.outcome).toEqual({ kind: 'aborted' });
  expect(queue.activeGroups).toBe(1);
  release();
  await result;
  expect(queue.activeGroups).toBe(0);
});
