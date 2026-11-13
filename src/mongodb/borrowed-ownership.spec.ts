import { ClientSession, MongoClient } from 'mongodb';
import { GroupQueue } from './group-commit';
import { runMongoGroup } from './group-runner';

test('borrowed group reports cleanup only after session ends without acquiring another owner', async () => {
  const queue = new GroupQueue<string, string>({
    maxRequests: 1,
    maxEvents: 1,
    maxBytes: 1,
  });
  const member = queue.enqueue('a', 1, 1)!;
  const batch = queue.seal(1);
  const release = batch.retain();
  let finish!: () => void;
  let entered!: () => void;
  const ready = new Promise<void>((r) => {
    entered = r;
  });
  const gate = new Promise<void>((r) => {
    finish = r;
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
  const running = runMongoGroup(
    client,
    batch,
    (v: string) => Promise.resolve(v),
    { maxAttempts: 1, maxCommitAttempts: 1 },
    batch.borrow(release),
  );
  let completed = false;
  void running.then(
    () => {
      completed = true;
    },
    () => {
      completed = true;
    },
  );
  await Promise.race([ready, running]);
  expect(completed).toBe(false);
  expect(queue.activeGroups).toBe(1);
  finish();
  await expect(running).resolves.toEqual({ cleanup: 'complete' });
  expect(member.outcome).toEqual({ kind: 'committed', value: 'a' });
  expect(queue.activeGroups).toBe(1);
  release();
  expect(queue.activeGroups).toBe(0);
});
