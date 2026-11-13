import { CooperativeDispatcher } from './cooperative-dispatcher';

test('borrowed capability cannot release the dispatcher owner', async () => {
  let exposed = true;
  const dispatcher = new CooperativeDispatcher<string, string>(
    { maxRequests: 1, maxEvents: 1, maxBytes: 1 },
    0,
    (v) => v,
    (finalize, ownership) => {
      const batch = finalize()!;
      exposed = 'owner' in ownership();
      const attempt = batch.beginAttempt();
      batch.stage(attempt, batch.members[0], 'a');
      batch.commit(attempt);
      return Promise.resolve();
    },
  );
  await dispatcher.enqueue('a', 1, 1);
  expect(exposed).toBe(false);
  await dispatcher.shutdown(0);
});

test('an unconfirmed cleanup outcome retains dispatcher capacity', async () => {
  const dispatcher = new CooperativeDispatcher<string, string>(
    { maxRequests: 1, maxEvents: 1, maxBytes: 1, maxRetainedBytes: 1 },
    0,
    (v) => v,
    (finalize) => {
      const batch = finalize()!;
      const attempt = batch.beginAttempt();
      batch.stage(attempt, batch.members[0], 'a');
      batch.commit(attempt);
      return Promise.resolve({ cleanup: 'unconfirmed' as const });
    },
  );
  await expect(dispatcher.enqueue('a', 1, 1)).resolves.toBe('a');
  await expect(dispatcher.enqueue('b', 1, 1)).rejects.toMatchObject({
    code: 'capacity',
  });
  await dispatcher.shutdown(0);
});
