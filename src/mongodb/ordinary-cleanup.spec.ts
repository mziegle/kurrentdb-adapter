import { endOrdinarySession, OrdinaryCleanup } from './ordinary-cleanup';

test('marks cleanup complete only after endSession resolves', async () => {
  const outcome: OrdinaryCleanup = { cleanup: 'unconfirmed' };
  let finish!: () => void;
  const pending = endOrdinarySession(
    {
      endSession: () =>
        new Promise<void>((resolve) => {
          finish = resolve;
        }),
    },
    outcome,
  );
  expect(outcome.cleanup).toBe('unconfirmed');
  finish();
  await pending;
  expect(outcome.cleanup).toBe('complete');
});

test('retains unconfirmed and propagates endSession failure', async () => {
  const outcome: OrdinaryCleanup = { cleanup: 'unconfirmed' };
  const error = new Error('cleanup');
  await expect(
    endOrdinarySession({ endSession: () => Promise.reject(error) }, outcome),
  ).rejects.toBe(error);
  expect(outcome.cleanup).toBe('unconfirmed');
});
