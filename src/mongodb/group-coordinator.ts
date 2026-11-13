import { Batch, BorrowedOwnership } from './group-commit';

/**
 * Runner contract: invoke work serially for each full transaction attempt and
 * resolve only after its commit is acknowledged. It owns retry limits and all
 * transaction execution. The cleanup callback returns true only when resource
 * ownership can be released. The classifier may report aborted only when it
 * can prove that no attempt could have committed; otherwise use unknown.
 */
export async function executeGroup<T, R>(
  batch: Batch<T, R>,
  transaction: (work: () => Promise<void>) => Promise<void>,
  apply: (value: T) => Promise<R>,
  classify: (error: unknown) => 'aborted' | 'unknown' = () => 'unknown',
  cleanup: () => Promise<boolean> = () => Promise.resolve(true),
  borrowedOwner?: BorrowedOwnership,
): Promise<void> {
  if (borrowedOwner && !batch.ownsBorrow(borrowedOwner))
    throw new Error('invalid borrowed owner');
  const release = borrowedOwner ? undefined : batch.retain();
  let attempt = 0;
  let cleaned = false;
  try {
    await transaction(async () => {
      attempt = batch.beginAttempt();
      for (const member of batch.members)
        batch.stage(attempt, member, await apply(member.value));
    });
    if (!attempt) throw new Error('transaction did not execute callback');
    if (borrowedOwner) {
      cleaned = true;
      await cleanup();
    }
    batch.commit(attempt);
  } catch (error) {
    if (borrowedOwner && !cleaned) {
      cleaned = true;
      await cleanup();
    }
    // Shutdown may already have settled the batch as uncertain.
    if (!batch.members.length || batch.members[0].state !== 'settled') {
      try {
        if (!attempt) attempt = batch.beginAttempt();
        let outcome: 'aborted' | 'unknown' = 'unknown';
        try {
          outcome = classify(error);
        } catch {
          // A broken classifier cannot establish an abort.
        }
        if (outcome === 'aborted') batch.abort(attempt);
        else batch.unknown(attempt);
      } catch {
        // A stale token or a concurrent shutdown must not change an outcome.
      }
    }
    throw error;
  } finally {
    try {
      if (!cleaned && (await cleanup())) release?.();
    } catch {
      // Unknown cleanup state retains accounting and cannot mask an outcome.
    }
  }
}
