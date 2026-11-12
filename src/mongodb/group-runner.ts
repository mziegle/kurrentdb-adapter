import { ClientSession, MongoClient, MongoError } from 'mongodb';
import { Batch } from './group-commit';
import { executeGroup } from './group-coordinator';

/** Monitoring events are forgeable; replay authorization is disabled. */
export function isDefinitiveGroupAbort(
  _error: unknown,
  _batch: object,
): boolean {
  void _error;
  void _batch;
  return false;
}

export class GroupTransactionError extends Error {
  constructor(
    readonly outcome: 'aborted' | 'unknown',
    cause: unknown,
  ) {
    super(`group transaction ${outcome}`, { cause });
  }
}

function reportCleanupFailure(phase: 'abort' | 'session'): void {
  try {
    console.error(`group ${phase} cleanup failed`);
  } catch {
    // Diagnostics must not alter transaction outcomes.
  }
}

/** Isolated runner; apply must use the supplied session and be retry-safe. */
export async function runMongoGroup<T, R>(
  client: MongoClient,
  batch: Batch<T, R>,
  apply: (value: T, session: ClientSession) => Promise<R>,
  limits: { maxAttempts: number; maxCommitAttempts: number },
): Promise<void> {
  let session: ClientSession | undefined;
  let needsAbort = false;
  await executeGroup(
    batch,
    async (work) => {
      try {
        for (const value of [limits.maxAttempts, limits.maxCommitAttempts]) {
          if (!Number.isSafeInteger(value) || value < 1)
            throw new Error('invalid retry limit');
        }
        session = client.startSession();
        // The driver suppresses abort command failures, and public monitoring
        // events are forgeable. Until trusted acknowledgement is available,
        // never replay work, regardless of maxAttempts.
        if (batch.settled) throw new Error('batch settled');
        session.startTransaction({
          writeConcern: { w: 'majority', j: true },
        });
        try {
          await work();
          if (batch.settled) {
            needsAbort = true;
            throw new Error('batch settled');
          }
        } catch (error) {
          needsAbort = true;
          throw new GroupTransactionError('aborted', error);
        }
        for (let commit = 0; commit < limits.maxCommitAttempts; commit++) {
          try {
            await session.commitTransaction();
            return;
          } catch (error) {
            if (
              error instanceof MongoError &&
              error.hasErrorLabel('UnknownTransactionCommitResult') &&
              commit + 1 < limits.maxCommitAttempts
            )
              continue;
            // Once commit is attempted, even an unlabelled error is uncertain.
            throw new GroupTransactionError('unknown', error);
          }
        }
      } catch (error) {
        if (error instanceof GroupTransactionError) throw error;
        throw new GroupTransactionError('aborted', error);
      }
    },
    (value) => apply(value, session!),
    (error) =>
      error instanceof GroupTransactionError ? error.outcome : 'unknown',
    async () => {
      if (needsAbort) {
        try {
          if (session?.inTransaction()) {
            await session.abortTransaction();
          }
        } catch {
          reportCleanupFailure('abort');
        }
      }
      try {
        await session?.endSession();
        return true;
      } catch {
        reportCleanupFailure('session');
        return false;
      }
    },
  );
}
