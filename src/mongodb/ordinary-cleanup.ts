/** Mutable request-local evidence; never share between concurrent writes. */
export type OrdinaryCleanup = { cleanup: 'complete' | 'unconfirmed' };

/** Resolution confirms driver session cleanup only, not server abort acknowledgement. */
export async function endOrdinarySession(
  session: { endSession(): Promise<void> },
  outcome?: OrdinaryCleanup,
): Promise<void> {
  await session.endSession();
  if (outcome) outcome.cleanup = 'complete';
}
