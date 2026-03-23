import { StreamNotFoundError, jsonEvent } from '@kurrent/kurrentdb-client';
import { StreamsContractContext } from '../contract-test-context';

export function registerDeleteContractSuite(
  context: StreamsContractContext,
): void {
  describe('Delete', () => {
    it('deletes a stream and makes subsequent reads fail with stream not found', async () => {
      const streamName = context.createStreamName('delete');

      await context
        .backend()
        .getClient()
        .appendToStream(streamName, [
          jsonEvent({
            type: 'booking-created',
            data: { step: 1 },
          }),
          jsonEvent({
            type: 'booking-confirmed',
            data: { step: 2 },
          }),
        ]);

      const deleteResult = await context
        .backend()
        .getClient()
        .deleteStream(streamName);

      expect(deleteResult).toHaveProperty('position');
      await expect(context.readStreamEvents(streamName)).rejects.toBeInstanceOf(
        StreamNotFoundError,
      );
    });

    it('rejects stale expected revisions when deleting and keeps the stream intact', async () => {
      const streamName = context.createStreamName('delete-concurrency');

      await context
        .backend()
        .getClient()
        .appendToStream(streamName, [
          jsonEvent({
            type: 'booking-created',
            data: { step: 1 },
          }),
          jsonEvent({
            type: 'booking-confirmed',
            data: { step: 2 },
          }),
        ]);

      let caughtError: unknown;
      try {
        await context.backend().getClient().deleteStream(streamName, {
          expectedRevision: 0n,
        });
      } catch (error) {
        caughtError = error;
      }

      expect(caughtError).toMatchObject({
        type: 'wrong-expected-version',
        streamName,
        expectedState: 0n,
        actualState: 1n,
      });

      expect(await context.readStreamEvents(streamName)).toEqual([
        {
          type: 'booking-created',
          data: { step: 1 },
        },
        {
          type: 'booking-confirmed',
          data: { step: 2 },
        },
      ]);
    });
  });
}
