import { StreamDeletedError, jsonEvent } from '@kurrent/kurrentdb-client';
import { StreamsContractContext } from '../contract-test-context';

export function registerTombstoneContractSuite(
  context: StreamsContractContext,
): void {
  describe('Tombstone', () => {
    it('tombstones a stream and rejects subsequent appends as stream deleted', async () => {
      const streamName = context.createStreamName('tombstone');

      await context
        .backend()
        .getClient()
        .appendToStream(
          streamName,
          jsonEvent({
            type: 'booking-created',
            data: { step: 1 },
          }),
        );

      const tombstoneResult = await context
        .backend()
        .getClient()
        .tombstoneStream(streamName);

      expect(tombstoneResult).toHaveProperty('position');

      await expect(
        context
          .backend()
          .getClient()
          .appendToStream(
            streamName,
            jsonEvent({
              type: 'booking-should-fail',
              data: { step: 2 },
            }),
          ),
      ).rejects.toBeInstanceOf(StreamDeletedError);
    });

    it('rejects stale expected revisions when tombstoning and keeps the stream usable', async () => {
      const streamName = context.createStreamName('tombstone-concurrency');

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
        await context.backend().getClient().tombstoneStream(streamName, {
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

      const appendResult = await context
        .backend()
        .getClient()
        .appendToStream(
          streamName,
          jsonEvent({
            type: 'booking-completed',
            data: { step: 3 },
          }),
          {
            streamState: 1n,
          },
        );

      expect(appendResult).toMatchObject({
        success: true,
        nextExpectedRevision: 2n,
      });
    });
  });
}
