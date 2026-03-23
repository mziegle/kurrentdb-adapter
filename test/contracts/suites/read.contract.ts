import {
  BACKWARDS,
  END,
  FORWARDS,
  START,
  StreamDeletedError,
  StreamNotFoundError,
  jsonEvent,
} from '@kurrent/kurrentdb-client';
import { StreamsContractContext } from '../contract-test-context';

export function registerReadContractSuite(
  context: StreamsContractContext,
): void {
  describe('Read', () => {
    it('returns stream not found when reading a missing stream', async () => {
      const streamName = context.createStreamName('missing-stream');

      let caughtError: unknown;
      try {
        await context.readStreamEvents(streamName);
      } catch (error) {
        caughtError = error;
      }

      expect(caughtError).toBeInstanceOf(StreamNotFoundError);
      expect(caughtError).toMatchObject({
        type: 'stream-not-found',
        streamName,
      });
    });

    it('returns events newest-first when reading backwards', async () => {
      const streamName = context.createStreamName('read-backwards');

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
          jsonEvent({
            type: 'booking-completed',
            data: { step: 3 },
          }),
        ]);

      expect(
        await context.readStreamEvents(streamName, BACKWARDS, END),
      ).toEqual([
        {
          type: 'booking-completed',
          data: { step: 3 },
        },
        {
          type: 'booking-confirmed',
          data: { step: 2 },
        },
        {
          type: 'booking-created',
          data: { step: 1 },
        },
      ]);
    });

    it('reads the correct slice when starting from a specific revision', async () => {
      const streamName = context.createStreamName('read-slice');

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
          jsonEvent({
            type: 'booking-completed',
            data: { step: 3 },
          }),
        ]);

      expect(await context.readStreamEvents(streamName, FORWARDS, 1n)).toEqual([
        {
          type: 'booking-confirmed',
          data: { step: 2 },
        },
        {
          type: 'booking-completed',
          data: { step: 3 },
        },
      ]);

      expect(await context.readStreamEvents(streamName, BACKWARDS, 1n)).toEqual(
        [
          {
            type: 'booking-confirmed',
            data: { step: 2 },
          },
          {
            type: 'booking-created',
            data: { step: 1 },
          },
        ],
      );
    });

    it('limits reads with maxCount in both directions', async () => {
      const streamName = context.createStreamName('read-max-count');

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
          jsonEvent({
            type: 'booking-completed',
            data: { step: 3 },
          }),
        ]);

      expect(
        await context.readStreamEvents(streamName, FORWARDS, START, 2),
      ).toEqual([
        {
          type: 'booking-created',
          data: { step: 1 },
        },
        {
          type: 'booking-confirmed',
          data: { step: 2 },
        },
      ]);

      expect(
        await context.readStreamEvents(streamName, BACKWARDS, END, 2),
      ).toEqual([
        {
          type: 'booking-completed',
          data: { step: 3 },
        },
        {
          type: 'booking-confirmed',
          data: { step: 2 },
        },
      ]);
    });

    it('rejects reads from a tombstoned stream as stream deleted', async () => {
      const streamName = context.createStreamName('tombstone-read');

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

      await context.backend().getClient().tombstoneStream(streamName);

      await expect(context.readStreamEvents(streamName)).rejects.toBeInstanceOf(
        StreamDeletedError,
      );
    });
  });
}
