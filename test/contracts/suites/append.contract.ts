import {
  BACKWARDS,
  FORWARDS,
  jsonEvent,
  NO_STREAM,
  STREAM_EXISTS,
  StreamNotFoundError,
} from '@kurrent/kurrentdb-client';
import { StreamsContractContext } from '../contract-test-context';

export function registerAppendContractSuite(
  context: StreamsContractContext,
): void {
  describe('Append', () => {
    it('writes events and reads them back over the KurrentDB interface', async () => {
      const streamName = context.createStreamName('append-read');

      const event = jsonEvent({
        type: 'booking-created',
        data: {
          foo: 'bar',
        },
      });

      const appendResult = await context
        .backend()
        .getClient()
        .appendToStream(streamName, event);

      expect(appendResult).toMatchObject({
        success: true,
        nextExpectedRevision: 0n,
      });

      expect(await context.readStreamEvents(streamName)).toEqual([
        {
          type: 'booking-created',
          data: {
            foo: 'bar',
          },
        },
      ]);
    });

    it('rejects stale expected revisions and keeps stream contents unchanged', async () => {
      const streamName = context.createStreamName('append-concurrency');

      const firstAppend = await context
        .backend()
        .getClient()
        .appendToStream(
          streamName,
          jsonEvent({
            type: 'booking-created',
            data: { step: 1 },
          }),
        );

      expect(firstAppend).toMatchObject({
        success: true,
        nextExpectedRevision: 0n,
      });

      const secondAppend = await context
        .backend()
        .getClient()
        .appendToStream(
          streamName,
          jsonEvent({
            type: 'booking-confirmed',
            data: { step: 2 },
          }),
          {
            streamState: firstAppend.nextExpectedRevision,
          },
        );

      expect(secondAppend).toMatchObject({
        success: true,
        nextExpectedRevision: 1n,
      });

      let caughtError: unknown;
      try {
        await context
          .backend()
          .getClient()
          .appendToStream(
            streamName,
            jsonEvent({
              type: 'booking-should-fail',
              data: { step: 999 },
            }),
            {
              streamState: firstAppend.nextExpectedRevision,
            },
          );
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

    it('accepts NO_STREAM for a missing stream and rejects it once the stream exists', async () => {
      const streamName = context.createStreamName('append-no-stream');

      const firstAppend = await context
        .backend()
        .getClient()
        .appendToStream(
          streamName,
          jsonEvent({
            type: 'booking-created',
            data: { step: 1 },
          }),
          {
            streamState: NO_STREAM,
          },
        );

      expect(firstAppend).toMatchObject({
        success: true,
        nextExpectedRevision: 0n,
      });

      let caughtError: unknown;
      try {
        await context
          .backend()
          .getClient()
          .appendToStream(
            streamName,
            jsonEvent({
              type: 'booking-should-fail',
              data: { step: 2 },
            }),
            {
              streamState: NO_STREAM,
            },
          );
      } catch (error) {
        caughtError = error;
      }

      expect(caughtError).toMatchObject({
        type: 'wrong-expected-version',
        streamName,
        expectedState: 'no_stream',
        actualState: 0n,
      });
    });

    it('rejects STREAM_EXISTS for a missing stream and accepts it once the stream exists', async () => {
      const streamName = context.createStreamName('append-stream-exists');

      let caughtError: unknown;
      try {
        await context
          .backend()
          .getClient()
          .appendToStream(
            streamName,
            jsonEvent({
              type: 'booking-should-fail',
              data: { step: 0 },
            }),
            {
              streamState: STREAM_EXISTS,
            },
          );
      } catch (error) {
        caughtError = error;
      }

      expect(caughtError).toMatchObject({
        type: 'wrong-expected-version',
        streamName,
        expectedState: 'stream_exists',
        actualState: 'no_stream',
      });

      const createResult = await context
        .backend()
        .getClient()
        .appendToStream(
          streamName,
          jsonEvent({
            type: 'booking-created',
            data: { step: 1 },
          }),
        );

      expect(createResult).toMatchObject({
        success: true,
        nextExpectedRevision: 0n,
      });

      const appendResult = await context
        .backend()
        .getClient()
        .appendToStream(
          streamName,
          jsonEvent({
            type: 'booking-confirmed',
            data: { step: 2 },
          }),
          {
            streamState: STREAM_EXISTS,
          },
        );

      expect(appendResult).toMatchObject({
        success: true,
        nextExpectedRevision: 1n,
      });
    });

    it('treats an empty append as a no-op on an existing stream', async () => {
      const streamName = context.createStreamName('append-empty-existing');

      const createResult = await context
        .backend()
        .getClient()
        .appendToStream(
          streamName,
          jsonEvent({
            type: 'booking-created',
            data: { step: 1 },
          }),
        );

      expect(createResult).toMatchObject({
        success: true,
        nextExpectedRevision: 0n,
      });

      const emptyAppendResult = await context
        .backend()
        .getClient()
        .appendToStream(streamName, [], {
          streamState: 0n,
        });

      expect(emptyAppendResult).toMatchObject({
        success: true,
        nextExpectedRevision: 0n,
      });

      expect(await context.readStreamEvents(streamName)).toEqual([
        {
          type: 'booking-created',
          data: { step: 1 },
        },
      ]);
    });

    it('treats an empty append as a no-op on a missing stream', async () => {
      const streamName = context.createStreamName('append-empty-missing');

      const emptyAppendResult = await context
        .backend()
        .getClient()
        .appendToStream(streamName, [], {
          streamState: NO_STREAM,
        });

      expect(emptyAppendResult).toMatchObject({
        success: true,
      });

      await expect(context.readStreamEvents(streamName)).rejects.toBeInstanceOf(
        StreamNotFoundError,
      );
    });

    it('preserves event order when appending multiple events in one call', async () => {
      const streamName = context.createStreamName('append-multiple');

      const appendResult = await context
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

      expect(appendResult).toMatchObject({
        success: true,
        nextExpectedRevision: 2n,
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
        {
          type: 'booking-completed',
          data: { step: 3 },
        },
      ]);
    });

    it('handles explicit revision 0 correctly for append and read boundaries', async () => {
      const streamName = context.createStreamName('revision-zero');

      const firstAppend = await context
        .backend()
        .getClient()
        .appendToStream(
          streamName,
          jsonEvent({
            type: 'booking-created',
            data: { step: 1 },
          }),
          {
            streamState: NO_STREAM,
          },
        );

      expect(firstAppend).toMatchObject({
        success: true,
        nextExpectedRevision: 0n,
      });

      const secondAppend = await context
        .backend()
        .getClient()
        .appendToStream(
          streamName,
          jsonEvent({
            type: 'booking-confirmed',
            data: { step: 2 },
          }),
          {
            streamState: 0n,
          },
        );

      expect(secondAppend).toMatchObject({
        success: true,
        nextExpectedRevision: 1n,
      });

      expect(
        await context.readStreamEvents(streamName, FORWARDS, 0n, 1),
      ).toEqual([
        {
          type: 'booking-created',
          data: { step: 1 },
        },
      ]);

      expect(
        await context.readStreamEvents(streamName, BACKWARDS, 0n, 1),
      ).toEqual([
        {
          type: 'booking-created',
          data: { step: 1 },
        },
      ]);
    });
  });
}
