import { FORWARDS, START, jsonEvent } from '@kurrent/kurrentdb-client';
import { StreamsContractContext } from '../contract-test-context';

export function registerBatchAppendContractSuite(
  context: StreamsContractContext,
): void {
  describe('BatchAppend', () => {
    it('appends events through the official client when batching splits the request', async () => {
      const streamName = context.createStreamName('batch-append-multiple');

      const appendResult = await context
        .backend()
        .getClient()
        .appendToStream(
          streamName,
          [
            jsonEvent({
              type: 'booking-created',
              data: {
                step: 1,
                payload: 'a'.repeat(200),
              },
            }),
            jsonEvent({
              type: 'booking-confirmed',
              data: {
                step: 2,
                payload: 'b'.repeat(200),
              },
            }),
            jsonEvent({
              type: 'booking-completed',
              data: {
                step: 3,
                payload: 'c'.repeat(200),
              },
            }),
          ],
          {
            streamState: 'no_stream',
            batchAppendSize: 700,
          },
        );

      expect(appendResult).toMatchObject({
        success: true,
        nextExpectedRevision: 2n,
      });

      expect(await context.readStreamEvents(streamName)).toEqual([
        {
          type: 'booking-created',
          data: {
            step: 1,
            payload: 'a'.repeat(200),
          },
        },
        {
          type: 'booking-confirmed',
          data: {
            step: 2,
            payload: 'b'.repeat(200),
          },
        },
        {
          type: 'booking-completed',
          data: {
            step: 3,
            payload: 'c'.repeat(200),
          },
        },
      ]);
    });

    it('preserves wrong expected version behavior through the official client when batching is enabled', async () => {
      const streamName = context.createStreamName('batch-append-concurrency');

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
        await context
          .backend()
          .getClient()
          .appendToStream(
            streamName,
            jsonEvent({
              type: 'booking-should-fail',
              data: {
                step: 999,
                payload: 'z'.repeat(200),
              },
            }),
            {
              streamState: 0n,
              batchAppendSize: 700,
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

    it('handles many concurrent batch appends on distinct streams', async () => {
      const streamNames = Array.from({ length: 8 }, (_, index) =>
        context.createStreamName(`batch-append-parallel-${index}`),
      );

      const appendTasks = streamNames.map((streamName, streamIndex) =>
        context
          .backend()
          .getClient()
          .appendToStream(
            streamName,
            Array.from({ length: 200 }, (_, appendIndex) =>
              jsonEvent({
                type: 'booking-progressed',
                data: {
                  streamIndex,
                  appendIndex,
                  payload: 'x'.repeat(512),
                },
              }),
            ),
            {
              streamState: 'no_stream',
              batchAppendSize: 700,
            },
          ),
      );

      await expect(Promise.all(appendTasks)).resolves.toHaveLength(
        streamNames.length,
      );

      const reads = await Promise.all(
        streamNames.map((streamName) =>
          context.readStreamEvents(streamName, FORWARDS, START, 500),
        ),
      );

      reads.forEach((events, streamIndex) => {
        expect(events).toHaveLength(200);
        expect(events[0]).toEqual({
          type: 'booking-progressed',
          data: {
            streamIndex,
            appendIndex: 0,
            payload: 'x'.repeat(512),
          },
        });
        expect(events[199]).toEqual({
          type: 'booking-progressed',
          data: {
            streamIndex,
            appendIndex: 199,
            payload: 'x'.repeat(512),
          },
        });
      });
    });
  });
}
