import {
  FORWARDS,
  NO_STREAM,
  START,
  StreamNotFoundError,
  jsonEvent,
  streamNameFilter,
} from '@kurrent/kurrentdb-client';
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

      const allEvents = await readAllEventsForStream(context, streamName);
      expect(allEvents).toMatchObject([
        {
          streamId: streamName,
          type: 'booking-created',
          data: { step: 1 },
        },
        {
          streamId: streamName,
          type: 'booking-confirmed',
          data: { step: 2 },
        },
      ]);
    });

    it('allows recreating a soft-deleted stream from NO_STREAM without reviving old events in stream reads', async () => {
      const streamName = context.createStreamName('delete-recreate');

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

      await context.backend().getClient().deleteStream(streamName);

      const recreateResult = await context
        .backend()
        .getClient()
        .appendToStream(
          streamName,
          jsonEvent({
            type: 'booking-recreated',
            data: { step: 3 },
          }),
          {
            streamState: NO_STREAM,
          },
        );

      expect(recreateResult).toMatchObject({
        success: true,
        nextExpectedRevision: 2n,
      });

      await expect(
        waitForReadStreamEvents(context, streamName),
      ).resolves.toEqual([
        {
          type: 'booking-recreated',
          data: { step: 3 },
        },
      ]);

      const allEvents = await readAllEventsForStream(context, streamName);
      expect(allEvents).toHaveLength(3);
      expect(allEvents).toMatchObject([
        {
          streamId: streamName,
          type: 'booking-created',
          data: { step: 1 },
        },
        {
          streamId: streamName,
          type: 'booking-confirmed',
          data: { step: 2 },
        },
        {
          streamId: streamName,
          type: 'booking-recreated',
          data: { step: 3 },
        },
      ]);
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

async function readAllEventsForStream(
  context: StreamsContractContext,
  streamName: string,
): Promise<Array<{ streamId: string; type: string; data: unknown }>> {
  const events = context
    .backend()
    .getClient()
    .readAll({
      fromPosition: START,
      direction: FORWARDS,
      maxCount: 500,
      filter: streamNameFilter({
        prefixes: [streamName],
      }),
    });

  const received: Array<{ streamId: string; type: string; data: unknown }> = [];
  for await (const { event } of events) {
    if (!event) {
      continue;
    }

    received.push({
      streamId: event.streamId,
      type: event.type,
      data: event.data,
    });
  }

  return received;
}

async function waitForReadStreamEvents(
  context: StreamsContractContext,
  streamName: string,
  timeoutMs = 2_000,
  retryDelayMs = 100,
): Promise<Array<{ type: string; data: unknown }>> {
  const deadline = Date.now() + timeoutMs;
  let lastError: unknown;

  while (Date.now() < deadline) {
    try {
      return await context.readStreamEvents(streamName);
    } catch (error) {
      lastError = error;
      await new Promise((resolve) => setTimeout(resolve, retryDelayMs));
    }
  }

  throw lastError instanceof Error ? lastError : new Error(String(lastError));
}
