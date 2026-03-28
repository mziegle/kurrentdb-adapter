import {
  START,
  FORWARDS,
  jsonEvent,
  streamNameFilter,
} from '@kurrent/kurrentdb-client';
import { StreamsContractContext } from '../contract-test-context';

export function registerStreamMetadataContractSuite(
  context: StreamsContractContext,
): void {
  describe('Stream metadata', () => {
    it('stores deletion-policy metadata and returns it through the client', async () => {
      const streamName = context.createStreamName('stream-metadata-roundtrip');

      await context.backend().getClient().setStreamMetadata(streamName, {
        maxCount: 2,
        truncateBefore: 1,
      });

      const result = await context
        .backend()
        .getClient()
        .getStreamMetadata(streamName);

      expect(result.metadata).toMatchObject({
        maxCount: 2,
        truncateBefore: 1,
      });
    });

    it('applies maxCount to stream reads and $all reads', async () => {
      const streamName = context.createStreamName('stream-metadata-max-count');

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

      await context.backend().getClient().setStreamMetadata(streamName, {
        maxCount: 2,
      });

      await expect(
        context.readStreamEvents(streamName, FORWARDS, START, 50),
      ).resolves.toMatchObject([
        {
          type: 'booking-confirmed',
          data: { step: 2 },
        },
        {
          type: 'booking-completed',
          data: { step: 3 },
        },
      ]);

      const allEvents = await readAllEventsForStream(context, streamName);

      expect(allEvents).toMatchObject([
        {
          streamId: streamName,
          type: 'booking-confirmed',
          data: { step: 2 },
        },
        {
          streamId: streamName,
          type: 'booking-completed',
          data: { step: 3 },
        },
      ]);
    });

    it('applies truncateBefore to stream reads', async () => {
      const streamName = context.createStreamName('stream-metadata-truncate');

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

      await context.backend().getClient().setStreamMetadata(streamName, {
        truncateBefore: 2,
      });

      await expect(
        context.readStreamEvents(streamName, FORWARDS, START, 50),
      ).resolves.toMatchObject([
        {
          type: 'booking-completed',
          data: { step: 3 },
        },
      ]);
    });

    it('applies maxAge to stream reads and $all reads', async () => {
      const streamName = context.createStreamName('stream-metadata-max-age');

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

      await context.backend().getClient().setStreamMetadata(streamName, {
        maxAge: 1,
      });

      await delay(1_200);

      await context
        .backend()
        .getClient()
        .appendToStream(
          streamName,
          jsonEvent({
            type: 'booking-confirmed',
            data: { step: 2 },
          }),
        );

      await expect(
        context.readStreamEvents(streamName, FORWARDS, START, 50),
      ).resolves.toMatchObject([
        {
          type: 'booking-confirmed',
          data: { step: 2 },
        },
      ]);

      const allEvents = await readAllEventsForStream(context, streamName);

      expect(allEvents).toMatchObject([
        {
          streamId: streamName,
          type: 'booking-confirmed',
          data: { step: 2 },
        },
      ]);
      expect(allEvents).toHaveLength(1);
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

async function delay(ms: number): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, ms));
}
