import {
  BACKWARDS,
  END,
  FORWARDS,
  START,
  eventTypeFilter,
  streamNameFilter,
  jsonEvent,
} from '@kurrent/kurrentdb-client';
import { StreamsContractContext } from '../contract-test-context';

export function registerReadAllContractSuite(
  context: StreamsContractContext,
): void {
  describe('ReadAll', () => {
    it('returns events oldest-first when reading forwards from start', async () => {
      const firstStreamName = context.createStreamName(
        'read-all-forward-first',
      );
      const secondStreamName = context.createStreamName(
        'read-all-forward-second',
      );

      const firstAppend = await context
        .backend()
        .getClient()
        .appendToStream(
          firstStreamName,
          jsonEvent({
            type: 'booking-created',
            data: { step: 1 },
          }),
        );
      const secondAppend = await context
        .backend()
        .getClient()
        .appendToStream(
          secondStreamName,
          jsonEvent({
            type: 'booking-confirmed',
            data: { step: 2 },
          }),
        );

      expect(firstAppend.position).toBeDefined();
      expect(secondAppend.position).toBeDefined();

      const events = await context.readAllEvents(
        FORWARDS,
        expectPosition(firstAppend.position),
        10,
      );
      const firstIndex = events.findIndex(
        (event) =>
          event.streamId === firstStreamName &&
          event.type === 'booking-created',
      );
      const secondIndex = events.findIndex(
        (event) =>
          event.streamId === secondStreamName &&
          event.type === 'booking-confirmed',
      );

      expect(firstIndex).toBeGreaterThanOrEqual(0);
      expect(secondIndex).toBeGreaterThan(firstIndex);
    });

    it('returns events newest-first when reading backwards from end', async () => {
      const firstStreamName = context.createStreamName(
        'read-all-backward-first',
      );
      const secondStreamName = context.createStreamName(
        'read-all-backward-second',
      );

      await context
        .backend()
        .getClient()
        .appendToStream(
          firstStreamName,
          jsonEvent({
            type: 'booking-created',
            data: { step: 1 },
          }),
        );
      await context
        .backend()
        .getClient()
        .appendToStream(
          secondStreamName,
          jsonEvent({
            type: 'booking-confirmed',
            data: { step: 2 },
          }),
        );

      const events = await context.readAllEvents(BACKWARDS, END, 500);
      const firstIndex = events.findIndex(
        (event) =>
          event.streamId === firstStreamName &&
          event.type === 'booking-created',
      );
      const secondIndex = events.findIndex(
        (event) =>
          event.streamId === secondStreamName &&
          event.type === 'booking-confirmed',
      );

      expect(secondIndex).toBeGreaterThanOrEqual(0);
      expect(firstIndex).toBeGreaterThan(secondIndex);
    });

    it('reads the correct slice when starting from a specific position', async () => {
      const firstStreamName = context.createStreamName('read-all-slice-first');
      const secondStreamName = context.createStreamName(
        'read-all-slice-second',
      );
      const thirdStreamName = context.createStreamName('read-all-slice-third');

      await context
        .backend()
        .getClient()
        .appendToStream(
          firstStreamName,
          jsonEvent({
            type: 'booking-created',
            data: { step: 1 },
          }),
        );
      const secondAppend = await context
        .backend()
        .getClient()
        .appendToStream(
          secondStreamName,
          jsonEvent({
            type: 'booking-confirmed',
            data: { step: 2 },
          }),
        );
      const thirdAppend = await context
        .backend()
        .getClient()
        .appendToStream(
          thirdStreamName,
          jsonEvent({
            type: 'booking-completed',
            data: { step: 3 },
          }),
        );

      const secondPosition = expectPosition(secondAppend.position);
      expectPosition(thirdAppend.position);

      const slicedEvents = await context.readAllEvents(
        FORWARDS,
        {
          commit: secondPosition.commit,
          prepare: secondPosition.prepare,
        },
        10,
      );

      expect(slicedEvents).toMatchObject([
        expect.objectContaining({
          streamId: secondStreamName,
          type: 'booking-confirmed',
          data: { step: 2 },
        }),
        expect.objectContaining({
          streamId: thirdStreamName,
          type: 'booking-completed',
          data: { step: 3 },
        }),
      ]);
    });

    it('treats an explicit backward $all position as exclusive', async () => {
      const firstStreamName = context.createStreamName(
        'read-all-backward-exclusive-first',
      );
      const secondStreamName = context.createStreamName(
        'read-all-backward-exclusive-second',
      );

      await context
        .backend()
        .getClient()
        .appendToStream(
          firstStreamName,
          jsonEvent({
            type: 'booking-created',
            data: { step: 1 },
          }),
        );
      const secondAppend = await context
        .backend()
        .getClient()
        .appendToStream(
          secondStreamName,
          jsonEvent({
            type: 'booking-confirmed',
            data: { step: 2 },
          }),
        );

      const latestEvent = await context.readAllEvents(BACKWARDS, END, 1);
      expect(latestEvent).toEqual([
        expect.objectContaining({
          streamId: secondStreamName,
          type: 'booking-confirmed',
          data: { step: 2 },
          position: expectPosition(secondAppend.position),
        }),
      ]);

      const nextSlice = await context.readAllEvents(
        BACKWARDS,
        expectPosition(secondAppend.position),
        10,
      );

      const nextSliceSummary = nextSlice.map((event) => ({
        streamId: event.streamId,
        type: event.type,
        data: event.data,
      }));

      expect(
        nextSliceSummary.some(
          (event) =>
            event.streamId === secondStreamName &&
            event.type === 'booking-confirmed' &&
            event.data &&
            typeof event.data === 'object' &&
            'step' in event.data &&
            event.data.step === 2,
        ),
      ).toBe(false);
      expect(nextSliceSummary).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            streamId: firstStreamName,
            type: 'booking-created',
            data: { step: 1 },
          }),
        ]),
      );
    });

    it('limits $all reads with maxCount in both directions', async () => {
      const firstStreamName = context.createStreamName('read-all-limit-first');
      const secondStreamName = context.createStreamName(
        'read-all-limit-second',
      );
      const thirdStreamName = context.createStreamName('read-all-limit-third');

      await context
        .backend()
        .getClient()
        .appendToStream(
          firstStreamName,
          jsonEvent({
            type: 'booking-created',
            data: { step: 1 },
          }),
        );
      await context
        .backend()
        .getClient()
        .appendToStream(
          secondStreamName,
          jsonEvent({
            type: 'booking-confirmed',
            data: { step: 2 },
          }),
        );
      await context
        .backend()
        .getClient()
        .appendToStream(
          thirdStreamName,
          jsonEvent({
            type: 'booking-completed',
            data: { step: 3 },
          }),
        );

      const forwardEvents = await context.readAllEvents(FORWARDS, START, 2);
      const backwardEvents = await context.readAllEvents(BACKWARDS, END, 2);

      expect(forwardEvents).toHaveLength(2);
      expect(backwardEvents).toHaveLength(2);
    });

    it('filters $all reads by stream name prefix', async () => {
      const includedPrefix = context.createStreamName('read-all-filter');
      const includedFirstStream = `${includedPrefix}-first`;
      const includedSecondStream = `${includedPrefix}-second`;
      const excludedStream = context.createStreamName('read-all-excluded');

      await context
        .backend()
        .getClient()
        .appendToStream(
          includedFirstStream,
          jsonEvent({
            type: 'booking-created',
            data: { step: 1 },
          }),
        );
      await context
        .backend()
        .getClient()
        .appendToStream(
          excludedStream,
          jsonEvent({
            type: 'booking-ignored',
            data: { step: 2 },
          }),
        );
      await context
        .backend()
        .getClient()
        .appendToStream(
          includedSecondStream,
          jsonEvent({
            type: 'booking-confirmed',
            data: { step: 3 },
          }),
        );

      const filteredEvents = await readAllMatchingEvents(
        context,
        streamNameFilter({
          prefixes: [includedPrefix],
        }),
      );

      expect(filteredEvents).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            streamId: includedFirstStream,
            type: 'booking-created',
            data: { step: 1 },
          }),
          expect.objectContaining({
            streamId: includedSecondStream,
            type: 'booking-confirmed',
            data: { step: 3 },
          }),
        ]),
      );
      expect(
        filteredEvents.some((event) => event.streamId === excludedStream),
      ).toBe(false);
      expect(filteredEvents).toHaveLength(2);
    });

    it('filters $all reads by event type', async () => {
      const firstStreamName = context.createStreamName(
        'read-all-event-type-first',
      );
      const secondStreamName = context.createStreamName(
        'read-all-event-type-second',
      );
      const uniqueSuffix = context.createStreamName('event-type');
      const includedEventType = `booking-confirmed-${uniqueSuffix}`;
      const excludedEventType = `booking-created-${uniqueSuffix}`;

      await context
        .backend()
        .getClient()
        .appendToStream(
          firstStreamName,
          jsonEvent({
            type: excludedEventType,
            data: { step: 1 },
          }),
        );
      await context
        .backend()
        .getClient()
        .appendToStream(
          secondStreamName,
          jsonEvent({
            type: includedEventType,
            data: { step: 2 },
          }),
        );

      const filteredEvents = await readAllMatchingEvents(
        context,
        eventTypeFilter({
          prefixes: [includedEventType],
        }),
      );

      expect(filteredEvents).toEqual([
        expect.objectContaining({
          streamId: secondStreamName,
          type: includedEventType,
          data: { step: 2 },
        }),
      ]);
      expect(
        filteredEvents.some(
          (event) =>
            event.streamId === firstStreamName &&
            event.type === excludedEventType,
        ),
      ).toBe(false);
    });
  });
}

async function readAllMatchingEvents(
  context: StreamsContractContext,
  filter: ReturnType<typeof streamNameFilter>,
): Promise<Array<{ streamId: string; type: string; data: unknown }>> {
  const events = context.backend().getClient().readAll({
    fromPosition: START,
    direction: FORWARDS,
    maxCount: 500,
    filter,
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

function expectPosition(
  position: { commit: bigint; prepare: bigint } | undefined,
): { commit: bigint; prepare: bigint } {
  expect(position).toBeDefined();

  return position!;
}
