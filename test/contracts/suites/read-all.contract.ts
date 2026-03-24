import {
  BACKWARDS,
  END,
  FORWARDS,
  START,
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

      const events = await context.readAllEvents(FORWARDS, START, 500);
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

      const allEvents = await context.readAllEvents(FORWARDS, START, 500);
      const secondEvent = allEvents.find(
        (event) =>
          event.streamId === secondStreamName &&
          event.type === 'booking-confirmed',
      );

      expect(secondEvent?.position).toBeDefined();

      const slicedEvents = await context.readAllEvents(
        FORWARDS,
        {
          commit: secondEvent!.position!.commit,
          prepare: secondEvent!.position!.prepare,
        },
        500,
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
  });
}
