import { END, START, jsonEvent } from '@kurrent/kurrentdb-client';
import { once } from 'node:events';
import { StreamsContractContext } from '../contract-test-context';

export function registerSubscriptionContractSuite(
  context: StreamsContractContext,
): void {
  describe('Subscriptions', () => {
    it('keeps a stream subscription open after catch-up and emits newly appended events', async () => {
      const streamName = context.createStreamName('subscription-catch-up');

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

      const subscription = context
        .backend()
        .getClient()
        .subscribeToStream(streamName, {
          fromRevision: END,
        });

      await once(subscription, 'caughtUp');

      const iterator = subscription[Symbol.asyncIterator]();
      let settled = false;
      const nextEventPromise = iterator.next().then((result) => {
        settled = true;
        return result;
      });

      await new Promise((resolve) => setTimeout(resolve, 200));
      expect(settled).toBe(false);

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

      await expect(nextEventPromise).resolves.toMatchObject({
        done: false,
        value: {
          event: {
            type: 'booking-confirmed',
            data: { step: 2 },
          },
        },
      });

      await subscription.unsubscribe();
    });

    it('catches up from start and then continues delivering live events', async () => {
      const streamName = context.createStreamName('subscription-start');

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

      const subscription = context
        .backend()
        .getClient()
        .subscribeToStream(streamName, {
          fromRevision: START,
        });

      const iterator = subscription[Symbol.asyncIterator]();

      await expect(iterator.next()).resolves.toMatchObject({
        done: false,
        value: {
          event: {
            type: 'booking-created',
            data: { step: 1 },
          },
        },
      });

      await expect(iterator.next()).resolves.toMatchObject({
        done: false,
        value: {
          event: {
            type: 'booking-confirmed',
            data: { step: 2 },
          },
        },
      });

      await once(subscription, 'caughtUp');

      const liveEventPromise = iterator.next();

      await context
        .backend()
        .getClient()
        .appendToStream(
          streamName,
          jsonEvent({
            type: 'booking-completed',
            data: { step: 3 },
          }),
        );

      await expect(liveEventPromise).resolves.toMatchObject({
        done: false,
        value: {
          event: {
            type: 'booking-completed',
            data: { step: 3 },
          },
        },
      });

      await subscription.unsubscribe();
    });

    it('stops delivering events after unsubscribing from a stream subscription', async () => {
      const streamName = context.createStreamName('subscription-unsubscribe');

      const subscription = context
        .backend()
        .getClient()
        .subscribeToStream(streamName, {
          fromRevision: START,
        });

      await once(subscription, 'caughtUp');
      await subscription.unsubscribe();

      const iterator = subscription[Symbol.asyncIterator]();
      const nextEventPromise = iterator.next();

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

      await expect(nextEventPromise).resolves.toMatchObject({
        done: true,
      });
    });

    it('delivers newly appended events to multiple subscribers on the same stream', async () => {
      const streamName = context.createStreamName('subscription-multi');

      const firstSubscription = context
        .backend()
        .getClient()
        .subscribeToStream(streamName, {
          fromRevision: START,
        });
      const secondSubscription = context
        .backend()
        .getClient()
        .subscribeToStream(streamName, {
          fromRevision: START,
        });

      await Promise.all([
        once(firstSubscription, 'caughtUp'),
        once(secondSubscription, 'caughtUp'),
      ]);

      const firstIterator = firstSubscription[Symbol.asyncIterator]();
      const secondIterator = secondSubscription[Symbol.asyncIterator]();

      const firstEventPromise = firstIterator.next();
      const secondEventPromise = secondIterator.next();

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

      await expect(firstEventPromise).resolves.toMatchObject({
        done: false,
        value: {
          event: {
            type: 'booking-created',
            data: { step: 1 },
          },
        },
      });

      await expect(secondEventPromise).resolves.toMatchObject({
        done: false,
        value: {
          event: {
            type: 'booking-created',
            data: { step: 1 },
          },
        },
      });

      await Promise.all([
        firstSubscription.unsubscribe(),
        secondSubscription.unsubscribe(),
      ]);
    });

    it('keeps a $all subscription open after catch-up and emits newly appended events', async () => {
      const firstStreamName = context.createStreamName('all-live-first');
      const secondStreamName = context.createStreamName('all-live-second');

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

      const subscription = context.backend().getClient().subscribeToAll({
        fromPosition: END,
      });

      await once(subscription, 'caughtUp');

      const iterator = subscription[Symbol.asyncIterator]();
      let settled = false;
      const nextEventPromise = iterator.next().then((result) => {
        settled = true;
        return result;
      });

      await new Promise((resolve) => setTimeout(resolve, 200));
      expect(settled).toBe(false);

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

      await expect(nextEventPromise).resolves.toMatchObject({
        done: false,
        value: {
          event: {
            streamId: secondStreamName,
            type: 'booking-confirmed',
            data: { step: 2 },
          },
        },
      });

      await subscription.unsubscribe();
    });

    it('catches up a $all subscription from start and then continues delivering live events', async () => {
      const firstStreamName = context.createStreamName('all-start-first');
      const secondStreamName = context.createStreamName('all-start-second');

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

      const subscription = context.backend().getClient().subscribeToAll({
        fromPosition: START,
      });
      const iterator = subscription[Symbol.asyncIterator]();

      await expect(
        nextEventForStream(iterator, firstStreamName),
      ).resolves.toMatchObject({
        event: {
          streamId: firstStreamName,
          type: 'booking-created',
          data: { step: 1 },
        },
      });

      await expect(
        nextEventForStream(iterator, secondStreamName),
      ).resolves.toMatchObject({
        event: {
          streamId: secondStreamName,
          type: 'booking-confirmed',
          data: { step: 2 },
        },
      });

      await once(subscription, 'caughtUp');

      const thirdStreamName = context.createStreamName('all-start-third');
      const liveEventPromise = iterator.next();

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

      await expect(liveEventPromise).resolves.toMatchObject({
        done: false,
        value: {
          event: {
            streamId: thirdStreamName,
            type: 'booking-completed',
            data: { step: 3 },
          },
        },
      });

      await subscription.unsubscribe();
    });
  });
}

async function nextEventForStream(
  iterator: AsyncIterator<unknown>,
  streamId: string,
): Promise<unknown> {
  while (true) {
    const result = await iterator.next();
    if (result.done) {
      throw new Error(`Subscription completed before receiving ${streamId}.`);
    }

    if (
      typeof result.value === 'object' &&
      result.value !== null &&
      'event' in result.value &&
      typeof result.value.event === 'object' &&
      result.value.event !== null &&
      'streamId' in result.value.event &&
      result.value.event.streamId === streamId
    ) {
      return result.value;
    }
  }
}
