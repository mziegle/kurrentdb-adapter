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
  });
}
