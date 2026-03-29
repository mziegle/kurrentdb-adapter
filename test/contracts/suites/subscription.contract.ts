import {
  END,
  START,
  streamNameFilter,
  jsonEvent,
} from '@kurrent/kurrentdb-client';
import { once } from 'node:events';
import { StreamsContractContext } from '../contract-test-context';

const SUBSCRIPTION_TIMEOUT_MS = 10_000;

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

      await waitForCaughtUp(subscription, `${streamName} stream caught-up`);

      const iterator = subscription[Symbol.asyncIterator]();
      let settled = false;
      const nextEventPromise = waitForIteratorNext(
        iterator,
        `${streamName} next live stream event`,
      ).then((result) => {
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

      await expect(
        waitForIteratorNext(iterator, `${streamName} historical event #1`),
      ).resolves.toMatchObject({
        done: false,
        value: {
          event: {
            type: 'booking-created',
            data: { step: 1 },
          },
        },
      });

      await expect(
        waitForIteratorNext(iterator, `${streamName} historical event #2`),
      ).resolves.toMatchObject({
        done: false,
        value: {
          event: {
            type: 'booking-confirmed',
            data: { step: 2 },
          },
        },
      });

      await waitForCaughtUp(subscription, `${streamName} stream caught-up`);

      const liveEventPromise = waitForIteratorNext(
        iterator,
        `${streamName} live event after catch-up`,
      );

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

      await waitForCaughtUp(subscription, `${streamName} stream caught-up`);
      await subscription.unsubscribe();

      const iterator = subscription[Symbol.asyncIterator]();
      const nextEventPromise = waitForIteratorNext(
        iterator,
        `${streamName} post-unsubscribe next result`,
      );

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
        waitForCaughtUp(firstSubscription, `${streamName} first caught-up`),
        waitForCaughtUp(secondSubscription, `${streamName} second caught-up`),
      ]);

      const firstIterator = firstSubscription[Symbol.asyncIterator]();
      const secondIterator = secondSubscription[Symbol.asyncIterator]();

      const firstEventPromise = waitForIteratorNext(
        firstIterator,
        `${streamName} first subscriber live event`,
      );
      const secondEventPromise = waitForIteratorNext(
        secondIterator,
        `${streamName} second subscriber live event`,
      );

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

      await waitForCaughtUp(subscription, '$all caught-up from end');

      const iterator = subscription[Symbol.asyncIterator]();
      let settled = false;
      const nextEventPromise = waitForIteratorNext(
        iterator,
        '$all next live event',
      ).then((result) => {
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

      await waitForCaughtUp(subscription, '$all caught-up from start');

      const thirdStreamName = context.createStreamName('all-start-third');
      const liveEventPromise = waitForIteratorNext(
        iterator,
        `${thirdStreamName} live $all event`,
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

    it('filters $all subscriptions by stream name prefix during catch-up and live delivery', async () => {
      const includedPrefix = context.createStreamName('all-filter');
      const includedHistoricalStream = `${includedPrefix}-history`;
      const includedLiveStream = `${includedPrefix}-live`;
      const excludedStream = context.createStreamName('all-filter-excluded');

      await context
        .backend()
        .getClient()
        .appendToStream(
          includedHistoricalStream,
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

      const subscription = context
        .backend()
        .getClient()
        .subscribeToAll({
          fromPosition: START,
          filter: streamNameFilter({
            prefixes: [includedPrefix],
          }),
        });
      const iterator = subscription[Symbol.asyncIterator]();

      await expect(
        waitForIteratorNext(
          iterator,
          `${includedHistoricalStream} filtered historical event`,
        ),
      ).resolves.toMatchObject({
        done: false,
        value: {
          event: {
            streamId: includedHistoricalStream,
            type: 'booking-created',
            data: { step: 1 },
          },
        },
      });

      await waitForCaughtUp(subscription, '$all filtered caught-up');

      let settled = false;
      const nextEventPromise = waitForIteratorNext(
        iterator,
        `${includedLiveStream} filtered live event`,
      ).then((result) => {
        settled = true;
        return result;
      });

      await context
        .backend()
        .getClient()
        .appendToStream(
          excludedStream,
          jsonEvent({
            type: 'booking-ignored-again',
            data: { step: 3 },
          }),
        );

      await new Promise((resolve) => setTimeout(resolve, 200));
      expect(settled).toBe(false);

      await context
        .backend()
        .getClient()
        .appendToStream(
          includedLiveStream,
          jsonEvent({
            type: 'booking-confirmed',
            data: { step: 4 },
          }),
        );

      await expect(nextEventPromise).resolves.toMatchObject({
        done: false,
        value: {
          event: {
            streamId: includedLiveStream,
            type: 'booking-confirmed',
            data: { step: 4 },
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
    const result = await waitForIteratorNext(
      iterator,
      `${streamId} event in stream-filtered iteration`,
    );
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

function waitForCaughtUp(
  subscription: NodeJS.EventEmitter,
  label: string,
): Promise<unknown[]> {
  return withTimeout(once(subscription, 'caughtUp'), label);
}

function waitForIteratorNext<T>(
  iterator: AsyncIterator<T>,
  label: string,
): Promise<IteratorResult<T>> {
  return withTimeout(iterator.next(), label);
}

function withTimeout<T>(promise: Promise<T>, label: string): Promise<T> {
  let timeout: NodeJS.Timeout | undefined;

  const timeoutPromise = new Promise<never>((_, reject) => {
    timeout = setTimeout(() => {
      reject(
        new Error(
          `${label} did not complete within ${SUBSCRIPTION_TIMEOUT_MS}ms.`,
        ),
      );
    }, SUBSCRIPTION_TIMEOUT_MS);
  });

  return Promise.race([promise, timeoutPromise]).finally(() => {
    if (timeout) {
      clearTimeout(timeout);
    }
  });
}
