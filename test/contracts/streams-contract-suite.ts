import {
  BACKWARDS,
  Direction,
  END,
  FORWARDS,
  jsonEvent,
  KurrentDBClient,
  NO_STREAM,
  START,
  STREAM_EXISTS,
  StreamDeletedError,
  StreamNotFoundError,
} from '@kurrent/kurrentdb-client';
import { once } from 'node:events';
import { randomUUID } from 'node:crypto';

export type StreamsContractBackend = {
  getClient(): KurrentDBClient;
  restart(): Promise<void>;
  dispose(): Promise<void>;
  supportsRestart: boolean;
};

function createBackendLabel(backendName: string): string {
  return backendName.toLowerCase().replace(/\s+/g, '-');
}

export function registerStreamsContractSuite(
  backendName: string,
  setupBackend: () => Promise<StreamsContractBackend>,
  options?: {
    supportsRestart?: boolean;
  },
): void {
  describe(`Streams contract: ${backendName}`, () => {
    jest.setTimeout(120_000);

    let backend: StreamsContractBackend;

    beforeAll(async () => {
      backend = await setupBackend();
    });

    afterAll(async () => {
      await backend?.dispose();
    });

    function createStreamName(suffix: string): string {
      return `${createBackendLabel(backendName)}-${suffix}-${randomUUID()}`;
    }

    async function readStreamEvents(
      streamName: string,
      direction: Direction = FORWARDS,
      fromRevision: bigint | typeof START | typeof END = START,
      maxCount = 10,
    ): Promise<Array<{ type: string; data: unknown }>> {
      const readEvents = backend.getClient().readStream(streamName, {
        fromRevision,
        direction,
        maxCount,
      });

      const received: Array<{ type: string; data: unknown }> = [];
      for await (const { event: readEvent } of readEvents) {
        if (!readEvent) {
          continue;
        }

        received.push({
          type: readEvent.type,
          data: readEvent.data,
        });
      }

      return received;
    }

    it('writes events and reads them back over the KurrentDB interface', async () => {
      const streamName = createStreamName('append-read');

      const event = jsonEvent({
        type: 'booking-created',
        data: {
          foo: 'bar',
        },
      });

      const appendResult = await backend
        .getClient()
        .appendToStream(streamName, event);

      expect(appendResult).toMatchObject({
        success: true,
        nextExpectedRevision: 0n,
      });

      expect(await readStreamEvents(streamName)).toEqual([
        {
          type: 'booking-created',
          data: {
            foo: 'bar',
          },
        },
      ]);
    });

    it('rejects stale expected revisions and keeps stream contents unchanged', async () => {
      const streamName = createStreamName('append-concurrency');

      const firstAppend = await backend.getClient().appendToStream(
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

      const secondAppend = await backend.getClient().appendToStream(
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
        await backend.getClient().appendToStream(
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

      expect(await readStreamEvents(streamName)).toEqual([
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
      const streamName = createStreamName('append-no-stream');

      const firstAppend = await backend.getClient().appendToStream(
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
        await backend.getClient().appendToStream(
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
      const streamName = createStreamName('append-stream-exists');

      let caughtError: unknown;
      try {
        await backend.getClient().appendToStream(
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

      const createResult = await backend.getClient().appendToStream(
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

      const appendResult = await backend.getClient().appendToStream(
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
      const streamName = createStreamName('append-empty-existing');

      const createResult = await backend.getClient().appendToStream(
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

      const emptyAppendResult = await backend
        .getClient()
        .appendToStream(streamName, [], {
          streamState: 0n,
        });

      expect(emptyAppendResult).toMatchObject({
        success: true,
        nextExpectedRevision: 0n,
      });

      expect(await readStreamEvents(streamName)).toEqual([
        {
          type: 'booking-created',
          data: { step: 1 },
        },
      ]);
    });

    it('treats an empty append as a no-op on a missing stream', async () => {
      const streamName = createStreamName('append-empty-missing');

      const emptyAppendResult = await backend
        .getClient()
        .appendToStream(streamName, [], {
          streamState: NO_STREAM,
        });

      expect(emptyAppendResult).toMatchObject({
        success: true,
      });

      await expect(readStreamEvents(streamName)).rejects.toBeInstanceOf(
        StreamNotFoundError,
      );
    });

    it('returns stream not found when reading a missing stream', async () => {
      const streamName = createStreamName('missing-stream');

      let caughtError: unknown;
      try {
        await readStreamEvents(streamName);
      } catch (error) {
        caughtError = error;
      }

      expect(caughtError).toBeInstanceOf(StreamNotFoundError);
      expect(caughtError).toMatchObject({
        type: 'stream-not-found',
        streamName,
      });
    });

    it('preserves event order when appending multiple events in one call', async () => {
      const streamName = createStreamName('append-multiple');

      const appendResult = await backend
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

      expect(await readStreamEvents(streamName)).toEqual([
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

    it('returns events newest-first when reading backwards', async () => {
      const streamName = createStreamName('read-backwards');

      await backend.getClient().appendToStream(streamName, [
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

      expect(await readStreamEvents(streamName, BACKWARDS, END)).toEqual([
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
      const streamName = createStreamName('read-slice');

      await backend.getClient().appendToStream(streamName, [
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

      expect(await readStreamEvents(streamName, FORWARDS, 1n)).toEqual([
        {
          type: 'booking-confirmed',
          data: { step: 2 },
        },
        {
          type: 'booking-completed',
          data: { step: 3 },
        },
      ]);

      expect(await readStreamEvents(streamName, BACKWARDS, 1n)).toEqual([
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

    it('handles explicit revision 0 correctly for append and read boundaries', async () => {
      const streamName = createStreamName('revision-zero');

      const firstAppend = await backend.getClient().appendToStream(
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

      const secondAppend = await backend.getClient().appendToStream(
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

      expect(await readStreamEvents(streamName, FORWARDS, 0n, 1)).toEqual([
        {
          type: 'booking-created',
          data: { step: 1 },
        },
      ]);

      expect(await readStreamEvents(streamName, BACKWARDS, 0n, 1)).toEqual([
        {
          type: 'booking-created',
          data: { step: 1 },
        },
      ]);
    });

    it('limits reads with maxCount in both directions', async () => {
      const streamName = createStreamName('read-max-count');

      await backend.getClient().appendToStream(streamName, [
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

      expect(await readStreamEvents(streamName, FORWARDS, START, 2)).toEqual([
        {
          type: 'booking-created',
          data: { step: 1 },
        },
        {
          type: 'booking-confirmed',
          data: { step: 2 },
        },
      ]);

      expect(await readStreamEvents(streamName, BACKWARDS, END, 2)).toEqual([
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

    it('keeps a stream subscription open after catch-up and emits newly appended events', async () => {
      const streamName = createStreamName('subscription-catch-up');

      await backend.getClient().appendToStream(
        streamName,
        jsonEvent({
          type: 'booking-created',
          data: { step: 1 },
        }),
      );

      const subscription = backend.getClient().subscribeToStream(streamName, {
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

      await backend.getClient().appendToStream(
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
      const streamName = createStreamName('subscription-start');

      await backend.getClient().appendToStream(streamName, [
        jsonEvent({
          type: 'booking-created',
          data: { step: 1 },
        }),
        jsonEvent({
          type: 'booking-confirmed',
          data: { step: 2 },
        }),
      ]);

      const subscription = backend.getClient().subscribeToStream(streamName, {
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

      await backend.getClient().appendToStream(
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
      const streamName = createStreamName('subscription-unsubscribe');

      const subscription = backend.getClient().subscribeToStream(streamName, {
        fromRevision: START,
      });

      await once(subscription, 'caughtUp');
      await subscription.unsubscribe();

      const iterator = subscription[Symbol.asyncIterator]();
      const nextEventPromise = iterator.next();

      await backend.getClient().appendToStream(
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
      const streamName = createStreamName('subscription-multi');

      const firstSubscription = backend
        .getClient()
        .subscribeToStream(streamName, {
          fromRevision: START,
        });
      const secondSubscription = backend
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

      await backend.getClient().appendToStream(
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

    const restartTest = (options?.supportsRestart ?? true) ? it : it.skip;
    restartTest(
      'keeps persisted events after restarting the backend',
      async () => {
        const streamName = createStreamName('restart-persistence');

        await backend.getClient().appendToStream(streamName, [
          jsonEvent({
            type: 'booking-created',
            data: { step: 1 },
          }),
          jsonEvent({
            type: 'booking-confirmed',
            data: { step: 2 },
          }),
        ]);

        await backend.restart();

        expect(await readStreamEvents(streamName)).toEqual([
          {
            type: 'booking-created',
            data: { step: 1 },
          },
          {
            type: 'booking-confirmed',
            data: { step: 2 },
          },
        ]);
      },
    );

    it('deletes a stream and makes subsequent reads fail with stream not found', async () => {
      const streamName = createStreamName('delete');

      await backend.getClient().appendToStream(streamName, [
        jsonEvent({
          type: 'booking-created',
          data: { step: 1 },
        }),
        jsonEvent({
          type: 'booking-confirmed',
          data: { step: 2 },
        }),
      ]);

      const deleteResult = await backend.getClient().deleteStream(streamName);

      expect(deleteResult).toHaveProperty('position');
      await expect(readStreamEvents(streamName)).rejects.toBeInstanceOf(
        StreamNotFoundError,
      );
    });

    it('rejects stale expected revisions when deleting and keeps the stream intact', async () => {
      const streamName = createStreamName('delete-concurrency');

      await backend.getClient().appendToStream(streamName, [
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
        await backend.getClient().deleteStream(streamName, {
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

      expect(await readStreamEvents(streamName)).toEqual([
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

    it('tombstones a stream and rejects subsequent appends as stream deleted', async () => {
      const streamName = createStreamName('tombstone');

      await backend.getClient().appendToStream(
        streamName,
        jsonEvent({
          type: 'booking-created',
          data: { step: 1 },
        }),
      );

      const tombstoneResult = await backend
        .getClient()
        .tombstoneStream(streamName);

      expect(tombstoneResult).toHaveProperty('position');

      await expect(
        backend.getClient().appendToStream(
          streamName,
          jsonEvent({
            type: 'booking-should-fail',
            data: { step: 2 },
          }),
        ),
      ).rejects.toBeInstanceOf(StreamDeletedError);
    });

    it('rejects stale expected revisions when tombstoning and keeps the stream usable', async () => {
      const streamName = createStreamName('tombstone-concurrency');

      await backend.getClient().appendToStream(streamName, [
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
        await backend.getClient().tombstoneStream(streamName, {
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

      expect(await readStreamEvents(streamName)).toEqual([
        {
          type: 'booking-created',
          data: { step: 1 },
        },
        {
          type: 'booking-confirmed',
          data: { step: 2 },
        },
      ]);

      const appendResult = await backend.getClient().appendToStream(
        streamName,
        jsonEvent({
          type: 'booking-completed',
          data: { step: 3 },
        }),
        {
          streamState: 1n,
        },
      );

      expect(appendResult).toMatchObject({
        success: true,
        nextExpectedRevision: 2n,
      });
    });

    it('rejects reads from a tombstoned stream as stream deleted', async () => {
      const streamName = createStreamName('tombstone-read');

      await backend.getClient().appendToStream(
        streamName,
        jsonEvent({
          type: 'booking-created',
          data: { step: 1 },
        }),
      );

      await backend.getClient().tombstoneStream(streamName);

      await expect(readStreamEvents(streamName)).rejects.toBeInstanceOf(
        StreamDeletedError,
      );
    });
  });
}
