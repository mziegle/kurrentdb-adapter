import {
  FORWARDS,
  START,
  StreamDeletedError,
  StreamNotFoundError,
  jsonEvent,
  streamNameFilter,
} from '@kurrent/kurrentdb-client';
import { ScavengeCapableBackend } from './contract-test-context';

export function registerScavengeContractSuite(
  backendName: string,
  setupBackend: () => Promise<ScavengeCapableBackend>,
): void {
  describe(`Scavenge contract: ${backendName}`, () => {
    jest.setTimeout(120_000);

    let backend: ScavengeCapableBackend;

    beforeAll(async () => {
      backend = await setupBackend();
    });

    afterAll(async () => {
      await backend?.dispose();
    });

    it('removes retention-hidden events from storage without resetting the stream revision', async () => {
      const streamName = `${backendName.toLowerCase()}-scavenge-${Date.now()}`;

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

      await backend.getClient().setStreamMetadata(streamName, {
        maxCount: 2,
      });

      await expect(
        readAllEventsForStream(backend, streamName),
      ).resolves.toHaveLength(3);
      await expect(
        readStreamEvents(backend, streamName),
      ).resolves.toMatchObject([
        { type: 'booking-confirmed', data: { step: 2 } },
        { type: 'booking-completed', data: { step: 3 } },
      ]);

      const scavengeResult = await backend.startScavenge();

      expect(scavengeResult.scavengeId).not.toHaveLength(0);

      await waitFor(async () => {
        expect(await readAllEventsForStream(backend, streamName)).toMatchObject(
          [
            { type: 'booking-confirmed', data: { step: 2 } },
            { type: 'booking-completed', data: { step: 3 } },
          ],
        );
      });

      await expect(
        readStreamEvents(backend, streamName),
      ).resolves.toMatchObject([
        { type: 'booking-confirmed', data: { step: 2 } },
        { type: 'booking-completed', data: { step: 3 } },
      ]);

      const appendResult = await backend.getClient().appendToStream(
        streamName,
        jsonEvent({
          type: 'booking-archived',
          data: { step: 4 },
        }),
        {
          streamState: 2n,
        },
      );

      expect(appendResult).toMatchObject({
        success: true,
        nextExpectedRevision: 3n,
      });
    });

    it('keeps soft-deleted stream history in $all until scavenge and then removes it', async () => {
      const streamName = `${backendName.toLowerCase()}-delete-scavenge-${Date.now()}`;

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

      await backend.getClient().deleteStream(streamName);

      await expect(
        readStreamEvents(backend, streamName),
      ).rejects.toBeInstanceOf(StreamNotFoundError);
      await expect(
        readAllEventsForStream(backend, streamName),
      ).resolves.toHaveLength(2);

      const scavengeResult = await backend.startScavenge();
      expect(scavengeResult.scavengeId).not.toHaveLength(0);

      await waitFor(async () => {
        expect(await readAllEventsForStream(backend, streamName)).toMatchObject(
          [
            {
              type: 'booking-confirmed',
              data: { step: 2 },
            },
          ],
        );
      });
    });

    it('keeps tombstoned stream history in $all until scavenge and then removes it', async () => {
      const streamName = `${backendName.toLowerCase()}-tombstone-scavenge-${Date.now()}`;

      await backend.getClient().appendToStream(
        streamName,
        jsonEvent({
          type: 'booking-created',
          data: { step: 1 },
        }),
      );

      await backend.getClient().tombstoneStream(streamName);

      await expect(
        readStreamEvents(backend, streamName),
      ).rejects.toBeInstanceOf(StreamDeletedError);
      await expect(
        readAllEventsForStream(backend, streamName),
      ).resolves.toMatchObject([
        {
          type: 'booking-created',
          data: { step: 1 },
        },
        {
          type: '$streamDeleted',
          data: [],
        },
      ]);

      const scavengeResult = await backend.startScavenge();
      expect(scavengeResult.scavengeId).not.toHaveLength(0);

      await waitFor(async () => {
        expect(await readAllEventsForStream(backend, streamName)).toMatchObject(
          [
            {
              type: '$streamDeleted',
              data: [],
            },
          ],
        );
      });
    });

    it('keeps the last truncated event in storage even after scavenging', async () => {
      const streamName = `${backendName.toLowerCase()}-truncate-last-${Date.now()}`;

      await backend.getClient().appendToStream(
        streamName,
        jsonEvent({
          type: 'booking-created',
          data: { step: 1 },
        }),
      );

      await backend.getClient().setStreamMetadata(streamName, {
        truncateBefore: 1,
      });

      await expect(readStreamEvents(backend, streamName)).resolves.toEqual([]);
      await expect(
        readAllEventsForStream(backend, streamName),
      ).resolves.toHaveLength(1);

      const scavengeResult = await backend.startScavenge();
      expect(scavengeResult.scavengeId).not.toHaveLength(0);

      await waitFor(async () => {
        expect(await readAllEventsForStream(backend, streamName)).toHaveLength(
          1,
        );
      });
      await expect(readStreamEvents(backend, streamName)).resolves.toEqual([]);
    });
  });
}

async function readAllEventsForStream(
  backend: ScavengeCapableBackend,
  streamName: string,
): Promise<Array<{ type: string; data: unknown }>> {
  const events = backend.getClient().readAll({
    fromPosition: START,
    direction: FORWARDS,
    maxCount: 500,
    filter: streamNameFilter({
      prefixes: [streamName],
    }),
  });

  const received: Array<{ type: string; data: unknown }> = [];
  for await (const { event } of events) {
    if (!event) {
      continue;
    }

    received.push({
      type: event.type,
      data: event.data,
    });
  }

  return received;
}

async function readStreamEvents(
  backend: ScavengeCapableBackend,
  streamName: string,
): Promise<Array<{ type: string; data: unknown }>> {
  const events = backend.getClient().readStream(streamName, {
    fromRevision: START,
    direction: FORWARDS,
    maxCount: 50,
  });

  const received: Array<{ type: string; data: unknown }> = [];
  for await (const { event } of events) {
    if (!event) {
      continue;
    }

    received.push({
      type: event.type,
      data: event.data,
    });
  }

  return received;
}

async function waitFor(
  assertion: () => Promise<void>,
  timeoutMs = 15_000,
  intervalMs = 200,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;

  while (Date.now() < deadline) {
    try {
      await assertion();
      return;
    } catch (error) {
      if (Date.now() + intervalMs >= deadline) {
        throw error;
      }
    }

    await delay(intervalMs);
  }
}

async function delay(ms: number): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, ms));
}
