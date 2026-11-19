import {
  START,
  eventTypeFilter,
  jsonEvent,
  streamNameFilter,
} from '@kurrent/kurrentdb-client';
import { randomUUID } from 'node:crypto';
import { setupAdapterBackend } from './contracts/adapter-backend';
import { ScavengeCapableBackend } from './contracts/contract-test-context';
import { setupKurrentDbBackend } from './contracts/kurrentdb-backend';
import { setupMongoDBBackend } from './contracts/mongodb-backend';

type ObservedEvent = { event?: { type: string; streamId: string } };

describe.each([
  ['KurrentDB', setupKurrentDbBackend],
  ['PostgreSQL adapter', setupAdapterBackend],
  ['MongoDB adapter', setupMongoDBBackend],
] as const)('%s $all regex filters without prefixes', (_name, setup) => {
  let backend: ScavengeCapableBackend;
  beforeAll(async () => {
    backend = await setup();
  }, 120_000);
  afterAll(async () => {
    await backend?.dispose();
  });

  async function collect(
    subscription: AsyncIterable<ObservedEvent> & {
      unsubscribe(): Promise<void>;
    },
    count: number,
  ): Promise<string[]> {
    const seen: string[] = [];
    try {
      for await (const { event } of subscription) {
        if (!event) continue;
        seen.push(`${event.streamId}:${event.type}`);
        if (seen.length === count) break;
      }
    } finally {
      await subscription.unsubscribe();
    }
    return seen;
  }

  it('delivers events for a stream name regex subscription', async () => {
    const client = backend.getClient();
    const id = randomUUID();
    const wanted = `regex-wanted-${id}`;
    await client.appendToStream(
      `regex-other-${id}`,
      jsonEvent({ type: 'ignored', data: {} }),
    );
    await client.appendToStream(wanted, [
      jsonEvent({ type: 'first', data: {} }),
      jsonEvent({ type: 'second', data: {} }),
    ]);
    const subscription = client.subscribeToAll({
      fromPosition: START,
      filter: streamNameFilter({ regex: `^regex-wanted-${id}$` }),
    });
    expect(await collect(subscription, 2)).toEqual([
      `${wanted}:first`,
      `${wanted}:second`,
    ]);
  }, 30_000);

  it('delivers events for an event type regex subscription', async () => {
    const client = backend.getClient();
    const id = randomUUID();
    const stream = `regex-type-${id}`;
    await client.appendToStream(stream, [
      jsonEvent({ type: 'noise', data: {} }),
      jsonEvent({ type: `Wanted-${id}-a`, data: {} }),
      jsonEvent({ type: `Wanted-${id}-b`, data: {} }),
    ]);
    const subscription = client.subscribeToAll({
      fromPosition: START,
      filter: eventTypeFilter({ regex: `^Wanted-${id}-` }),
    });
    expect(await collect(subscription, 2)).toEqual([
      `${stream}:Wanted-${id}-a`,
      `${stream}:Wanted-${id}-b`,
    ]);
  }, 30_000);

  it('applies regex filters to $all reads', async () => {
    const client = backend.getClient();
    const id = randomUUID();
    const stream = `regex-read-${id}`;
    await client.appendToStream(stream, [
      jsonEvent({ type: 'read-a', data: {} }),
      jsonEvent({ type: 'read-b', data: {} }),
    ]);
    const types: string[] = [];
    for await (const { event } of client.readAll({
      fromPosition: START,
      maxCount: 1000,
      filter: streamNameFilter({ regex: `^${stream}$` }),
    })) {
      if (event) types.push(event.type);
    }
    expect(types).toEqual(['read-a', 'read-b']);
  }, 30_000);
});
