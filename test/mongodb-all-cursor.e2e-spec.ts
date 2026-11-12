import {
  END,
  START,
  jsonEvent,
  streamNameFilter,
} from '@kurrent/kurrentdb-client';
import { randomUUID } from 'node:crypto';
import { once } from 'node:events';
import { setupKurrentDbBackend } from './contracts/kurrentdb-backend';
import { setupMongoDBBackend } from './contracts/mongodb-backend';
import { ScavengeCapableBackend } from './contracts/contract-test-context';

type ObservedEvent = {
  event?: { type: string; position?: { commit: bigint; prepare: bigint } };
};

describe.each([
  ['KurrentDB', setupKurrentDbBackend],
  ['MongoDB', setupMongoDBBackend],
] as const)('%s $all subscription positions', (_name, setup) => {
  let backend: ScavengeCapableBackend;
  beforeAll(async () => {
    backend = await setup();
  }, 120_000);
  afterAll(async () => {
    await backend?.dispose();
  });

  it('resumes after an exact event position in order', async () => {
    const client = backend.getClient();
    const stream = `all-cursor-${randomUUID()}`;
    const filter = streamNameFilter({ prefixes: [stream] });
    const first = await client.appendToStream(
      stream,
      jsonEvent({ type: 'one', data: {} }),
    );
    await client.appendToStream(stream, jsonEvent({ type: 'two', data: {} }));
    await client.appendToStream(stream, jsonEvent({ type: 'three', data: {} }));
    const subscription = client.subscribeToAll({
      fromPosition: first.position!,
      filter,
    });
    try {
      const iterator = subscription[Symbol.asyncIterator]() as AsyncIterator<
        ObservedEvent,
        void
      >;
      expect((await iterator.next()).value?.event?.type).toBe('two');
      expect((await iterator.next()).value?.event?.type).toBe('three');
    } finally {
      await subscription.unsubscribe();
    }
  }, 20_000);

  it('resumes inside a multi-event append using the event position', async () => {
    const client = backend.getClient();
    const stream = `all-batch-${randomUUID()}`;
    const filter = streamNameFilter({ prefixes: [stream] });
    await client.appendToStream(
      stream,
      ['one', 'two', 'three'].map((type) => jsonEvent({ type, data: {} })),
    );
    const read = client.readAll({ fromPosition: START, filter, maxCount: 3 });
    const first = (await read.next()).value as ObservedEvent | undefined;
    expect(first?.event?.type).toBe('one');
    expect(first?.event?.position).toBeDefined();
    const subscription = client.subscribeToAll({
      fromPosition: first!.event!.position!,
      filter,
    });
    try {
      const iterator = subscription[Symbol.asyncIterator]() as AsyncIterator<
        ObservedEvent,
        void
      >;
      expect((await iterator.next()).value?.event?.type).toBe('two');
      expect((await iterator.next()).value?.event?.type).toBe('three');
    } finally {
      await subscription.unsubscribe();
    }
  }, 20_000);

  it('does not deliver events before a far-future position', async () => {
    const client = backend.getClient();
    const stream = `all-future-${randomUUID()}`;
    const filter = streamNameFilter({ prefixes: [stream] });
    const subscription = client.subscribeToAll({
      fromPosition: { commit: 9007199254740000n, prepare: 9007199254740000n },
      filter,
    });
    try {
      await once(subscription, 'caughtUp');
      const pending = subscription[Symbol.asyncIterator]().next();
      await client.appendToStream(
        stream,
        jsonEvent({ type: 'too-early', data: {} }),
      );
      let timer: NodeJS.Timeout | undefined;
      try {
        expect(
          await Promise.race([
            pending.then(() => 'delivered'),
            new Promise<string>((resolve) => {
              timer = setTimeout(() => resolve('waiting'), 700);
            }),
          ]),
        ).toBe('waiting');
      } finally {
        clearTimeout(timer);
      }
    } finally {
      await subscription.unsubscribe();
    }
  }, 20_000);

  it('START catches up then delivers filtered live events; END starts live', async () => {
    const client = backend.getClient();
    const stream = `all-boundary-${randomUUID()}`;
    const filter = streamNameFilter({ prefixes: [stream] });
    await client.appendToStream(
      stream,
      jsonEvent({ type: 'history', data: {} }),
    );
    const start = client.subscribeToAll({ fromPosition: START, filter });
    const caughtUp = once(start, 'caughtUp');
    try {
      const iter = start[Symbol.asyncIterator]() as AsyncIterator<
        ObservedEvent,
        void
      >;
      expect((await iter.next()).value?.event?.type).toBe('history');
      await caughtUp;
      const end = client.subscribeToAll({ fromPosition: END, filter });
      try {
        await once(end, 'caughtUp');
        await client.appendToStream(stream, [
          jsonEvent({ type: 'live1', data: {} }),
          jsonEvent({ type: 'live2', data: {} }),
        ]);
        expect((await iter.next()).value?.event?.type).toBe('live1');
        expect((await iter.next()).value?.event?.type).toBe('live2');
        const endIter = end[Symbol.asyncIterator]() as AsyncIterator<
          ObservedEvent,
          void
        >;
        expect((await endIter.next()).value?.event?.type).toBe('live1');
        expect((await endIter.next()).value?.event?.type).toBe('live2');
      } finally {
        await end.unsubscribe();
      }
    } finally {
      await start.unsubscribe();
    }
  }, 20_000);
});
