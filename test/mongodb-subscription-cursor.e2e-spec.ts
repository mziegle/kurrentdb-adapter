import { END, START, jsonEvent } from '@kurrent/kurrentdb-client';
import { randomUUID } from 'node:crypto';
import { once } from 'node:events';
import { setupKurrentDbBackend } from './contracts/kurrentdb-backend';
import { setupMongoDBBackend } from './contracts/mongodb-backend';
import { ScavengeCapableBackend } from './contracts/contract-test-context';

describe.each([
  ['KurrentDB', setupKurrentDbBackend],
  ['MongoDB', setupMongoDBBackend],
] as const)('%s stream subscription cursors', (_name, setup) => {
  let backend: ScavengeCapableBackend;
  beforeAll(async () => {
    backend = await setup();
  }, 120_000);
  afterAll(async () => {
    await backend?.dispose();
  });

  it.each([
    [0n, 1n],
    [1n, 2n],
  ] as const)(
    'resumes strictly after numeric revision %s',
    async (cursor, expected) => {
      const stream = `cursor-${randomUUID()}`;
      const client = backend.getClient();
      await client.appendToStream(
        stream,
        [0, 1, 2].map((n) => jsonEvent({ type: 'item', data: { n } })),
      );
      const read = client.readStream(stream, {
        fromRevision: cursor,
        maxCount: 1,
      });
      const readEvent = await read.next();
      expect(readEvent.value).toMatchObject({
        event: { revision: Number(cursor) },
      });
      const subscription = client.subscribeToStream(stream, {
        fromRevision: cursor,
      });
      try {
        const result = await subscription[Symbol.asyncIterator]().next();
        expect(result.value).toMatchObject({ event: { revision: expected } });
      } finally {
        await subscription.unsubscribe();
      }
    },
    20_000,
  );

  it('resumes from a saved revision after server restart', async () => {
    if (!backend.supportsRestart) return;
    const stream = `cursor-${randomUUID()}`;
    await backend.getClient().appendToStream(
      stream,
      [0, 1, 2].map((n) => jsonEvent({ type: 'item', data: { n } })),
    );
    await backend.restart();
    const subscription = backend
      .getClient()
      .subscribeToStream(stream, { fromRevision: 1n });
    try {
      expect(
        (await subscription[Symbol.asyncIterator]().next()).value,
      ).toMatchObject({ event: { revision: 2n } });
    } finally {
      await subscription.unsubscribe();
    }
  }, 60_000);

  it('resumes after the cursor while respecting maxCount retention', async () => {
    const stream = `cursor-${randomUUID()}`;
    const client = backend.getClient();
    await client.appendToStream(
      stream,
      [0, 1, 2, 3].map((n) => jsonEvent({ type: 'item', data: { n } })),
    );
    await client.setStreamMetadata(stream, { maxCount: 2 });
    for (const cursor of [0n, 2n]) {
      const subscription = client.subscribeToStream(stream, {
        fromRevision: cursor,
      });
      try {
        expect(
          (await subscription[Symbol.asyncIterator]().next()).value,
        ).toMatchObject({ event: { revision: cursor === 0n ? 2n : 3n } });
      } finally {
        await subscription.unsubscribe();
      }
    }
  }, 20_000);

  it('waits past a future numeric cursor before delivering', async () => {
    const stream = `cursor-${randomUUID()}`;
    const client = backend.getClient();
    await client.appendToStream(
      stream,
      jsonEvent({ type: 'item', data: { n: 0 } }),
    );
    const subscription = client.subscribeToStream(stream, { fromRevision: 3n });
    try {
      await once(subscription, 'caughtUp');
      const next = subscription[Symbol.asyncIterator]().next();
      await client.appendToStream(
        stream,
        [1, 2, 3, 4].map((n) => jsonEvent({ type: 'item', data: { n } })),
      );
      expect((await next).value).toMatchObject({ event: { revision: 4n } });
    } finally {
      await subscription.unsubscribe();
    }
  }, 20_000);

  it('START includes revision zero and END delivers only future events', async () => {
    const stream = `cursor-${randomUUID()}`;
    const client = backend.getClient();
    await client.appendToStream(
      stream,
      jsonEvent({ type: 'item', data: { n: 0 } }),
    );
    const start = client.subscribeToStream(stream, { fromRevision: START });
    try {
      expect((await start[Symbol.asyncIterator]().next()).value).toMatchObject({
        event: { revision: 0n },
      });
    } finally {
      await start.unsubscribe();
    }
    const end = client.subscribeToStream(stream, { fromRevision: END });
    try {
      await once(end, 'caughtUp');
      await client.appendToStream(
        stream,
        jsonEvent({ type: 'item', data: { n: 1 } }),
      );
      expect((await end[Symbol.asyncIterator]().next()).value).toMatchObject({
        event: { revision: 1n },
      });
    } finally {
      await end.unsubscribe();
    }
  }, 20_000);
});
