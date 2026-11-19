import {
  END,
  START,
  binaryEvent,
  jsonEvent,
  streamNameFilter,
} from '@kurrent/kurrentdb-client';
import { randomUUID } from 'node:crypto';
import { setupKurrentDbBackend } from './contracts/kurrentdb-backend';
import { setupMongoDBBackend } from './contracts/mongodb-backend';
import { ScavengeCapableBackend } from './contracts/contract-test-context';

type ObservedEvent = {
  event?: { type: string; revision: bigint };
  link?: unknown;
};

describe.each([
  ['KurrentDB', setupKurrentDbBackend],
  ['MongoDB', setupMongoDBBackend],
] as const)('%s subscriptions with resolveLinkTos', (_name, setup) => {
  let backend: ScavengeCapableBackend;
  beforeAll(async () => {
    backend = await setup();
  }, 120_000);
  afterAll(async () => {
    await backend?.dispose();
  });

  async function appendThree(stream: string): Promise<void> {
    await backend.getClient().appendToStream(
      stream,
      [0, 1, 2].map((n) => jsonEvent({ type: 'item', data: { n } })),
    );
  }

  it.each([
    ['START', START, [0n, 1n, 2n]],
    ['revision 0', 0n, [1n, 2n]],
    ['revision 1', 1n, [2n]],
  ] as const)(
    'delivers ordinary events unchanged from %s',
    async (_label, fromRevision, expected) => {
      const stream = `resolve-sub-${randomUUID()}`;
      await appendThree(stream);
      const subscription = backend
        .getClient()
        .subscribeToStream(stream, { fromRevision, resolveLinkTos: true });
      const revisions: bigint[] = [];
      try {
        for await (const resolved of subscription as AsyncIterable<ObservedEvent>) {
          expect(resolved.link).toBeUndefined();
          revisions.push(resolved.event!.revision);
          if (revisions.length === expected.length) break;
        }
      } finally {
        await subscription.unsubscribe();
      }
      expect(revisions).toEqual(expected);
    },
    20_000,
  );

  it('delivers a live write after END', async () => {
    const stream = `resolve-live-${randomUUID()}`;
    const client = backend.getClient();
    await appendThree(stream);
    const subscription = client.subscribeToStream(stream, {
      fromRevision: END,
      resolveLinkTos: true,
    });
    try {
      const iterator = subscription[Symbol.asyncIterator]() as AsyncIterator<
        ObservedEvent,
        void
      >;
      const next = iterator.next();
      await new Promise((resolve) => setTimeout(resolve, 300));
      await client.appendToStream(
        stream,
        jsonEvent({ type: 'live', data: {} }),
      );
      const result = await next;
      expect(result.value?.event).toMatchObject({
        type: 'live',
        revision: 3n,
      });
      expect(result.value?.link).toBeUndefined();
    } finally {
      await subscription.unsubscribe();
    }
  }, 20_000);

  it('delivers ordinary $all events with resolveLinkTos', async () => {
    const stream = `resolve-all-${randomUUID()}`;
    const client = backend.getClient();
    await appendThree(stream);
    const subscription = client.subscribeToAll({
      fromPosition: START,
      resolveLinkTos: true,
      filter: streamNameFilter({ prefixes: [stream] }),
    });
    const types: string[] = [];
    try {
      for await (const resolved of subscription as AsyncIterable<ObservedEvent>) {
        if (!resolved.event) continue;
        expect(resolved.link).toBeUndefined();
        types.push(resolved.event.type);
        if (types.length === 3) break;
      }
    } finally {
      await subscription.unsubscribe();
    }
    expect(types).toEqual(['item', 'item', 'item']);
  }, 20_000);
});

describe('MongoDB subscriptions with link events', () => {
  let backend: ScavengeCapableBackend;
  beforeAll(async () => {
    backend = await setupMongoDBBackend();
  }, 120_000);
  afterAll(async () => {
    await backend?.dispose();
  });

  it('rejects link events when link resolution is requested', async () => {
    const stream = `link-sub-${randomUUID()}`;
    const client = backend.getClient();
    await client.appendToStream(stream, [
      jsonEvent({ type: 'item', data: {} }),
      binaryLink('0@some-target'),
    ]);
    const subscription = client.subscribeToStream(stream, {
      fromRevision: START,
      resolveLinkTos: true,
    });
    const types: string[] = [];
    await expect(
      (async () => {
        try {
          for await (const { event } of subscription) {
            if (event) types.push(event.type);
          }
        } finally {
          await subscription.unsubscribe();
        }
      })(),
    ).rejects.toThrow();
    expect(types).toEqual(['item']);
  }, 20_000);

  it('passes link events through without link resolution', async () => {
    const stream = `link-plain-${randomUUID()}`;
    const client = backend.getClient();
    await client.appendToStream(stream, binaryLink('0@some-target'));
    const subscription = client.subscribeToStream(stream, {
      fromRevision: START,
    });
    try {
      const result = await subscription[Symbol.asyncIterator]().next();
      expect(result.value).toMatchObject({ event: { type: '$>' } });
    } finally {
      await subscription.unsubscribe();
    }
  }, 20_000);
});

function binaryLink(target: string) {
  return binaryEvent({ type: '$>', data: Buffer.from(target) });
}
