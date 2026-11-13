import { INestMicroservice } from '@nestjs/common';
import { Test } from '@nestjs/testing';
import { Transport } from '@nestjs/microservices';
import {
  KurrentDBClient,
  NO_STREAM,
  jsonEvent,
} from '@kurrent/kurrentdb-client';
import { Collection, Document, FindCursor, MongoClient } from 'mongodb';
import { GenericContainer, StartedTestContainer, Wait } from 'testcontainers';
import { join } from 'node:path';
import { AppModule } from '../src/app.module';
import { EVENT_STORE_BACKEND } from '../src/event-store/event-store-backend';
import { MongoEventStoreService } from '../src/mongodb/mongodb-event-store.service';
import { getFreePort } from './util/get-free-port';

describe('Mongo cursor cleanup during shutdown', () => {
  jest.setTimeout(120000);
  let container: StartedTestContainer;
  let uri: string;
  const previous = { ...process.env };
  let savedExitCode: typeof process.exitCode;
  beforeEach(() => {
    savedExitCode = process.exitCode;
  });
  afterEach(() => {
    process.exitCode = savedExitCode;
  });

  beforeAll(async () => {
    container = await new GenericContainer('mongo:7')
      .withCommand([
        'mongod',
        '--replSet',
        'rs0',
        '--bind_ip_all',
        '--setParameter',
        'enableTestCommands=1',
      ])
      .withExposedPorts(27017)
      .withWaitStrategy(Wait.forLogMessage('Waiting for connections'))
      .start();
    const init = await container.exec([
      'mongosh',
      '--quiet',
      '--eval',
      "rs.initiate({_id:'rs0',members:[{_id:0,host:'localhost:27017'}]}); while (!db.hello().isWritablePrimary) sleep(200)",
    ]);
    if (init.exitCode !== 0) throw new Error(init.output);
    uri = `mongodb://${container.getHost()}:${container.getMappedPort(27017)}/?directConnection=true`;
  });
  afterAll(async () => {
    process.env = previous;
    await container?.stop();
  });

  it('does not mistake driver close for server cursor destruction', async () => {
    const client = new MongoClient(uri);
    await client.connect();
    const session = client.startSession();
    const db = client.db('cursor_failpoint');
    const collection = db.collection('items');
    try {
      await collection.insertMany([{ n: 1 }, { n: 2 }, { n: 3 }]);
      const cursor = collection.find({}, { session, batchSize: 1 });
      expect(await cursor.next()).not.toBeNull();
      const id = cursor.id;
      expect(id?.isZero()).toBe(false);
      await db.admin().command({
        configureFailPoint: 'failCommand',
        mode: { times: 1 },
        data: { failCommands: ['killCursors'], errorCode: 11601 },
      });
      const cleanup: { cleanup: 'complete' | 'unconfirmed' } = {
        cleanup: 'complete',
      };
      const service = new MongoEventStoreService();
      await service['readCursor'](cursor, cleanup, () => Promise.resolve());
      expect(cleanup.cleanup).toBe('complete'); // local driver work only
      const reply = await db.command(
        { getMore: id, collection: 'items', batchSize: 1 },
        { session },
      );
      expect((reply.cursor as { nextBatch: unknown[] }).nextBatch).toHaveLength(
        1,
      );
      await db.command({ killCursors: 'items', cursors: [id] }, { session });
    } finally {
      await db
        .admin()
        .command({ configureFailPoint: 'failCommand', mode: 'off' });
      await session.endSession();
      await client.close();
    }
  });

  it.each([
    ['read', 'hang'],
    ['read', 'failure'],
    ['subscription', 'hang'],
    ['subscription', 'failure'],
  ] as const)(
    '%s retains the client on cursor cleanup %s',
    async (kind, outcome) => {
      process.env.EVENT_STORE_BACKEND = 'mongodb';
      process.env.MONGODB_URI = uri;
      process.env.MONGODB_DATABASE = `cursor_${kind}_${outcome}`;
      process.env.MONGODB_EXPERIMENTAL_GROUP_COMMIT = 'false';
      const fixture = await Test.createTestingModule({
        imports: [AppModule],
      }).compile();
      const backend = fixture.get<MongoEventStoreService>(EVENT_STORE_BACKEND);
      const mongo = backend['infrastructure'].getClient();
      const port = await getFreePort();
      const app: INestMicroservice = fixture.createNestMicroservice({
        transport: Transport.GRPC,
        options: {
          package: [
            'event_store.client.streams',
            'event_store.client.server_features',
          ],
          protoPath: [
            join(__dirname, '../proto/Grpc/streams.proto'),
            join(__dirname, '../proto/Grpc/serverfeatures.proto'),
          ],
          loader: { includeDirs: [join(__dirname, '../proto/Grpc')] },
          url: `127.0.0.1:${port}`,
        },
      });
      await app.listen();
      const client = KurrentDBClient.connectionString`kurrentdb://127.0.0.1:${port}?tls=false`;
      await client.appendToStream(
        'cursor-target',
        jsonEvent({ type: 'seed', data: {} }),
        { streamState: NO_STREAM },
      );
      let release!: () => void;
      let entered!: () => void;
      const gate = new Promise<void>((resolve) => {
        release = resolve;
      });
      const started = new Promise<void>((resolve) => {
        entered = resolve;
      });
      let closed!: () => void;
      const clientClosed = new Promise<void>((resolve) => {
        closed = resolve;
      });
      const close = backend['infrastructure'].close.bind(
        backend['infrastructure'],
      ) as () => Promise<void>;
      const closeClient = jest
        .spyOn(backend['infrastructure'], 'close')
        .mockImplementation(async () => {
          closed();
          await close();
        });
      // Preserve the prototype receiver when invoking the real driver method.
      // eslint-disable-next-line @typescript-eslint/unbound-method
      const originalFind = Collection.prototype.find;
      const find = jest
        .spyOn(Collection.prototype, 'find')
        .mockImplementation(function (
          this: Collection,
          ...args: Parameters<typeof originalFind>
        ) {
          const cursor = originalFind.apply(this, args) as FindCursor<Document>;
          if (this.collectionName === 'stream_events') {
            find.mockRestore();
            const toArray = cursor.toArray.bind(cursor) as () => Promise<
              Document[]
            >;
            jest.spyOn(cursor, 'toArray').mockImplementation(async () => {
              const rows = await toArray();
              // The driver may already have exhausted/closed this cursor. Gate
              // only the service's explicit close, not driver-internal closes.
              const close = cursor.close.bind(cursor) as () => Promise<void>;
              jest.spyOn(cursor, 'close').mockImplementation(async () => {
                entered();
                await gate;
                if (outcome === 'failure')
                  throw new Error('injected cursor close failure');
                await close();
              });
              return rows;
            });
          }
          return cursor;
        } as typeof originalFind);
      const diagnostic = jest.spyOn(console, 'error').mockImplementation();
      let subscription:
        | ReturnType<KurrentDBClient['subscribeToStream']>
        | undefined;
      let operation: Promise<unknown> | undefined;
      try {
        if (kind === 'read') {
          operation = (async () => {
            for await (const event of client.readStream('cursor-target'))
              void event;
          })().catch((error: unknown) => error);
        } else {
          subscription = client.subscribeToStream('cursor-target', {
            fromRevision: 0n,
          });
          operation = subscription[Symbol.asyncIterator]()
            .next()
            .catch((error: unknown) => error);
        }
        await Promise.race([
          started,
          new Promise<never>((_, reject) =>
            setTimeout(
              () => reject(new Error('cursor cleanup not reached')),
              10000,
            ).unref(),
          ),
        ]);
        const shutdown = backend.onModuleDestroy();
        await shutdown;
        expect(process.exitCode).toBe(1);
        expect(closeClient).not.toHaveBeenCalled();
        release();
        await operation;
        if (outcome === 'hang') {
          await Promise.race([
            clientClosed,
            new Promise<never>((_, reject) =>
              setTimeout(
                () =>
                  reject(
                    new Error('client did not close after cursor cleanup'),
                  ),
                5000,
              ).unref(),
            ),
          ]);
          expect(closeClient).toHaveBeenCalledTimes(1);
        } else {
          await new Promise((resolve) => setTimeout(resolve, 50));
          expect(closeClient).not.toHaveBeenCalled();
        }
      } finally {
        release();
        find.mockRestore();
        diagnostic.mockRestore();
        await subscription?.unsubscribe();
        await client.dispose();
        await app.close();
        closeClient.mockRestore();
        await mongo.close();
      }
    },
  );
});
