import { INestMicroservice } from '@nestjs/common';
import { Test } from '@nestjs/testing';
import { Transport } from '@nestjs/microservices';
import {
  ANY,
  NO_STREAM,
  KurrentDBClient,
  jsonEvent,
  StreamDeletedError,
  UnknownError,
  WrongExpectedVersionError,
} from '@kurrent/kurrentdb-client';
import { ClientSession, MongoClient } from 'mongodb';
import { GenericContainer, StartedTestContainer, Wait } from 'testcontainers';
import { join } from 'node:path';
import { AppModule } from '../src/app.module';
import { EVENT_STORE_BACKEND } from '../src/event-store/event-store-backend';
import { MongoEventStoreService } from '../src/mongodb/mongodb-event-store.service';
import { getFreePort } from './util/get-free-port';
import { SerialAdmission } from './serial-admission';
import { CooperativeDispatcher } from '../src/mongodb/cooperative-dispatcher';
import { TransactionAdmission } from '../src/mongodb/transaction-admission';
import { runMongoGroup } from '../src/mongodb/group-runner';
import { AppendReq, AppendResp } from '../src/interfaces/streams';
import { StreamDeletedServiceError } from '../src/event-store/event-store.errors';

type GroupRequest = {
  stream: string;
  options: NonNullable<AppendReq['options']>;
  events: NonNullable<AppendReq['proposedMessage']>[];
};
type GroupResult =
  | { kind: 'result'; value: AppendResp }
  | { kind: 'error'; error: StreamDeletedServiceError };

/** Test-only wiring; not a production admission or shutdown implementation. */
function installSharedAdmission(backend: MongoEventStoreService) {
  const admission = new TransactionAdmission(256, 16 * 1024 * 1024);
  const mongo = backend['infrastructure'].getClient();
  const db = mongo.db(backend['infrastructure'].getDatabaseName());
  const dispatcher = new CooperativeDispatcher<GroupRequest, GroupResult>(
    { maxRequests: 32, maxEvents: 32, maxBytes: 1024 * 1024 },
    1,
    (request) => request.stream,
    async (finalize) => {
      const release = await admission.acquire(1);
      let cleaned = false;
      try {
        const batch = finalize();
        if (!batch) {
          cleaned = true;
          return;
        }
        const borrowed = new Proxy(batch, {
          get(target, property) {
            if (property === 'retain') return () => () => undefined;
            const value: unknown = Reflect.get(target, property, target);
            return (
              typeof value === 'function' ? value.bind(target) : value
            ) as unknown;
          },
        });
        const tracked = new Proxy(mongo, {
          get(target, property) {
            if (property === 'startSession')
              return () => {
                const session = target.startSession();
                const end = session.endSession.bind(
                  session,
                ) as () => Promise<void>;
                session.endSession = async () => {
                  await end();
                  cleaned = true;
                };
                return session;
              };
            const value: unknown = Reflect.get(target, property, target);
            return (
              typeof value === 'function' ? value.bind(target) : value
            ) as unknown;
          },
        });
        try {
          await runMongoGroup(
            tracked,
            borrowed,
            (request, session) =>
              backend['appendGroupMember'](
                session,
                db,
                request.options,
                request.stream,
                request.events,
              ),
            { maxAttempts: 1, maxCommitAttempts: 1 },
          );
        } catch (error) {
          if (!cleaned) throw error;
        }
        if (!cleaned) throw new Error('session cleanup unconfirmed');
      } finally {
        if (cleaned) release();
      }
    },
  );
  Object.assign(backend, { groupDispatcher: dispatcher });
  const append = backend.append.bind(
    backend,
  ) as MongoEventStoreService['append'];
  backend.append = async (messages, context) => {
    const options = messages.find((message) => message.options)?.options;
    const stream = options?.streamIdentifier?.streamName;
    const eligible =
      stream &&
      !Buffer.from(stream).toString().startsWith('$') &&
      options?.any !== undefined &&
      options.revision === undefined &&
      options.noStream === undefined &&
      options.streamExists === undefined &&
      messages.filter((message) => message.proposedMessage).length === 1;
    if (eligible) return append(messages, context);
    const release = await admission.acquire(1, context);
    try {
      return await append(messages, context);
    } finally {
      release();
    }
  };
  const deleteStream = backend.delete.bind(
    backend,
  ) as MongoEventStoreService['delete'];
  backend.delete = async (request) => {
    const release = await admission.acquire(1);
    try {
      return await deleteStream(request);
    } finally {
      release();
    }
  };
  const tombstone = backend.tombstone.bind(
    backend,
  ) as MongoEventStoreService['tombstone'];
  backend.tombstone = async (request) => {
    const release = await admission.acquire(1);
    try {
      return await tombstone(request);
    } finally {
      release();
    }
  };
  return async () => {
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      await Promise.race([
        admission.shutdown(),
        new Promise<never>((_, reject) => {
          timer = setTimeout(
            () => reject(new Error('admission did not drain')),
            2000,
          );
        }),
      ]);
    } finally {
      if (timer) clearTimeout(timer);
    }
  };
}

describe('Mongo group commit through AppModule', () => {
  jest.setTimeout(120000);
  let container: StartedTestContainer;
  let uri: string;
  const previous = { ...process.env };
  beforeAll(async () => {
    container = await new GenericContainer('mongo:7')
      .withCommand(['mongod', '--replSet', 'rs0', '--bind_ip_all'])
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

  async function start(
    enabled: boolean,
    database?: string,
    shared = false,
  ): Promise<{
    app: INestMicroservice;
    client: KurrentDBClient;
    mongo: MongoClient;
    shutdownShared: () => Promise<void>;
  }> {
    process.env.EVENT_STORE_BACKEND = 'mongodb';
    process.env.MONGODB_URI = uri;
    process.env.MONGODB_DATABASE =
      database ?? (enabled ? 'group_app_on' : 'group_app_off');
    if (enabled) {
      process.env.MONGODB_EXPERIMENTAL_GROUP_COMMIT = 'true';
      // Deliberate test-only exception: some cases exercise unsafe mixed instances.
      process.env.MONGODB_GROUP_COMMIT_ACKNOWLEDGE_SINGLE_ADAPTER = 'true';
    } else {
      delete process.env.MONGODB_EXPERIMENTAL_GROUP_COMMIT;
      delete process.env.MONGODB_GROUP_COMMIT_ACKNOWLEDGE_SINGLE_ADAPTER;
    }
    const fixture = await Test.createTestingModule({
      imports: [AppModule],
    }).compile();
    const backend = fixture.get<MongoEventStoreService>(EVENT_STORE_BACKEND);
    const shutdownShared = shared
      ? installSharedAdmission(backend)
      : () => Promise.resolve();
    const mongo = backend['infrastructure'].getClient();
    Object.assign(mongo.options, { monitorCommands: true });
    const port = await getFreePort();
    const app = fixture.createNestMicroservice({
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
    return { app, client, mongo, shutdownShared };
  }

  it.each([true, false])(
    'coalescing is enabled only with flag=%s',
    async (enabled) => {
      const { app, client, mongo } = await start(enabled);
      const commits: Record<string, unknown>[] = [];
      const listener = (event: {
        commandName: string;
        command: Record<string, unknown>;
      }) => {
        if (event.commandName === 'commitTransaction')
          commits.push(event.command);
      };
      mongo.on('commandStarted', listener);
      try {
        const results = await Promise.all(
          ['a', 'b'].map((stream) =>
            client.appendToStream(
              stream,
              jsonEvent({ type: 'test', data: {} }),
              { streamState: ANY, deadline: 5000 },
            ),
          ),
        );
        expect(results.every((result) => result.success)).toBe(true);
        expect(commits).toHaveLength(enabled ? 1 : 2);
        for (const command of commits)
          expect(command.writeConcern).toMatchObject({
            w: 'majority',
            j: true,
          });
      } finally {
        mongo.off('commandStarted', listener);
        await client.dispose();
        await app.close();
      }
    },
  );

  it('keeps ineligible requests on the ordinary path', async () => {
    const { app, client, mongo } = await start(true);
    const commits: string[] = [];
    const listener = (event: { commandName: string }) => {
      if (event.commandName === 'commitTransaction')
        commits.push(event.commandName);
    };
    mongo.on('commandStarted', listener);
    try {
      await client.appendToStream(
        'eligible-control',
        jsonEvent({ type: 'control', data: {} }),
        { streamState: ANY },
      );
      await Promise.all([
        client.appendToStream(
          'ineligible-expected',
          jsonEvent({ type: 'expected', data: {} }),
          { streamState: NO_STREAM },
        ),
        client.appendToStream(
          'ineligible-multi',
          [
            jsonEvent({ type: 'first', data: {} }),
            jsonEvent({ type: 'second', data: {} }),
          ],
          { streamState: ANY },
        ),
      ]);
      expect(commits).toHaveLength(3);
    } finally {
      mongo.off('commandStarted', listener);
      await client.dispose();
      await app.close();
    }
  });

  it('maps a tombstone without consuming the healthy peer position', async () => {
    const { app, client } = await start(true);
    try {
      const deleted = await client.tombstoneStream('actual-deleted');
      const results = await Promise.allSettled([
        client.appendToStream(
          'actual-deleted',
          jsonEvent({ type: 'bad', data: {} }),
          { streamState: ANY },
        ),
        client.appendToStream(
          'actual-live',
          jsonEvent({ type: 'live', data: {} }),
          { streamState: ANY },
        ),
      ]);
      expect(results[0].status).toBe('rejected');
      if (results[0].status !== 'rejected')
        throw new Error('expected tombstone');
      expect(results[0].reason).toBeInstanceOf(StreamDeletedError);
      if (results[1].status !== 'fulfilled')
        throw new Error('expected success');
      expect(results[1].value.position!.commit).toBe(
        deleted.position!.commit + 1n,
      );
    } finally {
      await client.dispose();
      await app.close();
    }
  });

  it('reports a lost grouped commit acknowledgement as unknown without replay', async () => {
    const { app, client, mongo } = await start(true);
    // eslint-disable-next-line @typescript-eslint/unbound-method
    const original = ClientSession.prototype.commitTransaction;
    const spy = jest
      .spyOn(ClientSession.prototype, 'commitTransaction')
      .mockImplementation(async function (this: ClientSession) {
        await original.call(this);
        if ((this as unknown as { client: MongoClient }).client === mongo)
          throw new Error('lost acknowledgement');
      });
    try {
      const results = await Promise.allSettled(
        ['actual-unknown-a', 'actual-unknown-b'].map((stream) =>
          client.appendToStream(
            stream,
            jsonEvent({ type: 'uncertain', data: {} }),
            { streamState: ANY, deadline: 3000 },
          ),
        ),
      );
      for (const result of results) {
        expect(result.status).toBe('rejected');
        if (result.status !== 'rejected') throw new Error('expected unknown');
        expect(result.reason).toBeInstanceOf(UnknownError);
      }
      spy.mockRestore();
      for (const stream of ['actual-unknown-a', 'actual-unknown-b']) {
        const types: string[] = [];
        for await (const { event } of client.readStream(stream))
          if (event) types.push(event.type);
        expect(types).toEqual(['uncertain']);
      }
    } finally {
      spy.mockRestore();
      await client.dispose();
      await app.close();
    }
  });

  it('orders grouped and ordinary writes across application instances', async () => {
    const first = await start(true, 'group_mixed');
    const second = await start(false, 'group_mixed');
    try {
      const grouped = await Promise.all(
        ['mixed-a', 'mixed-b'].map((stream) =>
          first.client.appendToStream(
            stream,
            jsonEvent({ type: 'group', data: {} }),
            { streamState: ANY },
          ),
        ),
      );
      const ordinary = await second.client.appendToStream(
        'mixed-c',
        jsonEvent({ type: 'ordinary', data: {} }),
        { streamState: ANY },
      );
      const later = await first.client.appendToStream(
        'mixed-d',
        jsonEvent({ type: 'later', data: {} }),
        { streamState: ANY },
      );
      const positions = [...grouped, ordinary, later]
        .map((result) => result.position!.commit)
        .sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
      expect(new Set(positions).size).toBe(4);
      expect(ordinary.position!.commit).toBeGreaterThan(
        grouped[0].position!.commit,
      );
      expect(ordinary.position!.commit).toBeGreaterThan(
        grouped[1].position!.commit,
      );
      expect(later.position!.commit).toBe(ordinary.position!.commit + 1n);
    } finally {
      await first.client.dispose();
      await second.client.dispose();
      await first.app.close();
      await second.app.close();
    }
  });

  it.each(['parallel', 'serial'] as const)(
    'reconciles %s grouped and ordinary instance writes and $all delivery',
    async (mode) => {
      const database = `group_concurrent_${mode}`;
      const first = await start(true, database);
      const second = await start(false, database);
      const admission = new SerialAdmission(3);
      try {
        const attempts: {
          stream: string;
          id: string;
          result: PromiseSettledResult<unknown>;
        }[] = [];
        const latencies: number[] = [];
        const started = performance.now();
        for (let round = 0; round < 20; round++) {
          const writes = [0, 1, 2].map((index) => {
            const stream = `concurrent-${round}-${index}`;
            const event = jsonEvent({
              type: 'concurrent',
              data: { round, index },
            });
            const client = index === 2 ? second.client : first.client;
            const append = () =>
              client.appendToStream(stream, event, {
                streamState: index === 2 ? NO_STREAM : ANY,
                deadline: 5000,
              });
            const submitted = performance.now();
            const promise =
              mode === 'serial' ? admission.run(append) : append();
            return {
              stream,
              event,
              promise: promise.finally(() =>
                latencies.push(performance.now() - submitted),
              ),
            };
          });
          const results = await Promise.allSettled(
            writes.map((write) => write.promise),
          );
          writes.forEach((write, index) =>
            attempts.push({
              stream: write.stream,
              id: write.event.id,
              result: results[index],
            }),
          );
        }
        const elapsed = (performance.now() - started) / 1000;
        expect(attempts).toHaveLength(60);
        const persisted: { id: string; stream: string; position: bigint }[] =
          [];
        for await (const { event } of second.client.readAll()) {
          if (event?.type === 'concurrent')
            persisted.push({
              id: event.id,
              stream: event.streamId,
              position: event.position.commit,
            });
        }
        expect(new Set(persisted.map((event) => event.id)).size).toBe(
          persisted.length,
        );
        for (const attempt of attempts) {
          const matches = persisted.filter((event) => event.id === attempt.id);
          expect(matches.length).toBeLessThanOrEqual(1);
          if (attempt.result.status === 'fulfilled')
            expect(matches).toHaveLength(1);
          else expect(attempt.result.reason).toBeInstanceOf(UnknownError);
          if (matches.length) expect(matches[0].stream).toBe(attempt.stream);
        }
        for (let i = 1; i < persisted.length; i++)
          expect(persisted[i].position).toBe(persisted[i - 1].position + 1n);
        const subscription = second.client.subscribeToAll();
        try {
          const iterator = subscription[Symbol.asyncIterator]();
          const delivered: string[] = [];
          for (let i = 0; i < persisted.length; i++) {
            const next = await Promise.race([
              iterator.next(),
              new Promise<never>((_, reject) =>
                setTimeout(
                  () => reject(new Error('subscription timed out')),
                  5000,
                ).unref(),
              ),
            ]);
            if (next.done) throw new Error('subscription ended early');
            if (next.value.event) delivered.push(next.value.event.id);
          }
          expect(delivered).toEqual(persisted.map((event) => event.id));
        } finally {
          await subscription.unsubscribe();
        }
        const failures = attempts.filter(
          (attempt) => attempt.result.status === 'rejected',
        ).length;
        expect(
          persisted.every((event) => attempts.some((a) => a.id === event.id)),
        ).toBe(true);
        latencies.sort((a, b) => a - b);
        console.log(
          `latency ${mode}: p95=${latencies[Math.ceil(latencies.length * 0.95) - 1].toFixed(3)}ms; samples=${latencies.length}`,
        );
        console.log(
          `mixed ${mode}: ${failures}/${attempts.length} unknown; ${persisted.length} persisted; ${elapsed.toFixed(3)}s; ${(attempts.length / elapsed).toFixed(1)} attempted/s; ${((attempts.length - failures) / elapsed).toFixed(1)} successful/s (driver transaction retries enabled)`,
        );
      } finally {
        await first.client.dispose();
        await second.client.dispose();
        await first.app.close();
        await second.app.close();
      }
    },
  );

  it.each(['parallel', 'serial', 'shared'] as const)(
    'reconciles single-instance mixed mutations with %s admission',
    async (mode) => {
      const { app, client, shutdownShared } = await start(
        true,
        `single_mixed_${mode}`,
        mode === 'shared',
      );
      const admission = new SerialAdmission(8);
      const counts: Record<string, Record<string, number>> = {};
      const attempted = new Map<string, { stream: string; outcome: string }>();
      try {
        for (let round = 0; round < 10; round++) {
          const soft = `soft-${round}`;
          const hard = `hard-${round}`;
          for (const stream of [soft, hard])
            await client.appendToStream(
              stream,
              jsonEvent({ type: 'seed', data: {} }),
              {
                streamState: NO_STREAM,
              },
            );
          const writes = [0, 1, 2].map((index) => ({
            stream: `single-${round}-${index}`,
            event: jsonEvent({ type: 'single-mixed', data: { round, index } }),
            index,
          }));
          const actions: { kind: string; run: () => Promise<unknown> }[] = [
            ...writes.map((write) => ({
              kind: write.index === 2 ? 'ordinary' : 'grouped',
              run: () =>
                client.appendToStream(write.stream, write.event, {
                  streamState: write.index === 2 ? NO_STREAM : ANY,
                  deadline: 5000,
                }),
            })),
            { kind: 'delete', run: () => client.deleteStream(soft) },
            { kind: 'tombstone', run: () => client.tombstoneStream(hard) },
          ];
          const results = await Promise.allSettled(
            actions.map(({ run }) =>
              mode === 'serial' ? admission.run(run) : run(),
            ),
          );
          results.forEach((result, index) => {
            const kind = actions[index].kind;
            const outcome =
              result.status === 'fulfilled'
                ? 'success'
                : result.reason instanceof UnknownError
                  ? 'unknown'
                  : 'other';
            counts[kind] ??= { success: 0, unknown: 0, other: 0 };
            counts[kind][outcome]++;
            if (index < writes.length)
              attempted.set(writes[index].event.id, {
                stream: writes[index].stream,
                outcome,
              });
          });
          if (results[3].status === 'fulfilled') {
            const replacement = jsonEvent({ type: 'replacement', data: {} });
            await client.appendToStream(soft, replacement, {
              streamState: ANY,
            });
            const visible: string[] = [];
            for await (const { event } of client.readStream(soft))
              if (event) visible.push(event.id);
            expect(visible).toEqual([replacement.id]);
          }
          if (results[4].status === 'fulfilled')
            await expect(
              client.appendToStream(
                hard,
                jsonEvent({ type: 'forbidden', data: {} }),
                {
                  streamState: ANY,
                  deadline: 5000,
                },
              ),
            ).rejects.toBeInstanceOf(StreamDeletedError);
        }
        const seen = new Set<string>();
        let previousPosition: bigint | undefined;
        for await (const { event } of client.readAll()) {
          if (!event) continue;
          if (previousPosition !== undefined)
            expect(event.position.commit).toBeGreaterThan(previousPosition);
          previousPosition = event.position.commit;
          if (event.type !== 'single-mixed') continue;
          expect(seen.has(event.id)).toBe(false);
          seen.add(event.id);
          expect(attempted.get(event.id)?.stream).toBe(event.streamId);
        }
        for (const [id, attempt] of attempted)
          if (attempt.outcome === 'success') expect(seen.has(id)).toBe(true);
        const unknown = [...attempted].filter(
          ([, value]) => value.outcome === 'unknown',
        );
        const unknownPersisted = unknown.filter(([id]) => seen.has(id)).length;
        console.log(
          `single mixed ${mode}: ${JSON.stringify(counts)}; attempted IDs persisted=${seen.size}/${attempted.size}; unknown append IDs observed=${unknownPersisted}/${unknown.length}`,
        );
        if (mode === 'shared') {
          expect(counts).toEqual({
            grouped: { success: 20, unknown: 0, other: 0 },
            ordinary: { success: 10, unknown: 0, other: 0 },
            delete: { success: 10, unknown: 0, other: 0 },
            tombstone: { success: 10, unknown: 0, other: 0 },
          });
          expect(seen.size).toBe(30);
        }
      } finally {
        try {
          await client.dispose();
        } finally {
          try {
            await app.close();
          } finally {
            await shutdownShared();
          }
        }
      }
    },
  );

  it('observes delete and tombstone bypassing test-only append admission', async () => {
    const first = await start(true, 'group_mutation_bypass');
    const second = await start(false, 'group_mutation_bypass');
    const admission = new SerialAdmission(3);
    let release!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    try {
      await second.client.appendToStream(
        'soft-target',
        jsonEvent({ type: 'seed', data: {} }),
        { streamState: NO_STREAM },
      );
      await second.client.appendToStream(
        'hard-target',
        jsonEvent({ type: 'seed', data: {} }),
        { streamState: NO_STREAM },
      );
      const blocked = admission.run(async () => {
        await gate;
        return first.client.appendToStream(
          'queued-target',
          jsonEvent({ type: 'queued', data: {} }),
          { streamState: ANY },
        );
      });
      const mutations = await Promise.allSettled([
        second.client.deleteStream('soft-target'),
        second.client.tombstoneStream('hard-target'),
      ]);
      release();
      const append = await blocked;
      const all: { type: string; position: bigint }[] = [];
      for await (const { event } of second.client.readAll())
        if (event)
          all.push({ type: event.type, position: event.position.commit });
      for (let i = 1; i < all.length; i++)
        expect(all[i].position).toBeGreaterThan(all[i - 1].position);
      expect(all.filter((event) => event.type === 'queued')).toHaveLength(1);
      expect(append.success).toBe(true);
      expect(mutations.every((r) => r.status === 'fulfilled')).toBe(true);
      await expect(
        second.client.appendToStream(
          'hard-target',
          jsonEvent({ type: 'forbidden', data: {} }),
          { streamState: ANY },
        ),
      ).rejects.toBeInstanceOf(StreamDeletedError);
      console.log(
        `mutation bypass: delete=${mutations[0].status} tombstone=${mutations[1].status}; both completed before admission released; $all positions=${all.map((e) => `${e.type}:${e.position}`).join(',')}`,
      );
    } finally {
      release();
      await first.client.dispose();
      await second.client.dispose();
      await first.app.close();
      await second.app.close();
    }
  });

  it('reconciles a grouped append racing an exact-revision append on the same stream', async () => {
    const first = await start(true, 'group_expected_conflicts');
    const second = await start(false, 'group_expected_conflicts');
    try {
      for (let round = 0; round < 10; round++) {
        const stream = `expected-race-${round}`;
        const seed = jsonEvent({ type: 'seed', data: {} });
        await second.client.appendToStream(stream, seed, {
          streamState: NO_STREAM,
        });
        const grouped = jsonEvent({ type: 'grouped', data: {} });
        const exact = jsonEvent({ type: 'exact', data: {} });
        const results = await Promise.allSettled([
          first.client.appendToStream(stream, grouped, {
            streamState: ANY,
            deadline: 5000,
          }),
          second.client.appendToStream(stream, exact, {
            streamState: 0n,
            deadline: 5000,
          }),
        ]);
        const events: { id: string; revision: bigint }[] = [];
        for await (const { event } of second.client.readStream(stream))
          if (event)
            events.push({ id: event.id, revision: BigInt(event.revision) });
        expect(events[0]).toEqual({ id: seed.id, revision: 0n });
        expect(new Set(events.map((event) => event.id)).size).toBe(
          events.length,
        );
        expect(events.map((event) => event.revision)).toEqual(
          events.map((_, index) => BigInt(index)),
        );
        for (const [index, event] of [grouped, exact].entries()) {
          const matches = events.filter((entry) => entry.id === event.id);
          expect(matches.length).toBeLessThanOrEqual(1);
          if (results[index].status === 'fulfilled')
            expect(matches).toHaveLength(1);
        }
        expect(
          events.every((event) =>
            [seed.id, grouped.id, exact.id].includes(event.id),
          ),
        ).toBe(true);
        if (events.length > 1)
          await expect(
            second.client.appendToStream(
              stream,
              jsonEvent({ type: 'stale', data: {} }),
              { streamState: 0n, deadline: 5000 },
            ),
          ).rejects.toBeInstanceOf(WrongExpectedVersionError);
      }
    } finally {
      await first.client.dispose();
      await second.client.dispose();
      await first.app.close();
      await second.app.close();
    }
  });

  it('rejects oversized eligible work without affecting its peer', async () => {
    const { app, client } = await start(true);
    try {
      const results = await Promise.allSettled([
        client.appendToStream(
          'actual-oversized',
          jsonEvent({
            type: 'oversized',
            data: { payload: 'x'.repeat(1100000) },
          }),
          { streamState: ANY, deadline: 3000 },
        ),
        client.appendToStream(
          'actual-capacity-peer',
          jsonEvent({ type: 'peer', data: {} }),
          { streamState: ANY, deadline: 3000 },
        ),
      ]);
      expect(results[0].status).toBe('rejected');
      if (results[0].status !== 'rejected')
        throw new Error('expected capacity error');
      expect(results[0].reason).toBeInstanceOf(UnknownError);
      expect(results[1].status).toBe('fulfilled');
    } finally {
      await client.dispose();
      await app.close();
    }
  });

  it('returns unknown on a sealed deadline even if the write later commits', async () => {
    const { app, client, mongo } = await start(true);
    let release!: () => void;
    let entered!: () => void;
    const gate = new Promise<void>((resolve) => (release = resolve));
    const atCommit = new Promise<void>((resolve) => (entered = resolve));
    // eslint-disable-next-line @typescript-eslint/unbound-method
    const original = ClientSession.prototype.commitTransaction;
    const spy = jest
      .spyOn(ClientSession.prototype, 'commitTransaction')
      .mockImplementation(async function (this: ClientSession) {
        if ((this as unknown as { client: MongoClient }).client === mongo) {
          entered();
          await gate;
        }
        await original.call(this);
      });
    try {
      const expired = client
        .appendToStream('actual-late', jsonEvent({ type: 'late', data: {} }), {
          streamState: ANY,
          deadline: 600,
        })
        .then(
          () => null,
          (error: unknown) => error,
        );
      const peer = client.appendToStream(
        'actual-late-peer',
        jsonEvent({ type: 'peer', data: {} }),
        { streamState: ANY, deadline: 5000 },
      );
      await atCommit;
      expect(await expired).toBeInstanceOf(UnknownError);
      release();
      expect((await peer).success).toBe(true);
      spy.mockRestore();
      const types: string[] = [];
      for await (const { event } of client.readStream('actual-late'))
        if (event) types.push(event.type);
      expect(types).toEqual(['late']);
    } finally {
      release();
      spy.mockRestore();
      await client.dispose();
      await app.close();
    }
  });
});
