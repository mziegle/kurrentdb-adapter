import { AddressInfo, Server, Socket } from 'node:net';
import { once } from 'node:events';
import { join } from 'node:path';
import { INestMicroservice } from '@nestjs/common';
import { Transport } from '@nestjs/microservices';
import { Test } from '@nestjs/testing';
import { END, KurrentDBClient, jsonEvent } from '@kurrent/kurrentdb-client';
import { GenericContainer, StartedTestContainer, Wait } from 'testcontainers';
import { AppModule } from '../src/app.module';
import { shutdownProbeProxy, startProbeProxy } from '../src/main';
import { AdapterStatsService } from '../src/operations/adapter-stats.service';
import { getFreePort } from './util/get-free-port';

// Importing main normally starts a second application. Keep its bootstrap pending;
// the real proxy function and the test's real AppModule are otherwise untouched.
jest.mock('@nestjs/core', () => {
  const actual =
    jest.requireActual<typeof import('@nestjs/core')>('@nestjs/core');
  return {
    ...actual,
    NestFactory: { createMicroservice: () => new Promise(() => {}) },
  };
});

async function bounded<T>(
  promise: Promise<T>,
  ms: number,
  label: string,
): Promise<T> {
  let timer: NodeJS.Timeout | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error(`${label} timed out`)), ms);
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

describe('official live subscription through the limited probe proxy', () => {
  jest.setTimeout(120000);
  const previous = { ...process.env };
  let mongo: StartedTestContainer;
  let app: INestMicroservice;
  let proxy: Server;
  let client: KurrentDBClient;
  const sockets = new Set<Socket>();

  beforeAll(async () => {
    mongo = await new GenericContainer('mongo:7')
      .withCommand(['mongod', '--replSet', 'rs0', '--bind_ip_all'])
      .withExposedPorts(27017)
      .withWaitStrategy(Wait.forLogMessage('Waiting for connections'))
      .start();
    const init = await mongo.exec([
      'mongosh',
      '--quiet',
      '--eval',
      "rs.initiate({_id:'rs0',members:[{_id:0,host:'localhost:27017'}]}); while (!db.hello().isWritablePrimary) sleep(200)",
    ]);
    if (init.exitCode !== 0) throw new Error(init.output);
    process.env.EVENT_STORE_BACKEND = 'mongodb';
    process.env.MONGODB_URI = `mongodb://${mongo.getHost()}:${mongo.getMappedPort(27017)}/?directConnection=true`;
    process.env.MONGODB_DATABASE = 'proxy_subscription';
    process.env.PROXY_MAX_CONNECTIONS = '8';
    process.env.PROXY_PREFACE_TIMEOUT_MS = '250';
    const fixture = await Test.createTestingModule({
      imports: [AppModule],
    }).compile();
    const internalPort = await getFreePort();
    app = fixture.createNestMicroservice({
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
        url: `127.0.0.1:${internalPort}`,
      },
    });
    await app.listen();
    proxy = await startProbeProxy(
      '127.0.0.1:0',
      `127.0.0.1:${internalPort}`,
      fixture.get(AdapterStatsService),
    );
    proxy.on('connection', (socket) => {
      sockets.add(socket);
      socket.once('close', () => sockets.delete(socket));
    });
    const port = (proxy.address() as AddressInfo).port;
    client = KurrentDBClient.connectionString`kurrentdb://127.0.0.1:${port}?tls=false`;
  });

  afterAll(async () => {
    try {
      await client?.dispose();
      for (const socket of sockets) socket.destroy();
      if (proxy?.listening)
        await bounded(
          new Promise<void>((resolve) => proxy.close(() => resolve())),
          5000,
          'proxy cleanup',
        );
      await bounded(app?.close() ?? Promise.resolve(), 10000, 'app cleanup');
    } finally {
      await mongo?.stop();
      process.env = previous;
    }
  });

  it('delivers after the preface deadline and close waits for an established session', async () => {
    const stream = 'proxy-live';
    await client.appendToStream(stream, jsonEvent({ type: 'seed', data: {} }));
    const subscription = client.subscribeToStream(stream, {
      fromRevision: END,
    });
    const caughtUp = once(subscription, 'caughtUp');
    const iterator = subscription[Symbol.asyncIterator]() as AsyncIterator<
      { event?: { id: string } },
      void
    >;
    let closed: Promise<void> | undefined;
    try {
      await bounded(caughtUp, 10000, 'subscription caught up');
      const established = [...sockets];
      expect(established.length).toBeGreaterThan(0);
      await new Promise((resolve) => setTimeout(resolve, 1000));
      expect(
        established.every((socket) => sockets.has(socket) && !socket.destroyed),
      ).toBe(true);
      const event = jsonEvent({ type: 'after-idle', data: { value: 42 } });
      await bounded(client.appendToStream(stream, event), 10000, 'append');
      const received = await bounded(iterator.next(), 10000, 'live event');
      expect(received.value?.event?.id).toBe(event.id);
      expect(sockets.size).toBeGreaterThan(0);
      closed = shutdownProbeProxy(proxy, 800);
      const state = await Promise.race([
        closed.then(() => 'closed'),
        new Promise<string>((resolve) =>
          setTimeout(() => resolve('waiting'), 500),
        ),
      ]);
      expect(state).toBe('waiting');
      await bounded(closed, 3000, 'bounded proxy shutdown');
      await bounded(
        Promise.all(
          established.map((socket) =>
            socket.destroyed ? Promise.resolve() : once(socket, 'close'),
          ),
        ),
        3000,
        'proxy sockets close',
      );
      expect(established.every((socket) => socket.destroyed)).toBe(true);
    } finally {
      await subscription.unsubscribe();
      await client.dispose();
      // Node's close is graceful: an established HTTP/2 connection may remain
      // open even after cancellation. Destroy only fixture-owned sockets.
      for (const socket of sockets) socket.destroy();
      if (closed)
        await bounded(closed, 5000, 'proxy close after socket cleanup');
    }
  });
});
