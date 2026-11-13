import { createServer, connect, AddressInfo, Socket } from 'node:net';
import { once } from 'node:events';
import { AdapterStatsService } from './operations/adapter-stats.service';
import { startProbeProxy } from './main';
import * as main from './main';
import { Server } from 'node:net';
import { MongoEventStoreService } from './mongodb/mongodb-event-store.service';

const shutdownProxy = (
  main as unknown as {
    shutdownProbeProxy: (server: Server, timeout: number) => Promise<void>;
  }
).shutdownProbeProxy;

jest.mock('@nestjs/core', () => ({
  NestFactory: { createMicroservice: () => new Promise(() => {}) },
}));

it('on shutdown signals drains the proxy before closing the application only once', async () => {
  const register = (
    main as unknown as {
      registerProxyShutdown: (
        proxy: Server,
        app: { close: () => Promise<void> },
        signals: import('node:events').EventEmitter,
        grace: number,
      ) => () => void;
    }
  ).registerProxyShutdown;
  const signals = new (await import('node:events')).EventEmitter();
  const upstream = createServer();
  upstream.listen(0, '127.0.0.1');
  await once(upstream, 'listening');
  const proxy = await startProbeProxy(
    '127.0.0.1:0',
    `127.0.0.1:${(upstream.address() as AddressInfo).port}`,
    {} as AdapterStatsService,
  );
  const client = connect((proxy.address() as AddressInfo).port, '127.0.0.1');
  client.resume();
  await once(client, 'connect');
  let listeningAtClose = true;
  const appClosed = jest.fn(() => {
    listeningAtClose = proxy.listening;
    return Promise.resolve();
  });
  let unregister = () => {};
  try {
    unregister = register(proxy, { close: appClosed }, signals, 100);
    signals.emit('SIGTERM');
    signals.emit('SIGINT');
    expect(proxy.listening).toBe(false);
    expect(appClosed).not.toHaveBeenCalled();
    await new Promise((resolve) => setTimeout(resolve, 250));
    expect(appClosed).toHaveBeenCalledTimes(1);
    expect(listeningAtClose).toBe(false);
  } finally {
    unregister();
    client.destroy();
    proxy.close();
    upstream.close();
  }
});

it('reports a Mongo client close rejection through the shutdown handler', async () => {
  const proxy = await startProbeProxy(
    '127.0.0.1:0',
    '127.0.0.1:1234',
    {} as AdapterStatsService,
  );
  const signals = new (await import('node:events')).EventEmitter();
  const service = new MongoEventStoreService();
  const originalExitCode = process.exitCode;
  let called!: () => void;
  const closeCalled = new Promise<void>((resolve) => {
    called = resolve;
  });
  jest.spyOn(service['infrastructure'], 'close').mockImplementation(() => {
    called();
    return Promise.reject(new Error('close failed'));
  });
  const unregister = main.registerProxyShutdown(
    proxy,
    { close: () => service.onModuleDestroy() },
    signals,
    100,
  );
  try {
    signals.emit('SIGTERM');
    await closeCalled;
    await new Promise((resolve) => setImmediate(resolve));
    expect(process.exitCode).toBe(1);
  } finally {
    unregister();
    proxy.close();
    process.exitCode = originalExitCode;
  }
});

it.each([
  ['clean', 0],
  ['drain', 1],
  ['close', 1],
  ['error', 1],
])(
  'real SIGTERM reports %s shutdown status without discarding cleanup',
  async (mode, expected) => {
    const { spawn } = await import('node:child_process');
    const child = spawn(
      process.execPath,
      [
        '-e',
        `
      require('ts-node/register/transpile-only');
      const Module = require('node:module');
      const load = Module._load;
      Module._load = function(id, ...rest) {
        if (id === '@nestjs/core') return { NestFactory: { createMicroservice: () => new Promise(() => {}) } };
        return load.call(this, id, ...rest);
      };
      const { startProbeProxy, registerProxyShutdown } = require('./src/main');
      const { MongoEventStoreService } = require('./src/mongodb/mongodb-event-store.service');
      const mode = process.argv[1];
      const service = new MongoEventStoreService();
      let release;
      if (mode === 'drain') service.liveReads.add(new Promise(resolve => { release = resolve; }));
      let closeFinished;
      const closed = new Promise(resolve => { closeFinished = resolve; });
      service.infrastructure.close = async () => {
        process.send('client-close');
        if (mode === 'close') await new Promise(resolve => setTimeout(resolve, 1300));
        closeFinished();
        if (mode === 'error') throw new Error('close failed');
      };
      startProbeProxy('127.0.0.1:0', '127.0.0.1:1234', {}).then(server => {
        registerProxyShutdown(server, { close: async () => {
          if (release) setTimeout(() => { process.send('release'); release(); }, 1300);
          try { await service.onModuleDestroy(); } finally {
            process.send('shutdown-returned');
            closed.then(() => setImmediate(() => process.disconnect()));
          }
        }});
        process.send('ready');
      });
      `,
        mode,
      ],
      { cwd: process.cwd(), stdio: ['ignore', 'ignore', 'ignore', 'ipc'] },
    );
    const messages: unknown[] = [];
    child.on('message', (message) => messages.push(message));
    const exited = new Promise<[number | null, NodeJS.Signals | null]>(
      (resolve) =>
        child.once('exit', (code, signal) => resolve([code, signal])),
    );
    const timer = setTimeout(() => child.kill('SIGKILL'), 8000);
    try {
      await once(child, 'message');
      child.kill('SIGTERM');
      const [code, signal] = await exited;
      expect(signal).toBeNull();
      expect(code).toBe(expected);
      expect(messages).toContain('shutdown-returned');
      expect(messages).toContain('client-close');
      if (mode === 'drain') {
        expect(messages.indexOf('shutdown-returned')).toBeLessThan(
          messages.indexOf('release'),
        );
        expect(messages.indexOf('release')).toBeLessThan(
          messages.indexOf('client-close'),
        );
      }
    } finally {
      clearTimeout(timer);
      if (child.exitCode === null && child.signalCode === null)
        child.kill('SIGKILL');
    }
  },
  10000,
);

it('terminates on a second real signal when application close hangs', async () => {
  const { spawn } = await import('node:child_process');
  const child = spawn(
    process.execPath,
    [
      '-e',
      `
    require('ts-node/register/transpile-only');
    const Module = require('node:module');
    const load = Module._load;
    Module._load = function(id, ...rest) {
      if (id === '@nestjs/core') return { NestFactory: { createMicroservice: () => new Promise(() => {}) } };
      return load.call(this, id, ...rest);
    };
    const { startProbeProxy, registerProxyShutdown } = require('./src/main');
    startProbeProxy('127.0.0.1:0', '127.0.0.1:1234', {}).then(server => {
      registerProxyShutdown(server, { close: () => { process.send('closing'); return new Promise(() => {}); } });
      process.send('ready');
      setInterval(() => {}, 1000);
    });
  `,
    ],
    { cwd: process.cwd(), stdio: ['ignore', 'ignore', 'ignore', 'ipc'] },
  );
  try {
    await once(child, 'message');
    child.kill('SIGTERM');
    await once(child, 'message');
    const exited = once(child, 'exit');
    child.kill('SIGINT');
    const result = await Promise.race([
      exited,
      new Promise<never>((_, reject) =>
        setTimeout(() => reject(new Error('second signal ignored')), 1000),
      ),
    ]);
    expect(result[0]).toBe(1);
  } finally {
    child.kill('SIGKILL');
  }
}, 10000);

it('shuts down the app once on a post-listen server error', async () => {
  const proxy = await startProbeProxy(
    '127.0.0.1:0',
    '127.0.0.1:1234',
    {} as AdapterStatsService,
  );
  const signals = new (await import('node:events')).EventEmitter();
  const close = jest.fn().mockResolvedValue(undefined);
  const unregister = main.registerProxyShutdown(proxy, { close }, signals, 10);
  try {
    proxy.emit('error', new Error('secret payload'));
    proxy.emit('error', new Error('another secret'));
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(close).toHaveBeenCalledTimes(1);
    expect(proxy.listening).toBe(false);
  } finally {
    unregister();
    proxy.close();
    process.exitCode = 0;
  }
});

it('bounds shutdown of an established proxy connection without touching another server', async () => {
  const upstream = createServer((socket) => socket.resume());
  upstream.listen(0, '127.0.0.1');
  await once(upstream, 'listening');
  const proxy = await startProbeProxy(
    '127.0.0.1:0',
    `127.0.0.1:${(upstream.address() as AddressInfo).port}`,
    {} as AdapterStatsService,
  );
  const client = connect((proxy.address() as AddressInfo).port, '127.0.0.1');
  client.resume();
  try {
    const accepted = once(upstream, 'connection');
    client.write('PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n');
    const [peer] = (await accepted) as [Socket];
    const peerClosed = once(peer, 'close');
    const closed = once(client, 'close');
    const shutdown = shutdownProxy(proxy, 150);
    expect(shutdownProxy(proxy, 150)).toBe(shutdown);
    expect(proxy.listening).toBe(false);
    await new Promise((resolve) => setTimeout(resolve, 50));
    expect(client.destroyed).toBe(false);
    await Promise.race([
      Promise.all([shutdown, closed, peerClosed]),
      new Promise((_, reject) =>
        setTimeout(() => reject(new Error('shutdown hung')), 1000),
      ),
    ]);
    expect(upstream.listening).toBe(true);
  } finally {
    client.destroy();
    proxy.close();
    upstream.close();
  }
}, 5000);

it('forwards a response after the client half-closes', async () => {
  const upstream = createServer({ allowHalfOpen: true }, (socket) => {
    const chunks: Buffer[] = [];
    socket.on('data', (chunk: Buffer) => chunks.push(chunk));
    socket.on('end', () => socket.end(Buffer.concat(chunks)));
  });
  upstream.listen(0, '127.0.0.1');
  await once(upstream, 'listening');
  const proxy = await startProbeProxy(
    '127.0.0.1:0',
    `127.0.0.1:${(upstream.address() as AddressInfo).port}`,
    {} as AdapterStatsService,
  );
  const client: Socket = connect(
    (proxy.address() as AddressInfo).port,
    '127.0.0.1',
  );
  const chunks: Buffer[] = [];
  client.on('data', (chunk: Buffer) => chunks.push(chunk));
  try {
    client.end('hello');
    await once(client, 'end');
    expect(Buffer.concat(chunks).toString()).toBe('hello');
  } finally {
    client.destroy();
    proxy.close();
    upstream.close();
  }
}, 10000);

it('bounds queued upstream writes while a loopback reader is stalled', async () => {
  let receiver: Socket | undefined;
  const received: Buffer[] = [];
  const upstream = createServer({ allowHalfOpen: true }, (socket) => {
    receiver = socket;
    socket.on('data', (chunk: Buffer) => received.push(chunk));
    socket.pause();
  });
  upstream.listen(0, '127.0.0.1');
  await once(upstream, 'listening');
  const port = (upstream.address() as AddressInfo).port;
  const proxy = await startProbeProxy(
    '127.0.0.1:0',
    `127.0.0.1:${port}`,
    {} as AdapterStatsService,
  );
  // Preserve the native method while observing real loopback socket queues.
  // eslint-disable-next-line @typescript-eslint/unbound-method
  const original = Socket.prototype.write;
  let peak = 0;
  const spy = jest
    .spyOn(Socket.prototype, 'write')
    .mockImplementation(function (
      this: Socket,
      ...args: Parameters<Socket['write']>
    ) {
      const result = Reflect.apply(original, this, args);
      if (this.remotePort === port) peak = Math.max(peak, this.writableLength);
      return result;
    });
  const client = connect((proxy.address() as AddressInfo).port, '127.0.0.1');
  try {
    const payload = Buffer.alloc(16 * 1024 * 1024, 42);
    client.end(payload);
    await new Promise((resolve) => setTimeout(resolve, 800));
    expect(peak).toBeGreaterThan(0);
    expect(peak).toBeLessThan(1024 * 1024);
    expect(receiver).toBeDefined();
    const ended = once(receiver!, 'end');
    receiver!.resume();
    await ended;
    expect(Buffer.concat(received).equals(payload)).toBe(true);
  } finally {
    spy.mockRestore();
    client.destroy();
    receiver?.destroy();
    proxy.close();
    upstream.close();
  }
}, 10000);

it('preserves a large upstream response through a stalled client and half-close', async () => {
  const payload = Buffer.allocUnsafe(8 * 1024 * 1024);
  for (let i = 0; i < payload.length; i++) payload[i] = i % 251;
  let peer: Socket | undefined;
  const upstream = createServer({ allowHalfOpen: true }, (socket) => {
    peer = socket;
    socket.once('data', () => socket.end(payload));
  });
  upstream.listen(0, '127.0.0.1');
  await once(upstream, 'listening');
  const proxy = await startProbeProxy(
    '127.0.0.1:0',
    `127.0.0.1:${(upstream.address() as AddressInfo).port}`,
    {} as AdapterStatsService,
  );
  const port = (proxy.address() as AddressInfo).port;
  // eslint-disable-next-line @typescript-eslint/unbound-method
  const original = Socket.prototype.write;
  let peak = 0;
  const spy = jest
    .spyOn(Socket.prototype, 'write')
    .mockImplementation(function (
      this: Socket,
      ...args: Parameters<Socket['write']>
    ) {
      const result = Reflect.apply(original, this, args);
      if (this.localPort === port) peak = Math.max(peak, this.writableLength);
      return result;
    });
  const client = connect({ port, host: '127.0.0.1', allowHalfOpen: true });
  const chunks: Buffer[] = [];
  try {
    client.write('request');
    client.pause();
    await new Promise((resolve) => setTimeout(resolve, 800));
    expect(peak).toBeGreaterThan(0);
    expect(peak).toBeLessThan(1024 * 1024);
    client.on('data', (chunk: Buffer) => chunks.push(chunk));
    client.resume();
    await once(client, 'end');
    expect(Buffer.concat(chunks).equals(payload)).toBe(true);
    client.end();
  } finally {
    spy.mockRestore();
    client.destroy();
    peer?.destroy();
    proxy.close();
    upstream.close();
  }
}, 15000);

it('closes the upstream when the client resets while a reader is stalled', async () => {
  const upstream = createServer((socket) => socket.pause());
  upstream.listen(0, '127.0.0.1');
  await once(upstream, 'listening');
  const proxy = await startProbeProxy(
    '127.0.0.1:0',
    `127.0.0.1:${(upstream.address() as AddressInfo).port}`,
    {} as AdapterStatsService,
  );
  const client = connect((proxy.address() as AddressInfo).port, '127.0.0.1');
  try {
    const accepted = once(upstream, 'connection');
    client.write('request');
    const [peer] = (await accepted) as [Socket];
    peer.on('error', () => {});
    const closed = once(peer, 'close');
    client.resetAndDestroy();
    peer.resume();
    await closed;
  } finally {
    client.destroy();
    proxy.close();
    upstream.close();
  }
}, 10000);

it('rejects excess silent connections and admits another after close', async () => {
  process.env.PROXY_MAX_CONNECTIONS = '1';
  const upstream = createServer();
  upstream.listen(0, '127.0.0.1');
  await once(upstream, 'listening');
  const proxy = await startProbeProxy(
    '127.0.0.1:0',
    `127.0.0.1:${(upstream.address() as AddressInfo).port}`,
    {} as AdapterStatsService,
  );
  const port = (proxy.address() as AddressInfo).port;
  const first = connect(port, '127.0.0.1');
  await once(first, 'connect');
  const second = connect(port, '127.0.0.1');
  try {
    await Promise.race([
      once(second, 'close'),
      new Promise((_, reject) =>
        setTimeout(
          () => reject(new Error('excess connection remained open')),
          300,
        ),
      ),
    ]);
    first.destroy();
    await new Promise((resolve) => setTimeout(resolve, 50));
    const third = connect(port, '127.0.0.1');
    await once(third, 'connect');
    await new Promise((resolve) => setTimeout(resolve, 50));
    expect(third.destroyed).toBe(false);
    third.destroy();
  } finally {
    delete process.env.PROXY_MAX_CONNECTIONS;
    first.destroy();
    second.destroy();
    proxy.close();
    upstream.close();
  }
}, 5000);

it('closes a slow partial HTTP/2 preface at the absolute deadline', async () => {
  process.env.PROXY_PREFACE_TIMEOUT_MS = '150';
  const upstream = createServer((socket) => socket.resume());
  upstream.listen(0, '127.0.0.1');
  await once(upstream, 'listening');
  const proxy = await startProbeProxy(
    '127.0.0.1:0',
    `127.0.0.1:${(upstream.address() as AddressInfo).port}`,
    {} as AdapterStatsService,
  );
  const client = connect((proxy.address() as AddressInfo).port, '127.0.0.1');
  try {
    await once(client, 'connect');
    const closed = once(client, 'close');
    const interval = setInterval(() => client.write('P'), 30);
    try {
      await Promise.race([
        closed,
        new Promise((_, reject) =>
          setTimeout(
            () => reject(new Error('partial preface remained open')),
            400,
          ),
        ),
      ]);
    } finally {
      clearInterval(interval);
    }
  } finally {
    delete process.env.PROXY_PREFACE_TIMEOUT_MS;
    client.destroy();
    proxy.close();
    upstream.close();
  }
}, 5000);

it('keeps an established HTTP/2 connection open past the preface deadline', async () => {
  process.env.PROXY_PREFACE_TIMEOUT_MS = '150';
  const http2 = await import('node:http2');
  const upstream = http2.createServer();
  upstream.on('stream', (stream) => {
    stream.respond({ ':status': 200 });
    stream.end('ok');
  });
  upstream.listen(0, '127.0.0.1');
  await once(upstream, 'listening');
  const proxy = await startProbeProxy(
    '127.0.0.1:0',
    `127.0.0.1:${(upstream.address() as AddressInfo).port}`,
    {} as AdapterStatsService,
  );
  const session = http2.connect(
    `http://127.0.0.1:${(proxy.address() as AddressInfo).port}`,
  );
  try {
    await once(session, 'remoteSettings');
    await new Promise((resolve) => setTimeout(resolve, 350));
    expect(session.closed).toBe(false);
    const request = session.request({ ':path': '/' });
    const chunks: Buffer[] = [];
    request.on('data', (chunk: Buffer) => chunks.push(chunk));
    await once(request, 'end');
    expect(Buffer.concat(chunks).toString()).toBe('ok');
  } finally {
    delete process.env.PROXY_PREFACE_TIMEOUT_MS;
    session.destroy();
    proxy.close();
    upstream.close();
  }
}, 5000);

it.each([
  ['PROXY_MAX_CONNECTIONS', '0'],
  ['PROXY_MAX_CONNECTIONS', '1e3'],
  ['PROXY_MAX_CONNECTIONS', '100001'],
  ['PROXY_PREFACE_TIMEOUT_MS', '-1'],
  ['PROXY_PREFACE_TIMEOUT_MS', '2147483648'],
  ['PROXY_PREFACE_TIMEOUT_MS', ''],
])('rejects invalid %s configuration', async (key, value) => {
  process.env[key] = value;
  try {
    await expect(
      startProbeProxy(
        '127.0.0.1:0',
        '127.0.0.1:1234',
        {} as AdapterStatsService,
      ),
    ).rejects.toThrow(`Invalid ${key}`);
  } finally {
    delete process.env[key];
  }
});

it('drains a silent connection on server close when its deadline expires', async () => {
  process.env.PROXY_PREFACE_TIMEOUT_MS = '100';
  const upstream = createServer();
  upstream.listen(0, '127.0.0.1');
  await once(upstream, 'listening');
  const proxy = await startProbeProxy(
    '127.0.0.1:0',
    `127.0.0.1:${(upstream.address() as AddressInfo).port}`,
    {} as AdapterStatsService,
  );
  const client = connect((proxy.address() as AddressInfo).port, '127.0.0.1');
  try {
    await once(client, 'connect');
    const stopped = once(proxy, 'close');
    proxy.close();
    await Promise.race([
      stopped,
      new Promise((_, reject) =>
        setTimeout(() => reject(new Error('shutdown did not drain')), 400),
      ),
    ]);
  } finally {
    delete process.env.PROXY_PREFACE_TIMEOUT_MS;
    client.destroy();
    proxy.close();
    upstream.close();
  }
}, 5000);
