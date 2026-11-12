import { createServer, connect, AddressInfo, Socket } from 'node:net';
import { once } from 'node:events';
import { AdapterStatsService } from './operations/adapter-stats.service';
import { startProbeProxy } from './main';

jest.mock('@nestjs/core', () => ({
  NestFactory: { createMicroservice: () => new Promise(() => {}) },
}));

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
