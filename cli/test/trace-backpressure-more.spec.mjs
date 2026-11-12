import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createServer, connect } from 'node:net';
import { once } from 'node:events';
import { createTracer } from '../dist/application/trace.js';

const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

for (const disconnect of [false, true]) {
  test(`slow downstream ${disconnect ? 'disconnect releases the upstream' : 'keeps proxy buffering bounded and receives all bytes'}`, { timeout: 15000 }, async () => {
    const frame = Buffer.alloc(16384 + 9, 0x79);
    frame.writeUIntBE(16384, 0, 3);
    frame[3] = 6;
    frame[4] = 0;
    frame.writeUInt32BE(0, 5);
    const payload = Buffer.concat(Array(1024).fill(frame));
    let upstreamPeer;
    const upstream = createServer((socket) => {
      upstreamPeer = socket;
      socket.on('error', () => {});
      socket.end(payload);
    });
    upstream.listen(0, '127.0.0.1');
    await once(upstream, 'listening');
    const tracer = createTracer({ proxyHost: '127.0.0.1', proxyPort: '0', upstreamHost: '127.0.0.1', upstreamPort: String(upstream.address().port) });
    let proxyPeer;
    tracer.server.on('connection', (socket) => { proxyPeer = socket; });
    let client;
    try {
      await tracer.listen();
      client = connect(tracer.server.address().port, '127.0.0.1');
      await once(client, 'connect');
      await delay(500);
      assert.ok(proxyPeer.writableLength < 1024 * 1024, `proxy queued ${proxyPeer.writableLength} bytes`);
      if (disconnect) {
        const upstreamClosed = new Promise((resolve) => upstreamPeer.once('close', resolve));
        client.destroy();
        await upstreamClosed;
      } else {
        const chunks = [];
        client.on('data', (chunk) => chunks.push(chunk));
        await once(client, 'end');
        assert.deepEqual(Buffer.concat(chunks), payload);
      }
    } finally {
      client?.destroy();
      proxyPeer?.destroy();
      upstreamPeer?.destroy();
      await tracer.close();
      await new Promise((resolve) => upstream.close(resolve));
    }
  });
}
