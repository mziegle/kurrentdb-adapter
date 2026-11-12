import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createServer, connect } from 'node:net';
import { once } from 'node:events';
import { createTracer } from '../dist/application/trace.js';

const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

test('upstream disconnect while its reader is stalled closes the client', { timeout: 10000 }, async () => {
  let peer;
  const upstream = createServer({ pauseOnConnect: true }, (socket) => { peer = socket; socket.on('error', () => {}); });
  upstream.listen(0, '127.0.0.1');
  await once(upstream, 'listening');
  const tracer = createTracer({ proxyHost: '127.0.0.1', proxyPort: '0', upstreamHost: '127.0.0.1', upstreamPort: String(upstream.address().port) });
  let proxyPeer;
  tracer.server.on('connection', (socket) => { proxyPeer = socket; });
  let client;
  try {
    await tracer.listen();
    client = connect(tracer.server.address().port, '127.0.0.1');
    client.on('error', () => {});
    const closed = new Promise((resolve) => client.once('close', resolve));
    await once(client, 'connect');
    client.write(Buffer.alloc(16 * 1024 * 1024, 0x78));
    await delay(500);
    assert.ok(proxyPeer.isPaused());
    peer.destroy();
    await closed;
  } finally {
    client?.destroy();
    proxyPeer?.destroy();
    peer?.destroy();
    await tracer.close();
    await new Promise((resolve) => upstream.close(resolve));
  }
});
