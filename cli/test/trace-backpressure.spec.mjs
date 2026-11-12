import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createServer, connect } from 'node:net';
import { once } from 'node:events';
import { createTracer } from '../dist/application/trace.js';

const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

test('slow upstream pauses the proxy reader and receives all bytes', { timeout: 15000 }, async () => {
  let upstreamPeer;
  const upstream = createServer({ pauseOnConnect: true }, (socket) => { upstreamPeer = socket; });
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
    client.resume();
    const clientClosed = once(client, 'close');
    const payload = Buffer.alloc(16 * 1024 * 1024, 0x78);
    client.end(payload);
    await delay(500);
    assert.ok(proxyPeer.isPaused(), 'proxy must pause reading while upstream is stalled');
    const chunks = [];
    upstreamPeer.on('data', (chunk) => chunks.push(chunk));
    upstreamPeer.resume();
    await once(upstreamPeer, 'end');
    assert.deepEqual(Buffer.concat(chunks), payload);
    upstreamPeer.end();
    await clientClosed;
  } finally {
    client?.destroy();
    upstreamPeer?.destroy();
    await tracer.close();
    await new Promise((resolve) => upstream.close(resolve));
  }
});
