import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createServer, connect } from 'node:net';
import { once } from 'node:events';
import { createTracer } from '../dist/application/trace.js';

test('trace proxy forwards requests without logging credentials or wire payloads', async () => {
  const secret = 'credential-unique-8282';
  const request = `POST /other?token=${secret} HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer ${secret}\r\nContent-Length: ${secret.length}\r\n\r\n${secret}`;
  const upstream = createServer((socket) => socket.on('data', (data) => {
    assert.equal(data.toString(), request);
    socket.end('HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n');
  }));
  upstream.listen(0, '127.0.0.1');
  await once(upstream, 'listening');
  const logs = [];
  const old = console.info;
  console.info = (...args) => logs.push(args.join(' '));
  const tracer = createTracer({ proxyHost: '127.0.0.1', proxyPort: '0', upstreamHost: '127.0.0.1', upstreamPort: String(upstream.address().port), verbosity: 'debug' });
  try {
    await tracer.listen();
    const client = connect(tracer.server.address().port, '127.0.0.1');
    client.on('data', () => {});
    await once(client, 'connect');
    client.end(request);
    await once(client, 'close');
    assert.match(logs.join('\n'), /HTTP\/1\.1/);
    assert.doesNotMatch(logs.join('\n'), new RegExp(secret));
    assert.doesNotMatch(logs.join('\n'), new RegExp(Buffer.from(secret).toString('hex')));
  } finally {
    await tracer.close();
    await new Promise((resolve) => upstream.close(resolve));
    console.info = old;
  }
});
