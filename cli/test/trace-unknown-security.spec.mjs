import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createServer, connect } from 'node:net';
import { once } from 'node:events';
import { createTracer } from '../dist/application/trace.js';

test('unknown wire payload is not printed', async () => {
  const secret = 'unknown-secret-129';
  const upstream = createServer((socket) => socket.on('data', () => socket.end()));
  upstream.listen(0, '127.0.0.1');
  await once(upstream, 'listening');
  const logs = [];
  const old = console.info;
  console.info = (...args) => logs.push(args.join(' '));
  const tracer = createTracer({ proxyPort: '0', upstreamPort: String(upstream.address().port), verbosity: 'debug' });
  try {
    await tracer.listen();
    const client = connect(tracer.server.address().port, '127.0.0.1');
    await once(client, 'connect');
    client.end(secret);
    await once(client, 'close');
    assert.match(logs.join('\n'), /unknown/);
    assert.doesNotMatch(logs.join('\n'), new RegExp(secret));
    assert.doesNotMatch(logs.join('\n'), new RegExp(Buffer.from(secret).toString('hex')));
  } finally {
    await tracer.close();
    await new Promise((resolve) => upstream.close(resolve));
    console.info = old;
  }
});
