import { test } from 'node:test';
import assert from 'node:assert/strict';
import { connect } from 'node:net';
import { once } from 'node:events';
import { createTracer } from '../dist/application/trace.js';

test('trace does not log upstream credentials in startup or connection errors', async () => {
  const secret = 'secret-unique-8394';
  const logs = [];
  const info = console.info;
  const warn = console.warn;
  console.info = console.warn = (...args) => logs.push(args.join(' '));
  const tracer = createTracer({ proxyPort: '0', upstreamHost: `http://user:${secret}@invalid`, upstreamPort: '2113' });
  try {
    await tracer.listen();
    const client = connect(tracer.server.address().port, '127.0.0.1');
    client.on('error', () => {});
    await once(client, 'close');
    assert.match(logs.join('\n'), /upstream error/);
    assert.doesNotMatch(logs.join('\n'), new RegExp(secret));
  } finally {
    await tracer.close();
    console.info = info;
    console.warn = warn;
  }
});

test('listen failure does not expose a credential-bearing bind address', async () => {
  const secret = 'bind-secret-8934';
  const tracer = createTracer({ proxyHost: `http://user:${secret}@invalid`, proxyPort: '0' });
  await assert.rejects(tracer.listen(), (error) => {
    assert.doesNotMatch(error.message, new RegExp(secret));
    return true;
  });
});
