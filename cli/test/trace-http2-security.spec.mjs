import { test } from 'node:test';
import assert from 'node:assert/strict';
import * as http2 from 'node:http2';
import { once } from 'node:events';
import { createTracer } from '../dist/application/trace.js';

test('HTTP/2 data and header bytes are not printed', async () => {
  const secret = 'http2-secret-123456789';
  const upstream = http2.createServer();
  upstream.on('stream', (stream) => {
    stream.on('data', () => {});
    stream.on('end', () => { stream.respond({ ':status': 200 }); stream.end(); });
  });
  upstream.listen(0, '127.0.0.1');
  await once(upstream, 'listening');
  const logs = [];
  const old = console.info;
  console.info = (...args) => logs.push(args.join(' '));
  const tracer = createTracer({ proxyPort: '0', upstreamPort: String(upstream.address().port), verbosity: 'debug' });
  try {
    await tracer.listen();
    const client = http2.connect(`http://127.0.0.1:${tracer.server.address().port}`);
    const request = client.request({ ':method': 'POST', ':path': `/?token=${secret}`, authorization: secret });
    request.on('data', () => {});
    request.end(secret);
    await once(request, 'end');
    client.close();
    await once(client, 'close');
    const output = logs.join('\n');
    assert.match(output, /HTTP\/2/);
    assert.doesNotMatch(output, new RegExp(secret));
    assert.doesNotMatch(output, new RegExp(Buffer.from(secret).toString('hex')));
  } finally {
    await tracer.close();
    await new Promise((resolve) => upstream.close(resolve));
    console.info = old;
  }
});
