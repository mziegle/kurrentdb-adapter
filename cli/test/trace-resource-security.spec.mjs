import { test } from 'node:test';
import assert from 'node:assert/strict';
import * as http2 from 'node:http2';
import { once } from 'node:events';
import { gzipSync, deflateSync, brotliCompressSync } from 'node:zlib';
import { createTracer } from '../dist/application/trace.js';

for (const [name, compress] of [['gzip', gzipSync], ['deflate', deflateSync], ['brotli', brotliCompressSync]]) {
  for (const [size, skipped] of [[5 * 1024 * 1024, true], [4 * 1024 * 1024, false], [100, false]]) {
    test(`trace ${skipped ? 'skips oversized' : 'accepts bounded'} ${name} message of ${size} bytes while forwarding`, async () => {
      const compressed = compress(Buffer.alloc(size));
      const envelope = Buffer.alloc(5 + compressed.length);
      envelope[0] = 1;
      envelope.writeUInt32BE(compressed.length, 1);
      compressed.copy(envelope, 5);
      const upstream = http2.createServer();
      upstream.on('stream', (stream) => {
        const chunks = [];
        stream.on('data', (chunk) => chunks.push(chunk));
        stream.on('end', () => {
          assert.deepEqual(Buffer.concat(chunks), envelope);
          stream.respond({ ':status': 200 });
          stream.end();
        });
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
        const request = client.request({ ':method': 'POST', ':path': '/trace-test' });
        request.on('data', () => {});
        request.end(envelope);
        await once(request, 'end');
        client.close();
        await once(client, 'close');
        const output = logs.join('\n');
        assert.match(output, /grpc compressed=1/);
        if (skipped) assert.match(output, /decodeSkipped=decompressed message exceeds 4 MiB limit/);
        else {
          assert.doesNotMatch(output, /decodeSkipped=/);
          assert.match(output, /grpc (undecoded|method=)/);
        }
      } finally {
        await tracer.close();
        await new Promise((resolve) => upstream.close(resolve));
        console.info = old;
      }
    });
  }
}
