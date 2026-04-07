import test from 'node:test';
import assert from 'node:assert/strict';
import {
  runAppendTest,
  runCompareTest,
  runReadTest,
  runSubscribeTest,
} from '../dist/application/test-cases.js';
import { MockBackendClient } from '../dist/infrastructure/backends/mock-backend-client.js';

test('runs append and read tests on a backend', async () => {
  const backend = new MockBackendClient('mock');

  const appendResult = await runAppendTest(backend);
  const readResult = await runReadTest(backend);

  assert.equal(appendResult.appended, 1);
  assert.equal(readResult.read, 2);
});

test('runs subscribe verification', async () => {
  const backend = new MockBackendClient('mock');
  const result = await runSubscribeTest(backend);

  assert.equal(result.delivered, 3);
});

test('compares reference and adapter streams', async () => {
  const reference = new MockBackendClient('reference');
  const adapter = new MockBackendClient('adapter');
  const stream = 'compare-flow';

  await reference.appendToStream(stream, [{ eventType: 'type-a', data: { x: 1 } }]);
  await adapter.appendToStream(stream, [{ eventType: 'type-a', data: { x: 1 } }]);

  const result = await runCompareTest(reference, adapter, stream);
  assert.equal(result.referenceCount, 1);
  assert.equal(result.adapterCount, 1);
});
