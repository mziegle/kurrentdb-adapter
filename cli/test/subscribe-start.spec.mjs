import test from 'node:test';
import assert from 'node:assert/strict';
import { runSubscribeTest } from '../dist/application/test-cases.js';
import { MockBackendClient } from '../dist/infrastructure/backends/mock-backend-client.js';

test('subscribe smoke test starts at the beginning, not after revision zero', async () => {
  class RecordingBackend extends MockBackendClient {
    subscribeToStream(stream, revision) {
      this.requestedRevision = revision;
      return super.subscribeToStream(stream, revision);
    }
  }
  const backend = new RecordingBackend();
  const result = await runSubscribeTest(backend);
  assert.equal(result.delivered, 3);
  assert.equal(backend.requestedRevision, undefined);
});
