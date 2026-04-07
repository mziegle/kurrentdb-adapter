import { randomUUID } from 'node:crypto';
import type { BackendClient } from '../domain/backend.js';
import { compareStreams, type StreamComparisonResult } from './compare.js';

function testStreamName(prefix: string): string {
  return `${prefix}-${Date.now()}-${randomUUID().slice(0, 8)}`;
}

export async function runAppendTest(backend: BackendClient): Promise<{ stream: string; appended: number }> {
  const stream = testStreamName('kcli-append-test');
  await backend.appendToStream(stream, [
    { eventType: 'append-test', data: { case: 'append', seq: 1 } },
  ]);

  return { stream, appended: 1 };
}

export async function runReadTest(backend: BackendClient): Promise<{ stream: string; read: number }> {
  const stream = testStreamName('kcli-read-test');
  await backend.appendToStream(stream, [
    { eventType: 'read-test', data: { case: 'read', seq: 1 } },
    { eventType: 'read-test', data: { case: 'read', seq: 2 } },
  ]);

  const events = await backend.readStream(stream, { fromRevision: 0n, limit: 10 });
  return { stream, read: events.length };
}

export async function runSubscribeTest(backend: BackendClient): Promise<{ stream: string; delivered: number }> {
  const stream = testStreamName('kcli-subscribe-test');
  const received: string[] = [];

  const readPromise = (async (): Promise<void> => {
    for await (const event of backend.subscribeToStream(stream, 0n)) {
      received.push(event.eventType);
      if (received.length >= 3) {
        break;
      }
    }
  })();

  await backend.appendToStream(stream, [
    { eventType: 'subscribe-test', data: { seq: 1 } },
    { eventType: 'subscribe-test', data: { seq: 2 } },
    { eventType: 'subscribe-test', data: { seq: 3 } },
  ]);

  await readPromise;
  return { stream, delivered: received.length };
}

export async function runCompareTest(
  reference: BackendClient,
  adapter: BackendClient,
  stream: string,
): Promise<StreamComparisonResult> {
  const [referenceEvents, adapterEvents] = await Promise.all([
    reference.readStream(stream, { fromRevision: 0n, limit: 1000 }),
    adapter.readStream(stream, { fromRevision: 0n, limit: 1000 }),
  ]);

  return compareStreams(stream, referenceEvents, adapterEvents);
}
