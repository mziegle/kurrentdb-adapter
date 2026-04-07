import { randomUUID } from 'node:crypto';
import type { BackendClient } from '../../domain/backend.js';
import type {
  AppendEventInput,
  AppendResult,
  EventData,
  ExpectedRevision,
  PingResult,
  ReadStreamOptions,
} from '../../domain/types.js';

export class MockBackendClient implements BackendClient {
  private readonly streams = new Map<string, EventData[]>();

  constructor(public readonly name = 'mock') {}

  ping(): Promise<PingResult> {
    return Promise.resolve({ ok: true, latencyMs: 1 });
  }

  readStream(stream: string, options: ReadStreamOptions = {}): Promise<EventData[]> {
    const events = this.streams.get(stream) ?? [];
    const from = Number(options.fromRevision ?? 0n);
    const limit = options.limit ?? events.length;
    return Promise.resolve(events.slice(from, from + limit));
  }

  appendToStream(
    stream: string,
    events: AppendEventInput[],
    expectedRevision?: ExpectedRevision,
  ): Promise<AppendResult> {
    const current = this.streams.get(stream) ?? [];
    const currentRevision = BigInt(Math.max(current.length - 1, -1));

    if (typeof expectedRevision === 'bigint' && expectedRevision !== currentRevision) {
      throw new Error(
        `Wrong expected revision. expected=${expectedRevision.toString()} actual=${currentRevision.toString()}`,
      );
    }

    const mapped = events.map((event, index): EventData => ({
      eventId: randomUUID(),
      eventType: event.eventType,
      data: event.data,
      metadata: event.metadata,
      revision: BigInt(current.length + index),
      position: BigInt(current.length + index),
    }));

    this.streams.set(stream, [...current, ...mapped]);

    return Promise.resolve({
      nextExpectedRevision: BigInt(current.length + mapped.length - 1),
    });
  }

  async *subscribeToStream(stream: string, fromRevision = 0n): AsyncIterable<EventData> {
    let cursor = Number(fromRevision);
    let idleLoops = 0;

    while (idleLoops < 25) {
      const events = await this.readStream(stream, {
        fromRevision: BigInt(cursor),
        limit: Number.MAX_SAFE_INTEGER,
      });

      if (events.length === 0) {
        idleLoops += 1;
        await new Promise((resolve) => setTimeout(resolve, 10));
        continue;
      }

      idleLoops = 0;
      for (const event of events) {
        cursor += 1;
        yield event;
      }
    }
  }

  async dispose(): Promise<void> {
    // no-op
  }
}
