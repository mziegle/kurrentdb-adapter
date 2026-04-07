import {
  FORWARDS,
  KurrentDBClient,
  START,
  jsonEvent,
  type StreamRevision,
} from '@kurrent/kurrentdb-client';
import type { BackendClient } from '../../domain/backend.js';
import type {
  AppendEventInput,
  AppendResult,
  EventData,
  ExpectedRevision,
  PingResult,
  ReadStreamOptions,
} from '../../domain/types.js';

function toExpectedRevision(revision: ExpectedRevision | undefined): bigint | 'any' | 'no_stream' | 'stream_exists' {
  if (revision === undefined) {
    return 'any';
  }

  return revision;
}

function toFromRevision(revision: bigint | undefined): StreamRevision {
  return revision === undefined ? START : revision;
}

function mapResolvedEvent(resolved: { event?: { id: { toString(): string }; type: string; data: unknown; metadata: unknown; revision: bigint; position?: { commit: bigint } } }): EventData | null {
  if (!resolved.event) {
    return null;
  }

  return {
    eventId: resolved.event.id.toString(),
    eventType: resolved.event.type,
    data: resolved.event.data,
    metadata: resolved.event.metadata,
    revision: resolved.event.revision,
    position: resolved.event.position?.commit,
  };
}

export class KurrentBackendClient implements BackendClient {
  private readonly client: KurrentDBClient;

  constructor(
    public readonly name: string,
    connectionString: string,
  ) {
    this.client = KurrentDBClient.connectionString([
      connectionString,
    ] as unknown as TemplateStringsArray);
  }

  async ping(): Promise<PingResult> {
    const started = Date.now();

    try {
      const iterator = this.client.readAll({
        direction: FORWARDS,
        fromPosition: START,
        maxCount: 1,
      })[Symbol.asyncIterator]();
      await iterator.next();
      const latencyMs = Date.now() - started;
      return { ok: true, latencyMs };
    } catch (error) {
      const latencyMs = Date.now() - started;
      return {
        ok: false,
        latencyMs,
        details: error instanceof Error ? error.message : String(error),
      };
    }
  }

  async readStream(stream: string, options: ReadStreamOptions = {}): Promise<EventData[]> {
    const events = this.client.readStream(stream, {
      direction: FORWARDS,
      fromRevision: toFromRevision(options.fromRevision),
      maxCount: options.limit ?? 100,
    });

    const results: EventData[] = [];
    for await (const resolved of events) {
      const mapped = mapResolvedEvent(resolved as never);
      if (mapped) {
        results.push(mapped);
      }
    }

    return results;
  }

  async appendToStream(
    stream: string,
    events: AppendEventInput[],
    expectedRevision?: ExpectedRevision,
  ): Promise<AppendResult> {
    const response = await this.client.appendToStream(
      stream,
      events.map((item) =>
        jsonEvent({
          type: item.eventType,
          data: item.data,
          metadata: item.metadata,
        }),
      ),
      {
        expectedRevision: toExpectedRevision(expectedRevision),
      },
    );

    return {
      nextExpectedRevision: response.nextExpectedRevision,
    };
  }

  async *subscribeToStream(stream: string, fromRevision?: bigint): AsyncIterable<EventData> {
    const subscription = this.client.subscribeToStream(stream, {
      fromRevision: toFromRevision(fromRevision),
    });

    for await (const resolved of subscription) {
      const mapped = mapResolvedEvent(resolved as never);
      if (mapped) {
        yield mapped;
      }
    }
  }

  async dispose(): Promise<void> {
    await this.client.dispose();
  }
}
