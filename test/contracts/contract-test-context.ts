import {
  Direction,
  END,
  FORWARDS,
  KurrentDBClient,
  START,
} from '@kurrent/kurrentdb-client';
import { randomUUID } from 'node:crypto';

export type StreamsContractBackend = {
  getClient(): KurrentDBClient;
  restart(): Promise<void>;
  dispose(): Promise<void>;
  supportsRestart: boolean;
};

export type StreamsContractContext = {
  backend(): StreamsContractBackend;
  backendName: string;
  createStreamName(suffix: string): string;
  readStreamEvents(
    streamName: string,
    direction?: Direction,
    fromRevision?: bigint | typeof START | typeof END,
    maxCount?: number,
  ): Promise<Array<{ type: string; data: unknown }>>;
};

function createBackendLabel(backendName: string): string {
  return backendName.toLowerCase().replace(/\s+/g, '-');
}

export function createStreamsContractContext(
  backendName: string,
  getBackend: () => StreamsContractBackend,
): StreamsContractContext {
  return {
    backend(): StreamsContractBackend {
      return getBackend();
    },
    backendName,
    createStreamName(suffix: string): string {
      return `${createBackendLabel(backendName)}-${suffix}-${randomUUID()}`;
    },
    async readStreamEvents(
      streamName: string,
      direction: Direction = FORWARDS,
      fromRevision: bigint | typeof START | typeof END = START,
      maxCount = 10,
    ): Promise<Array<{ type: string; data: unknown }>> {
      const readEvents = getBackend().getClient().readStream(streamName, {
        fromRevision,
        direction,
        maxCount,
      });

      const received: Array<{ type: string; data: unknown }> = [];
      for await (const { event: readEvent } of readEvents) {
        if (!readEvent) {
          continue;
        }

        received.push({
          type: readEvent.type,
          data: readEvent.data,
        });
      }

      return received;
    },
  };
}
