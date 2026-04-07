import type {
  AppendEventInput,
  AppendResult,
  EventData,
  ExpectedRevision,
  PingResult,
  ReadStreamOptions,
} from './types.js';

export interface BackendClient {
  readonly name: string;
  ping(): Promise<PingResult>;
  readStream(stream: string, options?: ReadStreamOptions): Promise<EventData[]>;
  appendToStream(
    stream: string,
    events: AppendEventInput[],
    expectedRevision?: ExpectedRevision,
  ): Promise<AppendResult>;
  subscribeToStream(stream: string, fromRevision?: bigint): AsyncIterable<EventData>;
  dispose(): Promise<void>;
}
