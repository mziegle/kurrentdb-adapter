export type BackendName = 'reference' | 'adapter';

export interface EventData {
  eventId: string;
  eventType: string;
  data: unknown;
  metadata?: unknown;
  revision?: bigint;
  position?: bigint;
}

export type ExpectedRevision =
  | 'any'
  | 'no_stream'
  | 'stream_exists'
  | bigint;

export interface ReadStreamOptions {
  fromRevision?: bigint;
  limit?: number;
}

export interface AppendEventInput {
  eventType: string;
  data: unknown;
  metadata?: unknown;
}

export interface AppendResult {
  nextExpectedRevision?: bigint;
}

export interface PingResult {
  ok: boolean;
  latencyMs: number;
  details?: string;
}
