declare const process: {
  argv: string[];
  env: Record<string, string | undefined>;
  exitCode?: number;
};

declare module 'node:fs/promises' {
  export function readFile(path: string, encoding: string): Promise<string>;
}

declare module 'node:path' {
  export function resolve(...paths: string[]): string;
}

declare module 'node:crypto' {
  export function randomUUID(): string;
}

declare module '@kurrent/kurrentdb-client' {
  export const FORWARDS: unknown;
  export const START: unknown;
  export type StreamRevision = bigint | unknown;

  export interface AppendResponse {
    nextExpectedRevision?: bigint;
  }

  export interface ResolvedEvent {
    event?: {
      id: { toString(): string };
      type: string;
      data: unknown;
      metadata: unknown;
      revision: bigint;
      position?: { commit: bigint };
    };
  }

  export class KurrentDBClient {
    static connectionString(strings: TemplateStringsArray): KurrentDBClient;
    readAll(options: unknown): AsyncIterable<unknown>;
    readStream(stream: string, options: unknown): AsyncIterable<ResolvedEvent>;
    appendToStream(stream: string, events: unknown[], options: unknown): Promise<AppendResponse>;
    subscribeToStream(stream: string, options: unknown): AsyncIterable<ResolvedEvent>;
    dispose(): Promise<void>;
  }

  export function jsonEvent(input: { type: string; data: unknown; metadata?: unknown }): unknown;
}
