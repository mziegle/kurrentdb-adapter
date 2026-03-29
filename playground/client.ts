import { KurrentDBClient } from '@kurrent/kurrentdb-client';

const DEFAULT_CONNECTION_STRING = 'kurrentdb://127.0.0.1:2113?tls=false';

export function createPlaygroundClient(
  connectionString = process.env.KURRENTDB_CONNECTION_STRING ??
    DEFAULT_CONNECTION_STRING,
): KurrentDBClient {
  return KurrentDBClient.connectionString([
    connectionString,
  ] as unknown as TemplateStringsArray);
}

export function createPlaygroundStreamName(suffix: string): string {
  return `playground-${suffix}-${Date.now()}`;
}
