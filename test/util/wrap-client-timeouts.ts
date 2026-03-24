import { KurrentDBClient } from '@kurrent/kurrentdb-client';

const CLIENT_TIMEOUT_MS = 5_000;

export function wrapClientTimeouts(client: KurrentDBClient): KurrentDBClient {
  type AppendToStream = KurrentDBClient['appendToStream'];
  type DeleteStream = KurrentDBClient['deleteStream'];
  type TombstoneStream = KurrentDBClient['tombstoneStream'];

  const appendToStream = client.appendToStream.bind(
    client,
  ) as unknown as AppendToStream;
  const deleteStream = client.deleteStream.bind(
    client,
  ) as unknown as DeleteStream;
  const tombstoneStream = client.tombstoneStream.bind(
    client,
  ) as unknown as TombstoneStream;

  client.appendToStream = ((...args: Parameters<AppendToStream>) =>
    withTimeout(appendToStream(...args), 'appendToStream')) as AppendToStream;
  client.deleteStream = ((...args: Parameters<DeleteStream>) =>
    withTimeout(deleteStream(...args), 'deleteStream')) as DeleteStream;
  client.tombstoneStream = ((...args: Parameters<TombstoneStream>) =>
    withTimeout(
      tombstoneStream(...args),
      'tombstoneStream',
    )) as TombstoneStream;

  return client;
}

function withTimeout<T>(promise: Promise<T>, label: string): Promise<T> {
  let timeout: NodeJS.Timeout | undefined;

  const timeoutPromise = new Promise<never>((_, reject) => {
    timeout = setTimeout(() => {
      reject(
        new Error(`${label} did not complete within ${CLIENT_TIMEOUT_MS}ms.`),
      );
    }, CLIENT_TIMEOUT_MS);
  });

  return Promise.race([promise, timeoutPromise]).finally(() => {
    if (timeout) {
      clearTimeout(timeout);
    }
  });
}
