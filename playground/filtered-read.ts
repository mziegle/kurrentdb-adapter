import {
  FORWARDS,
  START,
  jsonEvent,
  streamNameFilter,
} from '@kurrent/kurrentdb-client';
import { createPlaygroundClient, createPlaygroundStreamName } from './client';

async function main(): Promise<void> {
  const client = createPlaygroundClient();
  const prefix = createPlaygroundStreamName('filter');
  const includedA = `${prefix}-a`;
  const includedB = `${prefix}-b`;
  const excluded = createPlaygroundStreamName('other');

  try {
    await client.appendToStream(
      includedA,
      jsonEvent({
        type: 'included-event',
        data: { stream: includedA },
      }),
    );
    await client.appendToStream(
      excluded,
      jsonEvent({
        type: 'excluded-event',
        data: { stream: excluded },
      }),
    );
    await client.appendToStream(
      includedB,
      jsonEvent({
        type: 'included-event',
        data: { stream: includedB },
      }),
    );

    const events = client.readAll({
      direction: FORWARDS,
      fromPosition: START,
      maxCount: 100,
      filter: streamNameFilter({
        prefixes: [prefix],
      }),
    });

    console.log(`Filter prefix: ${prefix}`);

    for await (const resolved of events) {
      if (!resolved.event) {
        continue;
      }

      console.log(
        JSON.stringify(
          {
            streamId: resolved.event.streamId,
            type: resolved.event.type,
            data: resolved.event.data,
          },
          null,
          2,
        ),
      );
    }
  } finally {
    await client.dispose();
  }
}

void main().catch((error: unknown) => {
  console.error(error);
  process.exitCode = 1;
});
