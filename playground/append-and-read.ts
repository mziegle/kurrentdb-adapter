import { FORWARDS, START, jsonEvent } from '@kurrent/kurrentdb-client';
import { createPlaygroundClient, createPlaygroundStreamName } from './client';

async function main(): Promise<void> {
  const client = createPlaygroundClient();
  const streamName = createPlaygroundStreamName('append-read');

  try {
    await client.appendToStream(streamName, [
      jsonEvent({
        type: 'playground-created',
        data: { step: 1, message: 'hello from the playground' },
      }),
      jsonEvent({
        type: 'playground-confirmed',
        data: { step: 2, message: 'this came from the adapter' },
      }),
    ]);

    const events = client.readStream(streamName, {
      direction: FORWARDS,
      fromRevision: START,
      maxCount: 50,
    });

    console.log(`Stream: ${streamName}`);

    for await (const resolved of events) {
      if (!resolved.event) {
        continue;
      }

      console.log(resolved.event);
      console.log(
        JSON.stringify(
          {
            type: resolved.event.type,
            revision: resolved.event.revision.toString(),
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
