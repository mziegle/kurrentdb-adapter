import { FORWARDS, START, jsonEvent } from '@kurrent/kurrentdb-client';
import { createPlaygroundClient, createPlaygroundStreamName } from './client';

async function main(): Promise<void> {
  const client = createPlaygroundClient();
  const streamName = createPlaygroundStreamName('metadata');

  try {
    await client.appendToStream(streamName, [
      jsonEvent({
        type: 'before-metadata',
        data: { step: 1 },
      }),
      jsonEvent({
        type: 'before-metadata',
        data: { step: 2 },
      }),
      jsonEvent({
        type: 'before-metadata',
        data: { step: 3 },
      }),
    ]);

    await client.setStreamMetadata(streamName, {
      maxCount: 2,
      truncateBefore: 1,
    });

    const metadata = await client.getStreamMetadata(streamName);
    const events = client.readStream(streamName, {
      direction: FORWARDS,
      fromRevision: START,
      maxCount: 50,
    });

    console.log('Metadata');
    console.log(JSON.stringify(metadata.metadata ?? {}, null, 2));

    console.log('Visible events after retention');
    for await (const resolved of events) {
      if (!resolved.event) {
        continue;
      }

      console.log(
        JSON.stringify(
          {
            revision: resolved.event.revision.toString(),
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
