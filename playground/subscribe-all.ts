import { createPlaygroundClient } from './client';

async function main(): Promise<void> {
  const client = createPlaygroundClient();

  try {
    const subscription = client.subscribeToAll();

    subscription.on('data', (event) => {
      console.log('Received event:', event);
    });

  } finally {
    await client.dispose();
  }
}

void main().catch((error: unknown) => {
  console.error(error);
  process.exitCode = 1;
});
