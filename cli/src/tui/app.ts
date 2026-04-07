import type { BackendClient } from '../domain/backend.js';

export async function runTui(backend: BackendClient, stream: string): Promise<void> {
  console.log('kcli TUI');
  console.log(`stream=${stream}`);
  console.log('Listening for events. Press Ctrl+C to exit.');

  for await (const event of backend.subscribeToStream(stream, 0n)) {
    console.log(
      `[event] rev=${event.revision?.toString() ?? '-'} type=${event.eventType} id=${event.eventId}`,
    );
  }
}
