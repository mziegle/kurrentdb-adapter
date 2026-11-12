import {
  persistentSubscriptionToAllSettingsFromDefaults,
  persistentSubscriptionToStreamSettingsFromDefaults,
  UnsupportedError,
} from '@kurrent/kurrentdb-client';
import { setupMongoDBBackend } from './contracts/mongodb-backend';
import { ScavengeCapableBackend } from './contracts/contract-test-context';
import { status } from '@grpc/grpc-js';

describe('MongoDB feature negotiation', () => {
  let backend: ScavengeCapableBackend;

  beforeAll(async () => {
    backend = await setupMongoDBBackend();
  }, 120_000);

  afterAll(async () => {
    await backend?.dispose();
  });

  it('returns UNIMPLEMENTED for disabled scavenging', async () => {
    await expect(backend.startScavenge()).rejects.toMatchObject({
      code: status.UNIMPLEMENTED,
    });
  });

  it('lets the official client reject unsupported persistent subscriptions to $all', async () => {
    await expect(
      backend
        .getClient()
        .createPersistentSubscriptionToAll(
          'unsupported',
          persistentSubscriptionToAllSettingsFromDefaults(),
        ),
    ).rejects.toBeInstanceOf(UnsupportedError);
  });

  it('rejects stream persistent subscription creation even when the client bypasses negotiation', async () => {
    await expect(
      backend
        .getClient()
        .createPersistentSubscriptionToStream(
          'stream',
          'unsupported',
          persistentSubscriptionToStreamSettingsFromDefaults(),
        ),
    ).rejects.toThrow(/unimplemented|unsupported/i);
  });

  it('rejects reading an unsupported stream persistent subscription', async () => {
    const subscription = backend
      .getClient()
      .subscribeToPersistentSubscriptionToStream('stream', 'unsupported');
    try {
      await expect(subscription[Symbol.asyncIterator]().next()).rejects.toThrow(
        /unimplemented|unsupported/i,
      );
    } finally {
      subscription.destroy();
    }
  });

  it.each([
    [
      'update',
      () =>
        backend
          .getClient()
          .updatePersistentSubscriptionToStream(
            'stream',
            'group',
            persistentSubscriptionToStreamSettingsFromDefaults(),
          ),
    ],
    [
      'delete',
      () =>
        backend
          .getClient()
          .deletePersistentSubscriptionToStream('stream', 'group'),
    ],
  ])('rejects unsupported persistent subscription %s', async (_name, call) => {
    await expect(call()).rejects.toThrow(/unimplemented|unsupported/i);
  });
});
