import { status } from '@grpc/grpc-js';
import { MongoEventStoreService } from './mongodb-event-store.service';

describe('MongoDB scavenge safety', () => {
  const original = process.env.MONGODB_UNSAFE_ENABLE_SCAVENGE;
  afterEach(() => {
    if (original === undefined)
      delete process.env.MONGODB_UNSAFE_ENABLE_SCAVENGE;
    else process.env.MONGODB_UNSAFE_ENABLE_SCAVENGE = original;
  });

  it.each([undefined, '', 'false', '1', 'TRUE'])(
    'rejects start without exact opt-in %p',
    (value) => {
      if (value === undefined)
        delete process.env.MONGODB_UNSAFE_ENABLE_SCAVENGE;
      else process.env.MONGODB_UNSAFE_ENABLE_SCAVENGE = value;
      const service = new MongoEventStoreService();
      try {
        service.startScavenge();
        throw new Error('expected scavenging to be rejected');
      } catch (error: unknown) {
        expect(error).toMatchObject({ code: status.UNIMPLEMENTED });
      }
    },
  );
});
