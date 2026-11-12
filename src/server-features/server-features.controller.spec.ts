import { ServerFeaturesController } from './server-features.controller';

describe('server feature advertisement', () => {
  const originalBackend = process.env.EVENT_STORE_BACKEND;
  const originalScavenge = process.env.MONGODB_UNSAFE_ENABLE_SCAVENGE;
  afterEach(() => {
    if (originalBackend === undefined) delete process.env.EVENT_STORE_BACKEND;
    else process.env.EVENT_STORE_BACKEND = originalBackend;
    if (originalScavenge === undefined)
      delete process.env.MONGODB_UNSAFE_ENABLE_SCAVENGE;
    else process.env.MONGODB_UNSAFE_ENABLE_SCAVENGE = originalScavenge;
  });

  it('does not advertise MongoDB statistics or stub persistent subscriptions', () => {
    process.env.EVENT_STORE_BACKEND = 'mongodb';
    const response = new ServerFeaturesController().getSupportedMethods({});
    expect(
      response.methods.filter(
        (method) =>
          method.serviceName ===
            'event_store.client.persistent_subscriptions.persistentsubscriptions' ||
          (method.serviceName === 'event_store.client.monitoring.monitoring' &&
            method.methodName === 'stats'),
      ),
    ).toEqual([]);
    expect(response.methods).toContainEqual({
      serviceName: 'event_store.client.streams.streams',
      methodName: 'batchappend',
      features: ['deadline_duration'],
    });
  });

  it('does not advertise MongoDB user and no-op administration methods', () => {
    process.env.EVENT_STORE_BACKEND = 'mongodb';
    const response = new ServerFeaturesController().getSupportedMethods({});
    expect(
      response.methods.filter(
        (method) =>
          method.serviceName === 'event_store.client.users.users' ||
          (method.serviceName === 'event_store.client.operations.operations' &&
            [
              'mergeindexes',
              'restartpersistentsubscriptions',
              'resignnode',
              'setnodepriority',
              'shutdown',
            ].includes(method.methodName)),
      ),
    ).toEqual([]);
    expect(
      response.methods.filter(
        (method) =>
          method.serviceName === 'event_store.client.operations.operations',
      ),
    ).toEqual([]);
  });

  it('advertises MongoDB scavenge only when explicitly enabled', () => {
    process.env.EVENT_STORE_BACKEND = 'mongodb';
    process.env.MONGODB_UNSAFE_ENABLE_SCAVENGE = 'true';
    const response = new ServerFeaturesController().getSupportedMethods({});
    expect(response.methods).toContainEqual({
      serviceName: 'event_store.client.operations.operations',
      methodName: 'startscavenge',
      features: [],
    });
  });

  it('preserves PostgreSQL advertisements', () => {
    process.env.EVENT_STORE_BACKEND = 'postgres';
    const response = new ServerFeaturesController().getSupportedMethods({});
    expect(response.methods).toContainEqual({
      serviceName: 'event_store.client.monitoring.monitoring',
      methodName: 'stats',
      features: [],
    });
    expect(response.methods).toContainEqual({
      serviceName:
        'event_store.client.persistent_subscriptions.persistentsubscriptions',
      methodName: 'create',
      features: ['stream', 'all'],
    });
    expect(response.eventStoreServerVersion).toBe('24.0.0');
  });
});
