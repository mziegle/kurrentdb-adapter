import { Test } from '@nestjs/testing';
import { EVENT_STORE_BACKEND } from '../event-store/event-store-backend';
import { PostgresEventStoreService } from '../postgres/postgres-event-store.service';
import { PostgresModule } from '../postgres/postgres.module';
import { MongoEventStoreService } from './mongodb-event-store.service';

describe('experimental group configuration', () => {
  const previous = { ...process.env };
  afterEach(() => {
    process.env = { ...previous };
  });

  it.each([undefined, 'false', 'TRUE'])(
    'refuses MongoDB startup with acknowledgement %s',
    (acknowledgement) => {
      process.env.EVENT_STORE_BACKEND = 'mongodb';
      process.env.MONGODB_EXPERIMENTAL_GROUP_COMMIT = 'true';
      if (acknowledgement === undefined)
        delete process.env.MONGODB_GROUP_COMMIT_ACKNOWLEDGE_SINGLE_ADAPTER;
      else
        process.env.MONGODB_GROUP_COMMIT_ACKNOWLEDGE_SINGLE_ADAPTER =
          acknowledgement;
      expect(() => new MongoEventStoreService()).toThrow(
        'MONGODB_GROUP_COMMIT_ACKNOWLEDGE_SINGLE_ADAPTER=true',
      );
    },
  );

  it('rejects MongoDB module startup before connecting without acknowledgement', async () => {
    process.env.EVENT_STORE_BACKEND = 'mongodb';
    process.env.MONGODB_EXPERIMENTAL_GROUP_COMMIT = 'true';
    delete process.env.MONGODB_GROUP_COMMIT_ACKNOWLEDGE_SINGLE_ADAPTER;
    await expect(
      Test.createTestingModule({ imports: [PostgresModule] }).compile(),
    ).rejects.toThrow('MONGODB_GROUP_COMMIT_ACKNOWLEDGE_SINGLE_ADAPTER=true');
  });

  it('defaults to the ordinary append path even with acknowledgement', async () => {
    delete process.env.MONGODB_EXPERIMENTAL_GROUP_COMMIT;
    process.env.MONGODB_GROUP_COMMIT_ACKNOWLEDGE_SINGLE_ADAPTER = 'true';
    const service = new MongoEventStoreService();
    try {
      expect(Reflect.get(service, 'groupDispatcher')).toBeUndefined();
    } finally {
      await service.onModuleDestroy();
    }
  });

  it('enables grouping with both explicit flags', async () => {
    process.env.MONGODB_EXPERIMENTAL_GROUP_COMMIT = 'true';
    process.env.MONGODB_GROUP_COMMIT_ACKNOWLEDGE_SINGLE_ADAPTER = 'true';
    const service = new MongoEventStoreService();
    try {
      const dispatcher = Reflect.get(service, 'groupDispatcher') as object;
      expect(Reflect.get(dispatcher, 'waitMs')).toBe(1);
    } finally {
      await service.onModuleDestroy();
    }
  });

  it('does not affect PostgreSQL startup when the Mongo opt-in is set', async () => {
    process.env.EVENT_STORE_BACKEND = 'postgres';
    process.env.MONGODB_EXPERIMENTAL_GROUP_COMMIT = 'true';
    delete process.env.MONGODB_GROUP_COMMIT_ACKNOWLEDGE_SINGLE_ADAPTER;
    const module = await Test.createTestingModule({
      imports: [PostgresModule],
    }).compile();
    try {
      expect(module.get(EVENT_STORE_BACKEND)).toBeInstanceOf(
        PostgresEventStoreService,
      );
    } finally {
      await module.close();
    }
  });
});
