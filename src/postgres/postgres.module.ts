import { Module } from '@nestjs/common';
import { EVENT_STORE_BACKEND } from '../event-store-backend';
import { PostgresEventStoreService } from './postgres-event-store.service';

@Module({
  providers: [
    PostgresEventStoreService,
    {
      provide: EVENT_STORE_BACKEND,
      useExisting: PostgresEventStoreService,
    },
  ],
  exports: [EVENT_STORE_BACKEND, PostgresEventStoreService],
})
export class PostgresModule {}
