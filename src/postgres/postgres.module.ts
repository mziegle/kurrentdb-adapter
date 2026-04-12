import { Module } from '@nestjs/common';
import { EVENT_STORE_BACKEND } from '../event-store/event-store-backend';
import { PostgresEventStoreService } from './postgres-event-store.service';
import { PostgresInfrastructureService } from './postgres-infrastructure.service';
import { PostgresProtocolService } from './postgres-protocol.service';
import { PostgresSubscriptionService } from './postgres-subscription.service';

@Module({
  providers: [
    PostgresInfrastructureService,
    PostgresProtocolService,
    PostgresSubscriptionService,
    PostgresEventStoreService,
    {
      provide: EVENT_STORE_BACKEND,
      useExisting: PostgresEventStoreService,
    },
  ],
  exports: [EVENT_STORE_BACKEND, PostgresEventStoreService],
})
export class PostgresModule {}
