import { Module } from '@nestjs/common';
import { EVENT_STORE_BACKEND } from '../event-store/event-store-backend';
import { PostgresEventStoreService } from './postgres-event-store.service';
import { PostgresInfrastructureService } from './postgres-infrastructure.service';
import { PostgresProtocolService } from './postgres-protocol.service';
import { PostgresRetentionService } from './postgres-retention.service';
import { PostgresSubscriptionService } from './postgres-subscription.service';
import { PostgresValueService } from './postgres-value.service';

@Module({
  providers: [
    PostgresInfrastructureService,
    PostgresProtocolService,
    PostgresRetentionService,
    PostgresSubscriptionService,
    PostgresValueService,
    PostgresEventStoreService,
    {
      provide: EVENT_STORE_BACKEND,
      useExisting: PostgresEventStoreService,
    },
  ],
  exports: [EVENT_STORE_BACKEND, PostgresEventStoreService],
})
export class PostgresModule {}
