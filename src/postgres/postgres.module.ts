import { Module } from '@nestjs/common';
import { EVENT_STORE_BACKEND } from '../event-store/event-store-backend';
import { MongoEventStoreService } from '../mongodb/mongodb-event-store.service';
import { PostgresEventStoreService } from './postgres-event-store.service';
import { PostgresInfrastructureService } from './postgres-infrastructure.service';
import { PostgresProtocolService } from './postgres-protocol.service';
import { PostgresSubscriptionService } from './postgres-subscription.service';

@Module({
  providers: [
    PostgresInfrastructureService,
    PostgresProtocolService,
    PostgresSubscriptionService,
    {
      provide: EVENT_STORE_BACKEND,
      inject: [
        PostgresInfrastructureService,
        PostgresProtocolService,
        PostgresSubscriptionService,
      ],
      useFactory: (
        infrastructure: PostgresInfrastructureService,
        protocol: PostgresProtocolService,
        subscriptions: PostgresSubscriptionService,
      ) => {
        const backend = process.env.EVENT_STORE_BACKEND ?? 'postgres';
        if (backend === 'mongodb') return new MongoEventStoreService();
        if (backend === 'postgres')
          return new PostgresEventStoreService(
            infrastructure,
            protocol,
            subscriptions,
          );
        throw new Error(`Unknown EVENT_STORE_BACKEND: ${backend}`);
      },
    },
  ],
  exports: [EVENT_STORE_BACKEND],
})
export class PostgresModule {}
