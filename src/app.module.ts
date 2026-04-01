import { Module } from '@nestjs/common';
import { AppService } from './app.service';
import { GossipStubController } from './gossip.stub.controller';
import { MonitoringStubController } from './monitoring.stub.controller';
import { OperationsStubController } from './operations.stub.controller';
import { PersistentSubscriptionsStubController } from './persistent.stub.controller';
import { PostgresEventStoreService } from './postgres-event-store.service';
import { ServerFeaturesController } from './server-features.controller';
import { StreamsController } from './streams.controller';
import { UsersStubController } from './users.stub.controller';
import { AdapterStatsService } from './adapter-stats.service';

@Module({
  imports: [],
  controllers: [
    GossipStubController,
    MonitoringStubController,
    OperationsStubController,
    PersistentSubscriptionsStubController,
    ServerFeaturesController,
    StreamsController,
    UsersStubController,
  ],
  providers: [AppService, PostgresEventStoreService, AdapterStatsService],
})
export class AppModule {}
