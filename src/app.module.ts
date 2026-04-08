import { Module } from '@nestjs/common';
import { AppService } from './app.service';
import { GossipModule } from './gossip/gossip.module';
import { MonitoringStubController } from './monitoring.stub.controller';
import { OperationsModule } from './operations/operations.module';
import { PersistentSubscriptionsModule } from './persistent-subscriptions/persistent-subscriptions.module';
import { PostgresModule } from './postgres/postgres.module';
import { ServerFeaturesModule } from './server-features/server-features.module';
import { StreamsModule } from './streams/streams.module';
import { UsersModule } from './users/users.module';

@Module({
  imports: [
    GossipModule,
    PostgresModule,
    OperationsModule,
    PersistentSubscriptionsModule,
    ServerFeaturesModule,
    StreamsModule,
    UsersModule,
  ],
  controllers: [MonitoringStubController],
  providers: [AppService],
})
export class AppModule {}
