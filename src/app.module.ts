import { Module } from '@nestjs/common';
import { AppService } from './app.service';
import { GossipModule } from './gossip/gossip.module';
import { MonitoringStubController } from './monitoring.stub.controller';
import { OperationsModule } from './operations/operations.module';
import { PersistentSubscriptionsStubController } from './persistent.stub.controller';
import { PostgresModule } from './postgres/postgres.module';
import { ServerFeaturesController } from './server-features.controller';
import { StreamsModule } from './streams/streams.module';
import { UsersModule } from './users/users.module';

@Module({
  imports: [
    GossipModule,
    PostgresModule,
    OperationsModule,
    StreamsModule,
    UsersModule,
  ],
  controllers: [
    MonitoringStubController,
    PersistentSubscriptionsStubController,
    ServerFeaturesController,
  ],
  providers: [AppService],
})
export class AppModule {}
