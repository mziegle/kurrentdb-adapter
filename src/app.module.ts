import { Module } from '@nestjs/common';
import { AppService } from './app.service';
import { GossipStubController } from './gossip.stub.controller';
import { MonitoringStubController } from './monitoring.stub.controller';
import { OperationsStubController } from './operations.stub.controller';
import { PersistentSubscriptionsStubController } from './persistent.stub.controller';
import { PostgresModule } from './postgres/postgres.module';
import { ServerFeaturesController } from './server-features.controller';
import { StreamsModule } from './streams/streams.module';
import { UsersStubController } from './users.stub.controller';

@Module({
  imports: [PostgresModule, StreamsModule],
  controllers: [
    GossipStubController,
    MonitoringStubController,
    OperationsStubController,
    PersistentSubscriptionsStubController,
    ServerFeaturesController,
    UsersStubController,
  ],
  providers: [AppService],
})
export class AppModule {}
