import { Module } from '@nestjs/common';
import { AppService } from './app.service';
import { PostgresEventStoreService } from './postgres-event-store.service';
import { ServerFeaturesController } from './server-features.controller';
import { StreamsController } from './streams.controller';

@Module({
  imports: [],
  controllers: [ServerFeaturesController, StreamsController],
  providers: [AppService, PostgresEventStoreService],
})
export class AppModule {}
