import { Module } from '@nestjs/common';
import { AppService } from './app.service';
import { PostgresEventStoreService } from './postgres-event-store.service';
import { StreamsController } from './streams.controller';

@Module({
  imports: [],
  controllers: [StreamsController],
  providers: [AppService, PostgresEventStoreService],
})
export class AppModule {}
