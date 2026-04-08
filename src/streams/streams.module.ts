import { Module } from '@nestjs/common';
import { AdapterStatsService } from '../adapter-stats.service';
import { PostgresEventStoreService } from '../postgres-event-store.service';
import { StreamsController } from './streams.controller';

@Module({
  controllers: [StreamsController],
  providers: [PostgresEventStoreService, AdapterStatsService],
  exports: [PostgresEventStoreService, AdapterStatsService],
})
export class StreamsModule {}
