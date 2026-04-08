import { Module } from '@nestjs/common';
import { AdapterStatsService } from '../adapter-stats.service';
import { PostgresModule } from '../postgres/postgres.module';
import { StreamsController } from './streams.controller';

@Module({
  imports: [PostgresModule],
  controllers: [StreamsController],
  providers: [AdapterStatsService],
  exports: [AdapterStatsService, PostgresModule],
})
export class StreamsModule {}
