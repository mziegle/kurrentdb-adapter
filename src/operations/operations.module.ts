import { Module } from '@nestjs/common';
import { PostgresModule } from '../postgres/postgres.module';
import { AdapterStatsService } from './adapter-stats.service';
import { OperationsController } from './operations.controller';

@Module({
  imports: [PostgresModule],
  controllers: [OperationsController],
  providers: [AdapterStatsService],
  exports: [AdapterStatsService],
})
export class OperationsModule {}
