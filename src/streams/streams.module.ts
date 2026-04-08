import { Module } from '@nestjs/common';
import { OperationsModule } from '../operations/operations.module';
import { PostgresModule } from '../postgres/postgres.module';
import { StreamsController } from './streams.controller';

@Module({
  imports: [PostgresModule, OperationsModule],
  controllers: [StreamsController],
  exports: [PostgresModule],
})
export class StreamsModule {}
