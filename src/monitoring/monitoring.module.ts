import { Module } from '@nestjs/common';
import { OperationsModule } from '../operations/operations.module';
import { MonitoringController } from './monitoring.controller';

@Module({
  imports: [OperationsModule],
  controllers: [MonitoringController],
})
export class MonitoringModule {}
