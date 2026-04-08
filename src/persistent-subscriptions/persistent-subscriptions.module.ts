import { Module } from '@nestjs/common';
import { PersistentSubscriptionsController } from './persistent-subscriptions.controller';

@Module({
  controllers: [PersistentSubscriptionsController],
})
export class PersistentSubscriptionsModule {}
