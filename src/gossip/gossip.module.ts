import { Module } from '@nestjs/common';
import { GossipController } from './gossip.controller';

@Module({
  controllers: [GossipController],
})
export class GossipModule {}
