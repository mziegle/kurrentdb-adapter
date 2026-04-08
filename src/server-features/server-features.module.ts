import { Module } from '@nestjs/common';
import { ServerFeaturesController } from './server-features.controller';

@Module({
  controllers: [ServerFeaturesController],
})
export class ServerFeaturesModule {}
