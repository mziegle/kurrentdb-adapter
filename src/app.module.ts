import { Module } from '@nestjs/common';
import { AppService } from './app.service';
import { StreamsController } from './streams.controller';

@Module({
  imports: [],
  controllers: [StreamsController],
  providers: [AppService],
})
export class AppModule {}
