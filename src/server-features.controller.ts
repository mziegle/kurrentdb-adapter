import { Controller } from '@nestjs/common';
import { GrpcMethod } from '@nestjs/microservices';
import { SupportedMethods, SupportedMethod } from './interfaces/serverfeatures';

const STREAMS_SERVICE_NAME = 'event_store.client.streams.Streams';

@Controller()
export class ServerFeaturesController {
  @GrpcMethod('ServerFeatures', 'getSupportedMethods')
  getSupportedMethods(): SupportedMethods {
    return {
      eventStoreServerVersion: '24.0.0',
      methods: [
        this.createSupportedMethod('Read'),
        this.createSupportedMethod('Append'),
        this.createSupportedMethod('Delete'),
        this.createSupportedMethod('Tombstone'),
        this.createSupportedMethod('BatchAppend'),
      ],
    };
  }

  private createSupportedMethod(methodName: string): SupportedMethod {
    return {
      methodName,
      serviceName: STREAMS_SERVICE_NAME,
      features: [],
    };
  }
}
