import { Controller } from '@nestjs/common';
import {
  GossipController,
  GossipControllerMethods,
  ClusterInfo,
} from './interfaces/gossip';
import { Metadata } from '@grpc/grpc-js';
import { Empty } from './interfaces/shared';
import { createStubClusterInfo } from './stub-utils';
import { logHotPath, summarizeGrpcMetadata } from './debug-log';

@Controller()
@GossipControllerMethods()
export class GossipStubController implements GossipController {
  read(request: Empty, metadata?: Metadata): ClusterInfo {
    void request;
    logHotPath('gRPC Gossip.Read', {
      detail: summarizeGrpcMetadata(metadata),
    });
    return createStubClusterInfo();
  }
}
