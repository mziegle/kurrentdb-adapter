import { Controller } from '@nestjs/common';
import {
  GossipController as GossipControllerContract,
  GossipControllerMethods,
  ClusterInfo,
} from '../interfaces/gossip';
import { Metadata } from '@grpc/grpc-js';
import { Empty } from '../interfaces/shared';
import { createStubClusterInfo } from '../stub-utils';
import { logHotPath, summarizeGrpcMetadata } from '../debug-log';

@Controller()
@GossipControllerMethods()
export class GossipController implements GossipControllerContract {
  read(request: Empty, metadata?: Metadata): ClusterInfo {
    void request;
    logHotPath('gRPC Gossip.Read', {
      detail: summarizeGrpcMetadata(metadata),
    });
    return createStubClusterInfo();
  }
}
