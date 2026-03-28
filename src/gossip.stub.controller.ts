import { Controller } from '@nestjs/common';
import {
  GossipController,
  GossipControllerMethods,
  ClusterInfo,
} from './interfaces/gossip';
import { Empty } from './interfaces/shared';
import { createStubClusterInfo } from './stub-utils';

@Controller()
@GossipControllerMethods()
export class GossipStubController implements GossipController {
  read(request: Empty): ClusterInfo {
    void request;
    return createStubClusterInfo();
  }
}
