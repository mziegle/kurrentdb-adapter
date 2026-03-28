import { Controller } from '@nestjs/common';
import {
  OperationsController,
  OperationsControllerMethods,
  ScavengeResp,
  ScavengeResp_ScavengeResult,
  SetNodePriorityReq,
  StartScavengeReq,
  StopScavengeReq,
} from './interfaces/operations';
import { Empty } from './interfaces/shared';
import { createScavengeResponse } from './stub-utils';

@Controller()
@OperationsControllerMethods()
export class OperationsStubController implements OperationsController {
  startScavenge(request: StartScavengeReq): ScavengeResp {
    void request;
    return createScavengeResponse(
      'stub-scavenge',
      ScavengeResp_ScavengeResult.Started,
    );
  }

  stopScavenge(request: StopScavengeReq): ScavengeResp {
    return createScavengeResponse(
      request.options?.scavengeId ?? 'stub-scavenge',
      ScavengeResp_ScavengeResult.Stopped,
    );
  }

  shutdown(request: Empty): Empty {
    void request;
    return {};
  }

  mergeIndexes(request: Empty): Empty {
    void request;
    return {};
  }

  resignNode(request: Empty): Empty {
    void request;
    return {};
  }

  setNodePriority(request: SetNodePriorityReq): Empty {
    void request;
    return {};
  }

  restartPersistentSubscriptions(request: Empty): Empty {
    void request;
    return {};
  }
}
