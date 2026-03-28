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
import { logHotPath } from './debug-log';

@Controller()
@OperationsControllerMethods()
export class OperationsStubController implements OperationsController {
  startScavenge(request: StartScavengeReq): ScavengeResp {
    void request;
    logHotPath('gRPC Operations.StartScavenge');
    return createScavengeResponse(
      'stub-scavenge',
      ScavengeResp_ScavengeResult.Started,
    );
  }

  stopScavenge(request: StopScavengeReq): ScavengeResp {
    logHotPath('gRPC Operations.StopScavenge');
    return createScavengeResponse(
      request.options?.scavengeId ?? 'stub-scavenge',
      ScavengeResp_ScavengeResult.Stopped,
    );
  }

  shutdown(request: Empty): Empty {
    void request;
    logHotPath('gRPC Operations.Shutdown');
    return {};
  }

  mergeIndexes(request: Empty): Empty {
    void request;
    logHotPath('gRPC Operations.MergeIndexes');
    return {};
  }

  resignNode(request: Empty): Empty {
    void request;
    logHotPath('gRPC Operations.ResignNode');
    return {};
  }

  setNodePriority(request: SetNodePriorityReq): Empty {
    void request;
    logHotPath('gRPC Operations.SetNodePriority');
    return {};
  }

  restartPersistentSubscriptions(request: Empty): Empty {
    void request;
    logHotPath('gRPC Operations.RestartPersistentSubscriptions');
    return {};
  }
}
