import { Controller } from '@nestjs/common';
import {
  OperationsController,
  OperationsControllerMethods,
  ScavengeResp,
  SetNodePriorityReq,
  StartScavengeReq,
  StopScavengeReq,
} from './interfaces/operations';
import { Empty } from './interfaces/shared';
import { logHotPath } from './debug-log';
import { PostgresEventStoreService } from './postgres-event-store.service';

@Controller()
@OperationsControllerMethods()
export class OperationsStubController implements OperationsController {
  constructor(private readonly eventStore: PostgresEventStoreService) {}

  startScavenge(request: StartScavengeReq): ScavengeResp {
    logHotPath('gRPC Operations.StartScavenge');
    return this.eventStore.startScavenge(request);
  }

  stopScavenge(request: StopScavengeReq): ScavengeResp {
    logHotPath('gRPC Operations.StopScavenge');
    return this.eventStore.stopScavenge(request);
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
