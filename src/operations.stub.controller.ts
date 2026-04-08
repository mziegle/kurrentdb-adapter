import { Controller, Inject } from '@nestjs/common';
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
import { AdapterStatsService } from './adapter-stats.service';
import { EVENT_STORE_BACKEND, EventStoreBackend } from './event-store-backend';

@Controller()
@OperationsControllerMethods()
export class OperationsStubController implements OperationsController {
  constructor(
    @Inject(EVENT_STORE_BACKEND)
    private readonly eventStore: EventStoreBackend,
    private readonly stats: AdapterStatsService,
  ) {}

  startScavenge(request: StartScavengeReq): ScavengeResp {
    logHotPath('gRPC Operations.StartScavenge');
    const operation = this.stats.startOperation('startScavenge');
    try {
      const response = this.eventStore.startScavenge(request);
      operation.succeeded();
      return response;
    } catch (error) {
      operation.failed();
      throw error;
    }
  }

  stopScavenge(request: StopScavengeReq): ScavengeResp {
    logHotPath('gRPC Operations.StopScavenge');
    const operation = this.stats.startOperation('stopScavenge');
    try {
      const response = this.eventStore.stopScavenge(request);
      operation.succeeded();
      return response;
    } catch (error) {
      operation.failed();
      throw error;
    }
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
