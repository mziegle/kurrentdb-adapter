import { Controller, Inject } from '@nestjs/common';
import {
  OperationsController as OperationsControllerContract,
  OperationsControllerMethods,
  ScavengeResp,
  SetNodePriorityReq,
  StartScavengeReq,
  StopScavengeReq,
} from '../interfaces/operations';
import { Empty } from '../interfaces/shared';
import { AdapterStatsService } from './adapter-stats.service';
import {
  EVENT_STORE_BACKEND,
  EventStoreBackend,
} from '../event-store/event-store-backend';
import { Metadata } from '@grpc/grpc-js';
import {
  extractGrpcMetadata,
  logHotPath,
  summarizeGrpcMetadata,
} from '../shared/debug-log';
@Controller()
@OperationsControllerMethods()
export class OperationsController implements OperationsControllerContract {
  constructor(
    @Inject(EVENT_STORE_BACKEND)
    private readonly eventStore: EventStoreBackend,
    private readonly stats: AdapterStatsService,
  ) {}

  startScavenge(request: StartScavengeReq, metadata?: Metadata): ScavengeResp {
    logHotPath('gRPC Operations.StartScavenge', {
      summary: summarizeGrpcMetadata(metadata),
      trace: {
        metadata: extractGrpcMetadata(metadata),
        request,
      },
    });
    const operation = this.stats.startOperation('startScavenge');
    try {
      const response = this.eventStore.startScavenge(request);
      operation.succeeded();
      return response;
    } catch (error: unknown) {
      operation.failed();
      throw error;
    }
  }

  stopScavenge(request: StopScavengeReq, metadata?: Metadata): ScavengeResp {
    logHotPath('gRPC Operations.StopScavenge', {
      summary: summarizeGrpcMetadata(metadata),
      trace: {
        metadata: extractGrpcMetadata(metadata),
        request,
      },
    });
    const operation = this.stats.startOperation('stopScavenge');
    try {
      const response = this.eventStore.stopScavenge(request);
      operation.succeeded();
      return response;
    } catch (error: unknown) {
      operation.failed();
      throw error;
    }
  }

  shutdown(request: Empty, metadata?: Metadata): Empty {
    logHotPath('gRPC Operations.Shutdown', {
      summary: summarizeGrpcMetadata(metadata),
      trace: {
        metadata: extractGrpcMetadata(metadata),
        request,
      },
    });
    return {};
  }

  mergeIndexes(request: Empty, metadata?: Metadata): Empty {
    logHotPath('gRPC Operations.MergeIndexes', {
      summary: summarizeGrpcMetadata(metadata),
      trace: {
        metadata: extractGrpcMetadata(metadata),
        request,
      },
    });
    return {};
  }

  resignNode(request: Empty, metadata?: Metadata): Empty {
    logHotPath('gRPC Operations.ResignNode', {
      summary: summarizeGrpcMetadata(metadata),
      trace: {
        metadata: extractGrpcMetadata(metadata),
        request,
      },
    });
    return {};
  }

  setNodePriority(request: SetNodePriorityReq, metadata?: Metadata): Empty {
    logHotPath('gRPC Operations.SetNodePriority', {
      summary: summarizeGrpcMetadata(metadata),
      trace: {
        metadata: extractGrpcMetadata(metadata),
        request,
      },
    });
    return {};
  }

  restartPersistentSubscriptions(request: Empty, metadata?: Metadata): Empty {
    logHotPath('gRPC Operations.RestartPersistentSubscriptions', {
      summary: summarizeGrpcMetadata(metadata),
      trace: {
        metadata: extractGrpcMetadata(metadata),
        request,
      },
    });
    return {};
  }
}
