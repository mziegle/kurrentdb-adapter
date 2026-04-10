import { Controller } from '@nestjs/common';
import { Observable } from 'rxjs';
import {
  CreateReq,
  CreateResp,
  DeleteReq,
  DeleteResp,
  GetInfoReq,
  GetInfoResp,
  ListReq,
  ListResp,
  PersistentSubscriptionsController as PersistentSubscriptionsControllerContract,
  PersistentSubscriptionsControllerMethods,
  ReadReq,
  ReadResp,
  ReplayParkedReq,
  ReplayParkedResp,
  UpdateReq,
  UpdateResp,
} from '../interfaces/persistent';
import { Empty } from '../interfaces/shared';
import {
  createPersistentGetInfoResponse,
  createPersistentListResponse,
  createPersistentReadStream,
} from '../stub-utils';
import { Metadata } from '@grpc/grpc-js';
import {
  extractGrpcMetadata,
  logHotPath,
  summarizeGrpcMetadata,
} from '../shared/debug-log';

@Controller()
@PersistentSubscriptionsControllerMethods()
export class PersistentSubscriptionsController implements PersistentSubscriptionsControllerContract {
  create(request: CreateReq, metadata?: Metadata): CreateResp {
    logHotPath('gRPC PersistentSubscriptions.Create', {
      summary:
        [
          summarizeGrpcMetadata(metadata),
          this.summarizeCreateOrUpdateRequest(request.options?.groupName),
        ]
          .filter(Boolean)
          .join(' ') || undefined,
      trace: {
        metadata: extractGrpcMetadata(metadata),
        request,
      },
    });
    return {};
  }

  update(request: UpdateReq, metadata?: Metadata): UpdateResp {
    logHotPath('gRPC PersistentSubscriptions.Update', {
      summary:
        [
          summarizeGrpcMetadata(metadata),
          this.summarizeCreateOrUpdateRequest(request.options?.groupName),
        ]
          .filter(Boolean)
          .join(' ') || undefined,
      trace: {
        metadata: extractGrpcMetadata(metadata),
        request,
      },
    });
    return {};
  }

  delete(request: DeleteReq, metadata?: Metadata): DeleteResp {
    logHotPath('gRPC PersistentSubscriptions.Delete', {
      summary:
        [
          summarizeGrpcMetadata(metadata),
          this.summarizeGroupName(request.options?.groupName),
        ]
          .filter(Boolean)
          .join(' ') || undefined,
      trace: {
        metadata: extractGrpcMetadata(metadata),
        request,
      },
    });
    return {};
  }

  read(
    request: Observable<ReadReq>,
    metadata?: Metadata,
  ): Observable<ReadResp> {
    logHotPath('gRPC PersistentSubscriptions.Read', {
      summary: summarizeGrpcMetadata(metadata),
      trace: {
        metadata: extractGrpcMetadata(metadata),
        request: 'observable-stream',
      },
    });
    return createPersistentReadStream();
  }

  getInfo(request: GetInfoReq, metadata?: Metadata): GetInfoResp {
    logHotPath('gRPC PersistentSubscriptions.GetInfo', {
      summary: [
        summarizeGrpcMetadata(metadata),
        this.summarizeGetInfoRequest(request),
      ]
        .filter(Boolean)
        .join(' '),
      trace: {
        metadata: extractGrpcMetadata(metadata),
        request,
      },
    });
    return createPersistentGetInfoResponse(request);
  }

  replayParked(
    request: ReplayParkedReq,
    metadata?: Metadata,
  ): ReplayParkedResp {
    logHotPath('gRPC PersistentSubscriptions.ReplayParked', {
      summary: summarizeGrpcMetadata(metadata),
      trace: {
        metadata: extractGrpcMetadata(metadata),
        request,
      },
    });
    return {};
  }

  list(request: ListReq, metadata?: Metadata): ListResp {
    logHotPath('gRPC PersistentSubscriptions.List', {
      summary:
        [summarizeGrpcMetadata(metadata), this.summarizeListRequest(request)]
          .filter(Boolean)
          .join(' ') || undefined,
      trace: {
        metadata: extractGrpcMetadata(metadata),
        request,
      },
    });
    return createPersistentListResponse();
  }

  restartSubsystem(request: Empty, metadata?: Metadata): Empty {
    logHotPath('gRPC PersistentSubscriptions.RestartSubsystem', {
      summary: summarizeGrpcMetadata(metadata),
      trace: {
        metadata: extractGrpcMetadata(metadata),
        request,
      },
    });
    return {};
  }

  private summarizeCreateOrUpdateRequest(
    groupName?: string,
  ): string | undefined {
    return this.summarizeGroupName(groupName);
  }

  private summarizeGroupName(groupName?: string): string | undefined {
    return groupName ? `group=${groupName}` : undefined;
  }

  private summarizeGetInfoRequest(request: GetInfoReq): string {
    const groupName = this.summarizeGroupName(request.options?.groupName);
    const streamName = request.options?.streamIdentifier?.streamName
      ? Buffer.from(request.options.streamIdentifier.streamName).toString(
          'utf8',
        )
      : '$all';
    return [groupName, `stream=${streamName}`].filter(Boolean).join(' ');
  }

  private summarizeListRequest(request: ListReq): string | undefined {
    if (request.options?.listAllSubscriptions) {
      return 'scope=all';
    }

    const streamName = request.options?.listForStream?.stream?.streamName;
    if (streamName) {
      return `stream=${Buffer.from(streamName).toString('utf8')}`;
    }

    return undefined;
  }
}
