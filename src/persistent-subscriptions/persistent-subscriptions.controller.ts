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
import { logHotPath } from '../debug-log';

@Controller()
@PersistentSubscriptionsControllerMethods()
export class PersistentSubscriptionsController implements PersistentSubscriptionsControllerContract {
  create(request: CreateReq): CreateResp {
    void request;
    logHotPath('gRPC PersistentSubscriptions.Create', {
      detail: this.summarizeCreateOrUpdateRequest(request.options?.groupName),
    });
    return {};
  }

  update(request: UpdateReq): UpdateResp {
    logHotPath('gRPC PersistentSubscriptions.Update', {
      detail: this.summarizeCreateOrUpdateRequest(request.options?.groupName),
    });
    return {};
  }

  delete(request: DeleteReq): DeleteResp {
    logHotPath('gRPC PersistentSubscriptions.Delete', {
      detail: this.summarizeGroupName(request.options?.groupName),
    });
    return {};
  }

  read(request: Observable<ReadReq>): Observable<ReadResp> {
    void request;
    logHotPath('gRPC PersistentSubscriptions.Read');
    return createPersistentReadStream();
  }

  getInfo(request: GetInfoReq): GetInfoResp {
    logHotPath('gRPC PersistentSubscriptions.GetInfo', {
      detail: this.summarizeGetInfoRequest(request),
    });
    return createPersistentGetInfoResponse(request);
  }

  replayParked(request: ReplayParkedReq): ReplayParkedResp {
    void request;
    logHotPath('gRPC PersistentSubscriptions.ReplayParked');
    return {};
  }

  list(request: ListReq): ListResp {
    logHotPath('gRPC PersistentSubscriptions.List', {
      detail: this.summarizeListRequest(request),
    });
    return createPersistentListResponse();
  }

  restartSubsystem(request: Empty): Empty {
    void request;
    logHotPath('gRPC PersistentSubscriptions.RestartSubsystem');
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
