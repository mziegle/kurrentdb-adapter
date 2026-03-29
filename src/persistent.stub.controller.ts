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
  PersistentSubscriptionsController,
  PersistentSubscriptionsControllerMethods,
  ReadReq,
  ReadResp,
  ReplayParkedReq,
  ReplayParkedResp,
  UpdateReq,
  UpdateResp,
} from './interfaces/persistent';
import { Empty } from './interfaces/shared';
import {
  createPersistentGetInfoResponse,
  createPersistentListResponse,
  createPersistentReadStream,
} from './stub-utils';
import { logHotPath } from './debug-log';

@Controller()
@PersistentSubscriptionsControllerMethods()
export class PersistentSubscriptionsStubController implements PersistentSubscriptionsController {
  create(request: CreateReq): CreateResp {
    void request;
    logHotPath('gRPC PersistentSubscriptions.Create');
    return {};
  }

  update(request: UpdateReq): UpdateResp {
    void request;
    logHotPath('gRPC PersistentSubscriptions.Update');
    return {};
  }

  delete(request: DeleteReq): DeleteResp {
    void request;
    logHotPath('gRPC PersistentSubscriptions.Delete');
    return {};
  }

  read(request: Observable<ReadReq>): Observable<ReadResp> {
    void request;
    logHotPath('gRPC PersistentSubscriptions.Read');
    return createPersistentReadStream();
  }

  getInfo(request: GetInfoReq): GetInfoResp {
    logHotPath('gRPC PersistentSubscriptions.GetInfo');
    return createPersistentGetInfoResponse(request);
  }

  replayParked(request: ReplayParkedReq): ReplayParkedResp {
    void request;
    logHotPath('gRPC PersistentSubscriptions.ReplayParked');
    return {};
  }

  list(request: ListReq): ListResp {
    void request;
    logHotPath('gRPC PersistentSubscriptions.List');
    return createPersistentListResponse();
  }

  restartSubsystem(request: Empty): Empty {
    void request;
    logHotPath('gRPC PersistentSubscriptions.RestartSubsystem');
    return {};
  }
}
