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

@Controller()
@PersistentSubscriptionsControllerMethods()
export class PersistentSubscriptionsStubController
  implements PersistentSubscriptionsController
{
  create(request: CreateReq): CreateResp {
    void request;
    return {};
  }

  update(request: UpdateReq): UpdateResp {
    void request;
    return {};
  }

  delete(request: DeleteReq): DeleteResp {
    void request;
    return {};
  }

  read(request: Observable<ReadReq>): Observable<ReadResp> {
    void request;
    return createPersistentReadStream();
  }

  getInfo(request: GetInfoReq): GetInfoResp {
    return createPersistentGetInfoResponse(request);
  }

  replayParked(request: ReplayParkedReq): ReplayParkedResp {
    void request;
    return {};
  }

  list(request: ListReq): ListResp {
    void request;
    return createPersistentListResponse();
  }

  restartSubsystem(request: Empty): Empty {
    void request;
    return {};
  }
}
