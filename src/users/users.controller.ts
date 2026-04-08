import { Controller } from '@nestjs/common';
import { Observable, of } from 'rxjs';
import {
  ChangePasswordReq,
  ChangePasswordResp,
  CreateReq,
  CreateResp,
  DeleteReq,
  DeleteResp,
  DetailsReq,
  DetailsResp,
  DisableReq,
  DisableResp,
  EnableReq,
  EnableResp,
  ResetPasswordReq,
  ResetPasswordResp,
  UpdateReq,
  UpdateResp,
  UsersController as UsersControllerContract,
  UsersControllerMethods,
} from '../interfaces/users';
import { createStubUserDetails } from '../stub-utils';
import { logHotPath } from '../shared/debug-log';

@Controller()
@UsersControllerMethods()
export class UsersController implements UsersControllerContract {
  create(request: CreateReq): CreateResp {
    void request;
    logHotPath('gRPC Users.Create');
    return {};
  }

  update(request: UpdateReq): UpdateResp {
    void request;
    logHotPath('gRPC Users.Update');
    return {};
  }

  delete(request: DeleteReq): DeleteResp {
    void request;
    logHotPath('gRPC Users.Delete');
    return {};
  }

  disable(request: DisableReq): DisableResp {
    void request;
    logHotPath('gRPC Users.Disable');
    return {};
  }

  enable(request: EnableReq): EnableResp {
    void request;
    logHotPath('gRPC Users.Enable');
    return {};
  }

  details(request: DetailsReq): Observable<DetailsResp> {
    logHotPath('gRPC Users.Details');
    return of(createStubUserDetails(request));
  }

  changePassword(request: ChangePasswordReq): ChangePasswordResp {
    void request;
    logHotPath('gRPC Users.ChangePassword');
    return {};
  }

  resetPassword(request: ResetPasswordReq): ResetPasswordResp {
    void request;
    logHotPath('gRPC Users.ResetPassword');
    return {};
  }
}
