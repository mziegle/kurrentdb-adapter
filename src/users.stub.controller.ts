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
  UsersController,
  UsersControllerMethods,
} from './interfaces/users';
import { createStubUserDetails } from './stub-utils';

@Controller()
@UsersControllerMethods()
export class UsersStubController implements UsersController {
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

  disable(request: DisableReq): DisableResp {
    void request;
    return {};
  }

  enable(request: EnableReq): EnableResp {
    void request;
    return {};
  }

  details(request: DetailsReq): Observable<DetailsResp> {
    return of(createStubUserDetails(request));
  }

  changePassword(request: ChangePasswordReq): ChangePasswordResp {
    void request;
    return {};
  }

  resetPassword(request: ResetPasswordReq): ResetPasswordResp {
    void request;
    return {};
  }
}
