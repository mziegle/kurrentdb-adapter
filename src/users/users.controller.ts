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
import { Metadata } from '@grpc/grpc-js';
import {
  extractGrpcMetadata,
  logHotPath,
  summarizeGrpcMetadata,
} from '../shared/debug-log';

@Controller()
@UsersControllerMethods()
export class UsersController implements UsersControllerContract {
  create(request: CreateReq, metadata?: Metadata): CreateResp {
    logHotPath('gRPC Users.Create', {
      summary: summarizeGrpcMetadata(metadata),
      trace: {
        metadata: extractGrpcMetadata(metadata),
        request,
      },
    });
    return {};
  }

  update(request: UpdateReq, metadata?: Metadata): UpdateResp {
    logHotPath('gRPC Users.Update', {
      summary: summarizeGrpcMetadata(metadata),
      trace: {
        metadata: extractGrpcMetadata(metadata),
        request,
      },
    });
    return {};
  }

  delete(request: DeleteReq, metadata?: Metadata): DeleteResp {
    logHotPath('gRPC Users.Delete', {
      summary: summarizeGrpcMetadata(metadata),
      trace: {
        metadata: extractGrpcMetadata(metadata),
        request,
      },
    });
    return {};
  }

  disable(request: DisableReq, metadata?: Metadata): DisableResp {
    logHotPath('gRPC Users.Disable', {
      summary: summarizeGrpcMetadata(metadata),
      trace: {
        metadata: extractGrpcMetadata(metadata),
        request,
      },
    });
    return {};
  }

  enable(request: EnableReq, metadata?: Metadata): EnableResp {
    logHotPath('gRPC Users.Enable', {
      summary: summarizeGrpcMetadata(metadata),
      trace: {
        metadata: extractGrpcMetadata(metadata),
        request,
      },
    });
    return {};
  }

  details(request: DetailsReq, metadata?: Metadata): Observable<DetailsResp> {
    logHotPath('gRPC Users.Details', {
      summary: summarizeGrpcMetadata(metadata),
      trace: {
        metadata: extractGrpcMetadata(metadata),
        request,
      },
    });
    return of(createStubUserDetails(request));
  }

  changePassword(
    request: ChangePasswordReq,
    metadata?: Metadata,
  ): ChangePasswordResp {
    logHotPath('gRPC Users.ChangePassword', {
      summary: summarizeGrpcMetadata(metadata),
      trace: {
        metadata: extractGrpcMetadata(metadata),
        request,
      },
    });
    return {};
  }

  resetPassword(
    request: ResetPasswordReq,
    metadata?: Metadata,
  ): ResetPasswordResp {
    logHotPath('gRPC Users.ResetPassword', {
      summary: summarizeGrpcMetadata(metadata),
      trace: {
        metadata: extractGrpcMetadata(metadata),
        request,
      },
    });
    return {};
  }
}
