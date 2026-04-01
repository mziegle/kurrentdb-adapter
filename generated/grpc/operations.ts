import type * as grpc from '@grpc/grpc-js';
import type { MessageTypeDefinition } from '@grpc/proto-loader';

import type { OperationsClient as _event_store_client_operations_OperationsClient, OperationsDefinition as _event_store_client_operations_OperationsDefinition } from './event_store/client/operations/Operations';

type SubtypeConstructor<Constructor extends new (...args: any) => any, Subtype> = {
  new(...args: ConstructorParameters<Constructor>): Subtype;
};

export interface ProtoGrpcType {
  event_store: {
    client: {
      AccessDenied: MessageTypeDefinition
      AllStreamPosition: MessageTypeDefinition
      BadRequest: MessageTypeDefinition
      Empty: MessageTypeDefinition
      InvalidTransaction: MessageTypeDefinition
      MaximumAppendEventSizeExceeded: MessageTypeDefinition
      MaximumAppendSizeExceeded: MessageTypeDefinition
      StreamDeleted: MessageTypeDefinition
      StreamIdentifier: MessageTypeDefinition
      Timeout: MessageTypeDefinition
      UUID: MessageTypeDefinition
      Unknown: MessageTypeDefinition
      WrongExpectedVersion: MessageTypeDefinition
      operations: {
        Operations: SubtypeConstructor<typeof grpc.Client, _event_store_client_operations_OperationsClient> & { service: _event_store_client_operations_OperationsDefinition }
        ScavengeResp: MessageTypeDefinition
        SetNodePriorityReq: MessageTypeDefinition
        StartScavengeReq: MessageTypeDefinition
        StopScavengeReq: MessageTypeDefinition
      }
    }
  }
  google: {
    protobuf: {
      Empty: MessageTypeDefinition
    }
  }
}

