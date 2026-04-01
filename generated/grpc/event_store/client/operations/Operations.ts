// Original file: proto/Grpc/operations.proto

import type * as grpc from '@grpc/grpc-js'
import type { MethodDefinition } from '@grpc/proto-loader'
import type { Empty as _event_store_client_Empty, Empty__Output as _event_store_client_Empty__Output } from '../../../event_store/client/Empty';
import type { ScavengeResp as _event_store_client_operations_ScavengeResp, ScavengeResp__Output as _event_store_client_operations_ScavengeResp__Output } from '../../../event_store/client/operations/ScavengeResp';
import type { SetNodePriorityReq as _event_store_client_operations_SetNodePriorityReq, SetNodePriorityReq__Output as _event_store_client_operations_SetNodePriorityReq__Output } from '../../../event_store/client/operations/SetNodePriorityReq';
import type { StartScavengeReq as _event_store_client_operations_StartScavengeReq, StartScavengeReq__Output as _event_store_client_operations_StartScavengeReq__Output } from '../../../event_store/client/operations/StartScavengeReq';
import type { StopScavengeReq as _event_store_client_operations_StopScavengeReq, StopScavengeReq__Output as _event_store_client_operations_StopScavengeReq__Output } from '../../../event_store/client/operations/StopScavengeReq';

export interface OperationsClient extends grpc.Client {
  MergeIndexes(argument: _event_store_client_Empty, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_event_store_client_Empty__Output>): grpc.ClientUnaryCall;
  MergeIndexes(argument: _event_store_client_Empty, metadata: grpc.Metadata, callback: grpc.requestCallback<_event_store_client_Empty__Output>): grpc.ClientUnaryCall;
  MergeIndexes(argument: _event_store_client_Empty, options: grpc.CallOptions, callback: grpc.requestCallback<_event_store_client_Empty__Output>): grpc.ClientUnaryCall;
  MergeIndexes(argument: _event_store_client_Empty, callback: grpc.requestCallback<_event_store_client_Empty__Output>): grpc.ClientUnaryCall;
  mergeIndexes(argument: _event_store_client_Empty, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_event_store_client_Empty__Output>): grpc.ClientUnaryCall;
  mergeIndexes(argument: _event_store_client_Empty, metadata: grpc.Metadata, callback: grpc.requestCallback<_event_store_client_Empty__Output>): grpc.ClientUnaryCall;
  mergeIndexes(argument: _event_store_client_Empty, options: grpc.CallOptions, callback: grpc.requestCallback<_event_store_client_Empty__Output>): grpc.ClientUnaryCall;
  mergeIndexes(argument: _event_store_client_Empty, callback: grpc.requestCallback<_event_store_client_Empty__Output>): grpc.ClientUnaryCall;
  
  ResignNode(argument: _event_store_client_Empty, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_event_store_client_Empty__Output>): grpc.ClientUnaryCall;
  ResignNode(argument: _event_store_client_Empty, metadata: grpc.Metadata, callback: grpc.requestCallback<_event_store_client_Empty__Output>): grpc.ClientUnaryCall;
  ResignNode(argument: _event_store_client_Empty, options: grpc.CallOptions, callback: grpc.requestCallback<_event_store_client_Empty__Output>): grpc.ClientUnaryCall;
  ResignNode(argument: _event_store_client_Empty, callback: grpc.requestCallback<_event_store_client_Empty__Output>): grpc.ClientUnaryCall;
  resignNode(argument: _event_store_client_Empty, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_event_store_client_Empty__Output>): grpc.ClientUnaryCall;
  resignNode(argument: _event_store_client_Empty, metadata: grpc.Metadata, callback: grpc.requestCallback<_event_store_client_Empty__Output>): grpc.ClientUnaryCall;
  resignNode(argument: _event_store_client_Empty, options: grpc.CallOptions, callback: grpc.requestCallback<_event_store_client_Empty__Output>): grpc.ClientUnaryCall;
  resignNode(argument: _event_store_client_Empty, callback: grpc.requestCallback<_event_store_client_Empty__Output>): grpc.ClientUnaryCall;
  
  RestartPersistentSubscriptions(argument: _event_store_client_Empty, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_event_store_client_Empty__Output>): grpc.ClientUnaryCall;
  RestartPersistentSubscriptions(argument: _event_store_client_Empty, metadata: grpc.Metadata, callback: grpc.requestCallback<_event_store_client_Empty__Output>): grpc.ClientUnaryCall;
  RestartPersistentSubscriptions(argument: _event_store_client_Empty, options: grpc.CallOptions, callback: grpc.requestCallback<_event_store_client_Empty__Output>): grpc.ClientUnaryCall;
  RestartPersistentSubscriptions(argument: _event_store_client_Empty, callback: grpc.requestCallback<_event_store_client_Empty__Output>): grpc.ClientUnaryCall;
  restartPersistentSubscriptions(argument: _event_store_client_Empty, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_event_store_client_Empty__Output>): grpc.ClientUnaryCall;
  restartPersistentSubscriptions(argument: _event_store_client_Empty, metadata: grpc.Metadata, callback: grpc.requestCallback<_event_store_client_Empty__Output>): grpc.ClientUnaryCall;
  restartPersistentSubscriptions(argument: _event_store_client_Empty, options: grpc.CallOptions, callback: grpc.requestCallback<_event_store_client_Empty__Output>): grpc.ClientUnaryCall;
  restartPersistentSubscriptions(argument: _event_store_client_Empty, callback: grpc.requestCallback<_event_store_client_Empty__Output>): grpc.ClientUnaryCall;
  
  SetNodePriority(argument: _event_store_client_operations_SetNodePriorityReq, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_event_store_client_Empty__Output>): grpc.ClientUnaryCall;
  SetNodePriority(argument: _event_store_client_operations_SetNodePriorityReq, metadata: grpc.Metadata, callback: grpc.requestCallback<_event_store_client_Empty__Output>): grpc.ClientUnaryCall;
  SetNodePriority(argument: _event_store_client_operations_SetNodePriorityReq, options: grpc.CallOptions, callback: grpc.requestCallback<_event_store_client_Empty__Output>): grpc.ClientUnaryCall;
  SetNodePriority(argument: _event_store_client_operations_SetNodePriorityReq, callback: grpc.requestCallback<_event_store_client_Empty__Output>): grpc.ClientUnaryCall;
  setNodePriority(argument: _event_store_client_operations_SetNodePriorityReq, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_event_store_client_Empty__Output>): grpc.ClientUnaryCall;
  setNodePriority(argument: _event_store_client_operations_SetNodePriorityReq, metadata: grpc.Metadata, callback: grpc.requestCallback<_event_store_client_Empty__Output>): grpc.ClientUnaryCall;
  setNodePriority(argument: _event_store_client_operations_SetNodePriorityReq, options: grpc.CallOptions, callback: grpc.requestCallback<_event_store_client_Empty__Output>): grpc.ClientUnaryCall;
  setNodePriority(argument: _event_store_client_operations_SetNodePriorityReq, callback: grpc.requestCallback<_event_store_client_Empty__Output>): grpc.ClientUnaryCall;
  
  Shutdown(argument: _event_store_client_Empty, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_event_store_client_Empty__Output>): grpc.ClientUnaryCall;
  Shutdown(argument: _event_store_client_Empty, metadata: grpc.Metadata, callback: grpc.requestCallback<_event_store_client_Empty__Output>): grpc.ClientUnaryCall;
  Shutdown(argument: _event_store_client_Empty, options: grpc.CallOptions, callback: grpc.requestCallback<_event_store_client_Empty__Output>): grpc.ClientUnaryCall;
  Shutdown(argument: _event_store_client_Empty, callback: grpc.requestCallback<_event_store_client_Empty__Output>): grpc.ClientUnaryCall;
  shutdown(argument: _event_store_client_Empty, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_event_store_client_Empty__Output>): grpc.ClientUnaryCall;
  shutdown(argument: _event_store_client_Empty, metadata: grpc.Metadata, callback: grpc.requestCallback<_event_store_client_Empty__Output>): grpc.ClientUnaryCall;
  shutdown(argument: _event_store_client_Empty, options: grpc.CallOptions, callback: grpc.requestCallback<_event_store_client_Empty__Output>): grpc.ClientUnaryCall;
  shutdown(argument: _event_store_client_Empty, callback: grpc.requestCallback<_event_store_client_Empty__Output>): grpc.ClientUnaryCall;
  
  StartScavenge(argument: _event_store_client_operations_StartScavengeReq, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_event_store_client_operations_ScavengeResp__Output>): grpc.ClientUnaryCall;
  StartScavenge(argument: _event_store_client_operations_StartScavengeReq, metadata: grpc.Metadata, callback: grpc.requestCallback<_event_store_client_operations_ScavengeResp__Output>): grpc.ClientUnaryCall;
  StartScavenge(argument: _event_store_client_operations_StartScavengeReq, options: grpc.CallOptions, callback: grpc.requestCallback<_event_store_client_operations_ScavengeResp__Output>): grpc.ClientUnaryCall;
  StartScavenge(argument: _event_store_client_operations_StartScavengeReq, callback: grpc.requestCallback<_event_store_client_operations_ScavengeResp__Output>): grpc.ClientUnaryCall;
  startScavenge(argument: _event_store_client_operations_StartScavengeReq, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_event_store_client_operations_ScavengeResp__Output>): grpc.ClientUnaryCall;
  startScavenge(argument: _event_store_client_operations_StartScavengeReq, metadata: grpc.Metadata, callback: grpc.requestCallback<_event_store_client_operations_ScavengeResp__Output>): grpc.ClientUnaryCall;
  startScavenge(argument: _event_store_client_operations_StartScavengeReq, options: grpc.CallOptions, callback: grpc.requestCallback<_event_store_client_operations_ScavengeResp__Output>): grpc.ClientUnaryCall;
  startScavenge(argument: _event_store_client_operations_StartScavengeReq, callback: grpc.requestCallback<_event_store_client_operations_ScavengeResp__Output>): grpc.ClientUnaryCall;
  
  StopScavenge(argument: _event_store_client_operations_StopScavengeReq, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_event_store_client_operations_ScavengeResp__Output>): grpc.ClientUnaryCall;
  StopScavenge(argument: _event_store_client_operations_StopScavengeReq, metadata: grpc.Metadata, callback: grpc.requestCallback<_event_store_client_operations_ScavengeResp__Output>): grpc.ClientUnaryCall;
  StopScavenge(argument: _event_store_client_operations_StopScavengeReq, options: grpc.CallOptions, callback: grpc.requestCallback<_event_store_client_operations_ScavengeResp__Output>): grpc.ClientUnaryCall;
  StopScavenge(argument: _event_store_client_operations_StopScavengeReq, callback: grpc.requestCallback<_event_store_client_operations_ScavengeResp__Output>): grpc.ClientUnaryCall;
  stopScavenge(argument: _event_store_client_operations_StopScavengeReq, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_event_store_client_operations_ScavengeResp__Output>): grpc.ClientUnaryCall;
  stopScavenge(argument: _event_store_client_operations_StopScavengeReq, metadata: grpc.Metadata, callback: grpc.requestCallback<_event_store_client_operations_ScavengeResp__Output>): grpc.ClientUnaryCall;
  stopScavenge(argument: _event_store_client_operations_StopScavengeReq, options: grpc.CallOptions, callback: grpc.requestCallback<_event_store_client_operations_ScavengeResp__Output>): grpc.ClientUnaryCall;
  stopScavenge(argument: _event_store_client_operations_StopScavengeReq, callback: grpc.requestCallback<_event_store_client_operations_ScavengeResp__Output>): grpc.ClientUnaryCall;
  
}

export interface OperationsHandlers extends grpc.UntypedServiceImplementation {
  MergeIndexes: grpc.handleUnaryCall<_event_store_client_Empty__Output, _event_store_client_Empty>;
  
  ResignNode: grpc.handleUnaryCall<_event_store_client_Empty__Output, _event_store_client_Empty>;
  
  RestartPersistentSubscriptions: grpc.handleUnaryCall<_event_store_client_Empty__Output, _event_store_client_Empty>;
  
  SetNodePriority: grpc.handleUnaryCall<_event_store_client_operations_SetNodePriorityReq__Output, _event_store_client_Empty>;
  
  Shutdown: grpc.handleUnaryCall<_event_store_client_Empty__Output, _event_store_client_Empty>;
  
  StartScavenge: grpc.handleUnaryCall<_event_store_client_operations_StartScavengeReq__Output, _event_store_client_operations_ScavengeResp>;
  
  StopScavenge: grpc.handleUnaryCall<_event_store_client_operations_StopScavengeReq__Output, _event_store_client_operations_ScavengeResp>;
  
}

export interface OperationsDefinition extends grpc.ServiceDefinition {
  MergeIndexes: MethodDefinition<_event_store_client_Empty, _event_store_client_Empty, _event_store_client_Empty__Output, _event_store_client_Empty__Output>
  ResignNode: MethodDefinition<_event_store_client_Empty, _event_store_client_Empty, _event_store_client_Empty__Output, _event_store_client_Empty__Output>
  RestartPersistentSubscriptions: MethodDefinition<_event_store_client_Empty, _event_store_client_Empty, _event_store_client_Empty__Output, _event_store_client_Empty__Output>
  SetNodePriority: MethodDefinition<_event_store_client_operations_SetNodePriorityReq, _event_store_client_Empty, _event_store_client_operations_SetNodePriorityReq__Output, _event_store_client_Empty__Output>
  Shutdown: MethodDefinition<_event_store_client_Empty, _event_store_client_Empty, _event_store_client_Empty__Output, _event_store_client_Empty__Output>
  StartScavenge: MethodDefinition<_event_store_client_operations_StartScavengeReq, _event_store_client_operations_ScavengeResp, _event_store_client_operations_StartScavengeReq__Output, _event_store_client_operations_ScavengeResp__Output>
  StopScavenge: MethodDefinition<_event_store_client_operations_StopScavengeReq, _event_store_client_operations_ScavengeResp, _event_store_client_operations_StopScavengeReq__Output, _event_store_client_operations_ScavengeResp__Output>
}
