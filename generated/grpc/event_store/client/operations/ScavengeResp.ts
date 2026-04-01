// Original file: proto/Grpc/operations.proto


// Original file: proto/Grpc/operations.proto

export const _event_store_client_operations_ScavengeResp_ScavengeResult = {
  Started: 0,
  InProgress: 1,
  Stopped: 2,
} as const;

export type _event_store_client_operations_ScavengeResp_ScavengeResult =
  | 'Started'
  | 0
  | 'InProgress'
  | 1
  | 'Stopped'
  | 2

export type _event_store_client_operations_ScavengeResp_ScavengeResult__Output = typeof _event_store_client_operations_ScavengeResp_ScavengeResult[keyof typeof _event_store_client_operations_ScavengeResp_ScavengeResult]

export interface ScavengeResp {
  'scavenge_id'?: (string);
  'scavenge_result'?: (_event_store_client_operations_ScavengeResp_ScavengeResult);
}

export interface ScavengeResp__Output {
  'scavenge_id': (string);
  'scavenge_result': (_event_store_client_operations_ScavengeResp_ScavengeResult__Output);
}
