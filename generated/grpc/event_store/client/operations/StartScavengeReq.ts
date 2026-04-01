// Original file: proto/Grpc/operations.proto


export interface _event_store_client_operations_StartScavengeReq_Options {
  'thread_count'?: (number);
  'start_from_chunk'?: (number);
}

export interface _event_store_client_operations_StartScavengeReq_Options__Output {
  'thread_count': (number);
  'start_from_chunk': (number);
}

export interface StartScavengeReq {
  'options'?: (_event_store_client_operations_StartScavengeReq_Options | null);
}

export interface StartScavengeReq__Output {
  'options': (_event_store_client_operations_StartScavengeReq_Options__Output | null);
}
