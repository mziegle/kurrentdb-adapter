// Original file: proto/Grpc/shared.proto

import type { Long } from '@grpc/proto-loader';

export interface _event_store_client_UUID_Structured {
  'most_significant_bits'?: (number | string | Long);
  'least_significant_bits'?: (number | string | Long);
}

export interface _event_store_client_UUID_Structured__Output {
  'most_significant_bits': (number);
  'least_significant_bits': (number);
}

export interface UUID {
  'structured'?: (_event_store_client_UUID_Structured | null);
  'string'?: (string);
  'value'?: "structured"|"string";
}

export interface UUID__Output {
  'structured'?: (_event_store_client_UUID_Structured__Output | null);
  'string'?: (string);
  'value'?: "structured"|"string";
}
