// Original file: proto/Grpc/shared.proto

import type { Empty as _google_protobuf_Empty, Empty__Output as _google_protobuf_Empty__Output } from '../../google/protobuf/Empty';
import type { Long } from '@grpc/proto-loader';

export interface WrongExpectedVersion {
  'current_stream_revision'?: (number | string | Long);
  'current_no_stream'?: (_google_protobuf_Empty | null);
  'expected_stream_position'?: (number | string | Long);
  'expected_any'?: (_google_protobuf_Empty | null);
  'expected_stream_exists'?: (_google_protobuf_Empty | null);
  'expected_no_stream'?: (_google_protobuf_Empty | null);
  'current_stream_revision_option'?: "current_stream_revision"|"current_no_stream";
  'expected_stream_position_option'?: "expected_stream_position"|"expected_any"|"expected_stream_exists"|"expected_no_stream";
}

export interface WrongExpectedVersion__Output {
  'current_stream_revision'?: (number);
  'current_no_stream'?: (_google_protobuf_Empty__Output | null);
  'expected_stream_position'?: (number);
  'expected_any'?: (_google_protobuf_Empty__Output | null);
  'expected_stream_exists'?: (_google_protobuf_Empty__Output | null);
  'expected_no_stream'?: (_google_protobuf_Empty__Output | null);
  'current_stream_revision_option'?: "current_stream_revision"|"current_no_stream";
  'expected_stream_position_option'?: "expected_stream_position"|"expected_any"|"expected_stream_exists"|"expected_no_stream";
}
