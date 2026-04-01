// Original file: proto/Grpc/shared.proto

import type { Long } from '@grpc/proto-loader';

export interface AllStreamPosition {
  'commit_position'?: (number | string | Long);
  'prepare_position'?: (number | string | Long);
}

export interface AllStreamPosition__Output {
  'commit_position': (number);
  'prepare_position': (number);
}
