// Original file: proto/Grpc/shared.proto

import type { StreamIdentifier as _event_store_client_StreamIdentifier, StreamIdentifier__Output as _event_store_client_StreamIdentifier__Output } from '../../event_store/client/StreamIdentifier';

export interface StreamDeleted {
  'stream_identifier'?: (_event_store_client_StreamIdentifier | null);
}

export interface StreamDeleted__Output {
  'stream_identifier': (_event_store_client_StreamIdentifier__Output | null);
}
