import { Injectable } from '@nestjs/common';
import { Metadata, status } from '@grpc/grpc-js';
import {
  Empty as GrpcEmpty,
  StreamDeleted as GrpcStreamDeleted,
  StreamIdentifier as GrpcStreamIdentifier,
  WrongExpectedVersion as GrpcWrongExpectedVersion,
} from '@kurrent/kurrentdb-client/generated/kurrentdb/protocols/v1/shared_pb';
import { randomUUID } from 'node:crypto';
import {
  AppendResp,
  BatchAppendReq_Options,
  BatchAppendResp,
  ReadReq,
  ReadResp,
} from '../interfaces/streams';
import { Code } from '../interfaces/code';
import { Any } from '../interfaces/google/protobuf/any';
import { createInfoResponseBody } from '../stub-utils';
import { PersistedEventRow } from './postgres.types';
import {
  isBackwardsRead,
  toNumber,
  toTicksSinceUnixEpoch,
} from './postgres-value';

type EmptyMessage = {
  serializeBinary(): Uint8Array;
};

type WrongExpectedVersionMessage = {
  setCurrentStreamRevision(value: string): void;
  setCurrentNoStream(value: EmptyMessage): void;
  setExpectedStreamPosition(value: string): void;
  setExpectedAny(value: EmptyMessage): void;
  setExpectedStreamExists(value: EmptyMessage): void;
  setExpectedNoStream(value: EmptyMessage): void;
  serializeBinary(): Uint8Array;
};

type StreamIdentifierMessage = {
  setStreamName(value: Uint8Array | string): void;
};

type StreamDeletedMessage = {
  setStreamIdentifier(value: StreamIdentifierMessage): void;
  serializeBinary(): Uint8Array;
};

type EmptyMessageConstructor = new () => EmptyMessage;
type WrongExpectedVersionMessageConstructor =
  new () => WrongExpectedVersionMessage;
type StreamIdentifierMessageConstructor = new () => StreamIdentifierMessage;
type StreamDeletedMessageConstructor = new () => StreamDeletedMessage;

@Injectable()
export class PostgresProtocolService {
  mapRowToReadResponse(row: PersistedEventRow): ReadResp {
    const grpcMetadata = {
      type: row.metadata?.type ?? '<no-event-type-provided>',
      created: toTicksSinceUnixEpoch(row.created_at),
      'content-type':
        row.metadata?.['content-type'] ?? 'application/octet-stream',
    };

    return {
      event: {
        event: {
          id: { string: row.event_id },
          streamIdentifier: {
            streamName: Buffer.from(row.stream_name),
          },
          streamRevision: toNumber(row.stream_revision),
          preparePosition: toNumber(row.global_position),
          commitPosition: toNumber(row.global_position),
          metadata: grpcMetadata,
          customMetadata: Buffer.from(row.custom_metadata ?? new Uint8Array()),
          data: Buffer.from(row.data ?? new Uint8Array()),
        },
        link: undefined,
        commitPosition: toNumber(row.global_position),
      },
    };
  }

  createSyntheticReadResponse(streamName: string, eventType: string): ReadResp {
    return {
      event: {
        event: {
          id: { string: randomUUID() },
          streamIdentifier: {
            streamName: Buffer.from(streamName),
          },
          streamRevision: 0,
          preparePosition: 0,
          commitPosition: 0,
          metadata: {
            type: eventType,
            created: String(Date.now() * 10_000),
            'content-type': 'application/json',
          },
          customMetadata: Buffer.alloc(0),
          data: Buffer.from(createInfoResponseBody(), 'utf8'),
        },
        link: undefined,
        commitPosition: 0,
      },
    };
  }

  createExistingEmptyStreamReadResponses(
    options: NonNullable<ReadReq['options']>,
  ): ReadResp[] {
    if (isBackwardsRead(options.readDirection)) {
      return [{ lastStreamPosition: 0 }];
    }

    return [{ firstStreamPosition: 0 }];
  }

  createWrongExpectedVersionError(
    streamName: string,
    expectedRevision: number | undefined,
    currentRevision: number | null,
  ): Error {
    const error = new Error('Wrong expected version.') as Error & {
      code?: number;
      details?: string;
      metadata?: Metadata;
      name?: string;
    };
    const metadata = new Metadata();
    metadata.set('exception', 'wrong-expected-version');
    metadata.set('stream-name', streamName);
    metadata.set(
      'expected-version',
      expectedRevision === undefined ? '-2' : expectedRevision.toString(),
    );
    if (currentRevision !== null) {
      metadata.set('actual-version', currentRevision.toString());
    }
    error.name = 'WrongExpectedVersionError';
    error.code = status.UNKNOWN;
    error.details = 'Wrong expected version.';
    error.metadata = metadata;
    return error;
  }

  createBatchAppendWrongExpectedVersionStatus(
    options: BatchAppendReq_Options,
    wrongExpectedVersion: NonNullable<AppendResp['wrongExpectedVersion']>,
  ): BatchAppendResp['error'] {
    const WrongExpectedVersion =
      GrpcWrongExpectedVersion as unknown as WrongExpectedVersionMessageConstructor;
    const Empty = GrpcEmpty as unknown as EmptyMessageConstructor;
    const details = new WrongExpectedVersion();

    if (wrongExpectedVersion.currentRevision !== undefined) {
      details.setCurrentStreamRevision(
        String(wrongExpectedVersion.currentRevision),
      );
    } else {
      details.setCurrentNoStream(new Empty());
    }

    if (options.streamPosition !== undefined) {
      details.setExpectedStreamPosition(String(options.streamPosition));
    } else if (options.any) {
      details.setExpectedAny(new Empty());
    } else if (options.streamExists) {
      details.setExpectedStreamExists(new Empty());
    } else {
      details.setExpectedNoStream(new Empty());
    }

    return {
      code: Code.UNKNOWN,
      message: 'Wrong expected version.',
      details: this.createStatusDetails(
        'event_store.client.WrongExpectedVersion',
        details.serializeBinary(),
      ),
    };
  }

  createBatchAppendStreamDeletedStatus(
    streamName: Uint8Array,
  ): BatchAppendResp['error'] {
    const StreamDeleted =
      GrpcStreamDeleted as unknown as StreamDeletedMessageConstructor;
    const StreamIdentifier =
      GrpcStreamIdentifier as unknown as StreamIdentifierMessageConstructor;
    const details = new StreamDeleted();
    const streamIdentifier = new StreamIdentifier();

    streamIdentifier.setStreamName(streamName);
    details.setStreamIdentifier(streamIdentifier);

    return {
      code: Code.UNKNOWN,
      message: 'Stream deleted.',
      details: this.createStatusDetails(
        'event_store.client.StreamDeleted',
        details.serializeBinary(),
      ),
    };
  }

  private createStatusDetails(
    typeName: string,
    value: Uint8Array,
  ): Any & { type_url: string } {
    const typeUrl = `type.googleapis.com/${typeName}`;

    return {
      typeUrl,
      type_url: typeUrl,
      value: Buffer.from(value),
    };
  }
}
