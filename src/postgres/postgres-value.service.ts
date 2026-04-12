import { Injectable } from '@nestjs/common';
import {
  ReadReq,
  ReadReq_Options_ReadDirection,
  AppendReq_ProposedMessage,
} from '../interfaces/streams';
import { Timestamp } from '../interfaces/google/protobuf/timestamp';
import { LongLike } from './postgres.types';

@Injectable()
export class PostgresValueService {
  toNumber(value: string | number | LongLike): number {
    if (typeof value === 'number') {
      return value;
    }

    if (typeof value === 'string') {
      return Number(value);
    }

    return Number(this.toBigInt(value));
  }

  decodeStreamName(streamName: Uint8Array): string {
    return Buffer.from(streamName).toString('utf8');
  }

  isBackwardsRead(
    readDirection: NonNullable<ReadReq['options']>['readDirection'] | string,
  ): boolean {
    return (
      readDirection === ReadReq_Options_ReadDirection.Backwards ||
      (typeof readDirection === 'string' &&
        (readDirection === 'Backwards' || readDirection === 'BACKWARDS'))
    );
  }

  getEventId(id: AppendReq_ProposedMessage['id']): string {
    if (!id?.string) {
      throw new Error('Only string UUID event identifiers are supported.');
    }

    return id.string;
  }

  createTimestamp(): Timestamp {
    const now = Date.now();

    return {
      seconds: Math.floor(now / 1000),
      nanos: (now % 1000) * 1_000_000,
    };
  }

  toTicksSinceUnixEpoch(value: Date | string): string {
    const date = typeof value === 'string' ? new Date(value) : value;
    return String(date.getTime() * 10_000);
  }

  private toBigInt(value: LongLike): bigint {
    const low = BigInt(value.low >>> 0);
    const high = BigInt(value.high >>> 0);
    const combined = (high << 32n) | low;

    if (value.unsigned) {
      return combined;
    }

    return value.high < 0 ? combined - (1n << 64n) : combined;
  }
}
