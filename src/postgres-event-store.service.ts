import { Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { Pool, PoolClient } from 'pg';
import {
  AppendReq,
  AppendReq_ProposedMessage,
  AppendResp,
  DeleteReq,
  DeleteResp,
  ReadReq,
  ReadResp,
} from './interfaces/streams';

type PersistedEventRow = {
  global_position: string | number;
  stream_name: string;
  stream_revision: string | number;
  event_id: string;
  metadata: Record<string, string> | null;
  custom_metadata: Buffer | Uint8Array | null;
  data: Buffer | Uint8Array | null;
};

type LongLike = {
  low: number;
  high: number;
  unsigned: boolean;
};

@Injectable()
export class PostgresEventStoreService
  implements OnModuleInit, OnModuleDestroy
{
  private readonly pool = new Pool(this.getPoolConfig());

  async onModuleInit(): Promise<void> {
    await this.pool.query(`
      CREATE TABLE IF NOT EXISTS stream_events (
        global_position BIGSERIAL PRIMARY KEY,
        stream_name TEXT NOT NULL,
        stream_revision BIGINT NOT NULL,
        event_id UUID NOT NULL UNIQUE,
        metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
        custom_metadata BYTEA NOT NULL DEFAULT '\\x',
        data BYTEA NOT NULL DEFAULT '\\x',
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE (stream_name, stream_revision)
      )
    `);

    await this.pool.query(`
      CREATE INDEX IF NOT EXISTS idx_stream_events_stream_revision
      ON stream_events (stream_name, stream_revision)
    `);
  }

  async onModuleDestroy(): Promise<void> {
    await this.pool.end();
  }

  async append(messages: AppendReq[]): Promise<AppendResp> {
    const options = messages.find((message) => message.options)?.options;
    const proposedMessages = messages
      .map((message) => message.proposedMessage)
      .filter((message) => message !== undefined);

    if (!options) {
      throw new Error('Append request must include options.');
    }

    if (!options.streamIdentifier?.streamName) {
      throw new Error('Append request is missing a stream identifier.');
    }

    const streamName = this.decodeStreamName(
      options.streamIdentifier.streamName,
    );

    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');

      const currentRevision = await this.getCurrentRevision(client, streamName);
      const mismatch = this.getExpectedVersionMismatch(options, currentRevision);
      if (mismatch) {
        await client.query('ROLLBACK');
        return mismatch;
      }

      let nextRevision = currentRevision ?? -1;
      let lastPosition = currentRevision === null ? null : 0;

      for (const proposedMessage of proposedMessages) {
        nextRevision += 1;

        const result = await client.query<{ global_position: string | number }>(
          `
            INSERT INTO stream_events (
              stream_name,
              stream_revision,
              event_id,
              metadata,
              custom_metadata,
              data
            )
            VALUES ($1, $2, $3::uuid, $4::jsonb, $5, $6)
            RETURNING global_position
          `,
          [
            streamName,
            nextRevision,
            this.getEventId(proposedMessage.id),
            JSON.stringify(proposedMessage.metadata ?? {}),
            Buffer.from(proposedMessage.customMetadata ?? new Uint8Array()),
            Buffer.from(proposedMessage.data ?? new Uint8Array()),
          ],
        );

        lastPosition = this.toNumber(result.rows[0].global_position);
      }

      await client.query('COMMIT');

      if (proposedMessages.length === 0) {
        return {
          success: {
            currentRevision: currentRevision ?? undefined,
            noStream: currentRevision === null ? {} : undefined,
            noPosition: {},
          },
        };
      }

      return {
        success: {
          currentRevision: nextRevision,
          position:
            lastPosition === null
              ? undefined
              : {
                  commitPosition: lastPosition,
                  preparePosition: lastPosition,
                },
        },
      };
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  async read(request: ReadReq): Promise<ReadResp[]> {
    const options = request.options;
    if (!options?.stream?.streamIdentifier?.streamName) {
      throw new Error('Only stream reads are currently supported.');
    }

    if (options.subscription) {
      throw new Error('Subscription reads are not supported.');
    }

    if (options.filter) {
      throw new Error('Filtered reads are not supported.');
    }

    const streamName = this.decodeStreamName(
      options.stream.streamIdentifier.streamName,
    );
    const exists = await this.streamExists(streamName);
    if (!exists) {
      return [
        {
          streamNotFound: {
            streamIdentifier: {
              streamName: Buffer.from(streamName),
            },
          },
        },
      ];
    }

    const limit = options.count !== undefined ? this.toNumber(options.count) : 100;
    const isBackwards = this.isBackwardsRead(options.readDirection);
    const order = isBackwards ? 'DESC' : 'ASC';
    const comparator = isBackwards ? '<=' : '>=';
    const boundary = this.resolveReadBoundary(options);

    const params: Array<number | string> = [streamName];
    let whereClause = 'WHERE stream_name = $1';
    if (boundary !== null) {
      params.push(boundary);
      whereClause += ` AND stream_revision ${comparator} $2`;
    }

    params.push(limit);

    const result = await this.pool.query<PersistedEventRow>(
      `
        SELECT
          global_position,
          stream_name,
          stream_revision,
          event_id,
          metadata,
          custom_metadata,
          data
        FROM stream_events
        ${whereClause}
        ORDER BY stream_revision ${order}
        LIMIT $${params.length}
      `,
      params,
    );

    return result.rows.map((row) => ({
      event: {
        event: {
          id: { string: row.event_id },
          streamIdentifier: {
            streamName: Buffer.from(row.stream_name),
          },
          streamRevision: this.toNumber(row.stream_revision),
          preparePosition: this.toNumber(row.global_position),
          commitPosition: this.toNumber(row.global_position),
          metadata: row.metadata ?? {},
          customMetadata: Buffer.from(row.custom_metadata ?? new Uint8Array()),
          data: Buffer.from(row.data ?? new Uint8Array()),
        },
        link: undefined,
        commitPosition: this.toNumber(row.global_position),
      },
    }));
  }

  async delete(request: DeleteReq): Promise<DeleteResp> {
    const options = request.options;
    if (!options?.streamIdentifier?.streamName) {
      throw new Error('Delete request is missing a stream identifier.');
    }

    const streamName = this.decodeStreamName(options.streamIdentifier.streamName);
    const client = await this.pool.connect();

    try {
      await client.query('BEGIN');

      const currentRevision = await this.getCurrentRevision(client, streamName);
      const mismatch = this.getDeleteExpectedVersionMismatch(
        options,
        currentRevision,
        streamName,
      );
      if (mismatch) {
        await client.query('ROLLBACK');
        throw mismatch;
      }

      if (currentRevision === null) {
        await client.query('COMMIT');
        return { noPosition: {} };
      }

      const deleted = await client.query<{ global_position: string | number }>(
        `
          DELETE FROM stream_events
          WHERE stream_name = $1
          RETURNING global_position
        `,
        [streamName],
      );

      await client.query('COMMIT');

      const lastPosition = deleted.rows.reduce(
        (max, row) => Math.max(max, this.toNumber(row.global_position)),
        0,
      );

      return {
        position: {
          commitPosition: lastPosition,
          preparePosition: lastPosition,
        },
      };
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  private getPoolConfig(): ConstructorParameters<typeof Pool>[0] {
    if (process.env.POSTGRES_URL) {
      return { connectionString: process.env.POSTGRES_URL };
    }

    return {
      host: process.env.POSTGRES_HOST ?? 'localhost',
      port: Number(process.env.POSTGRES_PORT ?? 5432),
      database: process.env.POSTGRES_DB ?? 'kurrentdb_adapter',
      user: process.env.POSTGRES_USER ?? 'postgres',
      password: process.env.POSTGRES_PASSWORD ?? 'postgres',
    };
  }

  private async getCurrentRevision(
    client: PoolClient,
    streamName: string,
  ): Promise<number | null> {
    const result = await client.query<{ stream_revision: string | number }>(
      `
        SELECT stream_revision
        FROM stream_events
        WHERE stream_name = $1
        ORDER BY stream_revision DESC
        LIMIT 1
      `,
      [streamName],
    );

    if (result.rows.length === 0) {
      return null;
    }

      return this.toNumber(result.rows[0].stream_revision);
  }

  private async streamExists(streamName: string): Promise<boolean> {
    const result = await this.pool.query<{ exists: boolean }>(
      `
        SELECT EXISTS(
          SELECT 1 FROM stream_events WHERE stream_name = $1
        ) AS exists
      `,
      [streamName],
    );

    return result.rows[0]?.exists ?? false;
  }

  private getExpectedVersionMismatch(
    options: NonNullable<AppendReq['options']>,
    currentRevision: number | null,
  ): AppendResp | null {
    const expectedRevision =
      options.revision === undefined ? undefined : this.toNumber(options.revision);
    const matches =
      options.any !== undefined ||
      (options.noStream !== undefined && currentRevision === null) ||
      (options.streamExists !== undefined && currentRevision !== null) ||
      (expectedRevision !== undefined && currentRevision === expectedRevision);

    if (matches) {
      return null;
    }

    return {
      wrongExpectedVersion: {
        currentRevision:
          currentRevision === null
            ? undefined
            : (String(currentRevision) as unknown as number),
        currentNoStream: currentRevision === null ? {} : undefined,
        expectedRevision:
          expectedRevision === undefined
            ? undefined
            : (String(expectedRevision) as unknown as number),
        expectedAny: options.any,
        expectedStreamExists: options.streamExists,
        expectedNoStream: options.noStream,
      },
    };
  }

  private getDeleteExpectedVersionMismatch(
    options: NonNullable<DeleteReq['options']>,
    currentRevision: number | null,
    streamName: string,
  ): Error | null {
    const expectedRevision =
      options.revision === undefined ? undefined : this.toNumber(options.revision);
    const matches =
      options.any !== undefined ||
      (options.noStream !== undefined && currentRevision === null) ||
      (options.streamExists !== undefined && currentRevision !== null) ||
      (expectedRevision !== undefined && currentRevision === expectedRevision);

    if (matches) {
      return null;
    }

    return this.createWrongExpectedVersionError(
      streamName,
      expectedRevision,
      currentRevision,
    );
  }

  private resolveReadBoundary(
    options: NonNullable<ReadReq['options']>,
  ): number | null {
    const stream = options.stream;
    if (!stream) {
      return null;
    }

    if (stream.revision !== undefined) {
      return this.toNumber(stream.revision);
    }

    if (stream.start !== undefined) {
      return this.isBackwardsRead(options.readDirection)
        ? Number.MAX_SAFE_INTEGER
        : 0;
    }

    if (stream.end !== undefined) {
      return this.isBackwardsRead(options.readDirection)
        ? Number.MAX_SAFE_INTEGER
        : null;
    }

    return null;
  }

  private decodeStreamName(streamName: Uint8Array): string {
    return Buffer.from(streamName).toString('utf8');
  }

  private createWrongExpectedVersionError(
    streamName: string,
    expectedRevision: number | undefined,
    currentRevision: number | null,
  ): Error {
    const error = new Error('Wrong expected version.') as Error & {
      code?: number;
      details?: string;
      metadata?: import('@grpc/grpc-js').Metadata;
    };
    const { Metadata, status } = require('@grpc/grpc-js') as typeof import('@grpc/grpc-js');
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
    error.code = status.UNKNOWN;
    error.details = 'Wrong expected version.';
    error.metadata = metadata;
    return error;
  }

  private isBackwardsRead(
    readDirection: NonNullable<ReadReq['options']>['readDirection'] | string,
  ): boolean {
    return (
      readDirection === 1 ||
      readDirection === 'Backwards' ||
      readDirection === 'BACKWARDS'
    );
  }

  private getEventId(id: AppendReq_ProposedMessage['id']): string {
    if (!id?.string) {
      throw new Error('Only string UUID event identifiers are supported.');
    }

    return id.string;
  }

  private toNumber(value: string | number | LongLike): number {
    if (typeof value === 'number') {
      return value;
    }

    if (typeof value === 'string') {
      return Number(value);
    }

    return Number(this.toBigInt(value));
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
