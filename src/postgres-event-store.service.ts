import { Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { Metadata, status } from '@grpc/grpc-js';
import {
  Empty as GrpcEmpty,
  StreamDeleted as GrpcStreamDeleted,
  StreamIdentifier as GrpcStreamIdentifier,
  WrongExpectedVersion as GrpcWrongExpectedVersion,
} from '@kurrent/kurrentdb-client/generated/kurrentdb/protocols/v1/shared_pb';
import { randomUUID } from 'node:crypto';
import { Pool, PoolClient } from 'pg';
import {
  AppendReq,
  AppendReq_ProposedMessage,
  AppendResp,
  BatchAppendReq,
  BatchAppendReq_Options,
  BatchAppendResp,
  DeleteReq,
  DeleteResp,
  ReadReq,
  ReadReq_Options_FilterOptions,
  ReadReq_Options_FilterOptions_Expression,
  ReadReq_Options_ReadDirection,
  ReadResp,
  TombstoneReq,
  TombstoneResp,
} from './interfaces/streams';
import { Code } from './interfaces/code';
import { Any } from './interfaces/google/protobuf/any';
import { Timestamp } from './interfaces/google/protobuf/timestamp';

type PersistedEventRow = {
  global_position: string | number;
  stream_name: string;
  stream_revision: string | number;
  event_id: string;
  metadata: Record<string, string> | null;
  custom_metadata: Buffer | Uint8Array | null;
  data: Buffer | Uint8Array | null;
};

type StreamRetentionPolicy = {
  currentRevision: number | null;
  maxAge: number | null;
  maxCount: number | null;
  truncateBefore: number | null;
};

type LongLike = {
  low: number;
  high: number;
  unsigned: boolean;
};

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

export class StreamDeletedServiceError extends Error {
  constructor(readonly streamName: string) {
    super(`Stream "${streamName}" is deleted.`);
  }
}

@Injectable()
export class PostgresEventStoreService
  implements OnModuleInit, OnModuleDestroy
{
  private static readonly ALL_STREAM_KEY = '$all';
  private readonly pool = new Pool(this.getPoolConfig());
  private readonly streamVersions = new Map<string, number>();
  private readonly streamListeners = new Map<string, Set<() => void>>();

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

    await this.pool.query(`
      CREATE TABLE IF NOT EXISTS tombstoned_streams (
        stream_name TEXT PRIMARY KEY,
        tombstoned_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        last_position BIGINT NOT NULL
      )
    `);

    await this.pool.query(`
      CREATE TABLE IF NOT EXISTS stream_retention_policies (
        stream_name TEXT PRIMARY KEY,
        current_revision BIGINT NULL,
        max_age BIGINT NULL,
        max_count BIGINT NULL,
        truncate_before BIGINT NULL
      )
    `);

    await this.pool.query(`
      ALTER TABLE stream_retention_policies
      ADD COLUMN IF NOT EXISTS max_age BIGINT NULL
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
    const metadataPolicyUpdate = this.parseMetadataPolicyUpdate(
      streamName,
      proposedMessages,
    );

    await this.ensureStreamIsNotTombstoned(streamName);

    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');

      const currentRevision = await this.getCurrentRevision(client, streamName);
      const mismatch = this.getExpectedVersionMismatch(
        options,
        currentRevision,
      );
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

      if (proposedMessages.length > 0 && !this.isMetastream(streamName)) {
        await this.upsertStreamCurrentRevision(
          client,
          streamName,
          nextRevision,
        );
      }

      if (metadataPolicyUpdate) {
        await this.upsertStreamRetentionPolicy(
          client,
          metadataPolicyUpdate.streamName,
          metadataPolicyUpdate.policy,
        );
      }

      await client.query('COMMIT');

      if (proposedMessages.length > 0) {
        this.notifyStreamUpdated(streamName);
      }

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

  async batchAppend(requests: BatchAppendReq[]): Promise<BatchAppendResp> {
    const firstRequest = requests[0];
    const options = requests.find((request) => request.options)?.options;

    if (!firstRequest?.correlationId) {
      throw new Error('Batch append request must include a correlation id.');
    }

    if (!options?.streamIdentifier?.streamName) {
      throw new Error('Batch append request must include stream options.');
    }

    const appendMessages: AppendReq[] = [
      {
        options: {
          streamIdentifier: options.streamIdentifier,
          revision: options.streamPosition,
          noStream: options.noStream ? {} : undefined,
          any: options.any ? {} : undefined,
          streamExists: options.streamExists ? {} : undefined,
        },
      },
      ...requests.flatMap((request) =>
        (request.proposedMessages ?? []).map((message) => ({
          proposedMessage: this.mapBatchProposedMessage(message),
        })),
      ),
    ];

    let appendResponse: AppendResp;

    try {
      appendResponse = await this.append(appendMessages);
    } catch (error) {
      if (error instanceof StreamDeletedServiceError) {
        return {
          correlationId: firstRequest.correlationId,
          streamIdentifier: options.streamIdentifier,
          streamPosition: options.streamPosition,
          noStream: options.noStream ? {} : undefined,
          any: options.any ? {} : undefined,
          streamExists: options.streamExists ? {} : undefined,
          error: this.createBatchAppendStreamDeletedStatus(
            options.streamIdentifier.streamName,
          ),
        };
      }

      throw error;
    }

    if (appendResponse.success) {
      const currentRevision =
        appendResponse.success.currentRevision ??
        (appendResponse.success.noStream ? -1 : undefined);

      return {
        correlationId: firstRequest.correlationId,
        streamIdentifier: options.streamIdentifier,
        streamPosition: options.streamPosition,
        noStream: options.noStream ? {} : undefined,
        any: options.any ? {} : undefined,
        streamExists: options.streamExists ? {} : undefined,
        success: {
          currentRevision,
          position: appendResponse.success.position
            ? {
                commitPosition: appendResponse.success.position.commitPosition,
                preparePosition:
                  appendResponse.success.position.preparePosition,
              }
            : undefined,
        },
      };
    }

    return {
      correlationId: firstRequest.correlationId,
      streamIdentifier: options.streamIdentifier,
      streamPosition: options.streamPosition,
      noStream: options.noStream ? {} : undefined,
      any: options.any ? {} : undefined,
      streamExists: options.streamExists ? {} : undefined,
      error: this.createBatchAppendWrongExpectedVersionStatus(
        options,
        appendResponse.wrongExpectedVersion!,
      ),
    };
  }

  async read(request: ReadReq): Promise<ReadResp[]> {
    const options = request.options;
    if (!options) {
      throw new Error('Read request must include options.');
    }

    if (options.stream?.streamIdentifier?.streamName) {
      if (options.filter) {
        throw new Error('Filtered reads are only supported on $all.');
      }

      const streamName = this.decodeStreamName(
        options.stream.streamIdentifier.streamName,
      );
      await this.ensureStreamIsNotTombstoned(streamName);
      return this.readStreamSnapshot(streamName, options);
    }

    if (options.all) {
      return this.readAllSnapshot(options);
    }

    throw new Error('Only stream and $all reads are currently supported.');
  }

  async *subscribeToStream(
    request: ReadReq,
    isCancelled: () => boolean,
  ): AsyncGenerator<ReadResp> {
    const options = request.options;
    if (!options) {
      throw new Error('Read request must include options.');
    }

    if (!options.subscription) {
      throw new Error('Subscription reads must include subscription options.');
    }

    if (this.isBackwardsRead(options.readDirection)) {
      throw new Error('Backwards subscriptions are not supported.');
    }

    yield {
      confirmation: {
        subscriptionId: randomUUID(),
      },
    };

    if (options.stream?.streamIdentifier?.streamName) {
      if (options.filter) {
        throw new Error('Filtered reads are only supported on $all.');
      }

      const streamName = this.decodeStreamName(
        options.stream.streamIdentifier.streamName,
      );
      await this.ensureStreamIsNotTombstoned(streamName);
      yield* this.subscribeToSingleStream(streamName, options, isCancelled);
      return;
    }

    if (options.all) {
      yield* this.subscribeToAll(options, isCancelled);
      return;
    }

    throw new Error(
      'Only stream and $all subscriptions are currently supported.',
    );
  }

  private async *subscribeToSingleStream(
    streamName: string,
    options: NonNullable<ReadReq['options']>,
    isCancelled: () => boolean,
  ): AsyncGenerator<ReadResp> {
    let nextRevisionExclusive = await this.resolveStreamSubscriptionBoundary(
      streamName,
      options,
    );
    let caughtUp = false;

    while (!isCancelled()) {
      const versionBeforeRead = this.getStreamVersion(streamName);
      const rows = await this.readStreamSubscriptionRows(
        streamName,
        nextRevisionExclusive,
      );

      if (rows.length > 0) {
        for (const row of rows) {
          if (isCancelled()) {
            return;
          }

          nextRevisionExclusive = this.toNumber(row.stream_revision);
          yield this.mapRowToReadResponse(row);
        }

        continue;
      }

      if (!caughtUp) {
        const caughtUpRevision =
          (await this.getCurrentRevision(this.pool, streamName)) ?? -1;
        nextRevisionExclusive = Math.max(
          nextRevisionExclusive,
          caughtUpRevision,
        );

        yield {
          caughtUp: {
            timestamp: this.createTimestamp(),
            streamRevision:
              caughtUpRevision >= 0 ? caughtUpRevision : undefined,
          },
        };
        caughtUp = true;
      }

      if (versionBeforeRead !== this.getStreamVersion(streamName)) {
        continue;
      }

      await this.waitForStreamUpdate(
        streamName,
        versionBeforeRead,
        isCancelled,
      );
    }
  }

  private async *subscribeToAll(
    options: NonNullable<ReadReq['options']>,
    isCancelled: () => boolean,
  ): AsyncGenerator<ReadResp> {
    let nextPositionExclusive =
      await this.resolveAllSubscriptionBoundary(options);
    let caughtUp = false;

    while (!isCancelled()) {
      const versionBeforeRead = this.getStreamVersion(
        PostgresEventStoreService.ALL_STREAM_KEY,
      );
      const rows = await this.readAllSubscriptionRows(
        nextPositionExclusive,
        options.filter,
      );

      if (rows.length > 0) {
        for (const row of rows) {
          if (isCancelled()) {
            return;
          }

          nextPositionExclusive = this.toNumber(row.global_position);
          yield this.mapRowToReadResponse(row);
        }

        continue;
      }

      if (!caughtUp) {
        const caughtUpPosition = await this.getCurrentGlobalPosition();
        nextPositionExclusive = Math.max(
          nextPositionExclusive,
          caughtUpPosition ?? -1,
        );

        yield {
          caughtUp: {
            timestamp: this.createTimestamp(),
            position:
              caughtUpPosition !== null && caughtUpPosition >= 0
                ? {
                    commitPosition: caughtUpPosition,
                    preparePosition: caughtUpPosition,
                  }
                : undefined,
          },
        };
        caughtUp = true;
      }

      if (
        versionBeforeRead !==
        this.getStreamVersion(PostgresEventStoreService.ALL_STREAM_KEY)
      ) {
        continue;
      }

      await this.waitForStreamUpdate(
        PostgresEventStoreService.ALL_STREAM_KEY,
        versionBeforeRead,
        isCancelled,
      );
    }
  }

  async delete(request: DeleteReq): Promise<DeleteResp> {
    const options = request.options;
    if (!options?.streamIdentifier?.streamName) {
      throw new Error('Delete request is missing a stream identifier.');
    }

    const streamName = this.decodeStreamName(
      options.streamIdentifier.streamName,
    );
    await this.ensureStreamIsNotTombstoned(streamName);
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

      await client.query(
        `
          UPDATE stream_retention_policies
          SET current_revision = NULL
          WHERE stream_name = $1
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

  async tombstone(request: TombstoneReq): Promise<TombstoneResp> {
    const options = request.options;
    if (!options?.streamIdentifier?.streamName) {
      throw new Error('Tombstone request is missing a stream identifier.');
    }

    const streamName = this.decodeStreamName(
      options.streamIdentifier.streamName,
    );
    const client = await this.pool.connect();

    try {
      await client.query('BEGIN');

      const alreadyTombstoned = await this.isStreamTombstoned(
        streamName,
        client,
      );
      if (alreadyTombstoned) {
        await client.query('ROLLBACK');
        throw new StreamDeletedServiceError(streamName);
      }

      const currentRevision = await this.getCurrentRevision(client, streamName);
      const mismatch = this.getTombstoneExpectedVersionMismatch(
        options,
        currentRevision,
        streamName,
      );
      if (mismatch) {
        await client.query('ROLLBACK');
        throw mismatch;
      }

      const deleted = await client.query<{ global_position: string | number }>(
        `
          DELETE FROM stream_events
          WHERE stream_name = $1
          RETURNING global_position
        `,
        [streamName],
      );

      const lastPosition = deleted.rows.reduce(
        (max, row) => Math.max(max, this.toNumber(row.global_position)),
        0,
      );

      await client.query(
        `
          INSERT INTO tombstoned_streams (stream_name, last_position)
          VALUES ($1, $2)
          ON CONFLICT (stream_name)
          DO UPDATE SET last_position = EXCLUDED.last_position
        `,
        [streamName, lastPosition],
      );

      await client.query(
        `
          UPDATE stream_retention_policies
          SET current_revision = NULL
          WHERE stream_name = $1
        `,
        [streamName],
      );

      await client.query('COMMIT');

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
    client: PoolClient | Pool,
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

  private async readStreamSnapshot(
    streamName: string,
    options: NonNullable<ReadReq['options']>,
  ): Promise<ReadResp[]> {
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

    const limit =
      options.count !== undefined ? this.toNumber(options.count) : 100;
    const isBackwards = this.isBackwardsRead(options.readDirection);
    const order = isBackwards ? 'DESC' : 'ASC';
    const comparator = isBackwards ? '<=' : '>=';
    const boundary = this.resolveReadBoundary(options);

    const params: Array<number | string> = [streamName];
    let whereClause = 'WHERE events.stream_name = $1';
    if (boundary !== null) {
      params.push(boundary);
      whereClause += ` AND events.stream_revision ${comparator} $2`;
    }

    params.push(limit);

    const result = await this.pool.query<PersistedEventRow>(
      `
        SELECT
          events.global_position,
          events.stream_name,
          events.stream_revision,
          events.event_id,
          events.metadata,
          events.custom_metadata,
          events.data
        FROM stream_events events
        LEFT JOIN stream_retention_policies retention
          ON retention.stream_name = events.stream_name
        ${whereClause}
          ${whereClause ? 'AND' : 'WHERE'}
          ${this.buildRetentionVisibilityClause('events', 'retention')}
        ORDER BY events.stream_revision ${order}
        LIMIT $${params.length}
      `,
      params,
    );

    return result.rows.map((row) => this.mapRowToReadResponse(row));
  }

  private async readAllSnapshot(
    options: NonNullable<ReadReq['options']>,
  ): Promise<ReadResp[]> {
    const limit =
      options.count !== undefined ? this.toNumber(options.count) : 100;
    const isBackwards = this.isBackwardsRead(options.readDirection);
    const order = isBackwards ? 'DESC' : 'ASC';
    const comparator = isBackwards ? '<=' : '>=';
    const boundary = this.resolveAllReadBoundary(options);

    const params: Array<number | string> = [];
    const whereClauses: string[] = [];
    if (boundary !== null) {
      params.push(boundary);
      whereClauses.push(
        `events.global_position ${comparator} $${params.length}`,
      );
    }

    if (options.filter) {
      const filterClause = this.buildReadFilterClause(
        options.filter,
        params,
        'events',
      );
      whereClauses.push(filterClause.sql);
    }

    params.push(limit);
    const whereClause =
      whereClauses.length > 0 ? `WHERE ${whereClauses.join(' AND ')}` : '';

    const result = await this.pool.query<PersistedEventRow>(
      `
        SELECT
          events.global_position,
          events.stream_name,
          events.stream_revision,
          events.event_id,
          events.metadata,
          events.custom_metadata,
          events.data
        FROM stream_events events
        LEFT JOIN stream_retention_policies retention
          ON retention.stream_name = events.stream_name
        ${whereClause}
          ${whereClause ? 'AND' : 'WHERE'}
          ${this.buildRetentionVisibilityClause('events', 'retention')}
        ORDER BY events.global_position ${order}
        LIMIT $${params.length}
      `,
      params,
    );

    return result.rows.map((row) => this.mapRowToReadResponse(row));
  }

  private async resolveStreamSubscriptionBoundary(
    streamName: string,
    options: NonNullable<ReadReq['options']>,
  ): Promise<number> {
    const stream = options.stream;
    if (!stream) {
      return -1;
    }

    if (stream.revision !== undefined) {
      return this.toNumber(stream.revision) - 1;
    }

    if (stream.end !== undefined) {
      return (await this.getCurrentRevision(this.pool, streamName)) ?? -1;
    }

    return -1;
  }

  private async resolveAllSubscriptionBoundary(
    options: NonNullable<ReadReq['options']>,
  ): Promise<number> {
    const all = options.all;
    if (!all) {
      return -1;
    }

    if (all.position) {
      return this.toNumber(all.position.commitPosition);
    }

    if (all.end !== undefined) {
      return (await this.getCurrentGlobalPosition()) ?? -1;
    }

    return -1;
  }

  private async readStreamSubscriptionRows(
    streamName: string,
    nextRevisionExclusive: number,
  ): Promise<PersistedEventRow[]> {
    const result = await this.pool.query<PersistedEventRow>(
      `
        SELECT
          events.global_position,
          events.stream_name,
          events.stream_revision,
          events.event_id,
          events.metadata,
          events.custom_metadata,
          events.data
        FROM stream_events events
        LEFT JOIN stream_retention_policies retention
          ON retention.stream_name = events.stream_name
        WHERE events.stream_name = $1
          AND events.stream_revision > $2
          AND ${this.buildRetentionVisibilityClause('events', 'retention')}
        ORDER BY events.stream_revision ASC
        LIMIT 100
      `,
      [streamName, nextRevisionExclusive],
    );

    return result.rows;
  }

  private async readAllSubscriptionRows(
    nextPositionExclusive: number,
    filter?: ReadReq_Options_FilterOptions,
  ): Promise<PersistedEventRow[]> {
    const params: Array<number | string> = [nextPositionExclusive];
    const whereClauses = [`events.global_position > $1`];

    if (filter) {
      const filterClause = this.buildReadFilterClause(filter, params, 'events');
      whereClauses.push(filterClause.sql);
    }

    const result = await this.pool.query<PersistedEventRow>(
      `
        SELECT
          events.global_position,
          events.stream_name,
          events.stream_revision,
          events.event_id,
          events.metadata,
          events.custom_metadata,
          events.data
        FROM stream_events events
        LEFT JOIN stream_retention_policies retention
          ON retention.stream_name = events.stream_name
        WHERE ${whereClauses.join(' AND ')}
          AND ${this.buildRetentionVisibilityClause('events', 'retention')}
        ORDER BY events.global_position ASC
        LIMIT 100
      `,
      params,
    );

    return result.rows;
  }

  private buildReadFilterClause(
    filter: ReadReq_Options_FilterOptions,
    params: Array<number | string>,
    eventsAlias: string,
  ): { sql: string } {
    const { columnSql, expression } = this.resolveReadFilterTarget(
      filter,
      eventsAlias,
    );
    const prefixes = expression.prefix.filter((prefix) => prefix.length > 0);

    if (prefixes.length > 0) {
      const likePredicates = prefixes.map((prefix) => {
        params.push(`${prefix}%`);
        return `${columnSql} LIKE $${params.length}`;
      });

      return {
        sql: `(${likePredicates.join(' OR ')})`,
      };
    }

    if (expression.regex.length > 0) {
      params.push(expression.regex);
      return {
        sql: `${columnSql} ~ $${params.length}`,
      };
    }

    throw new Error(
      'Read filters must include a stream name or event type regex/prefix.',
    );
  }

  private resolveReadFilterTarget(
    filter: ReadReq_Options_FilterOptions,
    eventsAlias: string,
  ): {
    columnSql: string;
    expression: ReadReq_Options_FilterOptions_Expression;
  } {
    if (filter.streamIdentifier) {
      return {
        columnSql: `${eventsAlias}.stream_name`,
        expression: filter.streamIdentifier,
      };
    }

    if (filter.eventType) {
      return {
        columnSql: `${eventsAlias}.metadata ->> 'type'`,
        expression: filter.eventType,
      };
    }

    throw new Error(
      'Read filters must target a stream identifier or event type.',
    );
  }

  private mapRowToReadResponse(row: PersistedEventRow): ReadResp {
    return {
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
    };
  }

  private createTimestamp(): Timestamp {
    const now = Date.now();

    return {
      seconds: Math.floor(now / 1000),
      nanos: (now % 1000) * 1_000_000,
    };
  }

  private getStreamVersion(streamName: string): number {
    return this.streamVersions.get(streamName) ?? 0;
  }

  private notifyStreamUpdated(streamName: string): void {
    this.bumpStreamVersion(streamName);
    this.bumpStreamVersion(PostgresEventStoreService.ALL_STREAM_KEY);
  }

  private bumpStreamVersion(streamName: string): void {
    const nextVersion = this.getStreamVersion(streamName) + 1;
    this.streamVersions.set(streamName, nextVersion);

    const listeners = this.streamListeners.get(streamName);
    if (!listeners) {
      return;
    }

    this.streamListeners.delete(streamName);
    for (const listener of listeners) {
      listener();
    }
  }

  private waitForStreamUpdate(
    streamName: string,
    observedVersion: number,
    isCancelled: () => boolean,
  ): Promise<void> {
    if (
      isCancelled() ||
      this.getStreamVersion(streamName) !== observedVersion
    ) {
      return Promise.resolve();
    }

    return new Promise((resolve) => {
      const listeners =
        this.streamListeners.get(streamName) ?? new Set<() => void>();
      let settled = false;

      const finish = () => {
        if (settled) {
          return;
        }

        settled = true;
        clearInterval(interval);

        listeners.delete(finish);
        if (listeners.size === 0) {
          this.streamListeners.delete(streamName);
        }

        resolve();
      };

      listeners.add(finish);
      this.streamListeners.set(streamName, listeners);

      const interval = setInterval(() => {
        if (
          isCancelled() ||
          this.getStreamVersion(streamName) !== observedVersion
        ) {
          finish();
        }
      }, 100);
    });
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

  private async getCurrentGlobalPosition(): Promise<number | null> {
    const result = await this.pool.query<{ global_position: string | number }>(
      `
        SELECT global_position
        FROM stream_events
        ORDER BY global_position DESC
        LIMIT 1
      `,
    );

    if (result.rows.length === 0) {
      return null;
    }

    return this.toNumber(result.rows[0].global_position);
  }

  private async ensureStreamIsNotTombstoned(streamName: string): Promise<void> {
    if (await this.isStreamTombstoned(streamName)) {
      throw new StreamDeletedServiceError(streamName);
    }
  }

  private async upsertStreamCurrentRevision(
    client: PoolClient,
    streamName: string,
    currentRevision: number,
  ): Promise<void> {
    await client.query(
      `
        INSERT INTO stream_retention_policies (stream_name, current_revision)
        VALUES ($1, $2)
        ON CONFLICT (stream_name)
        DO UPDATE SET current_revision = EXCLUDED.current_revision
      `,
      [streamName, currentRevision],
    );
  }

  private async upsertStreamRetentionPolicy(
    client: PoolClient,
    streamName: string,
    policy: Omit<StreamRetentionPolicy, 'currentRevision'>,
  ): Promise<void> {
    await client.query(
      `
        INSERT INTO stream_retention_policies (
          stream_name,
          current_revision,
          max_age,
          max_count,
          truncate_before
        )
        VALUES ($1, NULL, $2, $3, $4)
        ON CONFLICT (stream_name)
        DO UPDATE SET
          max_age = EXCLUDED.max_age,
          max_count = EXCLUDED.max_count,
          truncate_before = EXCLUDED.truncate_before
      `,
      [streamName, policy.maxAge, policy.maxCount, policy.truncateBefore],
    );
  }

  private parseMetadataPolicyUpdate(
    streamName: string,
    proposedMessages: AppendReq_ProposedMessage[],
  ): {
    streamName: string;
    policy: Omit<StreamRetentionPolicy, 'currentRevision'>;
  } | null {
    if (!this.isMetastream(streamName)) {
      return null;
    }

    let policy: Omit<StreamRetentionPolicy, 'currentRevision'> | null = null;
    for (const message of proposedMessages) {
      const metadata = message.metadata ?? {};
      if (
        metadata.type !== '$metadata' ||
        metadata['content-type'] !== 'application/json'
      ) {
        continue;
      }

      const payload = this.parseJsonPayload(message.data);
      policy = {
        maxAge: this.readMetadataInteger(payload, '$maxAge'),
        maxCount: this.readMetadataInteger(payload, '$maxCount'),
        truncateBefore: this.readMetadataInteger(payload, '$tb'),
      };
    }

    if (!policy) {
      return null;
    }

    return {
      streamName: streamName.slice(2),
      policy,
    };
  }

  private parseJsonPayload(
    data: Uint8Array | Buffer | undefined,
  ): Record<string, unknown> {
    if (!data || data.length === 0) {
      return {};
    }

    const parsed: unknown = JSON.parse(Buffer.from(data).toString('utf8'));
    if (
      typeof parsed !== 'object' ||
      parsed === null ||
      Array.isArray(parsed)
    ) {
      throw new Error('Stream metadata payload must be a JSON object.');
    }

    return parsed as Record<string, unknown>;
  }

  private readMetadataInteger(
    payload: Record<string, unknown>,
    key: string,
  ): number | null {
    const value = payload[key];
    if (value === undefined || value === null) {
      return null;
    }

    if (!Number.isInteger(value)) {
      throw new Error(`Stream metadata field "${key}" must be an integer.`);
    }

    return value as number;
  }

  private buildRetentionVisibilityClause(
    eventsAlias: string,
    retentionAlias: string,
  ): string {
    return `(
      (${retentionAlias}.max_age IS NULL OR ${eventsAlias}.created_at >= NOW() - (${retentionAlias}.max_age * INTERVAL '1 second'))
      AND
      (${retentionAlias}.truncate_before IS NULL OR ${eventsAlias}.stream_revision >= ${retentionAlias}.truncate_before)
      AND
      (
        ${retentionAlias}.max_count IS NULL
        OR ${retentionAlias}.current_revision IS NULL
        OR ${eventsAlias}.stream_revision > ${retentionAlias}.current_revision - ${retentionAlias}.max_count
      )
    )`;
  }

  private async isStreamTombstoned(
    streamName: string,
    client: PoolClient | Pool = this.pool,
  ): Promise<boolean> {
    const result = await client.query<{ exists: boolean }>(
      `
        SELECT EXISTS(
          SELECT 1 FROM tombstoned_streams WHERE stream_name = $1
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
      options.revision === undefined
        ? undefined
        : this.toNumber(options.revision);
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
      options.revision === undefined
        ? undefined
        : this.toNumber(options.revision);
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

  private getTombstoneExpectedVersionMismatch(
    options: NonNullable<TombstoneReq['options']>,
    currentRevision: number | null,
    streamName: string,
  ): Error | null {
    const expectedRevision =
      options.revision === undefined
        ? undefined
        : this.toNumber(options.revision);
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

  private resolveAllReadBoundary(
    options: NonNullable<ReadReq['options']>,
  ): number | null {
    const all = options.all;
    if (!all) {
      return null;
    }

    if (all.position !== undefined) {
      return this.toNumber(all.position.commitPosition);
    }

    if (all.start !== undefined) {
      return this.isBackwardsRead(options.readDirection)
        ? Number.MAX_SAFE_INTEGER
        : 0;
    }

    if (all.end !== undefined) {
      return this.isBackwardsRead(options.readDirection)
        ? Number.MAX_SAFE_INTEGER
        : null;
    }

    return null;
  }

  private decodeStreamName(streamName: Uint8Array): string {
    return Buffer.from(streamName).toString('utf8');
  }

  private isMetastream(streamName: string): boolean {
    return streamName.startsWith('$$');
  }

  private createWrongExpectedVersionError(
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

  private isBackwardsRead(
    readDirection: NonNullable<ReadReq['options']>['readDirection'] | string,
  ): boolean {
    return (
      readDirection === ReadReq_Options_ReadDirection.Backwards ||
      (typeof readDirection === 'string' &&
        (readDirection === 'Backwards' || readDirection === 'BACKWARDS'))
    );
  }

  private getEventId(id: AppendReq_ProposedMessage['id']): string {
    if (!id?.string) {
      throw new Error('Only string UUID event identifiers are supported.');
    }

    return id.string;
  }

  private mapBatchProposedMessage(
    message: BatchAppendReq['proposedMessages'][number],
  ): AppendReq_ProposedMessage {
    return {
      id: message.id,
      metadata: message.metadata,
      customMetadata: message.customMetadata,
      data: message.data,
    };
  }

  private createBatchAppendWrongExpectedVersionStatus(
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

  private createBatchAppendStreamDeletedStatus(
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
