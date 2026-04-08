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
} from '../interfaces/streams';
import {
  ScavengeResp,
  ScavengeResp_ScavengeResult,
  StartScavengeReq,
  StopScavengeReq,
} from '../interfaces/operations';
import { Code } from '../interfaces/code';
import { Any } from '../interfaces/google/protobuf/any';
import { Timestamp } from '../interfaces/google/protobuf/timestamp';
import { createInfoResponseBody } from '../stub-utils';
import {
  EventStoreBackend,
  EventStoreStatsSnapshot,
} from '../event-store/event-store-backend';
import {
  InvalidArgumentServiceError,
  StreamDeletedServiceError,
} from '../event-store/event-store.errors';

type PersistedEventRow = {
  global_position: string | number;
  stream_name: string;
  stream_generation?: string | number;
  stream_revision: string | number;
  event_id: string;
  created_at: Date | string;
  metadata: Record<string, string> | null;
  custom_metadata: Buffer | Uint8Array | null;
  data: Buffer | Uint8Array | null;
};

type StreamRetentionPolicy = {
  currentGeneration: number;
  currentRevision: number | null;
  deletedAt: Date | string | null;
  maxAge: number | null;
  maxCount: number | null;
  truncateBefore: number | null;
};

type StreamStateRow = {
  current_generation: string | number | null;
  current_revision: string | number | null;
  deleted_at: Date | string | null;
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

type ActiveScavenge = {
  cancelled: boolean;
};

@Injectable()
export class PostgresEventStoreService
  implements EventStoreBackend, OnModuleInit, OnModuleDestroy
{
  private static readonly ALL_STREAM_KEY = '$all';
  private static readonly STREAM_UPDATES_CHANNEL = 'kurrentdb_stream_updates';
  private static readonly SERVER_INFO_STREAM = '$server-info';
  private static readonly STREAMS_STREAM = '$streams';
  private static readonly SCAVENGES_STREAM = '$scavenges';
  private static readonly SCAVENGE_BATCH_SIZE = 500;
  private readonly pool = new Pool(this.getPoolConfig());
  private listenClient: PoolClient | null = null;
  private readonly streamVersions = new Map<string, number>();
  private readonly streamListeners = new Map<string, Set<() => void>>();
  private readonly activeScavenges = new Map<string, ActiveScavenge>();

  async onModuleInit(): Promise<void> {
    await this.pool.query(`
      CREATE TABLE IF NOT EXISTS stream_events (
        global_position BIGSERIAL PRIMARY KEY,
        stream_name TEXT NOT NULL,
        stream_generation BIGINT NOT NULL DEFAULT 0,
        stream_revision BIGINT NOT NULL,
        event_id UUID NOT NULL UNIQUE,
        metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
        custom_metadata BYTEA NOT NULL DEFAULT '\\x',
        data BYTEA NOT NULL DEFAULT '\\x',
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE (stream_name, stream_generation, stream_revision)
      )
    `);

    await this.pool.query(`
      ALTER TABLE stream_events
      ADD COLUMN IF NOT EXISTS stream_generation BIGINT NOT NULL DEFAULT 0
    `);

    await this.pool.query(`
      ALTER TABLE stream_events
      DROP CONSTRAINT IF EXISTS stream_events_stream_name_stream_revision_key
    `);

    await this.pool.query(`
      CREATE INDEX IF NOT EXISTS idx_stream_events_stream_revision
      ON stream_events (stream_name, stream_generation, stream_revision)
    `);

    await this.pool.query(`
      CREATE UNIQUE INDEX IF NOT EXISTS idx_stream_events_stream_generation_revision_unique
      ON stream_events (stream_name, stream_generation, stream_revision)
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
        current_generation BIGINT NOT NULL DEFAULT 0,
        current_revision BIGINT NULL,
        deleted_at TIMESTAMPTZ NULL,
        max_age BIGINT NULL,
        max_count BIGINT NULL,
        truncate_before BIGINT NULL
      )
    `);

    await this.pool.query(`
      ALTER TABLE stream_retention_policies
      ADD COLUMN IF NOT EXISTS current_generation BIGINT NOT NULL DEFAULT 0
    `);

    await this.pool.query(`
      ALTER TABLE stream_retention_policies
      ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ NULL
    `);

    await this.pool.query(`
      ALTER TABLE stream_retention_policies
      ADD COLUMN IF NOT EXISTS max_age BIGINT NULL
    `);

    await this.initializeStreamUpdateListener();
  }

  async onModuleDestroy(): Promise<void> {
    if (this.listenClient) {
      try {
        await this.listenClient.query(
          `UNLISTEN ${PostgresEventStoreService.STREAM_UPDATES_CHANNEL}`,
        );
      } finally {
        this.listenClient.release();
        this.listenClient = null;
      }
    }

    await this.pool.end();
  }

  async getStatsSnapshot(): Promise<EventStoreStatsSnapshot> {
    const result = await this.pool.query<{
      current_global_position: string | number | null;
      retention_policy_count: string | number;
      stream_count: string | number;
      tombstoned_stream_count: string | number;
      total_events: string | number;
    }>(
      `
        SELECT
          (SELECT MAX(global_position) FROM stream_events) AS current_global_position,
          (SELECT COUNT(*) FROM stream_events) AS total_events,
          (SELECT COUNT(DISTINCT stream_name) FROM stream_events) AS stream_count,
          (SELECT COUNT(*) FROM tombstoned_streams) AS tombstoned_stream_count,
          (SELECT COUNT(*) FROM stream_retention_policies) AS retention_policy_count
      `,
    );
    const row = result.rows[0];

    return {
      activeScavenges: this.activeScavenges.size,
      currentGlobalPosition:
        row.current_global_position === null
          ? 0
          : this.toNumber(row.current_global_position),
      pgPoolIdleCount: this.pool.idleCount,
      pgPoolTotalCount: this.pool.totalCount,
      pgPoolWaitingCount: this.pool.waitingCount,
      retentionPolicyCount: this.toNumber(row.retention_policy_count),
      streamCount: this.toNumber(row.stream_count),
      tombstonedStreamCount: this.toNumber(row.tombstoned_stream_count),
      totalEvents: this.toNumber(row.total_events),
    };
  }

  async append(messages: AppendReq[]): Promise<AppendResp> {
    let options: AppendReq['options'] | undefined;
    const proposedMessages: AppendReq_ProposedMessage[] = [];

    for (const message of messages) {
      if (message.options && !options) {
        options = message.options;
      }

      if (message.proposedMessage) {
        proposedMessages.push(message.proposedMessage);
      }
    }

    return this.appendPrepared(options, proposedMessages);
  }

  private async appendPrepared(
    options: AppendReq['options'],
    proposedMessages: AppendReq_ProposedMessage[],
  ): Promise<AppendResp> {
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

      const streamState = await this.getStreamRetentionPolicy(
        client,
        streamName,
      );
      const currentRevision = this.getReadableCurrentRevision(streamState);
      const mismatch = this.getExpectedVersionMismatch(
        options,
        currentRevision,
      );
      if (mismatch) {
        await client.query('ROLLBACK');
        return mismatch;
      }

      const targetGeneration = streamState.deletedAt
        ? streamState.currentGeneration + 1
        : streamState.currentGeneration;
      let nextRevision = streamState.currentRevision ?? -1;
      let lastPosition = currentRevision === null ? null : 0;

      if (proposedMessages.length > 0) {
        const insertValues: Array<string | number | Buffer> = [];
        const rowPlaceholders: string[] = [];

        for (const proposedMessage of proposedMessages) {
          nextRevision += 1;

          const parameterOffset = insertValues.length;
          rowPlaceholders.push(
            `($${parameterOffset + 1}, $${parameterOffset + 2}, $${parameterOffset + 3}, $${parameterOffset + 4}::uuid, $${parameterOffset + 5}::jsonb, $${parameterOffset + 6}, $${parameterOffset + 7})`,
          );
          insertValues.push(
            streamName,
            targetGeneration,
            nextRevision,
            this.getEventId(proposedMessage.id),
            JSON.stringify(proposedMessage.metadata ?? {}),
            Buffer.from(proposedMessage.customMetadata ?? new Uint8Array()),
            Buffer.from(proposedMessage.data ?? new Uint8Array()),
          );
        }

        const result = await client.query<{ global_position: string | number }>(
          `
            INSERT INTO stream_events (
              stream_name,
              stream_generation,
              stream_revision,
              event_id,
              metadata,
              custom_metadata,
              data
            )
            VALUES ${rowPlaceholders.join(', ')}
            RETURNING global_position
          `,
          insertValues,
        );

        lastPosition = this.toNumber(
          result.rows[result.rows.length - 1].global_position,
        );
      }

      if (proposedMessages.length > 0 && !this.isMetastream(streamName)) {
        await this.upsertStreamCurrentRevision(
          client,
          streamName,
          targetGeneration,
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
        await this.notifyStreamUpdated(streamName);
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

    const appendOptions: AppendReq['options'] = {
      streamIdentifier: options.streamIdentifier,
      revision: options.streamPosition,
      noStream: options.noStream ? {} : undefined,
      any: options.any ? {} : undefined,
      streamExists: options.streamExists ? {} : undefined,
    };
    const proposedMessages: AppendReq_ProposedMessage[] = [];

    for (const request of requests) {
      for (const message of request.proposedMessages ?? []) {
        proposedMessages.push(message);
      }
    }

    let appendResponse: AppendResp;

    try {
      appendResponse = await this.appendPrepared(
        appendOptions,
        proposedMessages,
      );
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

    const streamOptionCase = this.getReadStreamOptionCase(options);
    if (streamOptionCase === 'None') {
      throw new InvalidArgumentServiceError(
        "'None' is not a valid EventStore.Client.Streams.ReadReq+Types+Options+StreamOptionOneofCase",
      );
    }

    this.assertSupportedReadCombination(options, streamOptionCase);

    if (options.stream?.streamIdentifier?.streamName) {
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

    const streamOptionCase = this.getReadStreamOptionCase(options);
    if (streamOptionCase === 'None') {
      throw new InvalidArgumentServiceError(
        "'None' is not a valid EventStore.Client.Streams.ReadReq+Types+Options+StreamOptionOneofCase",
      );
    }

    this.assertSupportedReadCombination(options, streamOptionCase);

    yield {
      confirmation: {
        subscriptionId: randomUUID(),
      },
    };

    if (options.stream?.streamIdentifier?.streamName) {
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
    const streamState = await this.getStreamRetentionPolicy(
      this.pool,
      streamName,
    );
    const activeGeneration = streamState.deletedAt
      ? streamState.currentGeneration + 1
      : streamState.currentGeneration;
    let nextRevisionExclusive = await this.resolveStreamSubscriptionBoundary(
      streamName,
      options,
    );
    let caughtUp = false;

    while (!isCancelled()) {
      const versionBeforeRead = this.getStreamVersion(streamName);
      const rows = await this.readStreamSubscriptionRows(
        streamName,
        activeGeneration,
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

  private getReadStreamOptionCase(
    options: NonNullable<ReadReq['options']>,
  ): 'Stream' | 'All' | 'None' {
    if (options.stream?.streamIdentifier?.streamName) {
      return 'Stream';
    }

    if (options.all) {
      return 'All';
    }

    return 'None';
  }

  private getReadCountOptionCase(
    options: NonNullable<ReadReq['options']>,
  ): 'Count' | 'Subscription' | 'None' {
    if (options.subscription) {
      return 'Subscription';
    }

    if (options.count !== undefined) {
      return 'Count';
    }

    return 'None';
  }

  private getReadFilterOptionCase(
    options: NonNullable<ReadReq['options']>,
  ): 'Filter' | 'NoFilter' {
    return options.filter ? 'Filter' : 'NoFilter';
  }

  private assertSupportedReadCombination(
    options: NonNullable<ReadReq['options']>,
    streamOptionCase: 'Stream' | 'All',
  ): void {
    const countOptionCase = this.getReadCountOptionCase(options);
    const readDirection = this.isBackwardsRead(options.readDirection)
      ? 'Backwards'
      : 'Forwards';
    const filterOptionCase = this.getReadFilterOptionCase(options);

    const isSupported =
      (streamOptionCase === 'Stream' &&
        countOptionCase === 'Count' &&
        filterOptionCase === 'NoFilter') ||
      (streamOptionCase === 'All' && countOptionCase === 'Count') ||
      (streamOptionCase === 'Stream' &&
        countOptionCase === 'Subscription' &&
        readDirection === 'Forwards' &&
        filterOptionCase === 'NoFilter') ||
      (streamOptionCase === 'All' &&
        countOptionCase === 'Subscription' &&
        readDirection === 'Forwards');

    if (isSupported) {
      return;
    }

    throw new InvalidArgumentServiceError(
      `The combination of (${streamOptionCase}, ${countOptionCase}, ${readDirection}, ${filterOptionCase}) is invalid.`,
    );
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

      const streamState = await this.getStreamRetentionPolicy(
        client,
        streamName,
      );
      const currentRevision = this.getReadableCurrentRevision(streamState);
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

      const lastEvent = await client.query<{
        global_position: string | number;
      }>(
        `
          SELECT global_position
          FROM stream_events
          WHERE stream_name = $1
            AND stream_generation = $2
          ORDER BY stream_revision DESC
          LIMIT 1
        `,
        [streamName, streamState.currentGeneration],
      );

      await this.markStreamDeleted(
        client,
        streamName,
        streamState.currentGeneration,
        streamState.currentRevision,
      );

      await client.query('COMMIT');

      await this.notifyStreamUpdated(streamName);

      const lastPosition = this.toNumber(lastEvent.rows[0].global_position);

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

      const streamState = await this.getStreamRetentionPolicy(
        client,
        streamName,
      );
      const currentRevision = this.getReadableCurrentRevision(streamState);
      const mismatch = this.getTombstoneExpectedVersionMismatch(
        options,
        currentRevision,
        streamName,
      );
      if (mismatch) {
        await client.query('ROLLBACK');
        throw mismatch;
      }

      const lastEvent = await client.query<{
        global_position: string | number;
      }>(
        `
          SELECT global_position
          FROM stream_events
          WHERE stream_name = $1
            AND stream_generation = $2
          ORDER BY stream_revision DESC
          LIMIT 1
        `,
        [streamName, streamState.currentGeneration],
      );

      let nextRevision = streamState.currentRevision ?? -1;
      let lastPosition =
        lastEvent.rows.length === 0
          ? 0
          : this.toNumber(lastEvent.rows[0].global_position);

      nextRevision += 1;

      const tombstoneEvent = await client.query<{
        global_position: string | number;
      }>(
        `
          INSERT INTO stream_events (
            stream_name,
            stream_generation,
            stream_revision,
            event_id,
            metadata,
            custom_metadata,
            data
          )
          VALUES ($1, $2, $3, $4::uuid, $5::jsonb, $6, $7)
          RETURNING global_position
        `,
        [
          streamName,
          streamState.currentGeneration,
          nextRevision,
          randomUUID(),
          JSON.stringify({
            type: '$streamDeleted',
            'content-type': 'application/octet-stream',
          }),
          Buffer.alloc(0),
          Buffer.alloc(0),
        ],
      );

      lastPosition = this.toNumber(tombstoneEvent.rows[0].global_position);

      await client.query(
        `
          INSERT INTO tombstoned_streams (stream_name, last_position)
          VALUES ($1, $2)
          ON CONFLICT (stream_name)
          DO UPDATE SET last_position = EXCLUDED.last_position
        `,
        [streamName, lastPosition],
      );

      await this.markStreamDeleted(
        client,
        streamName,
        streamState.currentGeneration,
        nextRevision,
      );

      await client.query('COMMIT');

      await this.notifyStreamUpdated(streamName);

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

  startScavenge(request: StartScavengeReq): ScavengeResp {
    void request;

    const scavengeId = randomUUID();
    const activeScavenge: ActiveScavenge = {
      cancelled: false,
    };

    this.activeScavenges.set(scavengeId, activeScavenge);
    void this.runScavenge(scavengeId, activeScavenge);

    return {
      scavengeId,
      scavengeResult: ScavengeResp_ScavengeResult.Started,
    };
  }

  stopScavenge(request: StopScavengeReq): ScavengeResp {
    const scavengeId = request.options?.scavengeId ?? '';
    const activeScavenge = this.activeScavenges.get(scavengeId);

    if (activeScavenge) {
      activeScavenge.cancelled = true;
    }

    return {
      scavengeId,
      scavengeResult: ScavengeResp_ScavengeResult.Stopped,
    };
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
    const streamState = await this.getStreamRetentionPolicy(client, streamName);
    return this.getReadableCurrentRevision(streamState);
  }

  private async getStreamRetentionPolicy(
    client: PoolClient | Pool,
    streamName: string,
  ): Promise<StreamRetentionPolicy> {
    const policyResult = await client.query<
      StreamStateRow & {
        max_age: string | number | null;
        max_count: string | number | null;
        truncate_before: string | number | null;
      }
    >(
      `
        SELECT
          current_generation,
          current_revision,
          deleted_at,
          max_age,
          max_count,
          truncate_before
        FROM stream_retention_policies
        WHERE stream_name = $1
      `,
      [streamName],
    );

    if (policyResult.rows.length > 0) {
      const row = policyResult.rows[0];
      return {
        currentGeneration: this.toNumber(row.current_generation ?? 0),
        currentRevision:
          row.current_revision === null
            ? null
            : this.toNumber(row.current_revision),
        deletedAt: row.deleted_at,
        maxAge: row.max_age === null ? null : this.toNumber(row.max_age),
        maxCount: row.max_count === null ? null : this.toNumber(row.max_count),
        truncateBefore:
          row.truncate_before === null
            ? null
            : this.toNumber(row.truncate_before),
      };
    }

    const fallbackResult = await client.query<{
      stream_generation: string | number;
      stream_revision: string | number;
    }>(
      `
        SELECT stream_generation, stream_revision
        FROM stream_events
        WHERE stream_name = $1
        ORDER BY stream_generation DESC, stream_revision DESC
        LIMIT 1
      `,
      [streamName],
    );

    if (fallbackResult.rows.length === 0) {
      return {
        currentGeneration: 0,
        currentRevision: null,
        deletedAt: null,
        maxAge: null,
        maxCount: null,
        truncateBefore: null,
      };
    }

    return {
      currentGeneration: this.toNumber(
        fallbackResult.rows[0].stream_generation,
      ),
      currentRevision: this.toNumber(fallbackResult.rows[0].stream_revision),
      deletedAt: null,
      maxAge: null,
      maxCount: null,
      truncateBefore: null,
    };
  }

  private async readStreamSnapshot(
    streamName: string,
    options: NonNullable<ReadReq['options']>,
  ): Promise<ReadResp[]> {
    if (streamName === PostgresEventStoreService.SERVER_INFO_STREAM) {
      return [this.createSyntheticReadResponse(streamName, '$ServerInfo')];
    }

    if (
      streamName === PostgresEventStoreService.STREAMS_STREAM ||
      streamName === PostgresEventStoreService.SCAVENGES_STREAM
    ) {
      return this.createExistingEmptyStreamReadResponses(options);
    }

    const streamState = await this.getStreamRetentionPolicy(
      this.pool,
      streamName,
    );
    if (this.getReadableCurrentRevision(streamState) === null) {
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

    const params: Array<number | string> = [
      streamName,
      streamState.currentGeneration,
    ];
    let whereClause =
      'WHERE events.stream_name = $1 AND events.stream_generation = $2';
    if (boundary !== null) {
      params.push(boundary);
      whereClause += ` AND events.stream_revision ${comparator} $${
        params.length
      }`;
    }

    params.push(limit);

    const result = await this.pool.query<PersistedEventRow>(
      `
        SELECT
          events.global_position,
          events.stream_name,
          events.stream_revision,
          events.event_id,
          events.created_at,
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
          events.created_at,
          events.metadata,
          events.custom_metadata,
          events.data
        FROM stream_events events
        LEFT JOIN stream_retention_policies retention
          ON retention.stream_name = events.stream_name
        ${whereClause}
        ORDER BY events.global_position ${order}
        LIMIT $${params.length}
      `,
      params,
    );

    if (result.rows.length === 0) {
      console.info(
        `[debug] ReadAll snapshot empty boundary=${boundary === null ? 'none' : String(boundary)} filter=${
          options.filter ? 'yes' : 'no'
        } returning=caughtUp`,
      );
      return [
        {
          // Emit a terminal frame so clients don't see an entirely empty
          // response stream when $all is read against an empty database.
          caughtUp: {
            timestamp: this.createTimestamp(),
          },
        },
      ];
    }

    return result.rows.map((row) => this.mapRowToReadResponse(row));
  }

  private async resolveStreamSubscriptionBoundary(
    streamName: string,
    options: NonNullable<ReadReq['options']>,
  ): Promise<number> {
    const streamState = await this.getStreamRetentionPolicy(
      this.pool,
      streamName,
    );
    if (this.getReadableCurrentRevision(streamState) === null) {
      return -1;
    }

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
    streamGeneration: number,
    nextRevisionExclusive: number,
  ): Promise<PersistedEventRow[]> {
    const result = await this.pool.query<PersistedEventRow>(
      `
        SELECT
          events.global_position,
          events.stream_name,
          events.stream_generation,
          events.stream_revision,
          events.event_id,
          events.created_at,
          events.metadata,
          events.custom_metadata,
          events.data
        FROM stream_events events
        LEFT JOIN stream_retention_policies retention
          ON retention.stream_name = events.stream_name
        WHERE events.stream_name = $1
          AND events.stream_generation = $2
          AND events.stream_revision > $3
          AND ${this.buildRetentionVisibilityClause('events', 'retention')}
        ORDER BY events.stream_revision ASC
        LIMIT 100
      `,
      [streamName, streamGeneration, nextRevisionExclusive],
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
          events.created_at,
          events.metadata,
          events.custom_metadata,
          events.data
        FROM stream_events events
        LEFT JOIN stream_retention_policies retention
          ON retention.stream_name = events.stream_name
        WHERE ${whereClauses.join(' AND ')}
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
    const grpcMetadata = {
      type: row.metadata?.type ?? '<no-event-type-provided>',
      created: this.toTicksSinceUnixEpoch(row.created_at),
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
          streamRevision: this.toNumber(row.stream_revision),
          preparePosition: this.toNumber(row.global_position),
          commitPosition: this.toNumber(row.global_position),
          metadata: grpcMetadata,
          customMetadata: Buffer.from(row.custom_metadata ?? new Uint8Array()),
          data: Buffer.from(row.data ?? new Uint8Array()),
        },
        link: undefined,
        commitPosition: this.toNumber(row.global_position),
      },
    };
  }

  private createSyntheticReadResponse(
    streamName: string,
    eventType: string,
  ): ReadResp {
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
      // Include a timestamped terminal-ish marker payload via event metadata only;
      // named stream snapshot reads do not have a dedicated "empty but exists" frame.
    };
  }

  private createExistingEmptyStreamReadResponses(
    options: NonNullable<ReadReq['options']>,
  ): ReadResp[] {
    if (this.isBackwardsRead(options.readDirection)) {
      return [{ lastStreamPosition: 0 }];
    }

    return [{ firstStreamPosition: 0 }];
  }

  private createTimestamp(): Timestamp {
    const now = Date.now();

    return {
      seconds: Math.floor(now / 1000),
      nanos: (now % 1000) * 1_000_000,
    };
  }

  private toTicksSinceUnixEpoch(value: Date | string): string {
    const date = value instanceof Date ? value : new Date(value);
    return String(date.getTime() * 10_000);
  }

  private getStreamVersion(streamName: string): number {
    return this.streamVersions.get(streamName) ?? 0;
  }

  private async initializeStreamUpdateListener(): Promise<void> {
    this.listenClient = await this.pool.connect();
    this.listenClient.on('notification', (notification) => {
      if (
        notification.channel !==
        PostgresEventStoreService.STREAM_UPDATES_CHANNEL
      ) {
        return;
      }

      const streamName = notification.payload;
      if (!streamName) {
        return;
      }

      this.bumpStreamVersion(streamName);
      this.bumpStreamVersion(PostgresEventStoreService.ALL_STREAM_KEY);
    });
    await this.listenClient.query(
      `LISTEN ${PostgresEventStoreService.STREAM_UPDATES_CHANNEL}`,
    );
  }

  private async notifyStreamUpdated(streamName: string): Promise<void> {
    await this.pool.query('SELECT pg_notify($1, $2)', [
      PostgresEventStoreService.STREAM_UPDATES_CHANNEL,
      streamName,
    ]);
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

  private async runScavenge(
    scavengeId: string,
    activeScavenge: ActiveScavenge,
  ): Promise<void> {
    try {
      while (!activeScavenge.cancelled) {
        const deletedCount = await this.deleteScavengedBatch();
        if (deletedCount === 0) {
          return;
        }
      }
    } finally {
      this.activeScavenges.delete(scavengeId);
    }
  }

  private async deleteScavengedBatch(): Promise<number> {
    const result = await this.pool.query<{ deleted_count: string | number }>(
      `
        WITH doomed AS (
          SELECT events.ctid
          FROM stream_events events
          LEFT JOIN stream_retention_policies retention
            ON retention.stream_name = events.stream_name
          LEFT JOIN tombstoned_streams tombstones
            ON tombstones.stream_name = events.stream_name
          WHERE ${this.buildScavengeEligibilityClause(
            'events',
            'retention',
            'tombstones',
          )}
          ORDER BY events.global_position ASC
          LIMIT $1
        )
        DELETE FROM stream_events events
        USING doomed
        WHERE events.ctid = doomed.ctid
        RETURNING 1 AS deleted_count
      `,
      [PostgresEventStoreService.SCAVENGE_BATCH_SIZE],
    );

    return result.rows.length;
  }

  private async upsertStreamCurrentRevision(
    client: PoolClient,
    streamName: string,
    currentGeneration: number,
    currentRevision: number,
  ): Promise<void> {
    await client.query(
      `
        INSERT INTO stream_retention_policies (
          stream_name,
          current_generation,
          current_revision,
          deleted_at
        )
        VALUES ($1, $2, $3, NULL)
        ON CONFLICT (stream_name)
        DO UPDATE SET
          current_generation = EXCLUDED.current_generation,
          current_revision = EXCLUDED.current_revision,
          deleted_at = NULL
      `,
      [streamName, currentGeneration, currentRevision],
    );
  }

  private async upsertStreamRetentionPolicy(
    client: PoolClient,
    streamName: string,
    policy: Omit<
      StreamRetentionPolicy,
      'currentGeneration' | 'currentRevision' | 'deletedAt'
    >,
  ): Promise<void> {
    await client.query(
      `
        INSERT INTO stream_retention_policies (
          stream_name,
          current_generation,
          current_revision,
          deleted_at,
          max_age,
          max_count,
          truncate_before
        )
        VALUES ($1, 0, NULL, NULL, $2, $3, $4)
        ON CONFLICT (stream_name)
        DO UPDATE SET
          max_age = EXCLUDED.max_age,
          max_count = EXCLUDED.max_count,
          truncate_before = EXCLUDED.truncate_before
      `,
      [streamName, policy.maxAge, policy.maxCount, policy.truncateBefore],
    );
  }

  private async markStreamDeleted(
    client: PoolClient,
    streamName: string,
    currentGeneration: number,
    currentRevision: number | null,
  ): Promise<void> {
    await client.query(
      `
        INSERT INTO stream_retention_policies (
          stream_name,
          current_generation,
          current_revision,
          deleted_at
        )
        VALUES ($1, $2, $3, NOW())
        ON CONFLICT (stream_name)
        DO UPDATE SET
          current_generation = EXCLUDED.current_generation,
          current_revision = EXCLUDED.current_revision,
          deleted_at = EXCLUDED.deleted_at
      `,
      [streamName, currentGeneration, currentRevision],
    );
  }

  private parseMetadataPolicyUpdate(
    streamName: string,
    proposedMessages: AppendReq_ProposedMessage[],
  ): {
    streamName: string;
    policy: Omit<
      StreamRetentionPolicy,
      'currentGeneration' | 'currentRevision' | 'deletedAt'
    >;
  } | null {
    if (!this.isMetastream(streamName)) {
      return null;
    }

    let policy: Omit<
      StreamRetentionPolicy,
      'currentGeneration' | 'currentRevision' | 'deletedAt'
    > | null = null;
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

  private buildScavengeEligibilityClause(
    eventsAlias: string,
    retentionAlias: string,
    tombstonesAlias: string,
  ): string {
    return `(
      (
        ${tombstonesAlias}.stream_name IS NOT NULL
        AND (
          ${retentionAlias}.current_revision IS NULL
          OR ${eventsAlias}.stream_revision <> ${retentionAlias}.current_revision
        )
      )
      OR (
        ${retentionAlias}.deleted_at IS NOT NULL
        AND (
          ${retentionAlias}.current_revision IS NULL
          OR ${eventsAlias}.stream_revision <> ${retentionAlias}.current_revision
        )
      )
      OR ${eventsAlias}.stream_generation < COALESCE(${retentionAlias}.current_generation, 0)
      OR (
        ${eventsAlias}.stream_generation = COALESCE(${retentionAlias}.current_generation, 0)
        AND NOT ${this.buildRetentionVisibilityClause(eventsAlias, retentionAlias)}
        AND (
          ${retentionAlias}.current_revision IS NULL
          OR ${eventsAlias}.stream_revision <> ${retentionAlias}.current_revision
        )
      )
    )`;
  }

  private getReadableCurrentRevision(
    streamState: Pick<StreamRetentionPolicy, 'currentRevision' | 'deletedAt'>,
  ): number | null {
    if (streamState.deletedAt) {
      return null;
    }

    return streamState.currentRevision;
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
      const commitPosition = this.toNumber(all.position.commitPosition);
      return this.isBackwardsRead(options.readDirection)
        ? commitPosition - 1
        : commitPosition;
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
