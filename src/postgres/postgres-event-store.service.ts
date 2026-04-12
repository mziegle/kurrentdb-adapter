import { Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { randomUUID } from 'node:crypto';
import { Pool, PoolClient } from 'pg';
import {
  AppendReq,
  AppendReq_ProposedMessage,
  AppendResp,
  BatchAppendReq,
  BatchAppendResp,
  DeleteReq,
  DeleteResp,
  ReadReq,
  ReadReq_Options_FilterOptions,
  ReadReq_Options_FilterOptions_Expression,
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
import { appLogger } from '../shared/logger';
import {
  EventStoreBackend,
  EventStoreStatsSnapshot,
} from '../event-store/event-store-backend';
import {
  InvalidArgumentServiceError,
  StreamDeletedServiceError,
} from '../event-store/event-store.errors';
import {
  POSTGRES_ALL_STREAM_KEY,
  POSTGRES_SCAVENGES_STREAM,
  POSTGRES_SCAVENGE_BATCH_SIZE,
  POSTGRES_SCHEMA_INIT_LOCK_KEY,
  POSTGRES_SERVER_INFO_STREAM,
  POSTGRES_STREAMS_STREAM,
  POSTGRES_STREAM_UPDATES_CHANNEL,
  POSTGRES_STREAM_WRITE_LOCK_NAMESPACE,
} from './postgres.constants';
import { PostgresInfrastructureService } from './postgres-infrastructure.service';
import { PostgresProtocolService } from './postgres-protocol.service';
import { PostgresSubscriptionService } from './postgres-subscription.service';
import {
  ActiveScavenge,
  PersistedEventRow,
  StreamRetentionPolicy,
  StreamStateRow,
} from './postgres.types';
import {
  buildRetentionVisibilityClause,
  buildScavengeEligibilityClause,
  getReadableCurrentRevision,
  isMetastream,
  parseMetadataPolicyUpdate,
} from './postgres-retention';
import {
  createTimestamp,
  decodeStreamName,
  getEventId,
  isBackwardsRead,
  toNumber,
} from './postgres-value';

@Injectable()
export class PostgresEventStoreService
  implements EventStoreBackend, OnModuleInit, OnModuleDestroy
{
  private readonly pool: Pool;
  private readonly activeScavenges = new Map<string, ActiveScavenge>();

  constructor(
    private readonly infrastructure: PostgresInfrastructureService,
    private readonly protocol: PostgresProtocolService,
    private readonly subscriptions: PostgresSubscriptionService,
  ) {
    this.pool = this.infrastructure.createPool();
  }

  async onModuleInit(): Promise<void> {
    const client = await this.pool.connect();

    try {
      await client.query('SELECT pg_advisory_lock($1)', [
        POSTGRES_SCHEMA_INIT_LOCK_KEY,
      ]);
      await this.infrastructure.initializeSchema(client);
    } finally {
      try {
        await client.query('SELECT pg_advisory_unlock($1)', [
          POSTGRES_SCHEMA_INIT_LOCK_KEY,
        ]);
      } finally {
        client.release();
      }
    }
    await this.subscriptions.initialize(
      this.pool,
      POSTGRES_STREAM_UPDATES_CHANNEL,
      POSTGRES_ALL_STREAM_KEY,
    );
  }

  async onModuleDestroy(): Promise<void> {
    await this.subscriptions.destroy(POSTGRES_STREAM_UPDATES_CHANNEL);
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
          : toNumber(row.current_global_position),
      pgPoolIdleCount: this.pool.idleCount,
      pgPoolTotalCount: this.pool.totalCount,
      pgPoolWaitingCount: this.pool.waitingCount,
      retentionPolicyCount: toNumber(row.retention_policy_count),
      streamCount: toNumber(row.stream_count),
      tombstonedStreamCount: toNumber(row.tombstoned_stream_count),
      totalEvents: toNumber(row.total_events),
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

    const streamName = decodeStreamName(options.streamIdentifier.streamName);
    const metadataPolicyUpdate = parseMetadataPolicyUpdate(
      streamName,
      proposedMessages,
    );

    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      await this.lockStreamTransaction(client, streamName);
      await this.ensureStreamIsNotTombstoned(streamName, client);

      const streamState = await this.getStreamRetentionPolicy(
        client,
        streamName,
      );
      const currentRevision = getReadableCurrentRevision(streamState);
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
            getEventId(proposedMessage.id),
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

        lastPosition = toNumber(
          result.rows[result.rows.length - 1].global_position,
        );
      }

      if (proposedMessages.length > 0 && !isMetastream(streamName)) {
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
        await this.subscriptions.notifyStreamUpdated(
          streamName,
          POSTGRES_STREAM_UPDATES_CHANNEL,
          client,
        );
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
          error: this.protocol.createBatchAppendStreamDeletedStatus(
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
      error: this.protocol.createBatchAppendWrongExpectedVersionStatus(
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
      const streamName = decodeStreamName(
        options.stream.streamIdentifier.streamName,
      );
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
      const streamName = decodeStreamName(
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
      const versionBeforeRead = this.subscriptions.getStreamVersion(streamName);
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

          nextRevisionExclusive = toNumber(row.stream_revision);
          yield this.protocol.mapRowToReadResponse(row);
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
            timestamp: createTimestamp(),
            streamRevision:
              caughtUpRevision >= 0 ? caughtUpRevision : undefined,
          },
        };
        caughtUp = true;
      }

      if (
        versionBeforeRead !== this.subscriptions.getStreamVersion(streamName)
      ) {
        continue;
      }

      await this.subscriptions.waitForStreamUpdate(
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
    const readDirection = isBackwardsRead(options.readDirection)
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
      const versionBeforeRead = this.subscriptions.getStreamVersion(
        POSTGRES_ALL_STREAM_KEY,
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

          nextPositionExclusive = toNumber(row.global_position);
          yield this.protocol.mapRowToReadResponse(row);
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
            timestamp: createTimestamp(),
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
        this.subscriptions.getStreamVersion(POSTGRES_ALL_STREAM_KEY)
      ) {
        continue;
      }

      await this.subscriptions.waitForStreamUpdate(
        POSTGRES_ALL_STREAM_KEY,
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

    const streamName = decodeStreamName(options.streamIdentifier.streamName);
    await this.ensureStreamIsNotTombstoned(streamName);
    const client = await this.pool.connect();

    try {
      await client.query('BEGIN');

      const streamState = await this.getStreamRetentionPolicy(
        client,
        streamName,
      );
      const currentRevision = getReadableCurrentRevision(streamState);
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

      await this.subscriptions.notifyStreamUpdated(
        streamName,
        POSTGRES_STREAM_UPDATES_CHANNEL,
        client,
      );

      const lastPosition = toNumber(lastEvent.rows[0].global_position);

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

    const streamName = decodeStreamName(options.streamIdentifier.streamName);
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
      const currentRevision = getReadableCurrentRevision(streamState);
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
          : toNumber(lastEvent.rows[0].global_position);

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

      lastPosition = toNumber(tombstoneEvent.rows[0].global_position);

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

      await this.subscriptions.notifyStreamUpdated(
        streamName,
        POSTGRES_STREAM_UPDATES_CHANNEL,
        client,
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

  private async getCurrentRevision(
    client: PoolClient | Pool,
    streamName: string,
  ): Promise<number | null> {
    const streamState = await this.getStreamRetentionPolicy(client, streamName);
    return getReadableCurrentRevision(streamState);
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
        currentGeneration: toNumber(row.current_generation ?? 0),
        currentRevision:
          row.current_revision === null ? null : toNumber(row.current_revision),
        deletedAt: row.deleted_at,
        maxAge: row.max_age === null ? null : toNumber(row.max_age),
        maxCount: row.max_count === null ? null : toNumber(row.max_count),
        truncateBefore:
          row.truncate_before === null ? null : toNumber(row.truncate_before),
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
      currentGeneration: toNumber(fallbackResult.rows[0].stream_generation),
      currentRevision: toNumber(fallbackResult.rows[0].stream_revision),
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
    if (streamName === POSTGRES_SERVER_INFO_STREAM) {
      return [
        this.protocol.createSyntheticReadResponse(streamName, '$ServerInfo'),
      ];
    }

    if (
      streamName === POSTGRES_STREAMS_STREAM ||
      streamName === POSTGRES_SCAVENGES_STREAM
    ) {
      return this.protocol.createExistingEmptyStreamReadResponses(options);
    }

    const streamState = await this.getReadStreamSnapshotState(streamName);
    if (getReadableCurrentRevision(streamState) === null) {
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

    const limit = options.count !== undefined ? toNumber(options.count) : 100;
    const isBackwards = isBackwardsRead(options.readDirection);
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
          ${buildRetentionVisibilityClause('events', 'retention')}
        ORDER BY events.stream_revision ${order}
        LIMIT $${params.length}
      `,
      params,
    );

    return result.rows.map((row) => this.protocol.mapRowToReadResponse(row));
  }

  private async getReadStreamSnapshotState(
    streamName: string,
  ): Promise<StreamRetentionPolicy> {
    const result = await this.pool.query<
      StreamStateRow & {
        fallback_generation: string | number | null;
        fallback_revision: string | number | null;
        is_tombstoned: boolean;
        max_age: string | number | null;
        max_count: string | number | null;
        truncate_before: string | number | null;
      }
    >(
      `
        SELECT
          EXISTS(
            SELECT 1
            FROM tombstoned_streams tombstones
            WHERE tombstones.stream_name = $1
          ) AS is_tombstoned,
          retention.current_generation,
          retention.current_revision,
          retention.deleted_at,
          retention.max_age,
          retention.max_count,
          retention.truncate_before,
          latest.stream_generation AS fallback_generation,
          latest.stream_revision AS fallback_revision
        FROM (SELECT 1) seed
        LEFT JOIN stream_retention_policies retention
          ON retention.stream_name = $1
        LEFT JOIN LATERAL (
          SELECT stream_generation, stream_revision
          FROM stream_events
          WHERE stream_name = $1
          ORDER BY stream_generation DESC, stream_revision DESC
          LIMIT 1
        ) latest ON TRUE
      `,
      [streamName],
    );

    const row = result.rows[0];

    if (row.is_tombstoned) {
      throw new StreamDeletedServiceError(streamName);
    }

    if (
      row.current_generation !== null &&
      row.current_generation !== undefined
    ) {
      return {
        currentGeneration: toNumber(row.current_generation),
        currentRevision:
          row.current_revision === null ? null : toNumber(row.current_revision),
        deletedAt: row.deleted_at,
        maxAge: row.max_age === null ? null : toNumber(row.max_age),
        maxCount: row.max_count === null ? null : toNumber(row.max_count),
        truncateBefore:
          row.truncate_before === null ? null : toNumber(row.truncate_before),
      };
    }

    if (row.fallback_generation === null || row.fallback_revision === null) {
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
      currentGeneration: toNumber(row.fallback_generation),
      currentRevision: toNumber(row.fallback_revision),
      deletedAt: null,
      maxAge: null,
      maxCount: null,
      truncateBefore: null,
    };
  }

  private async readAllSnapshot(
    options: NonNullable<ReadReq['options']>,
  ): Promise<ReadResp[]> {
    const limit = options.count !== undefined ? toNumber(options.count) : 100;
    const isBackwards = isBackwardsRead(options.readDirection);
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
      appLogger.debug(
        {
          event: 'read_all_empty_snapshot',
          boundary: boundary === null ? 'none' : String(boundary),
          filterApplied: Boolean(options.filter),
        },
        'ReadAll snapshot empty, returning caughtUp',
      );
      return [
        {
          // Emit a terminal frame so clients don't see an entirely empty
          // response stream when $all is read against an empty database.
          caughtUp: {
            timestamp: createTimestamp(),
          },
        },
      ];
    }

    return result.rows.map((row) => this.protocol.mapRowToReadResponse(row));
  }

  private async resolveStreamSubscriptionBoundary(
    streamName: string,
    options: NonNullable<ReadReq['options']>,
  ): Promise<number> {
    const streamState = await this.getStreamRetentionPolicy(
      this.pool,
      streamName,
    );
    if (getReadableCurrentRevision(streamState) === null) {
      return -1;
    }

    const stream = options.stream;
    if (!stream) {
      return -1;
    }

    if (stream.revision !== undefined) {
      return toNumber(stream.revision) - 1;
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
      return toNumber(all.position.commitPosition);
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
          AND ${buildRetentionVisibilityClause('events', 'retention')}
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

    return toNumber(result.rows[0].global_position);
  }

  private async ensureStreamIsNotTombstoned(
    streamName: string,
    client: PoolClient | Pool = this.pool,
  ): Promise<void> {
    if (await this.isStreamTombstoned(streamName, client)) {
      throw new StreamDeletedServiceError(streamName);
    }
  }

  private async lockStreamTransaction(
    client: PoolClient,
    streamName: string,
  ): Promise<void> {
    await client.query('SELECT pg_advisory_xact_lock($1, hashtext($2))', [
      POSTGRES_STREAM_WRITE_LOCK_NAMESPACE,
      streamName,
    ]);
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
          WHERE ${buildScavengeEligibilityClause(
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
      [POSTGRES_SCAVENGE_BATCH_SIZE],
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
      options.revision === undefined ? undefined : toNumber(options.revision);
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
      options.revision === undefined ? undefined : toNumber(options.revision);
    const matches =
      options.any !== undefined ||
      (options.noStream !== undefined && currentRevision === null) ||
      (options.streamExists !== undefined && currentRevision !== null) ||
      (expectedRevision !== undefined && currentRevision === expectedRevision);

    if (matches) {
      return null;
    }

    return this.protocol.createWrongExpectedVersionError(
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
      options.revision === undefined ? undefined : toNumber(options.revision);
    const matches =
      options.any !== undefined ||
      (options.noStream !== undefined && currentRevision === null) ||
      (options.streamExists !== undefined && currentRevision !== null) ||
      (expectedRevision !== undefined && currentRevision === expectedRevision);

    if (matches) {
      return null;
    }

    return this.protocol.createWrongExpectedVersionError(
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
      return toNumber(stream.revision);
    }

    if (stream.start !== undefined) {
      return isBackwardsRead(options.readDirection)
        ? Number.MAX_SAFE_INTEGER
        : 0;
    }

    if (stream.end !== undefined) {
      return isBackwardsRead(options.readDirection)
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
      const commitPosition = toNumber(all.position.commitPosition);
      return isBackwardsRead(options.readDirection)
        ? commitPosition - 1
        : commitPosition;
    }

    if (all.start !== undefined) {
      return isBackwardsRead(options.readDirection)
        ? Number.MAX_SAFE_INTEGER
        : 0;
    }

    if (all.end !== undefined) {
      return isBackwardsRead(options.readDirection)
        ? Number.MAX_SAFE_INTEGER
        : null;
    }

    return null;
  }
}
