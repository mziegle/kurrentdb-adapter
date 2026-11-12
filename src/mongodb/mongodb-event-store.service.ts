import { status } from '@grpc/grpc-js';
import { Binary, ClientSession, Db, Document } from 'mongodb';
import { StreamDeletedServiceError } from '../event-store/event-store.errors';
import { randomUUID } from 'node:crypto';
import { isDeepStrictEqual } from 'node:util';
import {
  EventStoreBackend,
  EventStoreStatsSnapshot,
  WriteRequestContext,
} from '../event-store/event-store-backend';
import {
  AppendReq,
  AppendResp,
  BatchAppendReq,
  BatchAppendResp,
  DeleteReq,
  DeleteResp,
  ReadReq,
  ReadReq_Options_FilterOptions,
  ReadResp,
  TombstoneReq,
  TombstoneResp,
} from '../interfaces/streams';
import {
  ScavengeResp,
  ScavengeResp_ScavengeResult,
  StopScavengeReq,
} from '../interfaces/operations';
import { PostgresProtocolService } from '../postgres/postgres-protocol.service';
import {
  createTimestamp,
  decodeStreamName,
  getEventId,
  isBackwardsRead,
  toNumber as toProtocolNumber,
} from '../postgres/postgres-value';
import { MongoInfrastructureService } from './mongodb-infrastructure.service';
import { parseMetadataPolicyUpdate } from '../postgres/postgres-retention';
import { GroupDispatcher } from './group-dispatcher';
import { runMongoGroup } from './group-runner';

type GroupRequest = {
  stream: string;
  options: NonNullable<AppendReq['options']>;
  events: NonNullable<AppendReq['proposedMessage']>[];
};
type GroupResult =
  | { kind: 'result'; value: AppendResp }
  | { kind: 'error'; error: StreamDeletedServiceError };

function safeInteger(value: unknown): number {
  const result = Number(value);
  if (!Number.isSafeInteger(result)) {
    throw Object.assign(
      new Error('MongoDB positions and revisions must be safe integers.'),
      {
        code: status.OUT_OF_RANGE,
      },
    );
  }
  return result;
}

function toNumber(value: Parameters<typeof toProtocolNumber>[0]): number {
  return safeInteger(toProtocolNumber(value));
}

function unsupported(operation: string): never {
  throw Object.assign(
    new Error(`MongoDB backend does not support ${operation}.`),
    { code: status.UNIMPLEMENTED },
  );
}

export class MongoEventStoreService implements EventStoreBackend {
  private readonly infrastructure: MongoInfrastructureService;
  private readonly groupDispatcher?: GroupDispatcher<GroupRequest, GroupResult>;

  private readonly scavengeEnabled =
    process.env.MONGODB_UNSAFE_ENABLE_SCAVENGE === 'true';
  private readonly protocol = new PostgresProtocolService();
  private readonly scavenges = new Map<
    string,
    { cancelled: boolean; task?: Promise<void> }
  >();

  constructor() {
    if (
      process.env.MONGODB_EXPERIMENTAL_GROUP_COMMIT === 'true' &&
      process.env.MONGODB_GROUP_COMMIT_ACKNOWLEDGE_SINGLE_ADAPTER !== 'true'
    ) {
      throw new Error(
        'Experimental MongoDB group commit requires MONGODB_GROUP_COMMIT_ACKNOWLEDGE_SINGLE_ADAPTER=true; this acknowledgement does not enforce single-adapter operation or provide fencing.',
      );
    }
    this.infrastructure = new MongoInfrastructureService(
      process.env.MONGODB_URI ?? 'mongodb://localhost:27017/?replicaSet=rs0',
      process.env.MONGODB_DATABASE ?? 'kurrentdb_adapter',
    );
    if (process.env.MONGODB_EXPERIMENTAL_GROUP_COMMIT === 'true') {
      const client = this.infrastructure.getClient();
      const db = client.db(this.infrastructure.getDatabaseName());
      this.groupDispatcher = new GroupDispatcher(
        {
          maxRequests: 32,
          maxEvents: 32,
          maxBytes: 1024 * 1024,
          maxActiveGroups: 1,
          maxRetainedBytes: 4 * 1024 * 1024,
        },
        1,
        (value) => value.stream,
        (batch) =>
          runMongoGroup(
            client,
            batch,
            (value, session) =>
              this.appendGroupMember(
                session,
                db,
                value.options,
                value.stream,
                value.events,
              ),
            { maxAttempts: 1, maxCommitAttempts: 1 },
          ),
      );
    }
  }

  async onModuleInit(): Promise<void> {
    await this.infrastructure.connect();
  }
  async onModuleDestroy(): Promise<void> {
    await this.groupDispatcher?.shutdown(1000);
    for (const scavenge of this.scavenges.values()) scavenge.cancelled = true;
    await Promise.all(
      [...this.scavenges.values()].flatMap((s) => (s.task ? [s.task] : [])),
    );

    await this.infrastructure.close();
  }

  async append(
    messages: AppendReq[],
    context?: WriteRequestContext,
  ): Promise<AppendResp> {
    const options = messages.find((message) => message.options)?.options;
    if (!options?.streamIdentifier?.streamName)
      throw new Error('Append request must include a stream identifier.');
    if (options.revision !== undefined) toNumber(options.revision);
    const streamName = decodeStreamName(options.streamIdentifier.streamName);
    const events = messages.flatMap((message) =>
      message.proposedMessage ? [message.proposedMessage] : [],
    );
    if (
      this.groupDispatcher &&
      !streamName.startsWith('$') &&
      options.any !== undefined &&
      options.revision === undefined &&
      options.noStream === undefined &&
      options.streamExists === undefined &&
      events.length === 1
    ) {
      const result = await this.groupDispatcher.enqueue(
        { stream: streamName, options, events },
        1,
        Buffer.byteLength(JSON.stringify(messages)),
        context,
      );
      if (result.kind === 'error') throw result.error;
      return result.value;
    }
    const policyUpdate = parseMetadataPolicyUpdate(streamName, events);
    if (policyUpdate) {
      if (policyUpdate.policy.truncateBefore != null)
        safeInteger(policyUpdate.policy.truncateBefore);
      if (policyUpdate.policy.maxCount != null)
        safeInteger(policyUpdate.policy.maxCount);
    }
    const client = this.infrastructure.getClient();
    const db = client.db(this.infrastructure.getDatabaseName());
    const session = client.startSession();
    try {
      const result = await session.withTransaction(() =>
        this.appendInTransaction(
          session,
          db,
          options,
          streamName,
          events,
          policyUpdate,
        ),
      );
      if (!result) throw new Error('MongoDB transaction returned no result');
      return result;
    } finally {
      await session.endSession();
    }
  }

  /** The only isolated member error occurs before event writes. */
  private async appendGroupMember(
    session: ClientSession,
    db: Db,
    options: NonNullable<AppendReq['options']>,
    streamName: string,
    events: NonNullable<AppendReq['proposedMessage']>[],
  ): Promise<
    | { kind: 'result'; value: AppendResp }
    | { kind: 'error'; error: StreamDeletedServiceError }
  > {
    try {
      return {
        kind: 'result',
        value: await this.appendInTransaction(
          session,
          db,
          options,
          streamName,
          events,
          null,
        ),
      };
    } catch (error) {
      if (error instanceof StreamDeletedServiceError) {
        // The helper acquired the global writer fence before checking the stream.
        // Undo only this member's reservation, retaining the transactional lock.
        await db
          .collection('counters')
          .updateOne(
            { _id: 'global' } as Document,
            { $inc: { value: -(events.length || 1) } },
            { session },
          );
        return { kind: 'error', error };
      }
      throw error;
    }
  }

  private async appendInTransaction(
    session: ClientSession,
    db: Db,
    options: NonNullable<AppendReq['options']>,
    streamName: string,
    events: NonNullable<AppendReq['proposedMessage']>[],
    policyUpdate: ReturnType<typeof parseMetadataPolicyUpdate>,
  ): Promise<AppendResp> {
    // One transactional counter serializes writers before inspecting stream state.
    const counter = await db
      .collection('counters')
      .findOneAndUpdate(
        { _id: 'global' } as Document,
        { $inc: { value: events.length || 1 } },
        { upsert: true, returnDocument: 'after', session },
      );
    const state = await db
      .collection('streams')
      .findOne({ _id: streamName } as Document, { session });
    if (state?.tombstoned) throw new StreamDeletedServiceError(streamName);
    const current =
      state?.revision === undefined || state.deleted
        ? null
        : safeInteger(state.revision);
    const expected =
      options.revision === undefined ? undefined : toNumber(options.revision);
    // Only a single initial event proves the original request boundary.
    if (
      options.noStream !== undefined &&
      current !== null &&
      !state?.deleted &&
      (state?.visibleFrom ?? 0) === 0 &&
      events.length === 1 &&
      current === 0
    ) {
      const stored = await db
        .collection('stream_events')
        .find({ streamName, generation: 0 }, { session })
        .sort({ revision: 1 })
        .toArray();
      if (
        stored.length === events.length &&
        stored.every(
          (row, index) =>
            safeInteger(row.revision) === index &&
            row.eventId === getEventId(events[index].id) &&
            isDeepStrictEqual(row.metadata, events[index].metadata ?? {}) &&
            this.toBuffer(row.data as Binary | Buffer).equals(
              Buffer.from(events[index].data ?? []),
            ) &&
            this.toBuffer(row.customMetadata as Binary | Buffer).equals(
              Buffer.from(events[index].customMetadata ?? []),
            ),
        )
      ) {
        const position = safeInteger(stored[stored.length - 1].position);
        return {
          success: {
            currentRevision: current,
            position: {
              commitPosition: position,
              preparePosition: position,
            },
          },
        };
      }
    }
    if (
      !(
        options.any !== undefined ||
        (options.noStream !== undefined && current === null) ||
        (options.streamExists !== undefined && current !== null) ||
        (expected !== undefined && expected === current)
      )
    ) {
      return {
        wrongExpectedVersion: {
          currentRevision:
            current === null
              ? undefined
              : (String(current) as unknown as number),
          currentNoStream: current === null ? {} : undefined,
          expectedRevision:
            expected === undefined
              ? undefined
              : (String(expected) as unknown as number),
          expectedAny: options.any,
          expectedNoStream: options.noStream,
          expectedStreamExists: options.streamExists,
        },
      };
    }
    if (!events.length)
      return {
        success: {
          currentRevision: current ?? undefined,
          noStream: current === null ? {} : undefined,
          noPosition: {},
        },
      };
    const lastPosition = safeInteger(counter!.value);
    const revision =
      state?.revision === undefined ? -1 : safeInteger(state.revision);
    await db.collection('stream_events').insertMany(
      events.map((event, i) => ({
        streamName,
        generation: 0,
        revision: safeInteger(revision + i + 1),
        eventId: getEventId(event.id),
        position: lastPosition - events.length + i + 1,
        createdAt: new Date(),
        metadata: event.metadata ?? {},
        customMetadata: Buffer.from(event.customMetadata ?? []),
        data: Buffer.from(event.data ?? []),
      })),
      { session },
    );
    if (policyUpdate) {
      await db
        .collection('retention')
        .updateOne(
          { _id: policyUpdate.streamName } as Document,
          { $set: policyUpdate.policy },
          { upsert: true, session },
        );
    }
    const lastRevision = safeInteger(revision + events.length);
    await db
      .collection('streams')
      .updateOne(
        { _id: streamName } as Document,
        { $set: { revision: lastRevision, deleted: false } },
        { upsert: true, session },
      );
    return {
      success: {
        currentRevision: lastRevision,
        position: {
          commitPosition: lastPosition,
          preparePosition: lastPosition,
        },
      },
    };
  }

  async read(request: ReadReq): Promise<ReadResp[]> {
    const options = request.options;
    if (!options) throw new Error('Read request must include options.');
    if (options.stream?.revision !== undefined)
      toNumber(options.stream.revision);
    if (options.all?.position) {
      toNumber(options.all.position.commitPosition ?? 0);
      toNumber(options.all.position.preparePosition ?? 0);
    }
    if (options.resolveLinks || (options.filter && !options.all))
      return unsupported('link resolution or filtered stream reads');
    if (options.all) {
      const backwards = isBackwardsRead(options.readDirection);
      const boundary = options.all.position
        ? toNumber(options.all.position.commitPosition ?? 0) -
          (backwards ? 1 : 0)
        : undefined;
      const filter: Document = {};
      if (options.filter)
        Object.assign(filter, this.readFilter(options.filter));
      if (boundary !== undefined)
        filter.position = backwards ? { $lte: boundary } : { $gte: boundary };
      const rows = await this.infrastructure
        .getClient()
        .db(this.infrastructure.getDatabaseName())
        .collection('stream_events')
        .find(filter)
        .sort({ position: backwards ? -1 : 1 })
        .limit(options.count === undefined ? 100 : toNumber(options.count))
        .toArray();
      return rows.length
        ? rows.map((row) => this.mapRow(row))
        : [{ caughtUp: { timestamp: undefined } }];
    }
    if (!options.stream?.streamIdentifier?.streamName)
      return unsupported('reads other than individual streams or $all');
    const streamName = decodeStreamName(
      options.stream.streamIdentifier.streamName,
    );
    const stream = options.stream;
    const client = this.infrastructure.getClient();
    const db = client.db(this.infrastructure.getDatabaseName());
    const session = client.startSession();
    try {
      return await session.withTransaction(
        async () => {
          const state = await db
            .collection('streams')
            .findOne({ _id: streamName } as Document, { session });
          if (state?.tombstoned)
            throw new StreamDeletedServiceError(streamName);
          if (!state || state.deleted)
            return [
              {
                streamNotFound: {
                  streamIdentifier: { streamName: Buffer.from(streamName) },
                },
              },
            ];
          const backwards = isBackwardsRead(options.readDirection);
          const boundary =
            stream.revision === undefined
              ? undefined
              : toNumber(stream.revision);
          const policy = await db
            .collection('retention')
            .findOne({ _id: streamName } as Document, { session });
          const visibleFrom = this.visibleFrom(state, policy);
          const filter: Document = {
            streamName,
            generation: 0,
            revision: { $gte: visibleFrom },
          };
          if (boundary !== undefined)
            filter.revision = backwards
              ? { $gte: visibleFrom, $lte: boundary }
              : { $gte: Math.max(visibleFrom, boundary) };
          if (policy?.maxAge != null)
            filter.createdAt = {
              $gte: new Date(Date.now() - Number(policy.maxAge) * 1000),
            };
          const rows = await db
            .collection('stream_events')
            .find(filter, { session })
            .sort({ revision: backwards ? -1 : 1 })
            .limit(options.count === undefined ? 100 : toNumber(options.count))
            .toArray();
          return rows.map((row) => this.mapRow(row));
        },
        { readConcern: { level: 'snapshot' } },
      );
    } finally {
      await session.endSession();
    }
  }

  private readFilter(filter: ReadReq_Options_FilterOptions): Document {
    const expression = filter.streamIdentifier ?? filter.eventType;
    if (!expression)
      throw new Error(
        'Read filters must target a stream identifier or event type.',
      );
    const prefixes = expression.prefix.filter((prefix) => prefix.length > 0);
    const pattern = prefixes.length
      ? `^(?:${prefixes.map((prefix) => prefix.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')).join('|')})`
      : expression.regex;
    if (!pattern)
      throw new Error(
        'Read filters must include a stream name or event type regex/prefix.',
      );
    return {
      [filter.streamIdentifier ? 'streamName' : 'metadata.type']: {
        $regex: pattern,
      },
    };
  }

  private visibleFrom(state: Document, policy: Document | null): number {
    safeInteger(state.revision ?? -1);
    return safeInteger(
      Math.max(
        safeInteger(state.visibleFrom ?? 0),
        safeInteger(policy?.truncateBefore ?? 0),
        policy?.maxCount == null
          ? 0
          : safeInteger(state.revision ?? -1) -
              safeInteger(policy.maxCount) +
              1,
      ),
    );
  }

  private mapRow(row: Document): ReadResp {
    return this.protocol.mapRowToReadResponse({
      global_position: safeInteger(row.position),
      stream_name: String(row.streamName),
      stream_revision: safeInteger(row.revision),
      event_id: String(row.eventId),
      created_at: row.createdAt as Date,
      metadata: row.metadata as Record<string, string>,
      custom_metadata: this.toBuffer(row.customMetadata as Binary | Buffer),
      data: this.toBuffer(row.data as Binary | Buffer),
    });
  }

  private toBuffer(value: Binary | Buffer): Buffer {
    return Buffer.isBuffer(value) ? value : Buffer.from(value.buffer);
  }
  getStatsSnapshot(): Promise<EventStoreStatsSnapshot> {
    return unsupported('statistics');
  }
  async batchAppend(
    requests: BatchAppendReq[],
    context?: WriteRequestContext,
  ): Promise<BatchAppendResp> {
    const first = requests[0];
    const options = requests.find((request) => request.options)?.options;
    if (!first?.correlationId || !options?.streamIdentifier?.streamName)
      throw new Error('Batch append requires correlation and stream options.');
    let result: AppendResp;
    try {
      result = await this.append(
        [
          {
            options: {
              streamIdentifier: options.streamIdentifier,
              revision: options.streamPosition,
              noStream: options.noStream,
              any: options.any,
              streamExists: options.streamExists,
            },
          },
          ...requests.flatMap((request) =>
            (request.proposedMessages ?? []).map((proposedMessage) => ({
              proposedMessage,
            })),
          ),
        ],
        context,
      );
    } catch (error) {
      if (error instanceof StreamDeletedServiceError) {
        return {
          correlationId: first.correlationId,
          streamIdentifier: options.streamIdentifier,
          streamPosition: options.streamPosition,
          noStream: options.noStream,
          any: options.any,
          streamExists: options.streamExists,
          error: this.protocol.createBatchAppendStreamDeletedStatus(
            options.streamIdentifier.streamName,
          ),
        };
      }
      throw error;
    }
    const common = {
      correlationId: first.correlationId,
      streamIdentifier: options.streamIdentifier,
      streamPosition: options.streamPosition,
      noStream: options.noStream,
      any: options.any,
      streamExists: options.streamExists,
    };
    if (result.success)
      return {
        ...common,
        success: {
          currentRevision:
            result.success.currentRevision ??
            (result.success.noStream ? -1 : undefined),
          position: result.success.position,
        },
      };
    return {
      ...common,
      error: this.protocol.createBatchAppendWrongExpectedVersionStatus(
        options,
        result.wrongExpectedVersion!,
      ),
    };
  }
  private async *subscribeToAll(
    options: NonNullable<ReadReq['options']>,
    isCancelled: () => boolean,
  ): AsyncGenerator<ReadResp> {
    const events = this.infrastructure
      .getClient()
      .db(this.infrastructure.getDatabaseName())
      .collection('stream_events');
    const filter = options.filter ? this.readFilter(options.filter) : {};
    if (options.all?.position) {
      toNumber(options.all.position.commitPosition ?? 0);
      toNumber(options.all.position.preparePosition ?? 0);
    }
    let position = options.all?.position
      ? toNumber(options.all.position.commitPosition ?? 0)
      : -1;
    if (options.all?.end !== undefined) {
      const last = await events.find().sort({ position: -1 }).limit(1).next();
      position = last ? safeInteger(last.position) : -1;
    }
    yield { confirmation: { subscriptionId: randomUUID() } };
    let caughtUp = false;
    while (!isCancelled()) {
      const rows = await events
        .find({ ...filter, position: { $gt: position } })
        .sort({ position: 1 })
        .limit(256)
        .toArray();
      if (rows.length) {
        for (const row of rows) {
          if (isCancelled()) return;
          position = safeInteger(row.position);
          yield this.mapRow(row);
        }
        continue;
      }
      if (!caughtUp) {
        yield { caughtUp: { timestamp: createTimestamp() } };
        caughtUp = true;
      }
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
  }

  async *subscribeToStream(
    request: ReadReq,
    isCancelled: () => boolean,
  ): AsyncGenerator<ReadResp> {
    const options = request.options;
    if (options?.all) {
      if (options.resolveLinks || isBackwardsRead(options.readDirection))
        return unsupported(
          'filtered, backwards or link-resolving subscriptions',
        );
      yield* this.subscribeToAll(options, isCancelled);
      return;
    }
    if (!options?.stream?.streamIdentifier?.streamName)
      return unsupported('$all subscriptions');
    if (
      options.resolveLinks ||
      options.filter ||
      isBackwardsRead(options.readDirection)
    )
      return unsupported('filtered, backwards or link-resolving subscriptions');
    const streamName = decodeStreamName(
      options.stream.streamIdentifier.streamName,
    );
    const db = this.infrastructure
      .getClient()
      .db(this.infrastructure.getDatabaseName());
    const events = db.collection('stream_events');
    let lastRevision =
      options.stream.revision !== undefined
        ? toNumber(options.stream.revision)
        : -1;
    if (options.stream.end !== undefined) {
      const state = await db
        .collection('streams')
        .findOne({ _id: streamName } as Document);
      lastRevision =
        state?.revision === undefined ? -1 : safeInteger(state.revision);
    }
    yield { confirmation: { subscriptionId: randomUUID() } };
    let caughtUp = false;
    while (!isCancelled()) {
      const session = this.infrastructure.getClient().startSession();
      let rows: Document[];
      try {
        const batch = await session.withTransaction(
          async () => {
            const state = await db
              .collection('streams')
              .findOne({ _id: streamName } as Document, { session });
            if (state?.tombstoned)
              throw new StreamDeletedServiceError(streamName);
            const policy = await db
              .collection('retention')
              .findOne({ _id: streamName } as Document, { session });
            const visibleRevision = Math.max(
              lastRevision,
              this.visibleFrom(state ?? {}, policy) - 1,
            );
            if (!state || state.deleted)
              return {
                rows: [],
                visibleRevision: Math.max(
                  lastRevision,
                  safeInteger(state?.visibleFrom ?? 0) - 1,
                ),
              };
            const rows = await events
              .find(
                {
                  streamName,
                  generation: 0,
                  revision: { $gt: visibleRevision },
                  ...(policy?.maxAge == null
                    ? {}
                    : {
                        createdAt: {
                          $gte: new Date(
                            Date.now() - Number(policy.maxAge) * 1000,
                          ),
                        },
                      }),
                },
                { session },
              )
              .sort({ revision: 1 })
              .limit(256)
              .toArray();
            return {
              rows,
              visibleRevision: Math.max(
                lastRevision,
                Math.min(visibleRevision, safeInteger(state.revision)),
              ),
            };
          },
          { readConcern: { level: 'snapshot' } },
        );
        rows = batch.rows;
        lastRevision = batch.visibleRevision;
      } finally {
        await session.endSession();
      }
      if (rows.length) {
        for (const row of rows) {
          if (isCancelled()) return;
          lastRevision = safeInteger(row.revision);
          yield this.mapRow(row);
        }
        continue;
      }
      if (!caughtUp) {
        yield {
          caughtUp: {
            timestamp: createTimestamp(),
            streamRevision: lastRevision >= 0 ? lastRevision : undefined,
          },
        };
        caughtUp = true;
      }
      await new Promise<void>((resolve) => setTimeout(resolve, 100));
    }
  }
  async delete(request: DeleteReq): Promise<DeleteResp> {
    const options = request.options;
    if (!options?.streamIdentifier?.streamName)
      throw new Error('Missing stream identifier');
    const streamName = decodeStreamName(options.streamIdentifier.streamName);
    if (streamName.startsWith('$$'))
      return unsupported('stream metadata and retention');
    const client = this.infrastructure.getClient();
    const db = client.db(this.infrastructure.getDatabaseName());
    const session = client.startSession();
    try {
      return await session.withTransaction(async () => {
        await db
          .collection('counters')
          .updateOne(
            { _id: 'global' } as Document,
            { $inc: { value: 0 } },
            { upsert: true, session },
          );
        const state = await db
          .collection('streams')
          .findOne({ _id: streamName } as Document, { session });
        if (state?.tombstoned) throw new StreamDeletedServiceError(streamName);
        const current =
          state?.revision === undefined || state.deleted
            ? null
            : safeInteger(state.revision);
        const expected =
          options.revision === undefined
            ? undefined
            : toNumber(options.revision);
        if (
          !(
            options.any !== undefined ||
            (options.noStream !== undefined && current === null) ||
            (options.streamExists !== undefined && current !== null) ||
            (expected !== undefined && expected === current)
          )
        ) {
          throw this.protocol.createWrongExpectedVersionError(
            streamName,
            expected,
            current,
          );
        }
        if (current === null) return { noPosition: {} };
        const last = await db
          .collection('stream_events')
          .findOne(
            { streamName, generation: 0, revision: current },
            { session },
          );
        await db
          .collection('streams')
          .updateOne(
            { _id: streamName } as Document,
            { $set: { deleted: true, visibleFrom: safeInteger(current + 1) } },
            { session },
          );
        const position = safeInteger(last!.position);
        return {
          position: { commitPosition: position, preparePosition: position },
        };
      });
    } finally {
      await session.endSession();
    }
  }
  async tombstone(request: TombstoneReq): Promise<TombstoneResp> {
    const options = request.options;
    if (!options?.streamIdentifier?.streamName)
      throw new Error('Missing stream identifier');
    const streamName = decodeStreamName(options.streamIdentifier.streamName);
    if (streamName.startsWith('$$'))
      return unsupported('stream metadata and retention');
    const client = this.infrastructure.getClient();
    const db = client.db(this.infrastructure.getDatabaseName());
    const session = client.startSession();
    try {
      return await session.withTransaction(async () => {
        const counter = await db
          .collection('counters')
          .findOneAndUpdate(
            { _id: 'global' } as Document,
            { $inc: { value: 1 } },
            { upsert: true, returnDocument: 'after', session },
          );
        const state = await db
          .collection('streams')
          .findOne({ _id: streamName } as Document, { session });
        if (state?.tombstoned) throw new StreamDeletedServiceError(streamName);
        const current =
          state?.revision === undefined || state.deleted
            ? null
            : safeInteger(state.revision);
        const expected =
          options.revision === undefined
            ? undefined
            : toNumber(options.revision);
        if (
          !(
            options.any !== undefined ||
            (options.noStream !== undefined && current === null) ||
            (options.streamExists !== undefined && current !== null) ||
            (expected !== undefined && expected === current)
          )
        ) {
          throw this.protocol.createWrongExpectedVersionError(
            streamName,
            expected,
            current,
          );
        }
        const revision = safeInteger(
          (state?.revision === undefined ? -1 : safeInteger(state.revision)) +
            1,
        );
        const position = safeInteger(counter!.value);
        await db.collection('stream_events').insertOne(
          {
            streamName,
            generation: 0,
            revision,
            eventId: randomUUID(),
            position,
            createdAt: new Date(),
            metadata: {
              type: '$streamDeleted',
              'content-type': 'application/octet-stream',
            },
            customMetadata: Buffer.alloc(0),
            data: Buffer.alloc(0),
          },
          { session },
        );
        await db
          .collection('streams')
          .updateOne(
            { _id: streamName } as Document,
            { $set: { revision, tombstoned: true } },
            { upsert: true, session },
          );
        return {
          position: { commitPosition: position, preparePosition: position },
        };
      });
    } finally {
      await session.endSession();
    }
  }
  startScavenge(): ScavengeResp {
    if (!this.scavengeEnabled) unsupported('scavenging (disabled by default)');
    const scavengeId = randomUUID();
    const state: { cancelled: boolean; task?: Promise<void> } = {
      cancelled: false,
    };
    this.scavenges.set(scavengeId, state);
    state.task = this.runScavenge(state)
      .catch((error: unknown) => {
        console.error('MongoDB scavenge failed', error);
      })
      .finally(() => this.scavenges.delete(scavengeId));
    return { scavengeId, scavengeResult: ScavengeResp_ScavengeResult.Started };
  }
  stopScavenge(request: StopScavengeReq): ScavengeResp {
    const scavengeId = request.options?.scavengeId ?? '';
    const state = this.scavenges.get(scavengeId);
    if (state) state.cancelled = true;
    return { scavengeId, scavengeResult: ScavengeResp_ScavengeResult.Stopped };
  }

  private async runScavenge(state: { cancelled: boolean }): Promise<void> {
    const client = this.infrastructure.getClient();
    const db = client.db(this.infrastructure.getDatabaseName());
    const cursor = db
      .collection('streams')
      .find({}, { projection: { _id: 1 } });
    try {
      for await (const stream of cursor) {
        if (state.cancelled) break;
        const streamName = String(stream._id);
        const session = client.startSession();
        try {
          await session.withTransaction(
            async () => {
              // A real write to the same counter document used by all mutations
              // prevents stale policy or stream snapshots from deleting live data.
              await db
                .collection('counters')
                .updateOne(
                  { _id: 'global' } as Document,
                  { $inc: { scavengeEpoch: 1 } },
                  { upsert: true, session },
                );
              const current = await db
                .collection('streams')
                .findOne({ _id: streamName } as Document, { session });
              if (!current || current.revision === undefined) return;
              const policy = await db
                .collection('retention')
                .findOne({ _id: streamName } as Document, { session });
              const last = safeInteger(current.revision);
              const before =
                current.tombstoned || current.deleted
                  ? last
                  : this.visibleFrom(current, policy);
              const conditions: Document[] = [{ revision: { $lt: before } }];
              if (streamName.startsWith('$$')) {
                const latestMetadata = await db
                  .collection('stream_events')
                  .findOne(
                    {
                      streamName,
                      'metadata.type': '$metadata',
                      'metadata.content-type': 'application/json',
                    },
                    { sort: { revision: -1 }, session },
                  );
                if (latestMetadata)
                  conditions.push({
                    'metadata.type': '$metadata',
                    'metadata.content-type': 'application/json',
                    revision: { $lt: safeInteger(latestMetadata.revision) },
                  });
              }
              if (policy?.maxAge != null)
                conditions.push({
                  createdAt: {
                    $lt: new Date(Date.now() - Number(policy.maxAge) * 1000),
                  },
                });
              await db.collection('stream_events').deleteMany(
                {
                  streamName,
                  revision: { $lt: last },
                  $or: conditions,
                },
                { session },
              );
            },
            { readConcern: { level: 'snapshot' } },
          );
        } finally {
          await session.endSession();
        }
      }
    } finally {
      await cursor.close();
    }
  }
}
