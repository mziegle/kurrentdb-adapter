import { status } from '@grpc/grpc-js';
import { Binary, ClientSession, Db, Document, FindCursor } from 'mongodb';
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
import { CooperativeDispatcher } from './cooperative-dispatcher';
import { GroupDispatchError } from './group-dispatcher';
import { TransactionAdmission } from './transaction-admission';
import { requestSize } from './request-size';
import { runMongoGroup } from './group-runner';
import { endOrdinarySession, OrdinaryCleanup } from './ordinary-cleanup';

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
  private readonly groupDispatcher?: CooperativeDispatcher<
    GroupRequest,
    GroupResult
  >;
  private readonly admission?: TransactionAdmission;
  private shuttingDown = false;
  private readonly liveOrdinary = new Set<Promise<void>>();
  private readonly liveReads = new Set<Promise<void>>();
  private readonly shutdownSignal = new AbortController();
  private closingClient?: Promise<void>;

  private readonly scavengeEnabled =
    process.env.MONGODB_UNSAFE_ENABLE_SCAVENGE === 'true';
  private readonly protocol = new PostgresProtocolService();
  private readonly liveScavengeTransactions = new Set<Promise<unknown>>();
  private unconfirmedScavengeCleanup = false;
  private readonly scavenges = new Map<
    string,
    { cancelled: boolean; controller: AbortController; task?: Promise<void> }
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
      const admission = (this.admission = new TransactionAdmission(
        256,
        16 * 1024 * 1024,
      ));
      this.groupDispatcher = new CooperativeDispatcher(
        {
          maxRequests: 32,
          maxEvents: 32,
          maxBytes: 1024 * 1024,
          maxActiveGroups: 1,
          maxRetainedBytes: 4 * 1024 * 1024,
        },
        1,
        (value) => value.stream,
        async (finalize, ownership) => {
          const release = await admission.acquire(1);
          const batch = finalize();
          if (!batch) {
            release();
            return;
          }
          const outcome = await runMongoGroup(
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
            ownership(),
          );
          if (outcome.cleanup === 'complete') release();
          return outcome;
        },
      );
    }
  }

  async onModuleInit(): Promise<void> {
    await this.infrastructure.connect();
  }
  private shutdownResponse?: Promise<void>;

  onModuleDestroy(): Promise<void> {
    if (this.shutdownResponse) return this.shutdownResponse;
    this.shuttingDown = true;
    this.shutdownSignal.abort();
    for (const scavenge of this.scavenges.values()) {
      scavenge.cancelled = true;
      scavenge.controller.abort();
    }
    const groups = this.groupDispatcher?.shutdown(1000);
    const admission = this.admission?.shutdown();
    const tasks = [...this.scavenges.values()].flatMap((s) =>
      s.task ? [s.task] : [],
    );
    // The timed group shutdown only settles callers. Admission and live work
    // independently prove that the underlying transactions and cleanup ended.
    const drained = Promise.all([
      groups,
      admission,
      ...this.liveOrdinary,
      ...this.liveReads,
      ...tasks,
    ]).then(async () => {
      if (this.liveScavengeTransactions.size || this.unconfirmedScavengeCleanup)
        throw new Error('Scavenge cleanup unconfirmed');
      await (this.closingClient ??= this.infrastructure.close());
    });
    const report = (message: string) => {
      try {
        console.error(message);
      } catch {
        /* Diagnostics cannot affect safety. */
      }
    };

    let timedOut = false;
    void drained.catch(() => {
      if (timedOut) {
        report('MongoDB shutdown or client close failed after timeout');
        process.exitCode = 1;
      }
    });
    this.shutdownResponse = (async () => {
      let timer: ReturnType<typeof setTimeout> | undefined;
      try {
        const complete = await Promise.race([
          drained.then(() => true),
          new Promise<boolean>((resolve) => {
            timer = setTimeout(() => {
              timedOut = true;
              process.exitCode = 1;
              resolve(false);
            }, 1000);
          }),
        ]);
        if (!complete) report('MongoDB shutdown or client close not confirmed');
      } finally {
        if (timer) clearTimeout(timer);
      }
    })();
    return this.shutdownResponse;
  }

  private async trackRead<T>(
    work: (cleanup: OrdinaryCleanup) => Promise<T>,
  ): Promise<T> {
    if (this.shuttingDown) throw new GroupDispatchError('closed');
    let release!: () => void;
    const live = new Promise<void>((resolve) => {
      release = resolve;
    });
    this.liveReads.add(live);
    const cleanup: OrdinaryCleanup = { cleanup: 'complete' };
    try {
      return await work(cleanup);
    } finally {
      if (cleanup.cleanup === 'complete') {
        this.liveReads.delete(live);
        release();
      }
    }
  }

  private async readCursor<T extends Document, R>(
    cursor: FindCursor<T>,
    cleanup: OrdinaryCleanup,
    work: () => Promise<R>,
  ): Promise<R> {
    const previouslyUnconfirmed = cleanup.cleanup === 'unconfirmed';
    cleanup.cleanup = 'unconfirmed';
    try {
      return await work();
    } finally {
      await cursor.close();
      // This only confirms local driver work has settled. The driver suppresses
      // killCursors errors; server-side cursor destruction is not confirmed.
      if (!previouslyUnconfirmed) cleanup.cleanup = 'complete';
    }
  }

  // findOne's internal cursor is only explicitly closed on success.
  private async readOne<T>(
    cleanup: OrdinaryCleanup,
    work: () => Promise<T>,
  ): Promise<T> {
    const uncertain = cleanup.cleanup === 'unconfirmed';
    cleanup.cleanup = 'unconfirmed';
    const result = await work();
    if (!uncertain) cleanup.cleanup = 'complete';
    return result;
  }

  private async pollDelay(): Promise<void> {
    if (this.shuttingDown) return;
    await new Promise<void>((resolve) => {
      const done = () => {
        clearTimeout(timer);
        this.shutdownSignal.signal.removeEventListener('abort', done);
        resolve();
      };
      const timer = setTimeout(done, 100);
      this.shutdownSignal.signal.addEventListener('abort', done, {
        once: true,
      });
    });
  }

  private async admitted<T>(
    bytes: number,
    context: WriteRequestContext | undefined,
    work: (cleanup: OrdinaryCleanup) => Promise<T>,
    externalCleanup?: OrdinaryCleanup,
    trackLive?: (task: Promise<T>, cleanup: OrdinaryCleanup) => void,
  ): Promise<T> {
    if (this.shuttingDown) throw new GroupDispatchError('closed');
    const cleanup = externalCleanup ?? { cleanup: 'complete' };
    const release = await this.admission?.acquire(bytes, context);
    if (this.shuttingDown) {
      release?.();
      throw new GroupDispatchError('closed');
    }
    if (
      context?.signal.aborted ||
      (context?.deadline !== undefined && context.deadline <= Date.now())
    ) {
      release?.();
      throw new GroupDispatchError('cancelled');
    }
    const task = (async () => {
      try {
        return await work(cleanup);
      } catch (error) {
        if (cleanup.cleanup === 'unconfirmed') {
          try {
            console.error('MongoDB ordinary cleanup unconfirmed', error);
          } catch {
            // Logging must not alter the uncertain transaction outcome.
          }
          throw new GroupDispatchError('unknown', error);
        }
        throw error;
      } finally {
        if (cleanup.cleanup === 'complete') release?.();
      }
    })();
    const settled = () =>
      cleanup.cleanup === 'complete'
        ? Promise.resolve()
        : new Promise<void>(() => {});
    const live = task.then(settled, settled);
    this.liveOrdinary.add(live);
    void live.then(() => this.liveOrdinary.delete(live));
    if (context?.retainUntil) {
      context.retainUntil(live);
    }
    trackLive?.(task, cleanup);
    if (!context) return task;
    return new Promise<T>((resolve, reject) => {
      let timer: ReturnType<typeof setTimeout> | undefined;
      let settled = false;
      const clear = () => {
        context.signal.removeEventListener('abort', cancel);
        if (timer !== undefined) clearTimeout(timer);
      };
      const cancel = () => {
        if (settled) return;
        settled = true;
        clear();
        reject(new GroupDispatchError('unknown'));
      };
      const expire = () => {
        if (
          context.signal.aborted ||
          (context.deadline !== undefined && context.deadline <= Date.now())
        ) {
          cancel();
        } else if (context.deadline !== undefined) {
          if (timer !== undefined) clearTimeout(timer);
          timer = setTimeout(
            expire,
            Math.min(context.deadline - Date.now(), 2147483647),
          );
        }
      };
      context.signal.addEventListener('abort', cancel, { once: true });
      expire();
      void task.then(
        (value) => {
          expire();
          if (settled) return;
          settled = true;
          clear();
          resolve(value);
        },
        (error: unknown) => {
          expire();
          if (settled) return;
          settled = true;
          clear();
          // Preserve protocol errors, including non-Error driver rejections.
          // eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors
          reject(error);
        },
      );
    });
  }

  async append(
    messages: AppendReq[],
    context?: WriteRequestContext,
    cleanup?: OrdinaryCleanup,
  ): Promise<AppendResp> {
    if (cleanup) cleanup.cleanup = 'complete';
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
      // Group cleanup is governed by its own ownership protocol.
      if (cleanup) cleanup.cleanup = 'unconfirmed';
      const result = await this.groupDispatcher.enqueue(
        { stream: streamName, options, events },
        1,
        requestSize(messages, 1024 * 1024, true),
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
    return this.admitted(
      this.admission ? requestSize(messages, 16 * 1024 * 1024, true) : 0,
      context,
      async (cleanup) => {
        const client = this.infrastructure.getClient();
        const db = client.db(this.infrastructure.getDatabaseName());
        const session = client.startSession();
        if (cleanup) cleanup.cleanup = 'unconfirmed';
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
          if (!result)
            throw new Error('MongoDB transaction returned no result');
          return result;
        } finally {
          await endOrdinarySession(session, cleanup);
        }
      },
      cleanup,
    );
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
    return this.trackRead((cleanup) => this.readTracked(request, cleanup));
  }

  private async readTracked(
    request: ReadReq,
    cleanup: OrdinaryCleanup,
  ): Promise<ReadResp[]> {
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
      const cursor = this.infrastructure
        .getClient()
        .db(this.infrastructure.getDatabaseName())
        .collection('stream_events')
        .find(filter)
        .sort({ position: backwards ? -1 : 1 })
        .limit(options.count === undefined ? 100 : toNumber(options.count));
      const rows = await this.readCursor(cursor, cleanup, () =>
        cursor.toArray(),
      );
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
    cleanup.cleanup = 'unconfirmed';
    const cursorCleanup: OrdinaryCleanup = { cleanup: 'complete' };
    try {
      return await session.withTransaction(
        async () => {
          const state = await this.readOne(cursorCleanup, () =>
            db
              .collection('streams')
              .findOne({ _id: streamName } as Document, { session }),
          );
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
          const policy = await this.readOne(cursorCleanup, () =>
            db
              .collection('retention')
              .findOne({ _id: streamName } as Document, { session }),
          );
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
          const cursor = db
            .collection('stream_events')
            .find(filter, { session })
            .sort({ revision: backwards ? -1 : 1 })
            .limit(options.count === undefined ? 100 : toNumber(options.count));
          const rows = await this.readCursor(cursor, cursorCleanup, () =>
            cursor.toArray(),
          );
          return rows.map((row) => this.mapRow(row));
        },
        { readConcern: { level: 'snapshot' } },
      );
    } finally {
      await session.endSession();
      cleanup.cleanup = cursorCleanup.cleanup;
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
      const last = await this.trackRead(async (cleanup) => {
        const cursor = events.find().sort({ position: -1 }).limit(1);
        return this.readCursor(cursor, cleanup, () => cursor.next());
      });
      position = last ? safeInteger(last.position) : -1;
    }
    yield { confirmation: { subscriptionId: randomUUID() } };
    let caughtUp = false;
    while (!this.shuttingDown && !isCancelled()) {
      const rows = await this.trackRead(async (cleanup) => {
        const cursor = events
          .find({ ...filter, position: { $gt: position } })
          .sort({ position: 1 })
          .limit(256);
        return this.readCursor(cursor, cleanup, () => cursor.toArray());
      });
      if (rows.length) {
        for (const row of rows) {
          if (this.shuttingDown || isCancelled()) return;
          position = safeInteger(row.position);
          yield this.mapRow(row);
        }
        continue;
      }
      if (!caughtUp) {
        yield { caughtUp: { timestamp: createTimestamp() } };
        caughtUp = true;
      }
      await this.pollDelay();
    }
  }

  async *subscribeToStream(
    request: ReadReq,
    isCancelled: () => boolean,
  ): AsyncGenerator<ReadResp> {
    if (this.shuttingDown) throw new GroupDispatchError('closed');
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
      const state = await this.trackRead((cleanup) =>
        this.readOne(cleanup, () =>
          db.collection('streams').findOne({ _id: streamName } as Document),
        ),
      );
      lastRevision =
        state?.revision === undefined ? -1 : safeInteger(state.revision);
    }
    yield { confirmation: { subscriptionId: randomUUID() } };
    let caughtUp = false;
    while (!this.shuttingDown && !isCancelled()) {
      const batch = await this.trackRead(async (cleanup) => {
        const session = this.infrastructure.getClient().startSession();
        cleanup.cleanup = 'unconfirmed';
        const cursorCleanup: OrdinaryCleanup = { cleanup: 'complete' };
        try {
          return await session.withTransaction(
            async () => {
              const state = await this.readOne(cursorCleanup, () =>
                db
                  .collection('streams')
                  .findOne({ _id: streamName } as Document, { session }),
              );
              if (state?.tombstoned)
                throw new StreamDeletedServiceError(streamName);
              const policy = await this.readOne(cursorCleanup, () =>
                db
                  .collection('retention')
                  .findOne({ _id: streamName } as Document, { session }),
              );
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
              const cursor = events
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
                .limit(256);
              const rows = await this.readCursor(cursor, cursorCleanup, () =>
                cursor.toArray(),
              );
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
        } finally {
          await session.endSession();
          cleanup.cleanup = cursorCleanup.cleanup;
        }
      });
      const rows = batch.rows;
      lastRevision = batch.visibleRevision;
      if (rows.length) {
        for (const row of rows) {
          if (this.shuttingDown || isCancelled()) return;
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
      await this.pollDelay();
    }
  }
  async delete(
    request: DeleteReq,
    context?: WriteRequestContext,
    cleanup?: OrdinaryCleanup,
  ): Promise<DeleteResp> {
    if (cleanup) cleanup.cleanup = 'complete';
    const options = request.options;
    if (!options?.streamIdentifier?.streamName)
      throw new Error('Missing stream identifier');
    const streamName = decodeStreamName(options.streamIdentifier.streamName);
    if (streamName.startsWith('$$'))
      return unsupported('stream metadata and retention');
    return this.admitted(
      this.admission ? requestSize(request, 16 * 1024 * 1024, true) : 0,
      context,
      async (cleanup) => {
        const client = this.infrastructure.getClient();
        const db = client.db(this.infrastructure.getDatabaseName());
        const session = client.startSession();
        if (cleanup) cleanup.cleanup = 'unconfirmed';
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
            if (state?.tombstoned)
              throw new StreamDeletedServiceError(streamName);
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
            await db.collection('streams').updateOne(
              { _id: streamName } as Document,
              {
                $set: {
                  deleted: true,
                  visibleFrom: safeInteger(current + 1),
                },
              },
              { session },
            );
            const position = safeInteger(last!.position);
            return {
              position: { commitPosition: position, preparePosition: position },
            };
          });
        } finally {
          await endOrdinarySession(session, cleanup);
        }
      },
      cleanup,
    );
  }
  async tombstone(
    request: TombstoneReq,
    context?: WriteRequestContext,
    cleanup?: OrdinaryCleanup,
  ): Promise<TombstoneResp> {
    if (cleanup) cleanup.cleanup = 'complete';
    const options = request.options;
    if (!options?.streamIdentifier?.streamName)
      throw new Error('Missing stream identifier');
    const streamName = decodeStreamName(options.streamIdentifier.streamName);
    if (streamName.startsWith('$$'))
      return unsupported('stream metadata and retention');
    return this.admitted(
      this.admission ? requestSize(request, 16 * 1024 * 1024, true) : 0,
      context,
      async (cleanup) => {
        const client = this.infrastructure.getClient();
        const db = client.db(this.infrastructure.getDatabaseName());
        const session = client.startSession();
        if (cleanup) cleanup.cleanup = 'unconfirmed';
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
            if (state?.tombstoned)
              throw new StreamDeletedServiceError(streamName);
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
              (state?.revision === undefined
                ? -1
                : safeInteger(state.revision)) + 1,
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
          await endOrdinarySession(session, cleanup);
        }
      },
      cleanup,
    );
  }
  startScavenge(): ScavengeResp {
    if (this.shuttingDown) throw new GroupDispatchError('closed');
    if (!this.scavengeEnabled) unsupported('scavenging (disabled by default)');
    const scavengeId = randomUUID();
    const state: {
      cancelled: boolean;
      controller: AbortController;
      task?: Promise<void>;
    } = {
      cancelled: false,
      controller: new AbortController(),
    };
    this.scavenges.set(scavengeId, state);
    state.task = this.runScavenge(state)
      .catch((error: unknown) => {
        if (!state.cancelled) console.error('MongoDB scavenge failed', error);
      })
      .finally(() => this.scavenges.delete(scavengeId));
    return { scavengeId, scavengeResult: ScavengeResp_ScavengeResult.Started };
  }
  stopScavenge(request: StopScavengeReq): ScavengeResp {
    const scavengeId = request.options?.scavengeId ?? '';
    const state = this.scavenges.get(scavengeId);
    if (state) {
      state.cancelled = true;
      state.controller.abort();
    }
    return { scavengeId, scavengeResult: ScavengeResp_ScavengeResult.Stopped };
  }

  private async runScavenge(state: {
    cancelled: boolean;
    controller: AbortController;
  }): Promise<void> {
    const client = this.infrastructure.getClient();
    const db = client.db(this.infrastructure.getDatabaseName());
    const cursor = db
      .collection('streams')
      .find({}, { projection: { _id: 1 }, signal: state.controller.signal });
    try {
      for await (const stream of cursor) {
        if (state.cancelled) break;
        const streamName = String(stream._id);
        await this.admitted(
          1,
          { signal: state.controller.signal },
          async (cleanup) => {
            if (state.cancelled) return;
            const session = client.startSession();
            cleanup.cleanup = 'unconfirmed';
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
                  const conditions: Document[] = [
                    { revision: { $lt: before } },
                  ];
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
                        $lt: new Date(
                          Date.now() - Number(policy.maxAge) * 1000,
                        ),
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
              await endOrdinarySession(session, cleanup);
            }
          },
          undefined,
          (task, cleanup) => {
            this.liveScavengeTransactions.add(task);
            void task.then(
              () => {
                if (cleanup.cleanup === 'complete')
                  this.liveScavengeTransactions.delete(task);
              },
              () => {
                if (cleanup.cleanup === 'complete')
                  this.liveScavengeTransactions.delete(task);
              },
            );
          },
        );
      }
    } finally {
      await cursor.close().catch((error: unknown) => {
        this.unconfirmedScavengeCleanup = true;
        throw error;
      });
    }
  }
}
