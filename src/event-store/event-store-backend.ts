import {
  AppendReq,
  AppendResp,
  BatchAppendReq,
  BatchAppendResp,
  DeleteReq,
  DeleteResp,
  ReadReq,
  ReadResp,
  TombstoneReq,
  TombstoneResp,
} from '../interfaces/streams';
import {
  ScavengeResp,
  StartScavengeReq,
  StopScavengeReq,
} from '../interfaces/operations';

export const EVENT_STORE_BACKEND = Symbol('EVENT_STORE_BACKEND');

export type EventStoreStatsSnapshot = {
  activeScavenges: number;
  currentGlobalPosition: number;
  pgPoolIdleCount: number;
  pgPoolTotalCount: number;
  pgPoolWaitingCount: number;
  retentionPolicyCount: number;
  streamCount: number;
  tombstonedStreamCount: number;
  totalEvents: number;
};

/** Cancellation is advisory: it does not imply a write was rolled back. */
export interface WriteRequestContext {
  signal: AbortSignal;
  /** Absolute Unix time in milliseconds, when finite. */
  deadline?: number;
}

export interface EventStoreBackend {
  getStatsSnapshot(): Promise<EventStoreStatsSnapshot>;
  append(
    messages: AppendReq[],
    context?: WriteRequestContext,
  ): Promise<AppendResp>;
  batchAppend(
    requests: BatchAppendReq[],
    context?: WriteRequestContext,
  ): Promise<BatchAppendResp>;
  read(request: ReadReq): Promise<ReadResp[]>;
  subscribeToStream(
    request: ReadReq,
    isCancelled: () => boolean,
  ): AsyncIterable<ReadResp>;
  delete(
    request: DeleteReq,
    context?: WriteRequestContext,
  ): Promise<DeleteResp>;
  tombstone(
    request: TombstoneReq,
    context?: WriteRequestContext,
  ): Promise<TombstoneResp>;
  startScavenge(request: StartScavengeReq): ScavengeResp;
  stopScavenge(request: StopScavengeReq): ScavengeResp;
}
