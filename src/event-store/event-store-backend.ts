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

export interface EventStoreBackend {
  getStatsSnapshot(): Promise<EventStoreStatsSnapshot>;
  append(messages: AppendReq[]): Promise<AppendResp>;
  batchAppend(requests: BatchAppendReq[]): Promise<BatchAppendResp>;
  read(request: ReadReq): Promise<ReadResp[]>;
  subscribeToStream(
    request: ReadReq,
    isCancelled: () => boolean,
  ): AsyncIterable<ReadResp>;
  delete(request: DeleteReq): Promise<DeleteResp>;
  tombstone(request: TombstoneReq): Promise<TombstoneResp>;
  startScavenge(request: StartScavengeReq): ScavengeResp;
  stopScavenge(request: StopScavengeReq): ScavengeResp;
}
