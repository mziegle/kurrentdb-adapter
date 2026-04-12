export type PersistedEventRow = {
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

export type StreamRetentionPolicy = {
  currentGeneration: number;
  currentRevision: number | null;
  deletedAt: Date | string | null;
  maxAge: number | null;
  maxCount: number | null;
  truncateBefore: number | null;
};

export type StreamStateRow = {
  current_generation: string | number | null;
  current_revision: string | number | null;
  deleted_at: Date | string | null;
};

export type LongLike = {
  low: number;
  high: number;
  unsigned: boolean;
};

export type ActiveScavenge = {
  cancelled: boolean;
};
