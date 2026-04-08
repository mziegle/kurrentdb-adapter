type HotPathState = {
  count: number;
  pending: number;
  lastLogAt: number;
};

const hotPathStates = new Map<string, HotPathState>();

type HotPathOptions = {
  burst?: number;
  intervalMs?: number;
  detail?: string;
};

export function logHotPath(
  label: string,
  { burst = 3, intervalMs = 5_000, detail }: HotPathOptions = {},
): void {
  const now = Date.now();
  const state = hotPathStates.get(label) ?? {
    count: 0,
    pending: 0,
    lastLogAt: 0,
  };

  state.count += 1;
  state.pending += 1;

  if (state.count <= burst) {
    state.pending = 0;
    state.lastLogAt = now;
    hotPathStates.set(label, state);
    console.info(formatImmediateLog(label, state.count, detail));
    return;
  }

  if (now - state.lastLogAt >= intervalMs) {
    const elapsed = state.lastLogAt === 0 ? intervalMs : now - state.lastLogAt;
    const pending = state.pending;
    state.pending = 0;
    state.lastLogAt = now;
    hotPathStates.set(label, state);
    console.info(formatSummaryLog(label, pending, elapsed, detail));
    return;
  }

  hotPathStates.set(label, state);
}

function formatImmediateLog(
  label: string,
  count: number,
  detail?: string,
): string {
  return detail
    ? `[debug] ${label} #${count} ${detail}`
    : `[debug] ${label} #${count}`;
}

function formatSummaryLog(
  label: string,
  count: number,
  elapsedMs: number,
  detail?: string,
): string {
  const elapsedSeconds = (elapsedMs / 1_000).toFixed(1);
  const base = `[debug] ${label} x${count} in ${elapsedSeconds}s`;
  return detail ? `${base} latest=${detail}` : base;
}

export function summarizeGrpcMetadata(metadata?: {
  get(key: string): unknown[];
}): string | undefined {
  if (!metadata) {
    return undefined;
  }

  const connectionName = getFirstMetadataValue(metadata, 'connection-name');
  const requiresLeader = getFirstMetadataValue(metadata, 'requires-leader');

  const parts = [
    connectionName ? `connection=${connectionName}` : undefined,
    requiresLeader ? `requiresLeader=${requiresLeader}` : undefined,
  ].filter(Boolean);

  return parts.length > 0 ? parts.join(' ') : undefined;
}

function getFirstMetadataValue(
  metadata: { get(key: string): unknown[] },
  key: string,
): string | undefined {
  const [value] = metadata.get(key);

  if (typeof value === 'string') {
    return value;
  }

  if (Buffer.isBuffer(value)) {
    return value.toString('utf8');
  }

  return undefined;
}
