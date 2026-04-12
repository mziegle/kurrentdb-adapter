import { AppendReq_ProposedMessage } from '../interfaces/streams';
import { StreamRetentionPolicy } from './postgres.types';

export function isMetastream(streamName: string): boolean {
  return streamName.startsWith('$$');
}

export function parseMetadataPolicyUpdate(
  streamName: string,
  proposedMessages: AppendReq_ProposedMessage[],
): {
  streamName: string;
  policy: Omit<
    StreamRetentionPolicy,
    'currentGeneration' | 'currentRevision' | 'deletedAt'
  >;
} | null {
  if (!isMetastream(streamName)) {
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

    const payload = parseJsonPayload(message.data);
    policy = {
      maxAge: readMetadataInteger(payload, '$maxAge'),
      maxCount: readMetadataInteger(payload, '$maxCount'),
      truncateBefore: readMetadataInteger(payload, '$tb'),
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

export function buildRetentionVisibilityClause(
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

export function buildScavengeEligibilityClause(
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
      AND NOT ${buildRetentionVisibilityClause(eventsAlias, retentionAlias)}
      AND (
        ${retentionAlias}.current_revision IS NULL
        OR ${eventsAlias}.stream_revision <> ${retentionAlias}.current_revision
      )
    )
  )`;
}

export function getReadableCurrentRevision(
  streamState: Pick<StreamRetentionPolicy, 'currentRevision' | 'deletedAt'>,
): number | null {
  if (streamState.deletedAt) {
    return null;
  }

  return streamState.currentRevision;
}

function parseJsonPayload(
  data: Uint8Array | Buffer | undefined,
): Record<string, unknown> {
  if (!data || data.length === 0) {
    return {};
  }

  const parsed: unknown = JSON.parse(Buffer.from(data).toString('utf8'));
  if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) {
    throw new Error('Stream metadata payload must be a JSON object.');
  }

  return parsed as Record<string, unknown>;
}

function readMetadataInteger(
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
