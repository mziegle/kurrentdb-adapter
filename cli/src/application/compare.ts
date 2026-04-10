import type { EventData } from '../domain/types.js';
import { stringifyCliJson } from '../infrastructure/output/format.js';

export interface ComparisonMismatch {
  index: number;
  reason:
    | 'missing_event'
    | 'event_id_mismatch'
    | 'event_type_mismatch'
    | 'revision_mismatch'
    | 'position_mismatch'
    | 'data_mismatch'
    | 'metadata_mismatch';
  expected?: unknown;
  actual?: unknown;
}

export interface StreamComparisonResult {
  stream: string;
  matches: boolean;
  referenceCount: number;
  adapterCount: number;
  mismatches: ComparisonMismatch[];
}

function stableStringify(value: unknown): string {
  if (value === undefined) {
    return 'undefined';
  }

  return stringifyCliJson(value);
}

function maybePushMismatch(
  mismatches: ComparisonMismatch[],
  index: number,
  reason: ComparisonMismatch['reason'],
  expected: unknown,
  actual: unknown,
): void {
  if (expected !== actual) {
    mismatches.push({ index, reason, expected, actual });
  }
}

export function compareStreams(
  stream: string,
  referenceEvents: EventData[],
  adapterEvents: EventData[],
): StreamComparisonResult {
  const mismatches: ComparisonMismatch[] = [];
  const max = Math.max(referenceEvents.length, adapterEvents.length);

  for (let index = 0; index < max; index += 1) {
    const reference = referenceEvents[index];
    const adapter = adapterEvents[index];

    if (!reference || !adapter) {
      mismatches.push({
        index,
        reason: 'missing_event',
        expected: reference,
        actual: adapter,
      });
      continue;
    }

    maybePushMismatch(mismatches, index, 'event_id_mismatch', reference.eventId, adapter.eventId);
    maybePushMismatch(mismatches, index, 'event_type_mismatch', reference.eventType, adapter.eventType);
    maybePushMismatch(
      mismatches,
      index,
      'revision_mismatch',
      reference.revision?.toString(),
      adapter.revision?.toString(),
    );
    maybePushMismatch(
      mismatches,
      index,
      'position_mismatch',
      reference.position?.toString(),
      adapter.position?.toString(),
    );
    maybePushMismatch(
      mismatches,
      index,
      'data_mismatch',
      stableStringify(reference.data),
      stableStringify(adapter.data),
    );
    maybePushMismatch(
      mismatches,
      index,
      'metadata_mismatch',
      stableStringify(reference.metadata),
      stableStringify(adapter.metadata),
    );
  }

  return {
    stream,
    matches: mismatches.length === 0,
    referenceCount: referenceEvents.length,
    adapterCount: adapterEvents.length,
    mismatches,
  };
}
