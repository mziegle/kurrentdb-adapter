import { appLogger } from './logger';

type GrpcMetadata = {
  get(key: string): unknown[];
  getMap?: () => Record<string, unknown>;
};

type LogBindings = Record<string, unknown>;

type HotPathOptions = {
  detail?: string;
  summary?: string;
  trace?: unknown;
  bindings?: LogBindings;
};

const requestSequenceByLabel = new Map<string, number>();

export function logHotPath(
  label: string,
  { detail, summary, trace, bindings }: HotPathOptions = {},
): void {
  const sequence = nextSequence(label);
  const infoSummary = summary ?? detail;
  const logger = bindings ? appLogger.child(bindings) : appLogger;
  const requestLog = {
    event: 'incoming_request',
    requestLabel: label,
    requestSequence: sequence,
    ...(infoSummary ? { summary: infoSummary } : {}),
  };

  logger.info(
    requestLog,
    infoSummary ? `${label} ${infoSummary}` : `${label} request received`,
  );

  logger.trace(
    {
      ...requestLog,
      detail,
      trace: sanitizeForLog(trace),
    },
    `${label} full request`,
  );
}

export function logStreamRequestMessage(
  label: string,
  message: unknown,
  bindings?: LogBindings,
): void {
  const logger = bindings ? appLogger.child(bindings) : appLogger;
  logger.trace(
    {
      event: 'incoming_request_message',
      requestLabel: label,
      message: sanitizeForLog(message),
    },
    `${label} request message`,
  );
}

export function summarizeGrpcMetadata(
  metadata?: GrpcMetadata,
): string | undefined {
  const fields = extractGrpcMetadata(metadata);
  if (!fields) {
    return undefined;
  }

  const parts = [
    typeof fields.connectionName === 'string'
      ? `connection=${fields.connectionName}`
      : undefined,
    typeof fields.requiresLeader === 'string'
      ? `requiresLeader=${fields.requiresLeader}`
      : undefined,
  ].filter(Boolean);

  return parts.length > 0 ? parts.join(' ') : undefined;
}

export function extractGrpcMetadata(
  metadata?: GrpcMetadata,
): Record<string, unknown> | undefined {
  if (!metadata) {
    return undefined;
  }

  const rawMap = metadata.getMap?.();
  if (rawMap) {
    return Object.fromEntries(
      Object.entries(rawMap).map(([key, value]) => [
        toCamelCase(key),
        normalizeMetadataValue(value),
      ]),
    );
  }

  const connectionName = getFirstMetadataValue(metadata, 'connection-name');
  const requiresLeader = getFirstMetadataValue(metadata, 'requires-leader');
  const fields = {
    ...(connectionName ? { connectionName } : {}),
    ...(requiresLeader ? { requiresLeader } : {}),
  };

  return Object.keys(fields).length > 0 ? fields : undefined;
}

function getFirstMetadataValue(
  metadata: GrpcMetadata,
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

function nextSequence(label: string): number {
  const next = (requestSequenceByLabel.get(label) ?? 0) + 1;
  requestSequenceByLabel.set(label, next);
  return next;
}

function sanitizeForLog(value: unknown, seen = new WeakSet<object>()): unknown {
  if (value === null || value === undefined) {
    return value;
  }

  if (typeof value === 'string') {
    return value.length <= 1_000
      ? value
      : `${value.slice(0, 1_000)}...[truncated]`;
  }

  if (
    typeof value === 'number' ||
    typeof value === 'boolean' ||
    typeof value === 'bigint'
  ) {
    return value;
  }

  if (Buffer.isBuffer(value)) {
    return summarizeBinary(value);
  }

  if (value instanceof Uint8Array) {
    return summarizeBinary(Buffer.from(value));
  }

  if (Array.isArray(value)) {
    return value.map((entry) => sanitizeForLog(entry, seen));
  }

  if (typeof value === 'object') {
    if (seen.has(value)) {
      return '[circular]';
    }

    seen.add(value);
    return Object.fromEntries(
      Object.entries(value).map(([key, entry]) => [
        key,
        sanitizeForLog(entry, seen),
      ]),
    );
  }

  if (typeof value === 'symbol') {
    return value.toString();
  }

  if (typeof value === 'function') {
    return `[function ${value.name || 'anonymous'}]`;
  }

  return Object.prototype.toString.call(value);
}

function summarizeBinary(value: Buffer): {
  type: 'binary';
  length: number;
  utf8Preview?: string;
  hexPreview: string;
} {
  const utf8Preview = value
    .subarray(0, 128)
    .toString('utf8')
    .replace(/\r/g, '\\r')
    .replace(/\n/g, '\\n');

  return {
    type: 'binary',
    length: value.length,
    ...(utf8Preview.trim() ? { utf8Preview } : {}),
    hexPreview: value.subarray(0, 32).toString('hex'),
  };
}

function normalizeMetadataValue(value: unknown): unknown {
  if (Buffer.isBuffer(value)) {
    return value.toString('utf8');
  }

  if (Array.isArray(value)) {
    return value.map((entry) => normalizeMetadataValue(entry));
  }

  return value;
}

function toCamelCase(value: string): string {
  return value.replace(/-([a-z])/g, (_, letter: string) =>
    letter.toUpperCase(),
  );
}
