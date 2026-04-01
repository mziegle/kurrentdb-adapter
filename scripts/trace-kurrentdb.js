const { createHash } = require('node:crypto');
const { createServer, Socket } = require('node:net');
const path = require('node:path');
const loader = require('@grpc/proto-loader');
const zlib = require('node:zlib');

const publicPort = Number(process.env.TRACE_PROXY_PORT ?? '2115');
const publicHost = process.env.TRACE_PROXY_HOST ?? '127.0.0.1';
const upstreamPort = Number(process.env.TRACE_UPSTREAM_PORT ?? '2114');
const upstreamHost = process.env.TRACE_UPSTREAM_HOST ?? '127.0.0.1';
const maxBodyBytes = Number(process.env.TRACE_MAX_BODY_BYTES ?? '4096');
const dedupeWindowMs = Number(process.env.TRACE_DEDUPE_WINDOW_MS ?? '1000');
const traceVerbosity = (process.env.TRACE_VERBOSITY ?? 'info').toLowerCase();
const useDefaultSuppressions =
  process.env.TRACE_USE_DEFAULT_SUPPRESSIONS !== '0';
const suppressedHttpPaths = parseCsvSet(
  process.env.TRACE_SUPPRESS_HTTP_PATHS,
  useDefaultSuppressions ? ['/gossip', '/stats'] : [],
);
const suppressedHttp2FrameTypes = parseCsvSet(
  process.env.TRACE_SUPPRESS_HTTP2_FRAME_TYPES,
  useDefaultSuppressions ? ['settings', 'window_update', 'ping'] : [],
);
const suppressHttp1Headers = process.env.TRACE_SUPPRESS_HTTP1_HEADERS === '1';
const suppressHttp1Bodies = process.env.TRACE_SUPPRESS_HTTP1_BODIES === '1';

const HTTP2_CLIENT_PREFACE = Buffer.from('PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n');

const HTTP2_FRAME_TYPES = {
  0x0: 'DATA',
  0x1: 'HEADERS',
  0x2: 'PRIORITY',
  0x3: 'RST_STREAM',
  0x4: 'SETTINGS',
  0x5: 'PUSH_PROMISE',
  0x6: 'PING',
  0x7: 'GOAWAY',
  0x8: 'WINDOW_UPDATE',
  0x9: 'CONTINUATION',
};

const HTTP2_SETTINGS = {
  0x1: 'HEADER_TABLE_SIZE',
  0x2: 'ENABLE_PUSH',
  0x3: 'MAX_CONCURRENT_STREAMS',
  0x4: 'INITIAL_WINDOW_SIZE',
  0x5: 'MAX_FRAME_SIZE',
  0x6: 'MAX_HEADER_LIST_SIZE',
  0x8: 'ENABLE_CONNECT_PROTOCOL',
};

const grpcMethodDescriptors = loadGrpcMethodDescriptors();

function previewAscii(buffer, length = 96) {
  return buffer
    .subarray(0, length)
    .toString('utf8')
    .replace(/\r/g, '\\r')
    .replace(/\n/g, '\\n');
}

function previewHex(buffer, length = 24) {
  return buffer.subarray(0, length).toString('hex');
}

function tryParseJson(text) {
  try {
    return JSON.stringify(JSON.parse(text), null, 2);
  } catch {
    return text;
  }
}

function truncateText(text, maxLength = maxBodyBytes) {
  if (text.length <= maxLength) {
    return text;
  }

  return `${text.slice(0, maxLength)}\n... [truncated ${text.length - maxLength} chars]`;
}

function createSignature(parts) {
  return createHash('sha1').update(parts.join('\n')).digest('hex');
}

function resolveProtoDir() {
  const candidates = [
    path.join(process.cwd(), 'proto/Grpc'),
    path.join(__dirname, '../proto/Grpc'),
    path.join(__dirname, 'proto/Grpc'),
  ];

  for (const candidate of candidates) {
    try {
      require('node:fs').accessSync(path.join(candidate, 'streams.proto'));
      return candidate;
    } catch {}
  }

  throw new Error('Could not locate proto/Grpc/streams.proto');
}

function loadGrpcMethodDescriptors() {
  const protoDir = resolveProtoDir();
  const definition = loader.loadSync(
    [
      path.join(protoDir, 'streams.proto'),
      path.join(protoDir, 'gossip.proto'),
      path.join(protoDir, 'persistent.proto'),
      path.join(protoDir, 'serverfeatures.proto'),
      path.join(protoDir, 'monitoring.proto'),
      path.join(protoDir, 'operations.proto'),
      path.join(protoDir, 'users.proto'),
    ],
    {
      includeDirs: [protoDir],
      keepCase: true,
      longs: Number,
      defaults: true,
      oneofs: true,
    },
  );

  const descriptors = [];
  for (const [serviceName, serviceDefinition] of Object.entries(definition)) {
    if (!serviceDefinition || typeof serviceDefinition !== 'object') {
      continue;
    }

    for (const [methodName, methodDefinition] of Object.entries(serviceDefinition)) {
      if (
        !methodDefinition ||
        typeof methodDefinition !== 'object' ||
        typeof methodDefinition.requestDeserialize !== 'function' ||
        typeof methodDefinition.responseDeserialize !== 'function'
      ) {
        continue;
      }

      descriptors.push({
        serviceName,
        methodName,
        label: `${serviceName}.${methodName}`,
        path: methodDefinition.path,
        requestDeserialize: methodDefinition.requestDeserialize,
        requestSerialize: methodDefinition.requestSerialize,
        responseDeserialize: methodDefinition.responseDeserialize,
        responseSerialize: methodDefinition.responseSerialize,
      });
    }
  }

  return descriptors;
}

function parseCsvSet(value, fallback = []) {
  const source = value ?? fallback.join(',');
  return new Set(
    source
      .split(',')
      .map((part) => part.trim().toLowerCase())
      .filter(Boolean),
  );
}

function formatBody(bodyBuffer, headers) {
  if (!bodyBuffer.length) {
    return '';
  }

  const contentTypeHeader = headers['content-type'];
  const contentType = Array.isArray(contentTypeHeader)
    ? contentTypeHeader[0]
    : contentTypeHeader;
  const text = bodyBuffer.toString('utf8');

  if (typeof contentType === 'string' && contentType.includes('application/json')) {
    return truncateText(tryParseJson(text));
  }

  return truncateText(text);
}

function normalizeHeaders(headerLines) {
  const headers = {};

  for (const line of headerLines) {
    const separatorIndex = line.indexOf(':');
    if (separatorIndex === -1) {
      continue;
    }

    const name = line.slice(0, separatorIndex).trim().toLowerCase();
    const value = line.slice(separatorIndex + 1).trim();
    if (!name) {
      continue;
    }

    if (headers[name] === undefined) {
      headers[name] = value;
      continue;
    }

    if (Array.isArray(headers[name])) {
      headers[name].push(value);
      continue;
    }

    headers[name] = [headers[name], value];
  }

  return headers;
}

const pendingDedupLogs = new Map();

function logTrace(lines, dedupeKey, summaryLabel) {
  if (!dedupeKey || !summaryLabel) {
    console.info(lines.join('\n'));
    return;
  }

  const existing = pendingDedupLogs.get(dedupeKey);
  if (!existing) {
    console.info(lines.join('\n'));
    pendingDedupLogs.set(dedupeKey, {
      count: 1,
      firstSeenAt: Date.now(),
      lastSeenAt: Date.now(),
      summaryLabel,
      timer: setTimeout(() => {
        flushDedupLog(dedupeKey);
      }, dedupeWindowMs),
    });
    return;
  }

  existing.count += 1;
  existing.lastSeenAt = Date.now();
  clearTimeout(existing.timer);
  existing.timer = setTimeout(() => {
    flushDedupLog(dedupeKey);
  }, dedupeWindowMs);
}

function isDebugVerbosity() {
  return traceVerbosity === 'debug';
}

function flushDedupLog(dedupeKey) {
  const entry = pendingDedupLogs.get(dedupeKey);
  if (!entry) {
    return;
  }

  pendingDedupLogs.delete(dedupeKey);
  if (entry.count <= 1) {
    return;
  }

  const durationSeconds = ((entry.lastSeenAt - entry.firstSeenAt) / 1000).toFixed(1);
  console.info(`[trace] repeated ${entry.summaryLabel} x${entry.count - 1} in ${durationSeconds}s`);
}

function flushAllDedupLogs() {
  for (const key of [...pendingDedupLogs.keys()]) {
    const entry = pendingDedupLogs.get(key);
    if (entry) {
      clearTimeout(entry.timer);
    }
    flushDedupLog(key);
  }
}

function isHttp1StartLine(line, direction) {
  if (direction === 'client->upstream') {
    return /^(GET|POST|PUT|PATCH|DELETE|HEAD|OPTIONS) .+ HTTP\/1\.[01]$/.test(line);
  }

  return /^HTTP\/1\.[01] \d{3} /.test(line);
}

function isLikelyHttp2(buffer, direction) {
  if (direction === 'client->upstream') {
    return buffer.subarray(0, HTTP2_CLIENT_PREFACE.length).equals(HTTP2_CLIENT_PREFACE);
  }

  return buffer.length >= 9;
}

function extractHttp1MethodAndPath(startLine) {
  const parts = startLine.split(' ');
  if (parts.length < 2) {
    return {};
  }

  return {
    method: parts[0],
    path: parts[1],
  };
}

function shouldSuppressHttp1(path) {
  if (!path) {
    return false;
  }

  return suppressedHttpPaths.has(path.toLowerCase());
}

function shouldSuppressHttp2(frameType) {
  return suppressedHttp2FrameTypes.has(frameType.toLowerCase());
}

class Http1MessageParser {
  constructor(direction, clientAddress, context) {
    this.direction = direction;
    this.clientAddress = clientAddress;
    this.context = context;
    this.buffer = Buffer.alloc(0);
  }

  push(chunk) {
    this.buffer = Buffer.concat([this.buffer, chunk]);

    while (true) {
      const headerEndIndex = this.buffer.indexOf('\r\n\r\n');
      if (headerEndIndex === -1) {
        return;
      }

      const headerText = this.buffer.subarray(0, headerEndIndex).toString('utf8');
      const [startLine = '', ...headerLines] = headerText.split('\r\n');
      const headers = normalizeHeaders(headerLines);
      const contentLengthValue = headers['content-length'];
      const contentLength = Number(
        Array.isArray(contentLengthValue) ? contentLengthValue[0] : contentLengthValue ?? 0,
      );
      const messageLength = headerEndIndex + 4 + contentLength;

      if (this.buffer.length < messageLength) {
        return;
      }

      const bodyBuffer = this.buffer.subarray(headerEndIndex + 4, messageLength);
      this.buffer = this.buffer.subarray(messageLength);
      logHttp1Message(
        this.direction,
        this.clientAddress,
        startLine,
        headers,
        bodyBuffer,
        this.context,
      );
    }
  }
}

class Http2FrameParser {
  constructor(direction, clientAddress, context) {
    this.direction = direction;
    this.clientAddress = clientAddress;
    this.context = context;
    this.buffer = Buffer.alloc(0);
    this.prefaceHandled = direction !== 'client->upstream';
  }

  push(chunk) {
    this.buffer = Buffer.concat([this.buffer, chunk]);

    if (!this.prefaceHandled) {
      if (this.buffer.length < HTTP2_CLIENT_PREFACE.length) {
        return;
      }

      if (this.buffer.subarray(0, HTTP2_CLIENT_PREFACE.length).equals(HTTP2_CLIENT_PREFACE)) {
        if (isDebugVerbosity()) {
          console.info(`[trace] ${this.direction} ${this.clientAddress} HTTP/2 client preface`);
        }
        this.buffer = this.buffer.subarray(HTTP2_CLIENT_PREFACE.length);
      }

      this.prefaceHandled = true;
    }

    while (this.buffer.length >= 9) {
      const frameLength =
        (this.buffer[0] << 16) | (this.buffer[1] << 8) | this.buffer[2];
      const totalFrameLength = 9 + frameLength;

      if (this.buffer.length < totalFrameLength) {
        return;
      }

      const type = this.buffer[3];
      const flags = this.buffer[4];
      const streamId = this.buffer.readUInt32BE(5) & 0x7fffffff;
      const payload = this.buffer.subarray(9, totalFrameLength);

      logHttp2Frame(this.direction, this.clientAddress, {
        type,
        flags,
        streamId,
        payload,
      }, this.context);

      this.buffer = this.buffer.subarray(totalFrameLength);
    }
  }
}

function logHttp1Message(
  direction,
  clientAddress,
  startLine,
  headers,
  bodyBuffer,
  context,
) {
  let requestMethod;
  let requestPath;

  if (direction === 'client->upstream') {
    const parsed = extractHttp1MethodAndPath(startLine);
    requestMethod = parsed.method;
    requestPath = parsed.path;

    if (requestMethod || requestPath) {
      context.http1RequestQueue.push({
        method: requestMethod,
        path: requestPath,
      });
    }
  } else {
    const request = context.http1RequestQueue.shift();
    requestMethod = request?.method;
    requestPath = request?.path;
  }

  if (shouldSuppressHttp1(requestPath)) {
    return;
  }

  const bodyText = formatBody(bodyBuffer, headers);
  const contentTypeHeader = headers['content-type'];
  const contentType = Array.isArray(contentTypeHeader)
    ? contentTypeHeader[0]
    : contentTypeHeader ?? 'unknown';
  const lines = [
    `[trace] ${direction} ${clientAddress} ${startLine} content-type=${contentType} bytes=${bodyBuffer.length}`,
  ];

  if (
    isDebugVerbosity() &&
    !suppressHttp1Headers &&
    Object.keys(headers).length > 0
  ) {
    lines.push(`[trace] ${direction} ${clientAddress} headers=${JSON.stringify(headers)}`);
  }

  if (!suppressHttp1Bodies && bodyText) {
    lines.push(`[trace] ${direction} ${clientAddress} body=\n${bodyText}`);
  }

  const dedupeKey = createSignature([
    'http1',
    direction,
    requestMethod ?? '',
    requestPath ?? '',
    startLine,
    JSON.stringify(headers),
    bodyBuffer.toString('base64'),
  ]);
  const summaryLabel =
    direction === 'client->upstream'
      ? `${direction} ${requestMethod ?? '?'} ${requestPath ?? startLine}`
      : `${direction} ${requestMethod ?? '?'} ${requestPath ?? '?'} -> ${startLine}`;
  logTrace(lines, dedupeKey, summaryLabel);
}

function logHttp2Frame(direction, clientAddress, frame, context) {
  const frameType = HTTP2_FRAME_TYPES[frame.type] ?? `UNKNOWN(${frame.type})`;
  if (shouldSuppressHttp2(frameType)) {
    return;
  }

  const prefix = `[trace] ${direction} ${clientAddress} HTTP/2 ${frameType} stream=${frame.streamId} flags=0x${frame.flags
    .toString(16)
    .padStart(2, '0')} length=${frame.payload.length}`;
  const details = [];

  if (frame.type === 0x0) {
    const grpcInfo = describeGrpcPayload(
      direction,
      frame.streamId,
      frame.payload,
      context,
    );
    if (grpcInfo.length > 0) {
      details.push(...grpcInfo);
    }
  } else if (frame.type === 0x4) {
    details.push(describeSettingsFrame(frame.flags, frame.payload));
  } else if (frame.type === 0x6) {
    details.push(`ack=${Boolean(frame.flags & 0x1)}`);
  } else if (frame.type === 0x7) {
    details.push(describeGoawayFrame(frame.payload));
  } else if (frame.type === 0x8 && frame.payload.length >= 4) {
    details.push(`windowSizeIncrement=${frame.payload.readUInt32BE(0) & 0x7fffffff}`);
  } else if (frame.type === 0x3 && frame.payload.length >= 4) {
    details.push(`errorCode=${frame.payload.readUInt32BE(0)}`);
  } else if (frame.type === 0x1 || frame.type === 0x9) {
    if (isDebugVerbosity()) {
      details.push(`headerBlockBytes=${frame.payload.length}`);
    }
  }

  if (details.length === 0 && frame.payload.length > 0 && isDebugVerbosity()) {
    details.push(`hex=${previewHex(frame.payload)}`);
  }

  if (!isDebugVerbosity()) {
    if (
      (frame.type === 0x1 || frame.type === 0x9) &&
      details.length === 0
    ) {
      return;
    }

    if (frame.type === 0x0 && details.every((detail) => shouldOmitGrpcDetailInInfo(detail))) {
      return;
    }
  }

  const lines = [
    prefix,
    ...details.map((detail) => `[trace] ${direction} ${clientAddress} ${detail}`),
  ];
  const dedupeKey = createSignature([
    'http2',
    direction,
    String(frame.type),
    String(frame.flags),
    String(frame.streamId),
    frame.payload.toString('base64'),
  ]);
  logTrace(lines, dedupeKey, `${direction} HTTP/2 ${frameType} stream=${frame.streamId}`);
}

function describeGrpcPayload(direction, streamId, payload, context) {
  const messages = parseGrpcMessages(payload);
  if (messages.length === 0) {
    return isDebugVerbosity() ? [`dataPreview=${previewHex(payload)}`] : [];
  }

  const details = [];
  for (const [index, message] of messages.entries()) {
    const labelPrefix =
      messages.length > 1 ? `grpc[${index}]` : 'grpc';
    details.push(
      `${labelPrefix} compressed=${message.compressedFlag} messageLength=${message.message.length}`,
    );

    const decompressed = decompressGrpcMessage(
      message.message,
      message.compressedFlag,
    );
    if (!decompressed.ok) {
      details.push(`${labelPrefix} decodeSkipped=${decompressed.reason}`);
      continue;
    }

    const decoded = decodeGrpcMessage(
      direction,
      streamId,
      decompressed.message,
      context,
    );
    if (decoded) {
      details.push(
        `${labelPrefix} method=${decoded.label} body=\n${truncateText(formatDecodedMessage(decoded.value), 2000)}`,
      );
      continue;
    }

    details.push(
      `${labelPrefix} undecoded hex=${previewHex(decompressed.message, 64)}`,
    );
  }

  return details;
}

function shouldOmitGrpcDetailInInfo(detail) {
  return (
    detail.startsWith('grpc compressed=') ||
    detail.startsWith('grpc[0] compressed=') ||
    detail.startsWith('grpc[1] compressed=') ||
    detail.startsWith('grpc[2] compressed=') ||
    detail.startsWith('grpc undecoded hex=') ||
    detail.startsWith('grpc[0] undecoded hex=') ||
    detail.startsWith('grpc[1] undecoded hex=') ||
    detail.startsWith('grpc[2] undecoded hex=') ||
    detail.startsWith('dataPreview=')
  );
}

function parseGrpcMessages(payload) {
  const messages = [];
  let offset = 0;

  while (offset + 5 <= payload.length) {
    const compressedFlag = payload[offset];
    const messageLength = payload.readUInt32BE(offset + 1);
    const messageStart = offset + 5;
    const messageEnd = messageStart + messageLength;

    if (
      (compressedFlag !== 0 && compressedFlag !== 1) ||
      messageEnd > payload.length
    ) {
      return [];
    }

    messages.push({
      compressedFlag,
      message: payload.subarray(messageStart, messageEnd),
    });
    offset = messageEnd;
  }

  return offset === payload.length ? messages : [];
}

function decompressGrpcMessage(message, compressedFlag) {
  if (compressedFlag === 0) {
    return {
      ok: true,
      message,
    };
  }

  try {
    return {
      ok: true,
      message: zlib.gunzipSync(message),
    };
  } catch {}

  try {
    return {
      ok: true,
      message: zlib.inflateSync(message),
    };
  } catch {}

  try {
    return {
      ok: true,
      message: zlib.brotliDecompressSync(message),
    };
  } catch {}

  return {
    ok: false,
    reason: 'compressed message uses unsupported encoding',
  };
}

function decodeGrpcMessage(direction, streamId, message, context) {
  const knownMethod = context.http2StreamMethods.get(streamId);
  if (knownMethod) {
    try {
      const value =
        direction === 'client->upstream'
          ? knownMethod.requestDeserialize(message)
          : knownMethod.responseDeserialize(message);
      return {
        label: knownMethod.label,
        value,
      };
    } catch {}
  }

  if (direction !== 'client->upstream') {
    return undefined;
  }

  const candidates = [];
  for (const descriptor of grpcMethodDescriptors) {
    try {
      const value = descriptor.requestDeserialize(message);
      const roundTrip = descriptor.requestSerialize(value);
      if (!Buffer.from(roundTrip).equals(message)) {
        continue;
      }

      candidates.push({
        descriptor,
        value,
        score: scoreDecodedValue(value),
      });
    } catch {}
  }

  candidates.sort((left, right) => right.score - left.score);
  const [best] = candidates;
  if (!best || best.score === 0) {
    return undefined;
  }

  context.http2StreamMethods.set(streamId, best.descriptor);
  return {
    label: best.descriptor.label,
    value: best.value,
  };
}

function scoreDecodedValue(value) {
  if (value === null || value === undefined) {
    return 0;
  }

  if (Buffer.isBuffer(value) || value instanceof Uint8Array) {
    return value.length > 0 ? 1 : 0;
  }

  if (Array.isArray(value)) {
    return value.reduce((sum, item) => sum + scoreDecodedValue(item), 0);
  }

  if (typeof value === 'object') {
    return Object.values(value).reduce((sum, item) => sum + scoreDecodedValue(item), 0);
  }

  if (typeof value === 'string') {
    return value.length > 0 ? 1 : 0;
  }

  if (typeof value === 'number') {
    return value !== 0 ? 1 : 0;
  }

  if (typeof value === 'boolean') {
    return value ? 1 : 0;
  }

  return 0;
}

function formatDecodedMessage(value) {
  const normalizedValue =
    traceVerbosity === 'debug' ? value : simplifyDecodedValue(value);

  return JSON.stringify(
    normalizedValue,
    (_key, currentValue) => {
      if (Buffer.isBuffer(currentValue) || currentValue instanceof Uint8Array) {
        const buffer = Buffer.from(currentValue);
        if (buffer.length === 0) {
          return 'Uint8Array(0)';
        }

        const ascii = buffer.toString('utf8');
        const printable = /^[\x20-\x7e]+$/.test(ascii);
        return printable
          ? `Uint8Array(${buffer.length}) ascii=${JSON.stringify(ascii)}`
          : `Uint8Array(${buffer.length}) hex=${buffer.toString('hex').slice(0, 96)}`;
      }

      return currentValue;
    },
    2,
  );
}

function simplifyDecodedValue(value, key = '') {
  if (value === null || value === undefined) {
    return value;
  }

  if (Buffer.isBuffer(value) || value instanceof Uint8Array) {
    return simplifyBinaryValue(Buffer.from(value), key);
  }

  if (Array.isArray(value)) {
    return value.map((item) => simplifyDecodedValue(item));
  }

  if (typeof value === 'object') {
    const output = {};
    for (const [childKey, childValue] of Object.entries(value)) {
      output[childKey] = simplifyDecodedValue(childValue, childKey);
    }
    return output;
  }

  return value;
}

function simplifyBinaryValue(buffer, key) {
  if (buffer.length === 0) {
    return '';
  }

  const utf8 = buffer.toString('utf8');
  const isMostlyPrintable = /^[\x09\x0a\x0d\x20-\x7e]+$/.test(utf8);
  const lowerKey = key.toLowerCase();

  if (
    isMostlyPrintable &&
    (lowerKey.includes('data') ||
      lowerKey.includes('metadata') ||
      lowerKey.includes('name'))
  ) {
    if (
      (utf8.startsWith('{') && utf8.endsWith('}')) ||
      (utf8.startsWith('[') && utf8.endsWith(']'))
    ) {
      try {
        return JSON.parse(utf8);
      } catch {}
    }

    return utf8;
  }

  if (isMostlyPrintable) {
    return utf8;
  }

  return `hex:${buffer.toString('hex')}`;
}

function describeSettingsFrame(flags, payload) {
  if (flags & 0x1) {
    return 'ack=true';
  }

  if (payload.length % 6 !== 0) {
    return `settingsBytes=${payload.length}`;
  }

  const entries = [];
  for (let offset = 0; offset < payload.length; offset += 6) {
    const id = payload.readUInt16BE(offset);
    const value = payload.readUInt32BE(offset + 2);
    entries.push(`${HTTP2_SETTINGS[id] ?? `SETTING_${id}`}=${value}`);
  }

  return `settings=${entries.join(', ')}`;
}

function describeGoawayFrame(payload) {
  if (payload.length < 8) {
    return `goawayBytes=${payload.length}`;
  }

  const lastStreamId = payload.readUInt32BE(0) & 0x7fffffff;
  const errorCode = payload.readUInt32BE(4);
  const debugData =
    payload.length > 8 ? truncateText(payload.subarray(8).toString('utf8'), 256) : '';

  return debugData
    ? `lastStreamId=${lastStreamId} errorCode=${errorCode} debug=${JSON.stringify(debugData)}`
    : `lastStreamId=${lastStreamId} errorCode=${errorCode}`;
}

function createProtocolSniffer(direction, clientAddress, context) {
  let parser;

  return {
    push(chunk) {
      if (!parser) {
        const firstLine = chunk.toString('utf8').split('\r\n', 1)[0] ?? '';

        if (isHttp1StartLine(firstLine, direction)) {
          parser = new Http1MessageParser(direction, clientAddress, context);
        } else if (isLikelyHttp2(chunk, direction)) {
          parser = new Http2FrameParser(direction, clientAddress, context);
        } else {
          console.info(
            `[trace] ${direction} ${clientAddress} unknown ascii="${previewAscii(chunk)}" hex=${previewHex(chunk)}`,
          );
          parser = {
            push() {},
          };
        }
      }

      parser.push(chunk);
    },
  };
}

const server = createServer((clientSocket) => {
  const upstreamSocket = new Socket();
  const clientAddress = `${clientSocket.remoteAddress ?? 'unknown'}:${clientSocket.remotePort ?? 0}`;
  const context = {
    http1RequestQueue: [],
    http2StreamMethods: new Map(),
  };
  const clientSniffer = createProtocolSniffer(
    'client->upstream',
    clientAddress,
    context,
  );
  const upstreamSniffer = createProtocolSniffer(
    'upstream->client',
    clientAddress,
    context,
  );

  upstreamSocket.connect(upstreamPort, upstreamHost);

  clientSocket.on('data', (chunk) => {
    clientSniffer.push(chunk);
    upstreamSocket.write(chunk);
  });

  upstreamSocket.on('data', (chunk) => {
    upstreamSniffer.push(chunk);
    clientSocket.write(chunk);
  });

  clientSocket.on('end', () => {
    upstreamSocket.end();
  });

  upstreamSocket.on('end', () => {
    clientSocket.end();
  });

  clientSocket.on('error', (error) => {
    console.warn(`[trace] client error ${clientAddress} ${error.message}`);
    upstreamSocket.destroy(error);
  });

  upstreamSocket.on('error', (error) => {
    console.warn(`[trace] upstream error ${clientAddress} ${error.message}`);
    clientSocket.destroy(error);
  });
});

server.listen(publicPort, publicHost, () => {
  console.info(
    `[trace] proxy listening on ${publicHost}:${publicPort} -> ${upstreamHost}:${upstreamPort}`,
  );
});

process.on('SIGINT', () => {
  flushAllDedupLogs();
  process.exit(0);
});

process.on('SIGTERM', () => {
  flushAllDedupLogs();
  process.exit(0);
});
