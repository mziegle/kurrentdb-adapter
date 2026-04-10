import { createHash } from 'node:crypto';
import { accessSync } from 'node:fs';
import { createServer, Socket, type Server } from 'node:net';
import path from 'node:path';
import * as loader from '@grpc/proto-loader';
import zlib from 'node:zlib';

type TraceOptions = {
  proxyHost?: string;
  proxyPort?: string;
  suppressHttp1Bodies?: boolean;
  suppressHttp1Headers?: boolean;
  suppressHttp2FrameTypes?: string;
  suppressHttpPaths?: string;
  upstreamHost?: string;
  upstreamPort?: string;
  useDefaultSuppressions?: boolean;
  verbosity?: string;
};

type GrpcMethodDescriptor = {
  label: string;
  path: string;
  requestDeserialize: (buffer: Buffer) => unknown;
  requestSerialize: (value: unknown) => Uint8Array;
  responseDeserialize: (buffer: Buffer) => unknown;
  responseSerialize: (value: unknown) => Uint8Array;
};

type HttpDirection = 'client->upstream' | 'upstream->client';

type TraceContext = {
  http1RequestQueue: Array<{ method?: string; path?: string }>;
  http2StreamMethods: Map<number, GrpcMethodDescriptor>;
};

type Http2Frame = {
  type: number;
  flags: number;
  streamId: number;
  payload: Buffer;
};

type ParsedGrpcMessage = {
  compressedFlag: number;
  message: Buffer;
};

type PendingDedupLog = {
  count: number;
  firstSeenAt: number;
  lastSeenAt: number;
  summaryLabel: string;
  timer: NodeJS.Timeout;
};

type HttpHeaders = Record<string, string | string[]>;

const HTTP2_CLIENT_PREFACE = Buffer.from('PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n');

const HTTP2_FRAME_TYPES: Record<number, string> = {
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

const HTTP2_SETTINGS: Record<number, string> = {
  0x1: 'HEADER_TABLE_SIZE',
  0x2: 'ENABLE_PUSH',
  0x3: 'MAX_CONCURRENT_STREAMS',
  0x4: 'INITIAL_WINDOW_SIZE',
  0x5: 'MAX_FRAME_SIZE',
  0x6: 'MAX_HEADER_LIST_SIZE',
  0x8: 'ENABLE_CONNECT_PROTOCOL',
};

export async function runTraceProxy(options: TraceOptions): Promise<void> {
  const tracer = createTracer(options);
  await tracer.listen();

  const shutdown = async () => {
    await tracer.close();
  };

  const handleSignal = () => {
    void shutdown().finally(() => process.exit(0));
  };

  process.once('SIGINT', handleSignal);
  process.once('SIGTERM', handleSignal);

  await new Promise<void>((resolve, reject) => {
    tracer.server.once('close', resolve);
    tracer.server.once('error', reject);
  });
}

function createTracer(options: TraceOptions): {
  close: () => Promise<void>;
  listen: () => Promise<void>;
  server: Server;
} {
  const repoRoot = resolveRepoRoot();
  const publicPort = Number(options.proxyPort ?? process.env.TRACE_PROXY_PORT ?? '2115');
  const publicHost = options.proxyHost ?? process.env.TRACE_PROXY_HOST ?? '127.0.0.1';
  const upstreamPort = Number(options.upstreamPort ?? process.env.TRACE_UPSTREAM_PORT ?? '2114');
  const upstreamHost = options.upstreamHost ?? process.env.TRACE_UPSTREAM_HOST ?? '127.0.0.1';
  const maxBodyBytes = Number(process.env.TRACE_MAX_BODY_BYTES ?? '4096');
  const dedupeWindowMs = Number(process.env.TRACE_DEDUPE_WINDOW_MS ?? '1000');
  const traceVerbosity = (options.verbosity ?? process.env.TRACE_VERBOSITY ?? 'info').toLowerCase();
  const useDefaultSuppressions = options.useDefaultSuppressions ?? process.env.TRACE_USE_DEFAULT_SUPPRESSIONS !== '0';
  const suppressedHttpPaths = parseCsvSet(
    options.suppressHttpPaths ?? process.env.TRACE_SUPPRESS_HTTP_PATHS,
    useDefaultSuppressions ? ['/gossip', '/stats'] : [],
  );
  const suppressedHttp2FrameTypes = parseCsvSet(
    options.suppressHttp2FrameTypes ?? process.env.TRACE_SUPPRESS_HTTP2_FRAME_TYPES,
    useDefaultSuppressions ? ['settings', 'window_update', 'ping'] : [],
  );
  const suppressHttp1Headers =
    options.suppressHttp1Headers ?? process.env.TRACE_SUPPRESS_HTTP1_HEADERS === '1';
  const suppressHttp1Bodies =
    options.suppressHttp1Bodies ?? process.env.TRACE_SUPPRESS_HTTP1_BODIES === '1';
  const grpcMethodDescriptors = loadGrpcMethodDescriptors(repoRoot);
  const pendingDedupLogs = new Map<string, PendingDedupLog>();

  const logTrace = (
    lines: string[],
    dedupeKey?: string,
    summaryLabel?: string,
  ): void => {
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
  };

  const flushDedupLog = (dedupeKey: string): void => {
    const entry = pendingDedupLogs.get(dedupeKey);
    if (!entry) {
      return;
    }

    pendingDedupLogs.delete(dedupeKey);
    if (entry.count <= 1) {
      return;
    }

    const durationSeconds = (
      (entry.lastSeenAt - entry.firstSeenAt) /
      1000
    ).toFixed(1);
    console.info(
      `[trace] repeated ${entry.summaryLabel} x${entry.count - 1} in ${durationSeconds}s`,
    );
  };

  const flushAllDedupLogs = (): void => {
    for (const key of [...pendingDedupLogs.keys()]) {
      const entry = pendingDedupLogs.get(key);
      if (entry) {
        clearTimeout(entry.timer);
      }
      flushDedupLog(key);
    }
  };

  const isDebugVerbosity = (): boolean => traceVerbosity === 'debug';

  const formatBody = (bodyBuffer: Buffer, headers: HttpHeaders): string => {
    if (!bodyBuffer.length) {
      return '';
    }

    const contentTypeHeader = headers['content-type'];
    const contentType = Array.isArray(contentTypeHeader)
      ? contentTypeHeader[0]
      : contentTypeHeader;
    const text = bodyBuffer.toString('utf8');

    if (
      typeof contentType === 'string' &&
      contentType.includes('application/json')
    ) {
      return truncateText(tryParseJson(text), maxBodyBytes);
    }

    return truncateText(text, maxBodyBytes);
  };

  const shouldSuppressHttp1 = (requestPath?: string): boolean =>
    Boolean(requestPath && suppressedHttpPaths.has(requestPath.toLowerCase()));

  const shouldSuppressHttp2 = (frameType: string): boolean =>
    suppressedHttp2FrameTypes.has(frameType.toLowerCase());

  const describeGrpcPayload = (
    direction: HttpDirection,
    streamId: number,
    payload: Buffer,
    context: TraceContext,
  ): string[] => {
    const messages = parseGrpcMessages(payload);
    if (messages.length === 0) {
      return isDebugVerbosity() ? [`dataPreview=${previewHex(payload)}`] : [];
    }

    const details: string[] = [];
    for (const [index, message] of messages.entries()) {
      const labelPrefix = messages.length > 1 ? `grpc[${index}]` : 'grpc';
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
        grpcMethodDescriptors,
      );
      if (decoded) {
        details.push(
          `${labelPrefix} method=${decoded.label} body=\n${truncateText(
            formatDecodedMessage(decoded.value, traceVerbosity),
            2000,
          )}`,
        );
        continue;
      }

      details.push(
        `${labelPrefix} undecoded hex=${previewHex(decompressed.message, 64)}`,
      );
    }

    return details;
  };

  const logHttp2Frame = (
    direction: HttpDirection,
    clientAddress: string,
    frame: Http2Frame,
    context: TraceContext,
  ): void => {
    const frameType = HTTP2_FRAME_TYPES[frame.type] ?? `UNKNOWN(${frame.type})`;
    if (shouldSuppressHttp2(frameType)) {
      return;
    }

    const prefix = `[trace] ${direction} ${clientAddress} HTTP/2 ${frameType} stream=${frame.streamId} flags=0x${frame.flags
      .toString(16)
      .padStart(2, '0')} length=${frame.payload.length}`;
    const details: string[] = [];

    if (frame.type === 0x0) {
      details.push(
        ...describeGrpcPayload(direction, frame.streamId, frame.payload, context),
      );
    } else if (frame.type === 0x4) {
      details.push(describeSettingsFrame(frame.flags, frame.payload));
    } else if (frame.type === 0x6) {
      details.push(`ack=${Boolean(frame.flags & 0x1)}`);
    } else if (frame.type === 0x7) {
      details.push(describeGoawayFrame(frame.payload, maxBodyBytes));
    } else if (frame.type === 0x8 && frame.payload.length >= 4) {
      details.push(
        `windowSizeIncrement=${frame.payload.readUInt32BE(0) & 0x7fffffff}`,
      );
    } else if (frame.type === 0x3 && frame.payload.length >= 4) {
      details.push(`errorCode=${frame.payload.readUInt32BE(0)}`);
    } else if ((frame.type === 0x1 || frame.type === 0x9) && isDebugVerbosity()) {
      details.push(`headerBlockBytes=${frame.payload.length}`);
    }

    if (details.length === 0 && frame.payload.length > 0 && isDebugVerbosity()) {
      details.push(`hex=${previewHex(frame.payload)}`);
    }

    if (!isDebugVerbosity()) {
      if ((frame.type === 0x1 || frame.type === 0x9) && details.length === 0) {
        return;
      }

      if (
        frame.type === 0x0 &&
        details.every((detail) => shouldOmitGrpcDetailInInfo(detail))
      ) {
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
  };

  const logHttp1Message = (
    direction: HttpDirection,
    clientAddress: string,
    startLine: string,
    headers: HttpHeaders,
    bodyBuffer: Buffer,
    context: TraceContext,
  ): void => {
    let requestMethod: string | undefined;
    let requestPath: string | undefined;

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
      lines.push(
        `[trace] ${direction} ${clientAddress} headers=${JSON.stringify(headers)}`,
      );
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
  };

  const createProtocolSniffer = (
    direction: HttpDirection,
    clientAddress: string,
    context: TraceContext,
  ): { push: (chunk: Buffer) => void } => {
    let parser:
      | Http1MessageParser
      | Http2FrameParser
      | { push: (chunk: Buffer) => void }
      | undefined;

    return {
      push(chunk: Buffer) {
        if (!parser) {
          const firstLine = chunk.toString('utf8').split('\r\n', 1)[0] ?? '';

          if (isHttp1StartLine(firstLine, direction)) {
            parser = new Http1MessageParser(
              direction,
              clientAddress,
              context,
              logHttp1Message,
            );
          } else if (isLikelyHttp2(chunk, direction)) {
            parser = new Http2FrameParser(
              direction,
              clientAddress,
              context,
              isDebugVerbosity,
              logHttp2Frame,
            );
          } else {
            console.info(
              `[trace] ${direction} ${clientAddress} unknown ascii="${previewAscii(chunk)}" hex=${previewHex(chunk)}`,
            );
            parser = { push() {} };
          }
        }

        parser.push(chunk);
      },
    };
  };

  const server = createServer((clientSocket) => {
    const upstreamSocket = new Socket();
    const clientAddress = `${clientSocket.remoteAddress ?? 'unknown'}:${clientSocket.remotePort ?? 0}`;
    const context: TraceContext = {
      http1RequestQueue: [],
      http2StreamMethods: new Map<number, GrpcMethodDescriptor>(),
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

  return {
    server,
    async listen(): Promise<void> {
      await new Promise<void>((resolve, reject) => {
        server.once('error', reject);
        server.listen(publicPort, publicHost, () => {
          server.off('error', reject);
          console.info(
            `[trace] proxy listening on ${publicHost}:${publicPort} -> ${upstreamHost}:${upstreamPort}`,
          );
          resolve();
        });
      });
    },
    async close(): Promise<void> {
      flushAllDedupLogs();
      await new Promise<void>((resolve, reject) => {
        server.close((error) => {
          if (error) {
            reject(error);
            return;
          }

          resolve();
        });
      });
    },
  };
}

class Http1MessageParser {
  private buffer = Buffer.alloc(0);

  constructor(
    private readonly direction: HttpDirection,
    private readonly clientAddress: string,
    private readonly context: TraceContext,
    private readonly logHttp1Message: (
      direction: HttpDirection,
      clientAddress: string,
      startLine: string,
      headers: HttpHeaders,
      bodyBuffer: Buffer,
      context: TraceContext,
    ) => void,
  ) {}

  push(chunk: Buffer): void {
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
        Array.isArray(contentLengthValue)
          ? contentLengthValue[0]
          : contentLengthValue ?? 0,
      );
      const messageLength = headerEndIndex + 4 + contentLength;

      if (this.buffer.length < messageLength) {
        return;
      }

      const bodyBuffer = this.buffer.subarray(headerEndIndex + 4, messageLength);
      this.buffer = this.buffer.subarray(messageLength);
      this.logHttp1Message(
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
  private buffer = Buffer.alloc(0);
  private prefaceHandled: boolean;

  constructor(
    private readonly direction: HttpDirection,
    private readonly clientAddress: string,
    private readonly context: TraceContext,
    private readonly isDebugVerbosity: () => boolean,
    private readonly logHttp2Frame: (
      direction: HttpDirection,
      clientAddress: string,
      frame: Http2Frame,
      context: TraceContext,
    ) => void,
  ) {
    this.prefaceHandled = direction !== 'client->upstream';
  }

  push(chunk: Buffer): void {
    this.buffer = Buffer.concat([this.buffer, chunk]);

    if (!this.prefaceHandled) {
      if (this.buffer.length < HTTP2_CLIENT_PREFACE.length) {
        return;
      }

      if (
        this.buffer
          .subarray(0, HTTP2_CLIENT_PREFACE.length)
          .equals(HTTP2_CLIENT_PREFACE)
      ) {
        if (this.isDebugVerbosity()) {
          console.info(
            `[trace] ${this.direction} ${this.clientAddress} HTTP/2 client preface`,
          );
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

      this.logHttp2Frame(
        this.direction,
        this.clientAddress,
        { type, flags, streamId, payload },
        this.context,
      );

      this.buffer = this.buffer.subarray(totalFrameLength);
    }
  }
}

function loadGrpcMethodDescriptors(repoRoot: string): GrpcMethodDescriptor[] {
  const protoDir = resolveProtoDir(repoRoot);
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
  ) as unknown as Record<string, Record<string, Record<string, unknown>>>;

  const descriptors: GrpcMethodDescriptor[] = [];
  for (const serviceDefinition of Object.values(definition)) {
    if (!serviceDefinition || typeof serviceDefinition !== 'object') {
      continue;
    }

    for (const methodDefinition of Object.values(serviceDefinition)) {
      if (
        !methodDefinition ||
        typeof methodDefinition !== 'object' ||
        typeof methodDefinition.requestDeserialize !== 'function' ||
        typeof methodDefinition.requestSerialize !== 'function' ||
        typeof methodDefinition.responseDeserialize !== 'function' ||
        typeof methodDefinition.responseSerialize !== 'function' ||
        typeof methodDefinition.path !== 'string'
      ) {
        continue;
      }

      descriptors.push({
        label: methodDefinition.path.replace(/^\//, '').replace(/\//g, '.'),
        path: methodDefinition.path,
        requestDeserialize: methodDefinition.requestDeserialize as (
          buffer: Buffer,
        ) => unknown,
        requestSerialize: methodDefinition.requestSerialize as (
          value: unknown,
        ) => Uint8Array,
        responseDeserialize: methodDefinition.responseDeserialize as (
          buffer: Buffer,
        ) => unknown,
        responseSerialize: methodDefinition.responseSerialize as (
          value: unknown,
        ) => Uint8Array,
      });
    }
  }

  return descriptors;
}

function resolveRepoRoot(): string {
  const candidates = [
    process.cwd(),
    path.resolve(process.cwd(), '..'),
    path.resolve(process.cwd(), '../..'),
  ];

  for (const candidate of candidates) {
    if (exists(path.join(candidate, 'proto', 'Grpc', 'streams.proto'))) {
      return candidate;
    }
  }

  throw new Error('Could not locate the repo root containing proto/Grpc/streams.proto.');
}

function resolveProtoDir(repoRoot: string): string {
  const candidates = [
    path.join(repoRoot, 'proto', 'Grpc'),
    path.join(process.cwd(), 'proto', 'Grpc'),
  ];

  for (const candidate of candidates) {
    if (exists(path.join(candidate, 'streams.proto'))) {
      return candidate;
    }
  }

  throw new Error('Could not locate proto/Grpc/streams.proto');
}

function exists(targetPath: string): boolean {
  try {
    accessSync(targetPath);
    return true;
  } catch {
    return false;
  }
}

function previewAscii(buffer: Buffer, length = 96): string {
  return buffer
    .subarray(0, length)
    .toString('utf8')
    .replace(/\r/g, '\\r')
    .replace(/\n/g, '\\n');
}

function previewHex(buffer: Buffer, length = 24): string {
  return buffer.subarray(0, length).toString('hex');
}

function tryParseJson(text: string): string {
  try {
    return JSON.stringify(JSON.parse(text), null, 2);
  } catch {
    return text;
  }
}

function truncateText(text: string, maxLength: number): string {
  if (text.length <= maxLength) {
    return text;
  }

  return `${text.slice(0, maxLength)}\n... [truncated ${text.length - maxLength} chars]`;
}

function createSignature(parts: string[]): string {
  return createHash('sha1').update(parts.join('\n')).digest('hex');
}

function parseCsvSet(value: string | undefined, fallback: string[] = []): Set<string> {
  const source = value ?? fallback.join(',');
  return new Set(
    source
      .split(',')
      .map((part) => part.trim().toLowerCase())
      .filter(Boolean),
  );
}

function normalizeHeaders(headerLines: string[]): HttpHeaders {
  const headers: HttpHeaders = {};

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

function isHttp1StartLine(line: string, direction: HttpDirection): boolean {
  if (direction === 'client->upstream') {
    return /^(GET|POST|PUT|PATCH|DELETE|HEAD|OPTIONS) .+ HTTP\/1\.[01]$/.test(line);
  }

  return /^HTTP\/1\.[01] \d{3} /.test(line);
}

function isLikelyHttp2(buffer: Buffer, direction: HttpDirection): boolean {
  if (direction === 'client->upstream') {
    return buffer.subarray(0, HTTP2_CLIENT_PREFACE.length).equals(HTTP2_CLIENT_PREFACE);
  }

  return buffer.length >= 9;
}

function extractHttp1MethodAndPath(startLine: string): {
  method?: string;
  path?: string;
} {
  const parts = startLine.split(' ');
  if (parts.length < 2) {
    return {};
  }

  return {
    method: parts[0],
    path: parts[1],
  };
}

function shouldOmitGrpcDetailInInfo(detail: string): boolean {
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

function parseGrpcMessages(payload: Buffer): ParsedGrpcMessage[] {
  const messages: ParsedGrpcMessage[] = [];
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

function decompressGrpcMessage(
  message: Buffer,
  compressedFlag: number,
):
  | { ok: true; message: Buffer }
  | { ok: false; reason: string } {
  if (compressedFlag === 0) {
    return {
      ok: true,
      message,
    };
  }

  try {
    return { ok: true, message: zlib.gunzipSync(message) };
  } catch {
    // Try the next supported compression format.
  }

  try {
    return { ok: true, message: zlib.inflateSync(message) };
  } catch {
    // Try the next supported compression format.
  }

  try {
    return { ok: true, message: zlib.brotliDecompressSync(message) };
  } catch {
    // Fall through to the unsupported-compression result below.
  }

  return {
    ok: false,
    reason: 'compressed message uses unsupported encoding',
  };
}

function decodeGrpcMessage(
  direction: HttpDirection,
  streamId: number,
  message: Buffer,
  context: TraceContext,
  grpcMethodDescriptors: GrpcMethodDescriptor[],
):
  | { label: string; value: unknown }
  | undefined {
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
    } catch {
      // Fall back to best-effort inference across all known methods.
    }
  }

  if (direction !== 'client->upstream') {
    return undefined;
  }

  const candidates: Array<{
    descriptor: GrpcMethodDescriptor;
    score: number;
    value: unknown;
  }> = [];

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
    } catch {
      // Ignore non-matching message shapes during inference.
    }
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

function scoreDecodedValue(value: unknown): number {
  if (value === null || value === undefined) {
    return 0;
  }

  if (Buffer.isBuffer(value) || value instanceof Uint8Array) {
    return value.length > 0 ? 1 : 0;
  }

  if (Array.isArray(value)) {
    return (value as unknown[]).reduce<number>(
      (sum, item) => sum + scoreDecodedValue(item),
      0,
    );
  }

  if (typeof value === 'object') {
    return Object.values(value as Record<string, unknown>).reduce<number>(
      (sum, item) => sum + scoreDecodedValue(item),
      0,
    );
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

function formatDecodedMessage(value: unknown, traceVerbosity: string): string {
  const normalizedValue =
    traceVerbosity === 'debug' ? value : simplifyDecodedValue(value);

  return JSON.stringify(
    normalizedValue,
    (_key: string, currentValue: unknown) => {
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

function simplifyDecodedValue(value: unknown, key = ''): unknown {
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
    const output: Record<string, unknown> = {};
    for (const [childKey, childValue] of Object.entries(
      value as Record<string, unknown>,
    )) {
      output[childKey] = simplifyDecodedValue(childValue, childKey);
    }
    return output;
  }

  return value;
}

function simplifyBinaryValue(buffer: Buffer, key: string): unknown {
  if (buffer.length === 0) {
    return '';
  }

  const utf8 = buffer.toString('utf8');
  const isMostlyPrintable = isPrintableText(utf8);
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
        const parsed: unknown = JSON.parse(utf8);
        return parsed;
      } catch {
        // Leave printable data as plain text when it is not valid JSON.
      }
    }

    return utf8;
  }

  if (isMostlyPrintable) {
    return utf8;
  }

  return `hex:${buffer.toString('hex')}`;
}

function isPrintableText(value: string): boolean {
  for (let index = 0; index < value.length; index += 1) {
    const codePoint = value.charCodeAt(index);
    const isTab = codePoint === 0x09;
    const isLineFeed = codePoint === 0x0a;
    const isCarriageReturn = codePoint === 0x0d;
    const isPrintableAscii = codePoint >= 0x20 && codePoint <= 0x7e;

    if (!isTab && !isLineFeed && !isCarriageReturn && !isPrintableAscii) {
      return false;
    }
  }

  return true;
}

function describeSettingsFrame(flags: number, payload: Buffer): string {
  if (flags & 0x1) {
    return 'ack=true';
  }

  if (payload.length % 6 !== 0) {
    return `settingsBytes=${payload.length}`;
  }

  const entries: string[] = [];
  for (let offset = 0; offset < payload.length; offset += 6) {
    const id = payload.readUInt16BE(offset);
    const value = payload.readUInt32BE(offset + 2);
    entries.push(`${HTTP2_SETTINGS[id] ?? `SETTING_${id}`}=${value}`);
  }

  return `settings=${entries.join(', ')}`;
}

function describeGoawayFrame(payload: Buffer, maxBodyBytes: number): string {
  if (payload.length < 8) {
    return `goawayBytes=${payload.length}`;
  }

  const lastStreamId = payload.readUInt32BE(0) & 0x7fffffff;
  const errorCode = payload.readUInt32BE(4);
  const debugData =
    payload.length > 8
      ? truncateText(payload.subarray(8).toString('utf8'), Math.min(maxBodyBytes, 256))
      : '';

  return debugData
    ? `lastStreamId=${lastStreamId} errorCode=${errorCode} debug=${JSON.stringify(debugData)}`
    : `lastStreamId=${lastStreamId} errorCode=${errorCode}`;
}
