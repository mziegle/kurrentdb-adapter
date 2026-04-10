import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import { MicroserviceOptions, Transport } from '@nestjs/microservices';
import { existsSync } from 'node:fs';
import { join } from 'node:path';
import {
  createServer,
  Server as NetServer,
  Socket,
  AddressInfo,
} from 'node:net';
import {
  createInfoResponseBody,
  createInfoOptionsResponseBody,
  createHttpGossipResponseBody,
} from './stub-utils';
import { logHotPath } from './shared/debug-log';
import { AdapterStatsService } from './operations/adapter-stats.service';
import { appLogger } from './shared/logger';

async function bootstrap() {
  const grpcUrl = process.env.GRPC_URL ?? '0.0.0.0:2113';
  const internalGrpcUrl =
    process.env.INTERNAL_GRPC_URL ?? deriveInternalGrpcUrl(grpcUrl);
  const protoDir = resolveProtoDir();

  const app = await NestFactory.createMicroservice<MicroserviceOptions>(
    AppModule,
    {
      transport: Transport.GRPC,
      options: {
        url: internalGrpcUrl,
        package: [
          'event_store.client.gossip',
          'event_store.client.monitoring',
          'event_store.client.operations',
          'event_store.client.persistent_subscriptions',
          'event_store.client.streams',
          'event_store.client.server_features',
          'event_store.client.users',
        ],
        protoPath: [
          join(protoDir, 'gossip.proto'),
          join(protoDir, 'monitoring.proto'),
          join(protoDir, 'operations.proto'),
          join(protoDir, 'persistent.proto'),
          join(protoDir, 'streams.proto'),
          join(protoDir, 'serverfeatures.proto'),
          join(protoDir, 'users.proto'),
        ],
        loader: {
          includeDirs: [protoDir],
        },
      },
    },
  );

  await app.listen();

  const stats = app.get(AdapterStatsService);
  const proxyServer = await startProbeProxy(grpcUrl, internalGrpcUrl, stats);
  const proxyAddress = proxyServer.address() as AddressInfo;
  appLogger.info(
    {
      event: 'probe_proxy_started',
      listenAddress: `${proxyAddress.address}:${proxyAddress.port}`,
      upstreamAddress: internalGrpcUrl,
    },
    'Probe proxy listening',
  );
}

bootstrap().catch((err) => {
  appLogger.error({ err }, 'Error during application bootstrap');
  process.exit(1);
});

function resolveProtoDir(): string {
  const candidates = [
    join(process.cwd(), 'proto/Grpc'),
    join(__dirname, '../proto/Grpc'),
    join(__dirname, 'proto/Grpc'),
  ];

  for (const candidate of candidates) {
    if (existsSync(join(candidate, 'streams.proto'))) {
      return candidate;
    }
  }

  throw new Error('Could not locate proto/Grpc/streams.proto.');
}

function deriveInternalGrpcUrl(publicGrpcUrl: string): string {
  const endpoint = parseTcpEndpoint(publicGrpcUrl);
  return `127.0.0.1:${endpoint.port + 100}`;
}

function parseTcpEndpoint(urlOrHostPort: string): {
  host: string;
  port: number;
} {
  const normalized = urlOrHostPort.includes('://')
    ? urlOrHostPort
    : `tcp://${urlOrHostPort}`;
  const parsed = new URL(normalized);
  const port = parsed.port ? Number(parsed.port) : 2113;

  return {
    host: parsed.hostname,
    port,
  };
}

async function startProbeProxy(
  publicGrpcUrl: string,
  internalGrpcUrl: string,
  stats: AdapterStatsService,
): Promise<NetServer> {
  const publicEndpoint = parseTcpEndpoint(publicGrpcUrl);
  const internalEndpoint = parseTcpEndpoint(internalGrpcUrl);

  const server = createServer((clientSocket) => {
    const upstreamSocket = new Socket();
    const pendingUpstreamWrites: Buffer[] = [];
    let loggedFirstChunk = false;
    let upstreamConnected = false;
    let upstreamConnecting = false;
    let interceptedHealthCheck = false;
    let proxyClosed = false;

    const connectUpstream = () => {
      if (
        upstreamConnected ||
        upstreamConnecting ||
        interceptedHealthCheck ||
        proxyClosed
      ) {
        return;
      }

      upstreamConnecting = true;
      upstreamSocket.connect(internalEndpoint.port, internalEndpoint.host);
    };

    const flushPendingUpstreamWrites = () => {
      if (!upstreamConnected || proxyClosed || upstreamSocket.destroyed) {
        return;
      }

      while (pendingUpstreamWrites.length > 0) {
        const chunk = pendingUpstreamWrites.shift();
        if (!chunk) {
          continue;
        }

        if (!safeWrite(upstreamSocket, chunk)) {
          pendingUpstreamWrites.unshift(chunk);
          return;
        }
      }
    };

    const closeProxy = (error?: Error) => {
      if (proxyClosed) {
        return;
      }

      proxyClosed = true;

      if (!clientSocket.destroyed) {
        if (error) {
          clientSocket.destroy(error);
        } else {
          clientSocket.end();
        }
      }

      if (!upstreamSocket.destroyed) {
        if (error) {
          upstreamSocket.destroy(error);
        } else {
          upstreamSocket.end();
        }
      }
    };

    clientSocket.on('data', (chunk) => {
      if (!loggedFirstChunk) {
        loggedFirstChunk = true;
        logIncomingProbe(clientSocket, chunk);
      }

      if (tryHandleHealthProbe(clientSocket, chunk)) {
        interceptedHealthCheck = true;
        return;
      }

      if (tryHandleInfoRequest(clientSocket, chunk)) {
        interceptedHealthCheck = true;
        return;
      }

      if (tryHandleInfoOptionsRequest(clientSocket, chunk)) {
        interceptedHealthCheck = true;
        return;
      }

      if (tryHandleGossipRequest(clientSocket, chunk)) {
        interceptedHealthCheck = true;
        return;
      }

      if (tryHandleStatsRequest(clientSocket, chunk, stats)) {
        interceptedHealthCheck = true;
        return;
      }

      if (tryHandlePingRequest(clientSocket, chunk)) {
        interceptedHealthCheck = true;
        return;
      }

      connectUpstream();
      if (!upstreamConnected) {
        pendingUpstreamWrites.push(Buffer.from(chunk));
        return;
      }

      if (!safeWrite(upstreamSocket, chunk)) {
        pendingUpstreamWrites.push(Buffer.from(chunk));
      }
    });

    upstreamSocket.on('connect', () => {
      upstreamConnecting = false;
      upstreamConnected = true;
      flushPendingUpstreamWrites();
    });

    upstreamSocket.on('data', (chunk) => {
      if (!safeWrite(clientSocket, chunk)) {
        closeProxy();
      }
    });

    clientSocket.on('end', () => {
      closeProxy();
    });

    upstreamSocket.on('end', () => {
      closeProxy();
    });

    clientSocket.on('close', () => {
      proxyClosed = true;
    });

    upstreamSocket.on('close', () => {
      proxyClosed = true;
    });

    clientSocket.on('error', (error) => {
      logProbeSocketError('Probe proxy client error', error);
      closeProxy(error);
    });

    upstreamSocket.on('error', (error) => {
      upstreamConnecting = false;
      logProbeSocketError('Probe proxy upstream error', error);
      closeProxy(error);
    });
  });

  await new Promise<void>((resolve, reject) => {
    server.once('error', reject);
    server.listen(publicEndpoint.port, publicEndpoint.host, () => {
      server.off('error', reject);
      resolve();
    });
  });

  return server;
}

function safeWrite(socket: Socket, chunk: Buffer | string): boolean {
  if (socket.destroyed || socket.writableEnded) {
    return false;
  }

  socket.write(chunk);
  return true;
}

function logProbeSocketError(message: string, error: Error): void {
  if (isBenignSocketCloseError(error)) {
    appLogger.debug({ err: error }, message);
    return;
  }

  appLogger.warn({ err: error }, message);
}

function isBenignSocketCloseError(error: Error & { code?: string }): boolean {
  return (
    error.code === 'ERR_SOCKET_CLOSED' ||
    error.code === 'EPIPE' ||
    error.code === 'ECONNRESET'
  );
}

function logIncomingProbe(clientSocket: Socket, chunk: Buffer): void {
  const remoteAddress = `${clientSocket.remoteAddress ?? 'unknown'}:${clientSocket.remotePort ?? 0}`;
  const requestLine = chunk.toString('utf8').split('\r\n', 1)[0] ?? '';
  const knownHttpPath = getKnownHttpPath(requestLine);

  if (knownHttpPath) {
    logHotPath(`HTTP ${knownHttpPath}`, {
      summary: `from ${remoteAddress}`,
      trace: {
        remoteAddress,
        requestLine,
      },
    });
    return;
  }

  if (isHttp1RequestLine(requestLine)) {
    appLogger.info(
      {
        event: 'incoming_request',
        requestLabel: 'HTTP unhandled',
        summary: `${requestLine} from ${remoteAddress}`,
      },
      'HTTP unhandled request',
    );
    appLogger.trace(
      {
        event: 'incoming_request',
        requestLabel: 'HTTP unhandled',
        trace: {
          remoteAddress,
          requestLine,
          rawChunk: {
            utf8Preview: chunk.subarray(0, 256).toString('utf8'),
            hexPreview: chunk.subarray(0, 64).toString('hex'),
          },
        },
      },
      'HTTP unhandled request full details',
    );
    return;
  }

  const asciiPreview = chunk
    .subarray(0, 96)
    .toString('utf8')
    .replace(/\r/g, '\\r')
    .replace(/\n/g, '\\n');
  const hexPreview = chunk.subarray(0, 24).toString('hex');
  const protocolGuess = guessProtocol(chunk);

  appLogger.info(
    {
      event: 'probe_request',
      remoteAddress,
      protocolGuess,
    },
    'Probe proxy received unknown request',
  );
  appLogger.trace(
    {
      event: 'probe_request',
      remoteAddress,
      protocolGuess,
      asciiPreview,
      hexPreview,
    },
    'Probe proxy request details',
  );
}

function guessProtocol(chunk: Buffer): string {
  const preview = chunk.subarray(0, 24).toString('utf8');

  if (preview.startsWith('PRI * HTTP/2.0')) {
    return 'http2-prior-knowledge';
  }

  if (/^(GET|POST|PUT|PATCH|DELETE|HEAD|OPTIONS) /.test(preview)) {
    return 'http1';
  }

  return 'unknown';
}

function getKnownHttpPath(requestLine: string): string | undefined {
  if (requestLine.startsWith('HEAD /health/live HTTP/1.1')) {
    return 'HEAD /health/live';
  }

  if (requestLine.startsWith('GET /health/live HTTP/1.1')) {
    return 'GET /health/live';
  }

  if (requestLine.startsWith('GET /info HTTP/1.1')) {
    return 'GET /info';
  }

  if (requestLine.startsWith('GET /info/options HTTP/1.1')) {
    return 'GET /info/options';
  }

  if (requestLine.startsWith('GET /gossip HTTP/1.1')) {
    return 'GET /gossip';
  }

  if (requestLine.startsWith('GET /stats HTTP/1.1')) {
    return 'GET /stats';
  }

  if (requestLine.startsWith('GET /ping HTTP/1.1')) {
    return 'GET /ping';
  }

  return undefined;
}

function isHttp1RequestLine(requestLine: string): boolean {
  return /^(GET|POST|PUT|PATCH|DELETE|HEAD|OPTIONS) .+ HTTP\/1\.[01]$/.test(
    requestLine,
  );
}

function tryHandleHealthProbe(clientSocket: Socket, chunk: Buffer): boolean {
  const requestLine = chunk.subarray(0, 64).toString('utf8');

  if (
    requestLine.startsWith('HEAD /health/live HTTP/1.1') ||
    requestLine.startsWith('GET /health/live HTTP/1.1')
  ) {
    writeHttpResponse(
      clientSocket,
      'HEAD /health/live',
      204,
      'No Content',
      'text/plain; charset=utf-8',
      '',
    );
    return true;
  }

  return false;
}

function tryHandleInfoRequest(clientSocket: Socket, chunk: Buffer): boolean {
  const requestLine = chunk.subarray(0, 64).toString('utf8');

  if (!requestLine.startsWith('GET /info HTTP/1.1')) {
    return false;
  }

  const body = createInfoResponseBody();
  writeHttpResponse(
    clientSocket,
    'GET /info',
    200,
    'OK',
    'application/json; charset=utf-8',
    body,
  );
  return true;
}

function tryHandleInfoOptionsRequest(
  clientSocket: Socket,
  chunk: Buffer,
): boolean {
  const requestLine = chunk.subarray(0, 64).toString('utf8');

  if (!requestLine.startsWith('GET /info/options HTTP/1.1')) {
    return false;
  }

  const body = createInfoOptionsResponseBody();
  writeHttpResponse(
    clientSocket,
    'GET /info/options',
    200,
    'OK',
    'application/json; charset=utf-8',
    body,
  );
  return true;
}

function tryHandleGossipRequest(clientSocket: Socket, chunk: Buffer): boolean {
  const requestLine = chunk.subarray(0, 64).toString('utf8');

  if (!requestLine.startsWith('GET /gossip HTTP/1.1')) {
    return false;
  }

  const body = createHttpGossipResponseBody();
  writeHttpResponse(
    clientSocket,
    'GET /gossip',
    200,
    'OK',
    'application/json; charset=utf-8',
    body,
  );
  return true;
}

function tryHandleStatsRequest(
  clientSocket: Socket,
  chunk: Buffer,
  stats: AdapterStatsService,
): boolean {
  const requestLine = chunk.subarray(0, 64).toString('utf8');

  if (!requestLine.startsWith('GET /stats HTTP/1.1')) {
    return false;
  }

  void stats
    .createHttpStatsResponseBody()
    .then((body) => {
      writeHttpResponse(
        clientSocket,
        'GET /stats',
        200,
        'OK',
        'application/json; charset=utf-8',
        body,
      );
    })
    .catch((error: unknown) => {
      appLogger.warn({ err: error }, 'Failed to build /stats response');
      writeHttpResponse(
        clientSocket,
        'GET /stats',
        500,
        'Internal Server Error',
        'text/plain; charset=utf-8',
        'stats unavailable',
      );
    });
  return true;
}

function tryHandlePingRequest(clientSocket: Socket, chunk: Buffer): boolean {
  const requestText = chunk.toString('utf8');
  const requestLine = requestText.split('\r\n', 1)[0] ?? '';

  if (!requestLine.startsWith('GET /ping HTTP/1.1')) {
    return false;
  }

  const acceptHeader = getHeaderValue(requestText, 'accept');
  const response = createPingHttpResponse(acceptHeader);
  writeHttpResponse(
    clientSocket,
    'GET /ping',
    200,
    'OK',
    response.contentType,
    response.body,
  );
  return true;
}

function writeHttpResponse(
  clientSocket: Socket,
  path: string,
  statusCode: number,
  statusText: string,
  contentType: string,
  body: string,
): void {
  const contentLength = Buffer.byteLength(body, 'utf8');
  appLogger.debug(
    {
      event: 'http_response',
      path,
      statusCode,
      bytes: contentLength,
    },
    'HTTP response sent',
  );
  clientSocket.write(
    [
      `HTTP/1.1 ${statusCode} ${statusText}`,
      `Content-Type: ${contentType}`,
      `Content-Length: ${contentLength}`,
      'Connection: keep-alive',
      '',
      body,
    ].join('\r\n'),
  );
}

function getHeaderValue(
  requestText: string,
  headerName: string,
): string | undefined {
  const pattern = new RegExp(`^${headerName}:\\s*(.+)$`, 'im');
  const matched = requestText.match(pattern);
  return matched?.[1]?.trim();
}

function createPingHttpResponse(acceptHeader?: string): {
  body: string;
  contentType: string;
} {
  if (acceptsMediaType(acceptHeader, 'text/plain')) {
    return {
      body: 'Text: Ping request successfully handled',
      contentType: 'text/plain; charset=utf-8',
    };
  }

  if (acceptsMediaType(acceptHeader, 'application/xml')) {
    return {
      body: [
        '<?xml version="1.0" encoding="utf-8"?>',
        '<TextMessage xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema">',
        '  <Text>Ping request successfully handled</Text>',
        '</TextMessage>',
      ].join(''),
      contentType: 'application/xml; charset=utf-8',
    };
  }

  if (acceptsMediaType(acceptHeader, 'text/xml')) {
    return {
      body: [
        '<?xml version="1.0" encoding="utf-8"?>',
        '<TextMessage xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema">',
        '  <Text>Ping request successfully handled</Text>',
        '</TextMessage>',
      ].join(''),
      contentType: 'text/xml; charset=utf-8',
    };
  }

  return {
    body: JSON.stringify({
      text: 'Ping request successfully handled',
      label: 'Http',
    }),
    contentType: 'application/json; charset=utf-8',
  };
}

function acceptsMediaType(
  acceptHeader: string | undefined,
  mediaType: string,
): boolean {
  if (!acceptHeader) {
    return false;
  }

  const normalizedAcceptHeader = acceptHeader.toLowerCase();
  const normalizedMediaType = mediaType.toLowerCase();

  if (
    normalizedAcceptHeader.includes('*/*') &&
    normalizedMediaType === 'application/json'
  ) {
    return true;
  }

  return normalizedAcceptHeader
    .split(',')
    .map((value) => value.split(';', 1)[0]?.trim())
    .includes(normalizedMediaType);
}
