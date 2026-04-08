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
  createHttpGossipResponseBody,
} from './stub-utils';
import { logHotPath } from './debug-log';
import { AdapterStatsService } from './operations/adapter-stats.service';

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
  console.info(
    `Probe proxy listening on ${proxyAddress.address}:${proxyAddress.port} and forwarding to ${internalGrpcUrl}`,
  );
}

bootstrap().catch((err) => {
  console.error('Error during application bootstrap:', err);
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
    let loggedFirstChunk = false;
    let upstreamConnected = false;
    let interceptedHealthCheck = false;

    const connectUpstream = () => {
      if (upstreamConnected || interceptedHealthCheck) {
        return;
      }

      upstreamConnected = true;
      upstreamSocket.connect(internalEndpoint.port, internalEndpoint.host);
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
      upstreamSocket.write(chunk);
    });

    upstreamSocket.on('data', (chunk) => {
      clientSocket.write(chunk);
    });

    clientSocket.on('end', () => {
      upstreamSocket.end();
    });

    upstreamSocket.on('end', () => {
      clientSocket.end();
    });

    clientSocket.on('error', (error) => {
      console.warn(`Probe proxy client error: ${error.message}`);
      upstreamSocket.destroy(error);
    });

    upstreamSocket.on('error', (error) => {
      console.warn(`Probe proxy upstream error: ${error.message}`);
      clientSocket.destroy(error);
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

function logIncomingProbe(clientSocket: Socket, chunk: Buffer): void {
  const remoteAddress = `${clientSocket.remoteAddress ?? 'unknown'}:${clientSocket.remotePort ?? 0}`;
  const requestLine = chunk.toString('utf8').split('\r\n', 1)[0] ?? '';
  const knownHttpPath = getKnownHttpPath(requestLine);

  if (knownHttpPath) {
    logHotPath(`HTTP ${knownHttpPath}`, {
      detail: `from ${remoteAddress}`,
    });
    return;
  }

  if (isHttp1RequestLine(requestLine)) {
    console.info(`[debug] HTTP unhandled ${requestLine} from ${remoteAddress}`);
    return;
  }

  const asciiPreview = chunk
    .subarray(0, 96)
    .toString('utf8')
    .replace(/\r/g, '\\r')
    .replace(/\n/g, '\\n');
  const hexPreview = chunk.subarray(0, 24).toString('hex');
  const protocolGuess = guessProtocol(chunk);

  console.info(
    `[probe] first bytes from ${remoteAddress} protocol=${protocolGuess} ascii="${asciiPreview}" hex=${hexPreview}`,
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
      const message = error instanceof Error ? error.message : String(error);
      console.warn(`Failed to build /stats response: ${message}`);
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
  console.info(
    `[debug] HTTP response ${path} status=${statusCode} bytes=${contentLength}`,
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
