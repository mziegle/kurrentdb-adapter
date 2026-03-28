import { randomUUID } from 'node:crypto';
import { freemem, hostname, loadavg, totalmem, type as osType } from 'node:os';
import { parse as parsePath } from 'node:path';
import { Observable, of } from 'rxjs';
import { ClusterInfo, MemberInfo_VNodeState } from './interfaces/gossip';
import {
  ScavengeResp,
  ScavengeResp_ScavengeResult,
} from './interfaces/operations';
import {
  GetInfoReq,
  GetInfoResp,
  ListResp,
  ReadResp,
  SubscriptionInfo,
} from './interfaces/persistent';
import { StreamIdentifier, UUID } from './interfaces/shared';
import { StatsResp } from './interfaces/monitoring';
import { DetailsReq, DetailsResp } from './interfaces/users';

const GRPC_DEFAULT_PORT = 2113;
const STUB_INSTANCE_ID = randomUUID();
const STUB_EPOCH_ID = randomUUID();
const STUB_START_TIME = new Date().toISOString();

export function getNodeAddress(): string {
  const advertisedHost = process.env.ADVERTISE_HOST;
  if (advertisedHost) {
    return advertisedHost;
  }

  const grpcUrl = process.env.GRPC_URL ?? `0.0.0.0:${GRPC_DEFAULT_PORT}`;
  const normalized = grpcUrl.includes('://') ? grpcUrl : `dns://${grpcUrl}`;

  try {
    const parsed = new URL(normalized);
    const host = parsed.hostname;
    return host === '0.0.0.0' || host === '::' ? '127.0.0.1' : host;
  } catch {
    return '127.0.0.1';
  }
}

export function getNodePort(): number {
  const advertisedPort = process.env.ADVERTISE_PORT;
  if (advertisedPort) {
    return Number(advertisedPort);
  }

  const grpcUrl = process.env.GRPC_URL ?? `0.0.0.0:${GRPC_DEFAULT_PORT}`;
  const matchedPort = grpcUrl.match(/:(\d+)$/);
  return matchedPort ? Number(matchedPort[1]) : GRPC_DEFAULT_PORT;
}

export function createUuid(value = randomUUID()): UUID {
  return {
    string: value,
  };
}

export function nowTicksSinceUnixEpoch(): number {
  return Date.now() * 10_000;
}

export function createStubClusterInfo(): ClusterInfo {
  return {
    members: [
      {
        instanceId: createUuid(STUB_INSTANCE_ID),
        timeStamp: nowTicksSinceUnixEpoch(),
        state: MemberInfo_VNodeState.Leader,
        isAlive: true,
        httpEndPoint: {
          address: getNodeAddress(),
          port: getNodePort(),
        },
      },
    ],
  };
}

export function createStatsStream(): Observable<StatsResp> {
  return of({
    stats: {
      'proc-startTime': STUB_START_TIME,
      'proc-id': String(process.pid),
      'sys-hostName': hostname(),
      'es-node-role': 'Leader',
      'es-node-alive': 'true',
      'es-stub-admin-apis': 'true',
    },
  });
}

export function createScavengeResponse(
  scavengeId: string,
  scavengeResult: ScavengeResp_ScavengeResult,
): ScavengeResp {
  return {
    scavengeId,
    scavengeResult,
  };
}

export function createSubscriptionInfo(
  eventSource: string,
  groupName: string,
): SubscriptionInfo {
  return {
    eventSource,
    groupName,
    status: 'Stubbed',
    connections: [],
    averagePerSecond: 0,
    totalItems: 0,
    countSinceLastMeasurement: 0,
    lastCheckpointedEventPosition: '',
    lastKnownEventPosition: '',
    resolveLinkTos: false,
    startFrom: '0',
    messageTimeoutMilliseconds: 0,
    extraStatistics: false,
    maxRetryCount: 0,
    liveBufferSize: 0,
    bufferSize: 0,
    readBatchSize: 0,
    checkPointAfterMilliseconds: 0,
    minCheckPointCount: 0,
    maxCheckPointCount: 0,
    readBufferCount: 0,
    liveBufferCount: 0,
    retryBufferCount: 0,
    totalInFlightMessages: 0,
    outstandingMessagesCount: 0,
    namedConsumerStrategy: 'DispatchToSingle',
    maxSubscriberCount: 0,
    parkedMessageCount: 0,
  };
}

export function createPersistentListResponse(): ListResp {
  return {
    subscriptions: [],
  };
}

export function createPersistentGetInfoResponse(
  request: GetInfoReq,
): GetInfoResp {
  return {
    subscriptionInfo: createSubscriptionInfo(
      getPersistentEventSource(request.options?.streamIdentifier),
      request.options?.groupName ?? '',
    ),
  };
}

export function createPersistentReadStream(): Observable<ReadResp> {
  return of({
    subscriptionConfirmation: {
      subscriptionId: `stub-${randomUUID()}`,
    },
  });
}

export function getPersistentEventSource(
  streamIdentifier?: StreamIdentifier,
): string {
  if (!streamIdentifier) {
    return '$all';
  }

  return Buffer.from(streamIdentifier.streamName).toString('utf8');
}

export function createStubUserDetails(request: DetailsReq): DetailsResp {
  return {
    userDetails: {
      loginName: request.options?.loginName ?? 'stub-user',
      fullName: 'Stub User',
      groups: ['$ops'],
      lastUpdated: {
        ticksSinceEpoch: nowTicksSinceUnixEpoch(),
      },
      disabled: false,
    },
  };
}

export function createInfoResponseBody(): string {
  return JSON.stringify({
    dbVersion: '24.0.0',
    esVersion: '24.0.0',
    state: 'leader',
    features: {
      projections: false,
      userManagement: true,
      atomPub: false,
    },
    authentication: {
      type: 'insecure',
      properties: {},
    },
  });
}

export function createHttpGossipResponseBody(): string {
  return JSON.stringify({
    serverIp: getNodeAddress(),
    serverPort: getNodePort(),
    members: [
      {
        instanceId: STUB_INSTANCE_ID,
        timeStamp: STUB_START_TIME,
        state: 'Leader',
        isAlive: true,
        internalTcpIp: getNodeAddress(),
        internalTcpPort: 0,
        internalSecureTcpPort: 0,
        externalTcpIp: getNodeAddress(),
        externalTcpPort: 0,
        externalSecureTcpPort: 0,
        internalHttpEndPointIp: getNodeAddress(),
        internalHttpEndPointPort: getNodePort(),
        httpEndPointIp: getNodeAddress(),
        httpEndPointPort: getNodePort(),
        advertiseHostToClientAs: getNodeAddress(),
        advertiseHttpPortToClientAs: getNodePort(),
        advertiseTcpPortToClientAs: 0,
        lastCommitPosition: 0,
        writerCheckpoint: 0,
        chaserCheckpoint: 0,
        epochPosition: 0,
        epochNumber: 0,
        epochId: STUB_EPOCH_ID,
        nodePriority: 0,
        isReadOnlyReplica: false,
        esVersion: '24.0.0',
      },
    ],
  });
}

export function createHttpStatsResponseBody(): string {
  const [load1m, load5m, load15m] = loadavg();
  const freeMemory = freemem();
  const totalMemory = totalmem();
  const driveName = getDriveName();
  const driveTotalBytes = totalMemory;
  const driveAvailableBytes = freeMemory;
  const driveUsedBytes = Math.max(driveTotalBytes - driveAvailableBytes, 0);
  const driveUsage =
    driveTotalBytes > 0 ? (driveUsedBytes / driveTotalBytes) * 100 : 0;

  return JSON.stringify({
    proc: {
      startTime: STUB_START_TIME,
      id: process.pid,
      mem: process.memoryUsage().rss,
      cpu: 0,
      cpuScaled: 0,
      threadsCount: 0,
      contentionsRate: 0,
      thrownExceptionsRate: 0,
      gc: {
        allocationSpeed: 0,
        gen0ItemsCount: 0,
        gen0Size: 0,
        gen1ItemsCount: 0,
        gen1Size: 0,
        gen2ItemsCount: 0,
        gen2Size: 0,
        largeHeapSize: 0,
        timeInGc: 0,
        totalBytesInHeaps: 0,
      },
      diskIo: {
        readBytes: 0,
        writtenBytes: 0,
        readOps: 0,
        writeOps: 0,
      },
      tcp: {
        connections: 0,
        receivingSpeed: 0,
        sendingSpeed: 0,
        inSend: 0,
        measureTime: 0,
        pendingReceived: 0,
        pendingSend: 0,
        receivedBytesSinceLastRun: 0,
        receivedBytesTotal: 0,
        sentBytesSinceLastRun: 0,
        sentBytesTotal: 0,
      },
    },
    sys: {
      cpu: 0,
      loadavg: {
        '1m': load1m,
        '5m': load5m,
        '15m': load15m,
      },
      freeMem: freeMemory,
      totalMem: totalMemory,
      drive: {
        [driveName]: {
          availableBytes: driveAvailableBytes,
          totalBytes: driveTotalBytes,
          usage: driveUsage,
          usedBytes: driveUsedBytes,
        },
      },
    },
    es: {
      checksum: 0,
      checksumNonFlushed: 0,
      queue: {
        mainQueue: {
          queueName: 'MainQueue',
          groupName: '',
          avgItemsPerSecond: 0,
          avgProcessingTime: 0,
          currentIdleTime: '0:00:00:00.0000000',
          currentItemProcessingTime: null,
          idleTimePercent: 100,
          length: 0,
          lengthCurrentTryPeak: 0,
          lengthLifetimePeak: 0,
          totalItemsProcessed: 0,
          inProgressMessage: '',
          lastProcessedMessage: '',
        },
      },
      writerCheckpoint: 0,
      chaserCheckpoint: 0,
      epochPosition: 0,
      epochNumber: 0,
      runTimeVersion: '24.0.0',
      state: 'Leader',
    },
  });
}

function getDriveName(): string {
  if (osType() === 'Windows_NT') {
    return parsePath(process.cwd()).root || 'C:\\';
  }

  return process.cwd();
}
