import { randomUUID } from 'node:crypto';
import { hostname } from 'node:os';
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

export function getNodeAddress(): string {
  const grpcUrl = process.env.GRPC_URL ?? `0.0.0.0:${GRPC_DEFAULT_PORT}`;
  const normalized = grpcUrl.includes('://') ? grpcUrl : `dns://${grpcUrl}`;

  try {
    const parsed = new URL(normalized);
    const host = parsed.hostname;
    return host === '0.0.0.0' || host === '::' ? hostname() : host;
  } catch {
    return hostname();
  }
}

export function getNodePort(): number {
  const grpcUrl = process.env.GRPC_URL ?? `0.0.0.0:${GRPC_DEFAULT_PORT}`;
  const matchedPort = grpcUrl.match(/:(\d+)$/);
  return matchedPort ? Number(matchedPort[1]) : GRPC_DEFAULT_PORT;
}

export function createStructuredUuid(value = randomUUID()): UUID {
  const hex = value.replace(/-/g, '');
  const mostSignificantBits = Number(BigInt(`0x${hex.slice(0, 16)}`));
  const leastSignificantBits = Number(BigInt(`0x${hex.slice(16)}`));

  return {
    structured: {
      mostSignificantBits,
      leastSignificantBits,
    },
  };
}

export function nowTicksSinceUnixEpoch(): number {
  return Date.now() * 10_000;
}

export function createStubClusterInfo(): ClusterInfo {
  return {
    members: [
      {
        instanceId: createStructuredUuid(),
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
      'proc-startTime': new Date().toISOString(),
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
