import { Injectable } from '@nestjs/common';
import {
  cpus,
  freemem,
  hostname,
  loadavg,
  totalmem,
  type as osType,
} from 'node:os';
import { parse as parsePath } from 'node:path';
import {
  PostgresEventStoreService,
  type EventStoreStatsSnapshot,
} from './postgres-event-store.service';
import { StatsResp } from './interfaces/monitoring';

type OperationName =
  | 'append'
  | 'batchAppend'
  | 'delete'
  | 'read'
  | 'tombstone'
  | 'startScavenge'
  | 'stopScavenge';

type OperationStats = {
  started: number;
  succeeded: number;
  failed: number;
  totalDurationMs: number;
  lastDurationMs: number | null;
  maxDurationMs: number;
  lastSuccessAt: string | null;
  lastFailureAt: string | null;
};

type AdapterStatsSnapshot = {
  processStartTime: string;
  storage: EventStoreStatsSnapshot;
  operations: Record<OperationName, OperationStats>;
};

@Injectable()
export class AdapterStatsService {
  private static readonly DEFAULT_SNAPSHOT_TTL_MS = 5000;
  private readonly processStartTime = new Date();
  private readonly snapshotTtlMs = this.resolveSnapshotTtlMs();
  private readonly operationStats: Record<OperationName, OperationStats> = {
    append: this.createEmptyOperationStats(),
    batchAppend: this.createEmptyOperationStats(),
    delete: this.createEmptyOperationStats(),
    read: this.createEmptyOperationStats(),
    tombstone: this.createEmptyOperationStats(),
    startScavenge: this.createEmptyOperationStats(),
    stopScavenge: this.createEmptyOperationStats(),
  };
  private cachedSnapshot:
    | { expiresAt: number; value: AdapterStatsSnapshot }
    | undefined;
  private pendingSnapshot: Promise<AdapterStatsSnapshot> | undefined;

  constructor(private readonly eventStore: PostgresEventStoreService) {}

  startOperation(name: OperationName): {
    failed: () => void;
    succeeded: () => void;
  } {
    const startedAt = Date.now();
    this.operationStats[name].started += 1;

    return {
      succeeded: () => {
        this.completeOperation(name, startedAt, 'succeeded');
      },
      failed: () => {
        this.completeOperation(name, startedAt, 'failed');
      },
    };
  }

  async createGrpcStats(): Promise<StatsResp> {
    const snapshot = await this.getSnapshot();

    return {
      stats: {
        'proc-startTime': snapshot.processStartTime,
        'proc-id': String(process.pid),
        'proc-mem-rssBytes': String(process.memoryUsage().rss),
        'proc-uptimeSeconds': this.formatFixed(process.uptime()),
        'sys-hostName': hostname(),
        'sys-freeMemBytes': String(freemem()),
        'sys-totalMemBytes': String(totalmem()),
        'sys-loadavg-1m': this.formatFixed(loadavg()[0] ?? 0),
        'es-node-role': 'Leader',
        'es-node-alive': 'true',
        'es-writerCheckpoint': String(snapshot.storage.currentGlobalPosition),
        'es-chaserCheckpoint': String(snapshot.storage.currentGlobalPosition),
        'adapter-totalEvents': String(snapshot.storage.totalEvents),
        'adapter-streamCount': String(snapshot.storage.streamCount),
        'adapter-tombstonedStreamCount': String(
          snapshot.storage.tombstonedStreamCount,
        ),
        'adapter-retentionPolicyCount': String(
          snapshot.storage.retentionPolicyCount,
        ),
        'adapter-activeScavenges': String(snapshot.storage.activeScavenges),
        'adapter-pgPool-totalCount': String(snapshot.storage.pgPoolTotalCount),
        'adapter-pgPool-idleCount': String(snapshot.storage.pgPoolIdleCount),
        'adapter-pgPool-waitingCount': String(
          snapshot.storage.pgPoolWaitingCount,
        ),
        ...this.createGrpcOperationStats(snapshot.operations),
      },
    };
  }

  async createHttpStatsResponseBody(): Promise<string> {
    const snapshot = await this.getSnapshot();
    const [load1m, load5m, load15m] = loadavg();
    const freeMemory = freemem();
    const totalMemory = totalmem();
    const driveName = this.getDriveName();
    const driveTotalBytes = totalMemory;
    const driveAvailableBytes = freeMemory;
    const driveUsedBytes = Math.max(driveTotalBytes - driveAvailableBytes, 0);
    const driveUsage =
      driveTotalBytes > 0
        ? this.roundToTwoDecimals((driveUsedBytes / driveTotalBytes) * 100)
        : 0;
    const cpuUsage = process.cpuUsage();
    const memoryUsage = process.memoryUsage();

    return JSON.stringify({
      proc: {
        startTime: snapshot.processStartTime,
        id: process.pid,
        mem: memoryUsage.rss,
        cpu: 0,
        threadsCount: 0,
        contentionsRate: 0,
        thrownExceptionsRate: 0,
        uptimeSeconds: process.uptime(),
        nodeVersion: process.version,
        cpuUsage: {
          user: cpuUsage.user,
          system: cpuUsage.system,
        },
        heap: {
          used: memoryUsage.heapUsed,
          total: memoryUsage.heapTotal,
          external: memoryUsage.external,
        },
        gc: {
          allocationSpeed: 0,
          fragmentation: 0,
          gen0ItemsCount: 0,
          gen0Size: 0,
          gen1ItemsCount: 0,
          gen1Size: 0,
          gen2ItemsCount: 0,
          gen2Size: 0,
          largeHeapSize: 0,
          timeInGc: 0,
          totalBytesInHeaps: memoryUsage.heapTotal,
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
          measureTime: '00:00:00.0000000',
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
        cpuCount: cpus().length,
        hostName: hostname(),
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
          'index Committer': this.createQueueStats('Index Committer', {
            currentIdleTime: '0:00:00:00.0000000',
            lastProcessedMessage: 'CommitChased',
          }),
          mainQueue: this.createQueueStats('MainQueue', {
            currentIdleTime: '0:00:00:00.0000000',
            lastProcessedMessage: 'Schedule',
          }),
          monitoringQueue: this.createQueueStats('MonitoringQueue', {
            currentIdleTime: null,
            currentItemProcessingTime: '0:00:00:00.0000000',
            inProgressMessage: 'GetFreshStats',
            lastProcessedMessage: 'GetFreshStats',
            totalItemsProcessed:
              snapshot.operations.read.started +
              snapshot.operations.append.started +
              snapshot.operations.batchAppend.started,
          }),
          persistentSubscriptions: this.createQueueStats(
            'PersistentSubscriptions',
            {
              currentIdleTime: '0:00:00:00.0000000',
              lastProcessedMessage: 'GetAllPersistentSubscriptionStats',
            },
          ),
          redaction: this.createQueueStats('Redaction'),
          'storage Chaser': this.createQueueStats('Storage Chaser', {
            currentIdleTime: '0:00:00:00.0000000',
            lastProcessedMessage: 'ChaserCheckpointFlush',
          }),
          storageReaderQueue: this.createQueueStats('StorageReaderQueue', {
            currentIdleTime: '0:00:00:00.0000000',
          }),
          storageWriterQueue: this.createQueueStats('StorageWriterQueue', {
            currentIdleTime: '0:00:00:00.0000000',
            lastProcessedMessage: 'WritePrepares',
          }),
          subscriptions: this.createQueueStats('Subscriptions', {
            currentIdleTime: '0:00:00:00.0000000',
            lastProcessedMessage: 'CheckPollTimeout',
          }),
          threadPool: this.createQueueStats('ThreadPool'),
          timer: this.createQueueStats('Timer', {
            currentIdleTime: '0:00:00:00.0000000',
            lastProcessedMessage: 'ExecuteScheduledTasks',
          }),
        },
        cache: {
          streamInfo: {
            lastEventNumber: this.createCacheStats(
              'LastEventNumber',
              snapshot.storage.streamCount,
              Math.max(snapshot.storage.streamCount, 100000),
            ),
            metadata: this.createCacheStats(
              'Metadata',
              snapshot.storage.retentionPolicyCount,
              Math.max(snapshot.storage.retentionPolicyCount, 100000),
            ),
            ...this.createCacheStats(
              'StreamInfo',
              snapshot.storage.streamCount,
              Math.max(snapshot.storage.streamCount, 200000),
            ),
          },
          ...this.createCacheStats(
            'cache',
            snapshot.storage.totalEvents,
            Math.max(snapshot.storage.totalEvents, 200000),
          ),
        },
        writer: {
          lastFlushSize: 0,
          lastFlushDelayMs: 0,
          meanFlushSize: 0,
          meanFlushDelayMs: 0,
          maxFlushSize: 0,
          maxFlushDelayMs: 0,
          queuedFlushMessages: 0,
        },
        readIndex: {
          cachedRecord: 0,
          notCachedRecord: 0,
          cachedStreamInfo: snapshot.storage.streamCount,
          notCachedStreamInfo: 0,
          cachedTransInfo: 0,
          notCachedTransInfo: 0,
        },
        writerCheckpoint: snapshot.storage.currentGlobalPosition,
        chaserCheckpoint: snapshot.storage.currentGlobalPosition,
        epochPosition: snapshot.storage.currentGlobalPosition,
        epochNumber: 0,
        runTimeVersion: process.env.STUB_SERVER_VERSION ?? '26.0.2.3257-None',
        state: 'Leader',
      },
      adapter: {
        storage: snapshot.storage,
        requests: this.createHttpOperationStats(snapshot.operations),
      },
    });
  }

  private async getSnapshot(): Promise<AdapterStatsSnapshot> {
    const now = Date.now();
    if (this.cachedSnapshot && this.cachedSnapshot.expiresAt > now) {
      return this.cachedSnapshot.value;
    }

    if (this.pendingSnapshot) {
      return this.pendingSnapshot;
    }

    this.pendingSnapshot = this.buildSnapshot();
    try {
      const snapshot = await this.pendingSnapshot;
      this.cachedSnapshot = {
        expiresAt: now + this.snapshotTtlMs,
        value: snapshot,
      };
      return snapshot;
    } finally {
      this.pendingSnapshot = undefined;
    }
  }

  private async buildSnapshot(): Promise<AdapterStatsSnapshot> {
    const storage = await this.eventStore.getStatsSnapshot();

    return {
      processStartTime: this.processStartTime.toISOString(),
      storage,
      operations: this.cloneOperationStats(),
    };
  }

  private resolveSnapshotTtlMs(): number {
    const configuredValue = Number(
      process.env.STATS_CACHE_TTL_MS ??
        AdapterStatsService.DEFAULT_SNAPSHOT_TTL_MS,
    );

    if (!Number.isFinite(configuredValue) || configuredValue <= 0) {
      return AdapterStatsService.DEFAULT_SNAPSHOT_TTL_MS;
    }

    return Math.floor(configuredValue);
  }

  private cloneOperationStats(): Record<OperationName, OperationStats> {
    return {
      append: { ...this.operationStats.append },
      batchAppend: { ...this.operationStats.batchAppend },
      delete: { ...this.operationStats.delete },
      read: { ...this.operationStats.read },
      tombstone: { ...this.operationStats.tombstone },
      startScavenge: { ...this.operationStats.startScavenge },
      stopScavenge: { ...this.operationStats.stopScavenge },
    };
  }

  private createGrpcOperationStats(
    operations: Record<OperationName, OperationStats>,
  ): Record<string, string> {
    return Object.fromEntries(
      Object.entries(operations).flatMap(([name, stats]) => [
        [`req-${name}-started`, String(stats.started)],
        [`req-${name}-succeeded`, String(stats.succeeded)],
        [`req-${name}-failed`, String(stats.failed)],
        [
          `req-${name}-avgMs`,
          this.formatFixed(this.getAverageDurationMs(stats)),
        ],
        [`req-${name}-maxMs`, this.formatFixed(stats.maxDurationMs)],
        [`req-${name}-lastMs`, this.formatNullableFixed(stats.lastDurationMs)],
      ]),
    );
  }

  private createHttpOperationStats(
    operations: Record<OperationName, OperationStats>,
  ): Record<string, object> {
    return Object.fromEntries(
      Object.entries(operations).map(([name, stats]) => [
        name,
        {
          started: stats.started,
          succeeded: stats.succeeded,
          failed: stats.failed,
          avgDurationMs: this.getAverageDurationMs(stats),
          maxDurationMs: stats.maxDurationMs,
          lastDurationMs: stats.lastDurationMs,
          lastSuccessAt: stats.lastSuccessAt,
          lastFailureAt: stats.lastFailureAt,
        },
      ]),
    );
  }

  private completeOperation(
    name: OperationName,
    startedAt: number,
    outcome: 'failed' | 'succeeded',
  ): void {
    const durationMs = Date.now() - startedAt;
    const stats = this.operationStats[name];
    stats[outcome] += 1;
    stats.totalDurationMs += durationMs;
    stats.lastDurationMs = durationMs;
    stats.maxDurationMs = Math.max(stats.maxDurationMs, durationMs);

    if (outcome === 'succeeded') {
      stats.lastSuccessAt = new Date().toISOString();
      return;
    }

    stats.lastFailureAt = new Date().toISOString();
  }

  private getAverageDurationMs(stats: OperationStats): number {
    const completed = stats.succeeded + stats.failed;
    return completed === 0 ? 0 : stats.totalDurationMs / completed;
  }

  private createEmptyOperationStats(): OperationStats {
    return {
      started: 0,
      succeeded: 0,
      failed: 0,
      totalDurationMs: 0,
      lastDurationMs: null,
      maxDurationMs: 0,
      lastSuccessAt: null,
      lastFailureAt: null,
    };
  }

  private formatFixed(value: number): string {
    return value.toFixed(2);
  }

  private formatNullableFixed(value: number | null): string {
    return value === null ? '' : value.toFixed(2);
  }

  private roundToTwoDecimals(value: number): number {
    return Number(value.toFixed(2));
  }

  private getDriveName(): string {
    if (osType() === 'Windows_NT') {
      return parsePath(process.cwd()).root || 'C:\\';
    }

    return process.cwd();
  }

  private createQueueStats(
    queueName: string,
    overrides?: Partial<{
      currentIdleTime: string | null;
      currentItemProcessingTime: string | null;
      inProgressMessage: string;
      lastProcessedMessage: string;
      totalItemsProcessed: number;
    }>,
  ) {
    return {
      queueName,
      groupName: '',
      avgItemsPerSecond: 0,
      avgProcessingTime: 0,
      currentIdleTime: overrides?.currentIdleTime ?? null,
      currentItemProcessingTime: overrides?.currentItemProcessingTime ?? null,
      idleTimePercent: 100,
      length: 0,
      lengthCurrentTryPeak: 0,
      lengthLifetimePeak: 0,
      totalItemsProcessed: overrides?.totalItemsProcessed ?? 0,
      inProgressMessage: overrides?.inProgressMessage ?? '<none>',
      lastProcessedMessage: overrides?.lastProcessedMessage ?? '<none>',
    };
  }

  private createCacheStats(
    name: string,
    count: number,
    capacityEntries: number,
  ) {
    return {
      name,
      count,
      sizeEntries: count,
      capacityEntries,
      utilizationPercent:
        capacityEntries > 0 ? (count / capacityEntries) * 100 : 0,
    };
  }
}
