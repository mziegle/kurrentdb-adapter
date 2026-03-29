import { hrtime } from 'node:process';
import { jsonEvent } from '@kurrent/kurrentdb-client';
import { createPlaygroundClient } from './client';

export type BenchmarkConfig = {
  connectionString: string;
  targetName: string;
  streamCount: number;
  appendsPerStream: number;
  eventsPerAppend: number;
  payloadBytes: number;
  concurrency: number;
  warmupAppends: number;
};

type WorkerResult = {
  appendCount: number;
  eventCount: number;
  elapsedMs: number;
};

export type AppendBenchmarkResult = {
  totalElapsedMs: number;
  totalAppends: number;
  totalEvents: number;
  appendThroughput: number;
  eventThroughput: number;
  averageAppendLatencyMs: number;
  workerElapsedP50Ms: number;
  workerElapsedP95Ms: number;
};

function getEnvNumber(name: string, fallback: number): number {
  const raw = process.env[name];
  if (raw === undefined || raw === '') {
    return fallback;
  }

  const value = Number(raw);
  if (!Number.isFinite(value) || value <= 0) {
    throw new Error(`Environment variable ${name} must be a positive number.`);
  }

  return Math.floor(value);
}

export function loadConfigFromEnv(): BenchmarkConfig {
  const connectionString =
    process.env.KURRENTDB_CONNECTION_STRING ??
    'kurrentdb://127.0.0.1:2113?tls=false';

  return {
    connectionString,
    targetName: process.env.BENCH_TARGET_NAME ?? connectionString,
    streamCount: getEnvNumber('BENCH_STREAM_COUNT', 8),
    appendsPerStream: getEnvNumber('BENCH_APPENDS_PER_STREAM', 250),
    eventsPerAppend: getEnvNumber('BENCH_EVENTS_PER_APPEND', 1),
    payloadBytes: getEnvNumber('BENCH_PAYLOAD_BYTES', 512),
    concurrency: getEnvNumber('BENCH_CONCURRENCY', 8),
    warmupAppends: getEnvNumber('BENCH_WARMUP_APPENDS', 20),
  };
}

function createPayload(payloadBytes: number): string {
  return 'x'.repeat(payloadBytes);
}

function chunkStreams(streams: string[], chunkCount: number): string[][] {
  const chunks = Array.from({ length: chunkCount }, () => [] as string[]);

  for (const [index, stream] of streams.entries()) {
    chunks[index % chunkCount].push(stream);
  }

  return chunks.filter((chunk) => chunk.length > 0);
}

async function runWarmup(
  config: BenchmarkConfig,
  payload: string,
): Promise<void> {
  const client = createPlaygroundClient(config.connectionString);
  const streamName = `bench-warmup-${Date.now()}`;

  try {
    for (
      let appendIndex = 0;
      appendIndex < config.warmupAppends;
      appendIndex += 1
    ) {
      await client.appendToStream(
        streamName,
        Array.from({ length: config.eventsPerAppend }, (_, eventIndex) =>
          jsonEvent({
            type: 'bench-warmup',
            data: {
              appendIndex,
              eventIndex,
              payload,
            },
          }),
        ),
      );
    }
  } finally {
    await client.dispose();
  }
}

async function runWorker(
  streams: string[],
  config: BenchmarkConfig,
  payload: string,
): Promise<WorkerResult> {
  const client = createPlaygroundClient(config.connectionString);
  const startedAt = hrtime.bigint();

  try {
    for (const streamName of streams) {
      for (
        let appendIndex = 0;
        appendIndex < config.appendsPerStream;
        appendIndex += 1
      ) {
        await client.appendToStream(
          streamName,
          Array.from({ length: config.eventsPerAppend }, (_, eventIndex) =>
            jsonEvent({
              type: 'bench-append',
              data: {
                appendIndex,
                eventIndex,
                payload,
              },
            }),
          ),
        );
      }
    }
  } finally {
    await client.dispose();
  }

  const elapsedMs = Number(hrtime.bigint() - startedAt) / 1_000_000;
  const appendCount = streams.length * config.appendsPerStream;

  return {
    appendCount,
    eventCount: appendCount * config.eventsPerAppend,
    elapsedMs,
  };
}

export function formatNumber(value: number, digits = 2): string {
  return value.toLocaleString('en-US', {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  });
}

export async function runAppendBenchmark(
  config: BenchmarkConfig,
): Promise<AppendBenchmarkResult> {
  const payload = createPayload(config.payloadBytes);
  const effectiveConcurrency = Math.min(config.concurrency, config.streamCount);
  const streamNames = Array.from(
    { length: config.streamCount },
    (_, index) => `bench-append-${Date.now()}-${index}`,
  );
  const streamChunks = chunkStreams(streamNames, effectiveConcurrency);

  console.log('Append throughput benchmark');
  console.log(`Target: ${config.targetName}`);
  console.log(
    `Workload: ${config.streamCount} streams x ${config.appendsPerStream} appends x ${config.eventsPerAppend} events`,
  );
  console.log(
    `Payload bytes per event: ${config.payloadBytes}; concurrency: ${effectiveConcurrency}; warmup appends: ${config.warmupAppends}`,
  );

  await runWarmup(config, payload);

  const startedAt = hrtime.bigint();
  const workerResults = await Promise.all(
    streamChunks.map((streams) => runWorker(streams, config, payload)),
  );
  const totalElapsedMs = Number(hrtime.bigint() - startedAt) / 1_000_000;

  const totalAppends = workerResults.reduce(
    (sum, result) => sum + result.appendCount,
    0,
  );
  const totalEvents = workerResults.reduce(
    (sum, result) => sum + result.eventCount,
    0,
  );
  const appendThroughput = totalAppends / (totalElapsedMs / 1_000);
  const eventThroughput = totalEvents / (totalElapsedMs / 1_000);
  const averageAppendLatencyMs = totalElapsedMs / totalAppends;
  const workerElapsedMs = workerResults
    .map((result) => result.elapsedMs)
    .sort((left, right) => left - right);
  const p50WorkerMs =
    workerElapsedMs[Math.floor((workerElapsedMs.length - 1) * 0.5)];
  const p95WorkerMs =
    workerElapsedMs[Math.floor((workerElapsedMs.length - 1) * 0.95)];

  return {
    totalElapsedMs,
    totalAppends,
    totalEvents,
    appendThroughput,
    eventThroughput,
    averageAppendLatencyMs,
    workerElapsedP50Ms: p50WorkerMs,
    workerElapsedP95Ms: p95WorkerMs,
  };
}

function printBenchmarkSummary(result: AppendBenchmarkResult): void {
  console.log('');
  console.log(`Total elapsed: ${formatNumber(result.totalElapsedMs)} ms`);
  console.log(`Total appends: ${formatNumber(result.totalAppends, 0)}`);
  console.log(`Total events: ${formatNumber(result.totalEvents, 0)}`);
  console.log(
    `Append throughput: ${formatNumber(result.appendThroughput)} appends/s`,
  );
  console.log(
    `Event throughput: ${formatNumber(result.eventThroughput)} events/s`,
  );
  console.log(
    `Average append latency: ${formatNumber(result.averageAppendLatencyMs)} ms/append`,
  );
  console.log(
    `Worker elapsed p50: ${formatNumber(result.workerElapsedP50Ms)} ms`,
  );
  console.log(
    `Worker elapsed p95: ${formatNumber(result.workerElapsedP95Ms)} ms`,
  );
}

async function main(): Promise<void> {
  const config = loadConfigFromEnv();

  console.log('Append throughput benchmark');
  console.log(`Target: ${config.targetName}`);
  console.log(
    `Workload: ${config.streamCount} streams x ${config.appendsPerStream} appends x ${config.eventsPerAppend} events`,
  );
  console.log(
    `Payload bytes per event: ${config.payloadBytes}; concurrency: ${Math.min(config.concurrency, config.streamCount)}; warmup appends: ${config.warmupAppends}`,
  );

  const result = await runAppendBenchmark(config);
  printBenchmarkSummary(result);
}

if (require.main === module) {
  void main().catch((error: unknown) => {
    console.error(error);
    process.exitCode = 1;
  });
}
