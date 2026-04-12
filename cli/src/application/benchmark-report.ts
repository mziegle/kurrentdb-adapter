import { mkdir, writeFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import { hrtime } from 'node:process';
import {
  FORWARDS,
  KurrentDBClient,
  START,
  jsonEvent,
} from '@kurrent/kurrentdb-client';

type Target = {
  name: string;
  connectionString: string;
};

type ScenarioKind = 'append' | 'read' | 'mixed';

type Scenario = {
  id: string;
  title: string;
  description: string;
  kind: ScenarioKind;
  streamCount: number;
  eventsPerStream: number;
  payloadBytes: number;
  concurrency: number;
  readMaxCount?: number;
  mixedReadEvery?: number;
  warmupOperations: number;
};

type Percentiles = {
  p50Ms: number;
  p95Ms: number;
  p99Ms: number;
};

type ScenarioResult = {
  scenarioId: string;
  scenarioTitle: string;
  targetName: string;
  kind: ScenarioKind;
  elapsedMs: number;
  operationCount: number;
  operationsPerSecond: number;
  eventCount: number;
  eventsPerSecond: number;
  avgLatencyMs: number;
  latency: Percentiles;
};

type BenchmarkReport = {
  generatedAt: string;
  nodeVersion: string;
  targets: Target[];
  scenarios: Scenario[];
  results: ScenarioResult[];
};

const DEFAULT_TARGETS: Target[] = [
  {
    name: process.env.BENCH_ADAPTER_NAME ?? 'Adapter',
    connectionString:
      process.env.BENCH_ADAPTER_CONNECTION_STRING ??
      'kurrentdb://127.0.0.1:2113?tls=false',
  },
  {
    name: process.env.BENCH_KURRENTDB_NAME ?? 'KurrentDB',
    connectionString:
      process.env.BENCH_KURRENTDB_CONNECTION_STRING ??
      'kurrentdb://127.0.0.1:2114?tls=false',
  },
];

const SCENARIOS: Scenario[] = [
  {
    id: 'append-single-stream-latency',
    title: 'Append: single stream latency',
    description:
      'Serialized appends to one stream. This stresses write latency and expected-revision flow.',
    kind: 'append',
    streamCount: 1,
    eventsPerStream: 4_000,
    payloadBytes: 512,
    concurrency: 1,
    warmupOperations: 50,
  },
  {
    id: 'append-multi-stream-throughput',
    title: 'Append: concurrent throughput',
    description:
      'Multiple streams written in parallel to highlight scheduler, lock, and I/O contention.',
    kind: 'append',
    streamCount: 12,
    eventsPerStream: 1_000,
    payloadBytes: 1_024,
    concurrency: 12,
    warmupOperations: 100,
  },
  {
    id: 'read-hot-stream-throughput',
    title: 'Read: hot stream scan',
    description:
      'Prefill stream data and repeatedly read slices from stream start.',
    kind: 'read',
    streamCount: 4,
    eventsPerStream: 4_000,
    payloadBytes: 256,
    concurrency: 8,
    readMaxCount: 512,
    warmupOperations: 30,
  },
  {
    id: 'mixed-read-write-balance',
    title: 'Mixed: read/write balance',
    description:
      'Append workload with periodic reads against the same streams to emulate realistic mixed traffic.',
    kind: 'mixed',
    streamCount: 8,
    eventsPerStream: 1_200,
    payloadBytes: 768,
    concurrency: 8,
    mixedReadEvery: 20,
    readMaxCount: 200,
    warmupOperations: 40,
  },
];

function formatNumber(value: number, digits = 2): string {
  return value.toLocaleString('en-US', {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  });
}

function uniqueStreamName(
  tag: string,
  scenarioId: string,
  streamIndex: number,
): string {
  return `benchmark-${tag}-${scenarioId}-${Date.now()}-${streamIndex}`;
}

function createPayload(payloadBytes: number): string {
  return 'x'.repeat(payloadBytes);
}

function toMilliseconds(deltaNs: bigint): number {
  return Number(deltaNs) / 1_000_000;
}

function percentile(sortedValues: number[], value: number): number {
  if (sortedValues.length === 0) {
    return 0;
  }

  const index = Math.floor((sortedValues.length - 1) * value);
  return sortedValues[index];
}

function summarizeLatencies(latenciesMs: number[]): Percentiles {
  const sorted = [...latenciesMs].sort((left, right) => left - right);

  return {
    p50Ms: percentile(sorted, 0.5),
    p95Ms: percentile(sorted, 0.95),
    p99Ms: percentile(sorted, 0.99),
  };
}

function splitStreams(streams: string[], workers: number): string[][] {
  const buckets = Array.from({ length: workers }, () => [] as string[]);

  for (const [index, streamName] of streams.entries()) {
    buckets[index % workers].push(streamName);
  }

  return buckets.filter((bucket) => bucket.length > 0);
}

async function appendOne(
  client: KurrentDBClient,
  streamName: string,
  payload: string,
  streamStep: number,
): Promise<number> {
  const startedAt = hrtime.bigint();

  await client.appendToStream(streamName, [
    jsonEvent({
      type: 'bench-event',
      data: {
        streamStep,
        payload,
      },
    }),
  ]);

  return toMilliseconds(hrtime.bigint() - startedAt);
}

async function readOne(
  client: KurrentDBClient,
  streamName: string,
  maxCount: number,
): Promise<{ latencyMs: number; eventsRead: number }> {
  const startedAt = hrtime.bigint();
  const events = client.readStream(streamName, {
    direction: FORWARDS,
    fromRevision: START,
    maxCount,
  });

  let eventsRead = 0;
  for await (const resolved of events) {
    if (!resolved.event) {
      continue;
    }

    eventsRead += 1;
  }

  return {
    latencyMs: toMilliseconds(hrtime.bigint() - startedAt),
    eventsRead,
  };
}

async function prefillStreams(
  client: KurrentDBClient,
  streamNames: string[],
  payload: string,
  eventsPerStream: number,
): Promise<void> {
  for (const streamName of streamNames) {
    for (let step = 0; step < eventsPerStream; step += 1) {
      await appendOne(client, streamName, payload, step);
    }
  }
}

async function warmup(
  client: KurrentDBClient,
  scenario: Scenario,
  streamNames: string[],
  payload: string,
): Promise<void> {
  const streamName = streamNames[0];

  for (let index = 0; index < scenario.warmupOperations; index += 1) {
    await appendOne(client, streamName, payload, index);
  }

  if (scenario.kind !== 'append') {
    await readOne(client, streamName, scenario.readMaxCount ?? 128);
  }
}

async function runAppendScenario(
  client: KurrentDBClient,
  scenario: Scenario,
  streamNames: string[],
  payload: string,
): Promise<{
  operationCount: number;
  latenciesMs: number[];
  eventCount: number;
  elapsedMs: number;
}> {
  const chunks = splitStreams(
    streamNames,
    Math.min(scenario.concurrency, streamNames.length),
  );
  const startedAt = hrtime.bigint();

  const workerResults = await Promise.all(
    chunks.map(async (chunk) => {
      const latenciesMs: number[] = [];

      for (const streamName of chunk) {
        for (let step = 0; step < scenario.eventsPerStream; step += 1) {
          latenciesMs.push(await appendOne(client, streamName, payload, step));
        }
      }

      return latenciesMs;
    }),
  );

  const latenciesMs = workerResults.flat();
  const elapsedMs = toMilliseconds(hrtime.bigint() - startedAt);

  return {
    operationCount: latenciesMs.length,
    latenciesMs,
    eventCount: latenciesMs.length,
    elapsedMs,
  };
}

async function runReadScenario(
  client: KurrentDBClient,
  scenario: Scenario,
  streamNames: string[],
): Promise<{
  operationCount: number;
  latenciesMs: number[];
  eventCount: number;
  elapsedMs: number;
}> {
  const readMaxCount = scenario.readMaxCount ?? 200;
  const chunks = splitStreams(
    streamNames,
    Math.min(scenario.concurrency, streamNames.length),
  );
  const startedAt = hrtime.bigint();

  const workerResults = await Promise.all(
    chunks.map(async (chunk) => {
      const latenciesMs: number[] = [];
      let eventCount = 0;

      for (let cycle = 0; cycle < scenario.eventsPerStream; cycle += 1) {
        const streamName = chunk[cycle % chunk.length];
        const readResult = await readOne(client, streamName, readMaxCount);

        latenciesMs.push(readResult.latencyMs);
        eventCount += readResult.eventsRead;
      }

      return {
        latenciesMs,
        eventCount,
      };
    }),
  );

  const elapsedMs = toMilliseconds(hrtime.bigint() - startedAt);

  return {
    operationCount: workerResults.reduce(
      (total, result) => total + result.latenciesMs.length,
      0,
    ),
    latenciesMs: workerResults.flatMap((result) => result.latenciesMs),
    eventCount: workerResults.reduce(
      (total, result) => total + result.eventCount,
      0,
    ),
    elapsedMs,
  };
}

async function runMixedScenario(
  client: KurrentDBClient,
  scenario: Scenario,
  streamNames: string[],
  payload: string,
): Promise<{
  operationCount: number;
  latenciesMs: number[];
  eventCount: number;
  elapsedMs: number;
}> {
  const readEvery = scenario.mixedReadEvery ?? 10;
  const readMaxCount = scenario.readMaxCount ?? 100;
  const chunks = splitStreams(
    streamNames,
    Math.min(scenario.concurrency, streamNames.length),
  );
  const startedAt = hrtime.bigint();

  const workerResults = await Promise.all(
    chunks.map(async (chunk) => {
      const latenciesMs: number[] = [];
      let eventCount = 0;

      for (const streamName of chunk) {
        for (let step = 0; step < scenario.eventsPerStream; step += 1) {
          latenciesMs.push(await appendOne(client, streamName, payload, step));
          eventCount += 1;

          if ((step + 1) % readEvery === 0) {
            const readResult = await readOne(client, streamName, readMaxCount);
            latenciesMs.push(readResult.latencyMs);
            eventCount += readResult.eventsRead;
          }
        }
      }

      return { latenciesMs, eventCount };
    }),
  );

  const elapsedMs = toMilliseconds(hrtime.bigint() - startedAt);

  return {
    operationCount: workerResults.reduce(
      (total, result) => total + result.latenciesMs.length,
      0,
    ),
    latenciesMs: workerResults.flatMap((result) => result.latenciesMs),
    eventCount: workerResults.reduce(
      (total, result) => total + result.eventCount,
      0,
    ),
    elapsedMs,
  };
}

async function runScenario(
  target: Target,
  scenario: Scenario,
): Promise<ScenarioResult> {
  const client = KurrentDBClient.connectionString([
    target.connectionString,
  ] as unknown as TemplateStringsArray);
  const payload = createPayload(scenario.payloadBytes);
  const streamNames = Array.from({ length: scenario.streamCount }, (_, index) =>
    uniqueStreamName(target.name.toLowerCase(), scenario.id, index),
  );

  try {
    if (scenario.kind === 'read' || scenario.kind === 'mixed') {
      await prefillStreams(
        client,
        streamNames,
        payload,
        scenario.eventsPerStream,
      );
    }

    await warmup(client, scenario, streamNames, payload);

    const rawResult =
      scenario.kind === 'append'
        ? await runAppendScenario(client, scenario, streamNames, payload)
        : scenario.kind === 'read'
          ? await runReadScenario(client, scenario, streamNames)
          : await runMixedScenario(client, scenario, streamNames, payload);

    const avgLatencyMs =
      rawResult.latenciesMs.length > 0
        ? rawResult.latenciesMs.reduce((sum, value) => sum + value, 0) /
          rawResult.latenciesMs.length
        : 0;

    return {
      scenarioId: scenario.id,
      scenarioTitle: scenario.title,
      targetName: target.name,
      kind: scenario.kind,
      elapsedMs: rawResult.elapsedMs,
      operationCount: rawResult.operationCount,
      operationsPerSecond:
        rawResult.operationCount / (rawResult.elapsedMs / 1_000),
      eventCount: rawResult.eventCount,
      eventsPerSecond: rawResult.eventCount / (rawResult.elapsedMs / 1_000),
      avgLatencyMs,
      latency: summarizeLatencies(rawResult.latenciesMs),
    };
  } finally {
    await client.dispose();
  }
}

function createHtmlReport(report: BenchmarkReport): string {
  const payload = JSON.stringify(report);

  return `<!doctype html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>KurrentDB Adapter Benchmark Report</title>
    <style>
      body { font-family: Inter, Arial, sans-serif; margin: 24px; color: #0f172a; background: #f8fafc; }
      h1, h2 { margin-bottom: 8px; }
      .meta { color: #334155; margin-bottom: 20px; }
      .card { background: white; border-radius: 12px; padding: 16px; margin-bottom: 16px; box-shadow: 0 1px 2px rgba(0,0,0,.08); }
      table { border-collapse: collapse; width: 100%; margin-top: 8px; }
      th, td { padding: 8px; border-bottom: 1px solid #e2e8f0; text-align: left; }
      th { background: #f1f5f9; font-weight: 600; }
      .badge { display: inline-block; padding: 4px 10px; border-radius: 9999px; font-size: 12px; background: #e2e8f0; }
    </style>
  </head>
  <body>
    <h1>KurrentDB Adapter Benchmark</h1>
    <p class="meta" id="meta"></p>
    <div id="overview" class="card"></div>
    <div id="details"></div>
    <script>
      const report = ${payload};

      function n(value, digits = 2) {
        return value.toLocaleString('en-US', { minimumFractionDigits: digits, maximumFractionDigits: digits });
      }

      const meta = document.getElementById('meta');
      meta.textContent = 'Generated at: ' + new Date(report.generatedAt).toLocaleString() + ' • Node: ' + report.nodeVersion;

      const overview = document.getElementById('overview');
      overview.innerHTML = [
        '<h2>Overview</h2>',
        '<p>Targets: ' + report.targets.map((t) => t.name + ' (' + t.connectionString + ')').join(' vs ') + '</p>',
        '<p>Scenarios: ' + report.scenarios.length + '</p>',
      ].join('');

      const details = document.getElementById('details');

      for (const scenario of report.scenarios) {
        const results = report.results.filter((r) => r.scenarioId === scenario.id);
        const rows = results.map((r) =>
          '<tr>' +
          '<td>' + r.targetName + '</td>' +
          '<td>' + n(r.operationsPerSecond) + '</td>' +
          '<td>' + n(r.eventsPerSecond) + '</td>' +
          '<td>' + n(r.avgLatencyMs) + '</td>' +
          '<td>' + n(r.latency.p95Ms) + '</td>' +
          '<td>' + n(r.latency.p99Ms) + '</td>' +
          '</tr>',
        ).join('');

        const card = document.createElement('section');
        card.className = 'card';
        card.innerHTML = [
          '<h2>' + scenario.title + '</h2>',
          '<p>' + scenario.description + '</p>',
          '<p><span class="badge">' + scenario.kind + '</span></p>',
          '<table>',
          '<thead><tr><th>Target</th><th>Ops/s</th><th>Events/s</th><th>Avg latency (ms)</th><th>P95 (ms)</th><th>P99 (ms)</th></tr></thead>',
          '<tbody>' + rows + '</tbody>',
          '</table>',
        ].join('');

        details.appendChild(card);
      }
    </script>
  </body>
</html>`;
}

export async function runBenchmarkReport(): Promise<void> {
  const outputDir = resolve(process.cwd(), 'benchmark', 'reports');
  await mkdir(outputDir, { recursive: true });

  const results: ScenarioResult[] = [];
  console.log('Starting benchmark suite...');

  for (const scenario of SCENARIOS) {
    console.log(`\nScenario: ${scenario.title}`);
    console.log(`Description: ${scenario.description}`);

    for (const target of DEFAULT_TARGETS) {
      console.log(
        `Running target: ${target.name} (${target.connectionString})`,
      );
      const result = await runScenario(target, scenario);
      results.push(result);

      console.log(
        `${target.name} => ${formatNumber(result.operationsPerSecond)} ops/s, ${formatNumber(result.avgLatencyMs)} ms avg latency, p95 ${formatNumber(result.latency.p95Ms)} ms`,
      );
    }
  }

  const report: BenchmarkReport = {
    generatedAt: new Date().toISOString(),
    nodeVersion: process.version,
    targets: DEFAULT_TARGETS,
    scenarios: SCENARIOS,
    results,
  };

  const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
  const jsonPath = resolve(outputDir, `benchmark-report-${timestamp}.json`);
  const htmlPath = resolve(outputDir, `benchmark-report-${timestamp}.html`);

  await writeFile(jsonPath, JSON.stringify(report, null, 2), 'utf8');
  await writeFile(htmlPath, createHtmlReport(report), 'utf8');

  console.log('\nBenchmark completed.');
  console.log(`JSON report: ${jsonPath}`);
  console.log(`HTML report: ${htmlPath}`);
}
