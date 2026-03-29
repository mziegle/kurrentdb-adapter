import {
  AppendBenchmarkResult,
  BenchmarkConfig,
  formatNumber,
  runAppendBenchmark,
} from './append-throughput';

type Target = {
  connectionString: string;
  name: string;
};

type Scenario = {
  name: string;
  description: string;
  streamCount: number;
  appendsPerStream: number;
  eventsPerAppend: number;
  payloadBytes: number;
  concurrency: number;
  warmupAppends: number;
};

type ScenarioResult = {
  scenario: Scenario;
  adapter: AppendBenchmarkResult;
  kurrentdb: AppendBenchmarkResult;
};

function getEnvString(name: string, fallback: string): string {
  return process.env[name] && process.env[name] !== ''
    ? process.env[name]
    : fallback;
}

function loadTargets(): { adapter: Target; kurrentdb: Target } {
  return {
    adapter: {
      name: process.env.BENCH_ADAPTER_NAME ?? 'Adapter',
      connectionString: getEnvString(
        'BENCH_ADAPTER_CONNECTION_STRING',
        'kurrentdb://127.0.0.1:2113?tls=false',
      ),
    },
    kurrentdb: {
      name: process.env.BENCH_KURRENTDB_NAME ?? 'KurrentDB',
      connectionString: getEnvString(
        'BENCH_KURRENTDB_CONNECTION_STRING',
        'kurrentdb://127.0.0.1:2114?tls=false',
      ),
    },
  };
}

function loadScenarioDefaults(): Scenario[] {
  return [
    {
      name: 'single-stream-small',
      description: 'Single stream, small events, serialized appends',
      streamCount: 1,
      appendsPerStream: 10_000,
      eventsPerAppend: 1,
      payloadBytes: 512,
      concurrency: 1,
      warmupAppends: 20,
    },
    {
      name: 'single-stream-large-payload',
      description: 'Single stream, larger payloads, serialized appends',
      streamCount: 1,
      appendsPerStream: 5_000,
      eventsPerAppend: 1,
      payloadBytes: 4_096,
      concurrency: 1,
      warmupAppends: 20,
    },
    {
      name: 'multi-stream-concurrent',
      description: 'Multiple streams with concurrent writers',
      streamCount: 8,
      appendsPerStream: 2_000,
      eventsPerAppend: 1,
      payloadBytes: 512,
      concurrency: 8,
      warmupAppends: 20,
    },
    {
      name: 'batched-appends',
      description: 'Concurrent appends with 10 events per append call',
      streamCount: 8,
      appendsPerStream: 500,
      eventsPerAppend: 10,
      payloadBytes: 512,
      concurrency: 8,
      warmupAppends: 20,
    },
  ];
}

function toBenchmarkConfig(
  target: Target,
  scenario: Scenario,
): BenchmarkConfig {
  return {
    connectionString: target.connectionString,
    targetName: `${target.name} (${target.connectionString})`,
    streamCount: scenario.streamCount,
    appendsPerStream: scenario.appendsPerStream,
    eventsPerAppend: scenario.eventsPerAppend,
    payloadBytes: scenario.payloadBytes,
    concurrency: scenario.concurrency,
    warmupAppends: scenario.warmupAppends,
  };
}

function printScenarioHeader(scenario: Scenario): void {
  console.log('');
  console.log(`Scenario: ${scenario.name}`);
  console.log(`Description: ${scenario.description}`);
  console.log(
    `Workload: ${scenario.streamCount} streams x ${scenario.appendsPerStream} appends x ${scenario.eventsPerAppend} events`,
  );
  console.log(
    `Payload bytes per event: ${scenario.payloadBytes}; concurrency: ${scenario.concurrency}; warmup appends: ${scenario.warmupAppends}`,
  );
}

function printTargetSummary(
  label: string,
  result: AppendBenchmarkResult,
): void {
  console.log(
    `${label}: ${formatNumber(result.appendThroughput)} appends/s, ${formatNumber(result.averageAppendLatencyMs)} ms/append, ${formatNumber(result.eventThroughput)} events/s`,
  );
}

function printComparisonTable(results: ScenarioResult[]): void {
  console.log('');
  console.log('Summary');

  for (const { scenario, adapter, kurrentdb } of results) {
    const throughputDelta =
      ((adapter.appendThroughput - kurrentdb.appendThroughput) /
        kurrentdb.appendThroughput) *
      100;
    const latencyDelta =
      ((adapter.averageAppendLatencyMs - kurrentdb.averageAppendLatencyMs) /
        kurrentdb.averageAppendLatencyMs) *
      100;

    console.log(
      `${scenario.name}: adapter ${formatNumber(adapter.appendThroughput)} appends/s vs kurrentdb ${formatNumber(kurrentdb.appendThroughput)} appends/s (${formatNumber(throughputDelta)}%)`,
    );
    console.log(
      `${scenario.name}: adapter ${formatNumber(adapter.averageAppendLatencyMs)} ms vs kurrentdb ${formatNumber(kurrentdb.averageAppendLatencyMs)} ms (${formatNumber(latencyDelta)}%)`,
    );
  }
}

async function main(): Promise<void> {
  const targets = loadTargets();
  const scenarios = loadScenarioDefaults();
  const results: ScenarioResult[] = [];

  console.log('Append benchmark matrix');
  console.log(
    `Target order: ${targets.adapter.name} first, then ${targets.kurrentdb.name}`,
  );

  for (const scenario of scenarios) {
    printScenarioHeader(scenario);

    console.log(`Running ${targets.adapter.name}...`);
    const adapter = await runAppendBenchmark(
      toBenchmarkConfig(targets.adapter, scenario),
    );
    printTargetSummary(targets.adapter.name, adapter);

    console.log(`Running ${targets.kurrentdb.name}...`);
    const kurrentdb = await runAppendBenchmark(
      toBenchmarkConfig(targets.kurrentdb, scenario),
    );
    printTargetSummary(targets.kurrentdb.name, kurrentdb);

    results.push({
      scenario,
      adapter,
      kurrentdb,
    });
  }

  printComparisonTable(results);
}

void main().catch((error: unknown) => {
  console.error(error);
  process.exitCode = 1;
});
