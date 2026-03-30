# Benchmark Suite

This folder contains a multi-scenario benchmark runner for the adapter and a reference KurrentDB target.

## Included scenarios

- **Append: single stream latency**
- **Append: concurrent throughput**
- **Read: hot stream scan**
- **Mixed: read/write balance**

## Run

```bash
npm run bench:report
```

By default it benchmarks:

- Adapter: `kurrentdb://127.0.0.1:2113?tls=false`
- KurrentDB: `kurrentdb://127.0.0.1:2114?tls=false`

## Optional environment variables

- `BENCH_ADAPTER_NAME`
- `BENCH_ADAPTER_CONNECTION_STRING`
- `BENCH_KURRENTDB_NAME`
- `BENCH_KURRENTDB_CONNECTION_STRING`

## Output

Reports are generated under `benchmark/reports/`:

- JSON machine-readable report
- HTML human-readable report
