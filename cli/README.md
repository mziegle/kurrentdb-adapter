# kcli

Developer-focused TypeScript CLI/TUI for working with a KurrentDB/EventStoreDB-compatible endpoint.

## What it supports

- Non-interactive commands for scripting:
  - `bench report`
  - `config show`
  - `ping`
  - `stream read <stream>`
  - `stream append <stream>`
  - `stream tail <stream>`
  - `test append`
  - `test read`
  - `test subscribe`
  - `test compare --stream <stream>`
- Optional interactive TUI (`tui --stream <stream>`) for live stream event watching.

## Install

```bash
cd cli
npm install
npm link
```

After that, the command is available as:

```bash
kcli ping
```

To remove the global link later:

```bash
npm unlink -g kurrentdb-adapter-cli
```

The CLI has its own `package.json` and should be installed and tooled from `cli/`, independent of the adapter's root package.

## Configuration

The default endpoint is:

```bash
kurrentdb://127.0.0.1:2113?tls=false
```

Configuration sources for the default endpoint, in order of precedence:

1. Environment variable: `KDB_CONNECTION`
2. Config file: `kcli.config.json` with `connectionString`
3. Built-in default: `kurrentdb://127.0.0.1:2113?tls=false`

Example config file:

```json
{
  "connectionString": "kurrentdb://127.0.0.1:2113?tls=false"
}
```

For `kcli test compare`, set a second endpoint with `KDB_COMPARE_CONNECTION` or `compareConnectionString`.

To inspect the effective configuration and its sources:

```bash
kcli config show
kcli config show --json
```

## Trace logging and sensitive data

`kcli trace` logs protocol metadata, not HTTP targets, headers, bodies, raw
wire previews, decoded gRPC bodies, or HTTP/2 GOAWAY debug data, at either info
or debug verbosity. The legacy `--suppress-http1-headers` and
`--suppress-http1-bodies` flags are accepted for compatibility but are now
redundant. HTTP path suppression still matches the complete request target,
including any query string. The proxy forwards traffic unchanged; it does not
provide encryption or authentication.

Trace logs can still contain client addresses, protocol metadata, and inferred
gRPC method names. Configured proxy and upstream hosts and socket error messages
are omitted from trace logs; listen failures are reported generically. Other application logs
may include identifiers or user-supplied strings, and key-name-based redaction
is not a guarantee against secrets in arbitrary values. Restrict log access,
avoid putting secrets in identifiers or endpoint host arguments (the adapter
also logs its configured upstream address), and review logs before sharing.
Historical logs may contain credentials and should be secured or removed and
exposed credentials rotated. For method inference, gzip, deflate, and Brotli
gRPC messages are decompressed to at most 4 MiB per message; larger messages are
forwarded unchanged but not decoded for tracing. This limit does not bound
uncompressed data, HTTP parser buffers, concurrent connections, or CPU use.
The proxy is a development diagnostic, not a hardened production network boundary.

## Commands

### Ping

```bash
kcli ping
```

### Benchmark report

```bash
kcli bench report
kcli bench report --fast
kcli bench report benchmark/reports
```

This writes JSON and HTML reports to the current working directory by default.
You can pass an output path to write them somewhere else, for example
`benchmark/reports/`. Use `--fast` for a reduced workload that returns quicker
when you only need a short comparison loop. It benchmarks the default endpoint from `KDB_CONNECTION`
or the built-in default `kurrentdb://127.0.0.1:2113?tls=false`, and compares it
to `KDB_COMPARE_CONNECTION` or the built-in compare default
`kurrentdb://127.0.0.1:2114?tls=false`.

Reports and progress output include target names but not connection strings;
keep secrets out of target names. Older reports may contain credentials and
should be removed or secured. Connection errors may still include details from
the client library, so review logs before sharing them.

For a useful comparison, use isolated, similarly provisioned endpoints with
equivalent durability, TLS, topology, network placement, and initial data state.
Record server versions, configuration, hardware, and background load separately;
the report does not capture them. Repeat runs, swap endpoint order, and compare
distributions rather than treating one result as proof of parity. The tool runs
targets sequentially in fixed order and does not randomize or aggregate runs.
Warmup uses a separate stream; prefill and warmup are outside measured timing.
Each run creates persistent benchmark streams and does not clean them up. The
read scenario repeatedly scans from the start of hot streams, concurrency is
limited by stream count, and mixed results combine read and append latencies and
event counts. Payload bytes describe the payload string, not total wire bytes.
These workloads are not a general performance or durability comparison.

### Stream read

```bash
kcli stream read my-stream --from 0 --limit 20
kcli stream read my-stream --json
```

### Stream append

```bash
kcli stream append my-stream \
  --type user-created \
  --data '{"userId":"u-1","name":"Alice"}' \
  --metadata '{"source":"cli"}' \
  --expected-revision any
```

You can also read JSON from files or stdin:

```bash
kcli stream append my-stream --type user-created --data @event.json
cat event.json | kcli stream append my-stream --type user-created --data -
```

### Stream tail

```bash
kcli stream tail my-stream
```

### Focused tests

```bash
kcli test append
kcli test read
kcli test subscribe
```

The subscription smoke test starts at `START`, not at numeric revision `0`.
In a differential test, KurrentDB delivered events *after* an explicit revision
while the MongoDB adapter also delivered that revision. This mismatch remains
unresolved; a passing smoke test does not prove subscription cursor parity.
The smoke test has no built-in deadline, so use an external timeout in automated
runs, for example `timeout 15s kcli test subscribe`. Run only against disposable
endpoints: the focused tests create streams and do not remove them.

The standalone CLI lockfile currently resolves client 1.1.1. A separate test of
1.2.0 passed the CLI suite and the four ping/append/read/subscribe smoke commands
against one disposable KurrentDB image and the MongoDB adapter. Its production
npm audit reported no vulnerabilities, but the full dependency audit still
reported two high and eight moderate findings, and broad compatibility/security
is not established. The upgrade has not been applied; the current production
lockfile audit reports the client/uuid moderate advisory GHSA-w5hq-g745-h8pq.
Audit results can change; re-run `npm audit` and `npm audit --omit=dev` before
release.

### Compare current endpoint vs another endpoint

```bash
kcli test compare --stream my-stream
```

This compares event id/type/revision/position/data/metadata/ordering between the default endpoint and the compare endpoint.

### Config inspection

```bash
kcli config show
kcli config show --json
```

This prints the effective default endpoint, the optional compare endpoint, and where each value came from.

### TUI

```bash
kcli tui --stream my-stream
```

## Scripts

- `npm run build` compile to `dist`
- `npm run dev -- <command>` build and run CLI locally
- `npm run bench:report` build and run the benchmark report workflow
- `npm link` expose the built CLI as the global `kcli` command
- `npm run test` run automated tests
- `npm run lint` run ESLint on CLI source
- `npm run typecheck` run TypeScript without emitting files
- Exit codes: `0` success, `2` usage error, `1` runtime failure

## Architecture

- `src/domain`: backend contracts + core types
- `src/application`: reusable use-case logic (tests, compare)
- `src/infrastructure`: concrete backend clients + output formatting
- `src/commands`: command registration and argument parsing
- `src/tui`: simple interactive stream tail view

## Assumptions

- Connection strings are valid KurrentDB/EventStoreDB URIs.
- The compare command targets two compatible endpoints with existing stream data.
- Current compare command targets existing stream data; seeding strategy can be added later.
