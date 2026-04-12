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

## Commands

### Ping

```bash
kcli ping
```

### Benchmark report

```bash
kcli bench report
```

This writes JSON and HTML reports under `benchmark/reports/` from the current
working directory.

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
