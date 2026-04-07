# kcli

Developer-focused TypeScript CLI/TUI for validating a KurrentDB/EventStoreDB-compatible backend (for example this adapter) against a reference backend.

## What it supports

- Non-interactive commands for scripting:
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
kcli ping --backend adapter
```

To remove the global link later:

```bash
npm unlink -g kurrentdb-adapter-cli
```

The CLI has its own `package.json` and should be installed and tooled from `cli/`, independent of the adapter's root package.

## Configuration

The CLI expects two named backends:

- `reference`
- `adapter`

Configuration sources (in order of precedence):

1. Environment variables:
   - `KDB_REFERENCE_CONNECTION`
   - `KDB_ADAPTER_CONNECTION`
2. Config file: `kcli.config.json` (or custom path via `KDB_CLI_CONFIG_PATH`)

Example config file:

```json
{
  "defaultBackend": "adapter",
  "backends": {
    "reference": {
      "kind": "kurrent",
      "connectionString": "kurrentdb://127.0.0.1:2114?tls=false"
    },
    "adapter": {
      "kind": "kurrent",
      "connectionString": "kurrentdb://127.0.0.1:2113?tls=false"
    }
  }
}
```

## Commands

### Ping

```bash
kcli ping --backend adapter
```

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

### Compare adapter vs reference

```bash
kcli test compare --stream my-stream
```

This compares event id/type/revision/position/data/metadata/ordering.

### TUI

```bash
kcli tui --stream my-stream
```

## Scripts

- `npm run build` compile to `dist`
- `npm run dev -- <command>` build and run CLI locally
- `npm link` expose the built CLI as the global `kcli` command
- `npm run test` run automated tests
- `npm run lint` run ESLint on CLI source
- `npm run typecheck` run TypeScript without emitting files

## Architecture

- `src/domain`: backend contracts + core types
- `src/application`: reusable use-case logic (tests, compare)
- `src/infrastructure`: concrete backend clients + output formatting
- `src/commands`: command registration and argument parsing
- `src/tui`: simple interactive stream tail view

## Assumptions

- Connection strings are valid KurrentDB/EventStoreDB URIs.
- Both configured backends implement compatible stream semantics.
- Current compare command targets existing stream data; seeding strategy can be added later.
