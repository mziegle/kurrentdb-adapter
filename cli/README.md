# kdb-cli

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

## Setup

```bash
cd cli
# dependencies are resolved from the repository root node_modules
# run npm install at the repository root if needed
```

## Configuration

The CLI expects two named backends:

- `reference`
- `adapter`

Configuration sources (in order of precedence):

1. Environment variables:
   - `KDB_REFERENCE_CONNECTION`
   - `KDB_ADAPTER_CONNECTION`
2. Config file: `kdb-cli.config.json` (or custom path via `KDB_CLI_CONFIG_PATH`)

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
npm run dev -- ping --backend adapter
```

### Stream read

```bash
npm run dev -- stream read my-stream --from 0 --limit 20
npm run dev -- stream read my-stream --json
```

### Stream append

```bash
npm run dev -- stream append my-stream \
  --type user-created \
  --data '{"userId":"u-1","name":"Alice"}' \
  --metadata '{"source":"cli"}' \
  --expected-revision any
```

### Stream tail

```bash
npm run dev -- stream tail my-stream
```

### Focused tests

```bash
npm run dev -- test append
npm run dev -- test read
npm run dev -- test subscribe
```

### Compare adapter vs reference

```bash
npm run dev -- test compare --stream my-stream
```

This compares event id/type/revision/position/data/metadata/ordering.

### TUI

```bash
npm run dev -- tui --stream my-stream
```

## Scripts

- `npm run build` compile to `dist`
- `npm run dev -- <command>` build and run CLI locally
- `npm run test` run automated tests
- `npm run lint` lint source and tests

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
