# Contributing

This document is for contributors working on `kurrentdb-adapter`.

User-facing adapter information lives in [README.md](README.md).

## Development Setup

### Prerequisites

- Node.js 24
- npm
- Docker Desktop or another supported container runtime for Testcontainers-based e2e runs

### Install

```bash
npm install
```

## Local Services

This repo includes a local Postgres stack and a real KurrentDB instance in [docker-compose.yml](docker-compose.yml).

Start PostgreSQL:

```bash
npm run db:up
```

Stop PostgreSQL:

```bash
npm run db:down
```

Start the local KurrentDB instance on `127.0.0.1:2114`:

```bash
npm run kurrentdb:up
```

Start both Postgres and KurrentDB:

```bash
npm run dev:up
```

Stop the whole local environment:

```bash
npm run dev:down
```

## Running The App

Start in watch mode:

```bash
npm run start:dev
```

Start Postgres and KurrentDB first, then launch the app in watch mode:

```bash
npm run start:dev:env
```

Run the compiled app:

```bash
npm run build
npm run start:prod
```

## Developer CLI

The developer CLI lives in [`cli/`](cli) and should be the default tool for agent-led testing, tracing, debugging, comparison, and benchmark workflows when it covers the task.

Examples:

```bash
cd cli
npm install
npm run dev -- ping
npm run dev -- test append
npm run dev -- trace
npm run dev -- config show
npm run bench:report
```

## Testing

The shared contract suite lives in [test/contracts/streams-contract-suite.ts](test/contracts/streams-contract-suite.ts).

`npm run e2e:dev` boots the current app in-process against PostgreSQL in Testcontainers:

```bash
npm run e2e:dev -- --runInBand
```

`npm run e2e` runs the contract suite against:

1. the locally built adapter container image
2. a real KurrentDB target

The `container` pass builds the local adapter image automatically:

```bash
npm run e2e -- --runInBand
```

If `KURRENTDB_TEST_CONNECTION_STRING` is set, the KurrentDB pass uses that instance. Otherwise it starts `docker.kurrent.io/kurrent-latest/kurrentdb:latest` in Testcontainers.

### Linting

Before finishing changes, run:

```bash
npx eslint "src/**/*.ts" "test/**/*.ts"
```

## Protobuf Generation

The protobuf source files live in `proto/`.

`npm run generate` refreshes:

- Nest/ts-proto server interfaces in `src/interfaces`
- typed gRPC contract-test helpers in `generated/grpc`

Regenerate them with:

```bash
npm run generate
```

## Scripts

```bash
npm run generate
npm run build
npm run container:build
npm run start:dev
npm run start:dev:env
npm run start:prod
npm run db:up
npm run db:down
npm run kurrentdb:up
npm run dev:up
npm run dev:down
npm run lint
npm run lint:fix
npm run test
npm run e2e
npm run e2e:dev
```

## Current Contract Coverage

The shared contract tests currently cover:

- single-event append/read
- stale expected revision rejection on append
- `NO_STREAM` append behavior
- `STREAM_EXISTS` append behavior
- `BatchAppend`
- empty append behavior on missing and existing streams
- missing stream reads
- multi-event append ordering
- backward reads
- reads from a specific revision
- explicit revision `0` handling
- `maxCount` slicing
- `$all` reads in both directions
- `$all` filtering by stream name prefix
- `$all` filtering by event type
- stream metadata retention behavior
- stream and `$all` subscriptions
- persistence across app restart on both the adapter and the KurrentDB targets
- `Delete`
- `Tombstone`
- scavenging behavior against both the adapter and real KurrentDB
