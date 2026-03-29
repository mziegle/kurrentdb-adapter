# kurrentdb-adapter

`kurrentdb-adapter` is a NestJS gRPC service that exposes the EventStore/KurrentDB `Streams` protobuf
contract and persists stream events in PostgreSQL.

It is still a partial adapter, not a full KurrentDB-compatible implementation.

Retention and scavenging behavior are documented in
[docs/scavenging.md](c:\Users\micha\Repos\kurrentdb-adapter\docs\scavenging.md).

## What It Does

- Boots a NestJS microservice over gRPC
- Uses the `event_store.client.streams` protobuf package
- Loads the service definition from `proto/streams.proto`
- Generates TypeScript interfaces from protobuf files with `ts-proto`
- Stores appended events in PostgreSQL
- Reads persisted stream events back over gRPC
- Applies stream metadata retention rules such as `$maxCount`, `$maxAge`, and `$tb`
- Supports scavenging of retention-hidden records
- Exposes a partial implementation of the `Streams` service

## Current Status

Implemented:

- `Read`
  Reads persisted events for a single stream from PostgreSQL, including forward and backward reads, revision-based reads, and `maxCount` limits.
- `Append`
  Persists appended events transactionally in PostgreSQL and returns the resulting revision/position.
- Stream metadata retention
  Supports metadata-driven visibility rules such as `$maxCount`, `$maxAge`, and `$tb`.
- `Delete`
  Deletes a stream with expected-revision checks.
- `Tombstone`
  Permanently tombstones a stream and prevents future appends, reads, and deletes.
- `StartScavenge` / `StopScavenge`
  Exposes scavenging operations and physically removes records that are already hidden by retention rules.

Not implemented:

- `BatchAppend`

Unsupported read modes still throw and should be treated as unavailable.

## Retention And Scavenging

The adapter now follows the same broad retention model that KurrentDB uses:

- `$tb`, `$maxCount`, and `$maxAge` affect stream reads immediately
- those hidden events are still present in storage before scavenging
- before scavenging they may still be visible through `$all`
- scavenging is the step that physically removes those records

One important compatibility detail is that KurrentDB does not scavenge away the
last event in a stream purely because of truncation metadata. The adapter keeps
that rule as well.

For the full notes and source references, see
[docs/scavenging.md](c:\Users\micha\Repos\kurrentdb-adapter\docs\scavenging.md).

## Stack

- Node.js
- TypeScript
- NestJS microservices
- gRPC
- Protocol Buffers
- PostgreSQL
- `pg`
- `ts-proto`

## Project Layout

```text
src/
  main.ts
  app.module.ts
  streams.controller.ts
  interfaces/

proto/

test/
  contracts/
  streams.e2e-spec.ts
  kurrentdb.e2e-spec.ts
```

## Getting Started

### Prerequisites

- Node.js 20+ recommended
- npm
- Docker Desktop or another supported container runtime for e2e tests

### Install

```bash
npm install
```

### Configure PostgreSQL

Use either `POSTGRES_URL` or the individual connection variables:

```bash
POSTGRES_URL=postgres://postgres:postgres@localhost:5432/kurrentdb_adapter
```

or

```bash
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=kurrentdb_adapter
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
```

The gRPC listener defaults to `0.0.0.0:2113`. Override it with:

```bash
GRPC_URL=0.0.0.0:2113
```

To help debug non-gRPC clients such as Navigator, the adapter now starts the
actual Nest gRPC server on an internal loopback port and places a lightweight
TCP probe proxy on `GRPC_URL`. Override the internal target with:

```bash
INTERNAL_GRPC_URL=127.0.0.1:2213
```

The probe proxy logs the first bytes of each incoming connection and labels the
traffic as `http1`, `http2-prior-knowledge`, or `unknown`.

### Start PostgreSQL for local development

This repo includes a local Postgres stack and a real KurrentDB instance in [docker-compose.yml](c:\Users\micha\Repos\kurrentdb-adapter\docker-compose.yml).

Bring the database up with:

```bash
npm run db:up
```

Stop it with:

```bash
npm run db:down
```

Bring up the local KurrentDB instance on `127.0.0.1:2114` with:

```bash
npm run kurrentdb:up
```

Bring up both Postgres and KurrentDB with:

```bash
npm run dev:up
```

Stop the whole local environment with:

```bash
npm run dev:down
```

### Start in development

```bash
npm run start:dev
```

If you want one command that starts Postgres and KurrentDB first and then launches the Nest app in watch mode, use:

```bash
npm run start:dev:env
```

### Playground

There is also a small `playground/` folder for experimenting against a running adapter.

By default the scripts connect to:

```bash
kurrentdb://127.0.0.1:2113?tls=false
```

Override that with:

```bash
KURRENTDB_CONNECTION_STRING=kurrentdb://127.0.0.1:2113?tls=false
```

To point a playground script at the local real KurrentDB instance instead of the adapter, use:

```bash
KURRENTDB_CONNECTION_STRING=kurrentdb://127.0.0.1:2114?tls=false
```

Available playground scripts:

```bash
npm run playground:append-read
npm run playground:filtered-read
npm run playground:stream-metadata
```

### Build

```bash
npm run build
```

### Run compiled app

```bash
npm run start:prod
```

## Protobuf Generation

The protobuf source files live in `proto/`. Generated TypeScript interfaces are written to `src/interfaces`.

Regenerate them with:

```bash
npm run generate
```

## Scripts

```bash
npm run generate
npm run build
npm run start
npm run start:dev
npm run start:dev:env
npm run start:debug
npm run start:prod
npm run db:up
npm run db:down
npm run kurrentdb:up
npm run dev:up
npm run dev:down
npm run playground:append-read
npm run playground:filtered-read
npm run playground:stream-metadata
npm run lint
npm run test
npm run test:e2e
npm run test:cov
```

## Service Contract

The adapter exposes the `Streams` gRPC service:

- `Read(ReadReq) returns (stream ReadResp)`
- `Append(stream AppendReq) returns (AppendResp)`
- `Delete(DeleteReq) returns (DeleteResp)`
- `Tombstone(TombstoneReq) returns (TombstoneResp)`
- `BatchAppend(stream BatchAppendReq) returns (stream BatchAppendResp)`

It also exposes a partial `Operations` service including scavenging endpoints.

## Testing

Contract coverage lives in `test/contracts/streams-contract-suite.ts` and is exercised by backend-specific runners. The adapter runner in `test/streams.e2e-spec.ts` starts PostgreSQL with Testcontainers and boots the Nest gRPC service. The KurrentDB runner in `test/kurrentdb.e2e-spec.ts` runs the same client-level assertions against a real KurrentDB target.

The shared contract tests currently cover:

- single-event append/read
- stale expected revision rejection on append
- `NO_STREAM` append behavior
- `STREAM_EXISTS` append behavior
- empty append behavior on missing and existing streams
- missing stream reads
- multi-event append ordering
- backward reads
- reads from a specific revision
- explicit revision `0` handling
- `maxCount` slicing
- stream metadata retention behavior
- stream subscriptions
- persistence across app restart on the adapter backend
- `Delete`
- `Tombstone`
- scavenging behavior against both the adapter and real KurrentDB

Run them with:

```bash
npm run test:e2e:adapter -- --runInBand
npm run test:e2e:kurrentdb -- --runInBand
npm run test:e2e:contracts -- --runInBand
```

`test:e2e:kurrentdb` uses `KURRENTDB_TEST_CONNECTION_STRING` when provided. Otherwise it tries to start `docker.kurrent.io/kurrent-latest/kurrentdb:latest` in Testcontainers. The default KurrentDB runner currently skips restart-persistence assertions because it uses an ephemeral container.

## Limitations

- `BatchAppend` is not implemented.
- Stream positions are backed by a simple Postgres global sequence, not full KurrentDB semantics.
- `Append` wrong-expected-version is mapped with an `AppendResp.wrongExpectedVersion` payload because that is what the Kurrent client expects.
- The default real-KurrentDB contract runner uses an ephemeral container, so restart-persistence is intentionally skipped there.
- The adapter is still only partially compatible with KurrentDB outside the currently tested contract surface.

## Recommended Next Steps

- implement `BatchAppend`
- add a persistent-volume KurrentDB runner for restart-persistence parity
- expand the contract suite as new RPCs are added
