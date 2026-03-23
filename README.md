# kurrentdb-adapter

`kurrentdb-adapter` is a NestJS gRPC service that exposes the EventStore/KurrentDB `Streams` protobuf
contract and persists stream events in PostgreSQL.

It is still a partial adapter, not a full KurrentDB-compatible implementation.

## What It Does

- Boots a NestJS microservice over gRPC
- Uses the `event_store.client.streams` protobuf package
- Loads the service definition from `proto/streams.proto`
- Generates TypeScript interfaces from protobuf files with `ts-proto`
- Stores appended events in PostgreSQL
- Reads persisted stream events back over gRPC
- Exposes a partial implementation of the `Streams` service

## Current Status

Implemented:

- `Read`
  Reads persisted events for a single stream from PostgreSQL, including forward and backward reads, revision-based reads, and `maxCount` limits.
- `Append`
  Persists appended events transactionally in PostgreSQL and returns the resulting revision/position.
- `Delete`
  Deletes a stream with expected-revision checks.
- `Tombstone`
  Permanently tombstones a stream and prevents future appends, reads, and deletes.

Not implemented:

- `BatchAppend`
- Filtered reads

Unsupported read modes still throw and should be treated as unavailable.

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

### Start in development

```bash
npm run start:dev
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
npm run start:debug
npm run start:prod
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
- stream subscriptions
- persistence across app restart on the adapter backend
- `Delete`
- `Tombstone`

Run them with:

```bash
npm run test:e2e:adapter -- --runInBand
npm run test:e2e:kurrentdb -- --runInBand
npm run test:e2e:contracts -- --runInBand
```

`test:e2e:kurrentdb` uses `KURRENTDB_TEST_CONNECTION_STRING` when provided. Otherwise it tries to start `docker.kurrent.io/kurrent-latest/kurrentdb:latest` in Testcontainers. The default KurrentDB runner currently skips restart-persistence assertions because it uses an ephemeral container.

## Limitations

- Only stream-scoped reads are supported.
- `BatchAppend` and filtered reads are not implemented.
- Stream positions are backed by a simple Postgres global sequence, not full KurrentDB semantics.
- `Append` wrong-expected-version is mapped with an `AppendResp.wrongExpectedVersion` payload because that is what the Kurrent client expects.
- The default real-KurrentDB contract runner uses an ephemeral container, so restart-persistence is intentionally skipped there.

## Recommended Next Steps

- implement `BatchAppend`
- add a persistent-volume KurrentDB runner for restart-persistence parity
- expand the contract suite as new RPCs are added
