# kurrentdb-adapter

`kurrentdb-adapter` is a NestJS gRPC service that exposes the EventStore/KurrentDB `Streams` protobuf contract and persists stream events in PostgreSQL.

It is still a partial adapter, not a full KurrentDB-compatible implementation.

## What It Does

- Boots a NestJS microservice over gRPC
- Uses the `event_store.client.streams` protobuf package
- Loads the service definition from `src/protos/streams.proto`
- Generates TypeScript interfaces from protobuf files with `ts-proto`
- Stores appended events in PostgreSQL
- Reads persisted stream events back over gRPC
- Exposes a partial implementation of the `Streams` service

## Current Status

Implemented:

- `Read`
  Reads persisted events for a single stream from PostgreSQL.
- `Append`
  Persists appended events transactionally in PostgreSQL and returns the resulting revision/position.

Not implemented:

- `Delete`
- `Tombstone`
- `BatchAppend`

These methods currently throw `Method not implemented.` and should be treated as unsupported.

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
  protos/
  interfaces/

test/
  streams.e2e-spec.ts
```

## Getting Started

### Prerequisites

- Node.js 20+ recommended
- npm

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

The protobuf source files live in `src/protos`. Generated TypeScript interfaces are written to `src/interfaces`.

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

## Limitations

- Only stream-scoped reads are supported.
- Filtered reads and subscription reads are not implemented.
- `Delete`, `Tombstone`, and `BatchAppend` are still unimplemented.
- Stream positions are backed by a simple Postgres global sequence, not full KurrentDB semantics.
- End-to-end coverage is still minimal.

## Recommended Next Steps

- implement `Delete`, `Tombstone`, and `BatchAppend`
- map gRPC errors to expected KurrentDB client behavior
- expand integration tests with real client request flows
