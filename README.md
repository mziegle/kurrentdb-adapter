# kurrentdb-adapter

`kurrentdb-adapter` is a NestJS gRPC service that exposes the EventStore/KurrentDB `Streams` protobuf contract and currently returns placeholder responses for early integration work.

It is best described as a mock or adapter skeleton, not a full KurrentDB-compatible implementation.

## What It Does

- Boots a NestJS microservice over gRPC
- Uses the `event_store.client.streams` protobuf package
- Loads the service definition from `src/protos/streams.proto`
- Generates TypeScript interfaces from protobuf files with `ts-proto`
- Exposes a partial implementation of the `Streams` service

## Current Status

Implemented:

- `Read`
  Returns one synthetic event and completes the response stream.
- `Append`
  Accepts a client stream and returns a static success response.

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

- Responses are hard-coded placeholder values.
- There is no persistence layer.
- Stream revision and position handling are not real yet.
- Runtime configuration via environment variables is not implemented.
- End-to-end coverage is still minimal.

## Recommended Next Steps

- add configurable gRPC host and port
- implement real append/read behavior backed by storage
- implement `Delete`, `Tombstone`, and `BatchAppend`
- map gRPC errors to expected KurrentDB client behavior
- expand integration tests with real client request flows
