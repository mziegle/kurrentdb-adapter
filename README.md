# kurrentdb-adapter

`kurrentdb-adapter` is a PostgreSQL-backed gRPC adapter for the KurrentDB/EventStore `Streams` API.

It is intended for users who want to point the official KurrentDB client at this service and work with a compatible subset of stream operations without running a full KurrentDB node.

Development and contributor information lives in [CONTRIBUTING.md](CONTRIBUTING.md).

## What It Supports

- `Append`
- `BatchAppend`
- `Read`
- `ReadAll`
- stream metadata retention with `$maxCount`, `$maxAge`, and `$tb`
- stream subscriptions
- `$all` subscriptions
- `Delete`
- `Tombstone`
- scavenging through the partial `Operations` service

## Behavior Notes

- The adapter persists events in PostgreSQL.
- It is compatible with the official `@kurrent/kurrentdb-client` for the currently supported surface.
- Retention rules affect stream reads immediately, but hidden records can remain visible in `$all` until scavenging runs.
- The adapter follows the KurrentDB rule that truncation metadata alone does not scavenge a stream down to absolute emptiness.

More scavenging detail is documented in [docs/scavenging.md](docs/scavenging.md).

## Limitations

- This is a partial adapter, not a full KurrentDB replacement.
- Stream positions are backed by a simple PostgreSQL global sequence, not full KurrentDB position semantics.
- `Append` wrong-expected-version failures are returned as `AppendResp.wrongExpectedVersion` because that is what the Kurrent client expects.
- Filtering is supported on `$all` reads and `$all` subscriptions, not direct named-stream reads or subscriptions.
- Backwards subscriptions are not supported.
- Requests that are neither named-stream operations nor `$all` operations are not supported.

## Configuration

Use either `POSTGRES_URL`:

```bash
POSTGRES_URL=postgres://postgres:postgres@localhost:5432/kurrentdb_adapter
```

Or the individual PostgreSQL settings:

```bash
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=kurrentdb_adapter
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
```

The external listener defaults to:

```bash
GRPC_URL=0.0.0.0:2113
```

The adapter also starts an internal Nest gRPC server, which defaults to:

```bash
INTERNAL_GRPC_URL=127.0.0.1:2213
```

The external listener acts as a lightweight TCP probe proxy in front of that internal gRPC server. This helps identify clients that connect with the wrong protocol and logs the first bytes of each incoming connection.

## Running The Adapter

This repo targets Node.js 24.

Install dependencies:

```bash
npm install
```

Run in development:

```bash
npm run start:dev
```

Run the compiled app:

```bash
npm run build
npm run start:prod
```

## Client Compatibility

The adapter is designed to be used through the official KurrentDB client:

```ts
import { KurrentDBClient } from '@kurrent/kurrentdb-client';

const client =
  KurrentDBClient.connectionString`kurrentdb://127.0.0.1:2113?tls=false`;
```

## Service Surface

The adapter exposes the `Streams` service with support for:

- `Read(ReadReq) returns (stream ReadResp)`
- `Append(stream AppendReq) returns (AppendResp)`
- `Delete(DeleteReq) returns (DeleteResp)`
- `Tombstone(TombstoneReq) returns (TombstoneResp)`
- `BatchAppend(stream BatchAppendReq) returns (stream BatchAppendResp)`

It also exposes a partial `Operations` service for scavenging endpoints.
