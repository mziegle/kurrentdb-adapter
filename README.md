# kurrentdb-adapter

`kurrentdb-adapter` is a gRPC adapter backed by PostgreSQL by default, with an experimental MongoDB backend, for the KurrentDB/EventStore `Streams` API.

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
- scavenging through the partial `Operations` service (MongoDB: disabled by default; see [MongoDB safety notes](docs/mongodb.md))

## Behavior Notes

- The adapter persists events in PostgreSQL by default; the experimental [MongoDB backend](docs/mongodb.md) is opt-in and is not production-ready.
- It is compatible with the official `@kurrent/kurrentdb-client` for the currently supported surface.
- Retention rules affect stream reads immediately, but hidden records can remain visible in `$all` until scavenging runs.
- The adapter follows the KurrentDB rule that truncation metadata alone does not scavenge a stream down to absolute emptiness.

More scavenging detail is documented in [docs/scavenging.md](docs/scavenging.md).

## Limitations

- This is a partial adapter, not a full KurrentDB replacement.
- PostgreSQL stream positions use a simple global sequence, not full KurrentDB position semantics; MongoDB uses a transactional global counter.
- `Append` wrong-expected-version failures are returned as `AppendResp.wrongExpectedVersion` because that is what the Kurrent client expects.
- Filtering is supported on `$all` reads and `$all` subscriptions, not direct named-stream reads or subscriptions.
- Backwards subscriptions are not supported.
- Requests that are neither named-stream operations nor `$all` operations are not supported.

## Configuration

PostgreSQL is the default. For the experimental MongoDB backend, its configuration, unsafe opt-ins and restrictions, see [MongoDB safety notes](docs/mongodb.md). Experimental group commit is default-off and does not provide production-ready multi-instance operation.

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

The external listener acts as a lightweight TCP probe proxy in front of that internal gRPC server. It logs request and protocol information; at trace level it also logs raw first-chunk previews.

### Security and deployment

**The adapter does not implement client authentication, authorization, or TLS.** The user-management endpoints are stubs and do not protect data. Anyone who can reach either listener can access the adapter's operations, including destructive operations. The default external bind (`0.0.0.0`) exposes it on all interfaces; `INTERNAL_GRPC_URL` must remain loopback-only and protected from untrusted local processes. Do not expose either listener directly to the internet or an untrusted network.

For a controlled deployment, bind `GRPC_URL` to loopback or a private interface and restrict access to trusted workloads using network isolation and a separately managed, authenticated TLS gateway that supports gRPC/HTTP2. Protect the gateway-to-adapter hop as well; the adapter itself speaks plaintext. Restrict database connectivity, use database authentication and TLS appropriate to the backend, manage credentials outside the repository, and replace the development PostgreSQL defaults. These controls are external to this project and are not verified by the adapter. The MongoDB backend remains experimental; see [its limitations](docs/mongodb.md).

Logs may contain sensitive data. Trace logging includes request fields, event metadata, and raw proxy bytes (including HTTP headers or query strings); even non-trace logs can include unhandled HTTP request targets and client-supplied identifiers. Credential-name redaction and metadata allowlisting do not guarantee that secrets are absent. Avoid trace logging with sensitive data, restrict and retain logs appropriately, and do not put secrets in URLs or client-controlled identifiers.

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
