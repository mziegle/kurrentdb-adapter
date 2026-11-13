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

The external listener acts as a lightweight TCP probe proxy in front of that internal gRPC server.

`GRPC_MAX_RECEIVE_MESSAGE_BYTES` sets the maximum inbound gRPC message size on the internal server, before protobuf decoding. It defaults to 4,194,304 bytes (4 MiB) and can be lowered to a positive decimal integer; 4 MiB is also the hard maximum. Empty, zero, negative, malformed, and larger values fail startup. This applies per message to all RPCs, not per append or stream. The official Node client's default BatchAppend batch size is 3 MiB; the extra MiB allows protobuf framing overhead, but unusually large individual events or metadata can exceed the cap, and lowering it may require smaller client batches. Rejected streaming writes can have unknown outcomes; do not assume rollback. The client may fail unrelated pending BatchAppend promises on a stream error.

This limit bounds individual received messages, not total process memory, connections, simultaneous RPCs, decompression overhead, or aggregate stream buffering. Deploy connection/rate limits and resource isolation separately; see [BatchAppend buffering limits](docs/batch-append-buffer-limits.md).

### Public proxy connection bounds

`PROXY_MAX_CONNECTIONS` defaults to 1024 concurrent accepted public TCP sockets (including probes and HTTP/2 connections); valid values are decimal integers from 1 to 100000. Excess sockets are immediately closed. A slot is released when the client socket closes. This is per process, not per IP, and includes long-lived subscriptions.

`PROXY_PREFACE_TIMEOUT_MS` defaults to 10000 ms and accepts decimal integers from 0 to 2147483647; 0 disables it. It is an absolute deadline from acceptance, not reset by incoming bytes. It closes both sides unless the client sends the complete 24-byte plaintext HTTP/2 connection preface in time. This protects silent and slow partial-preface sockets, but also closes HTTP/1 or other non-HTTP/2 connections still open after the deadline; increase or disable it if required. Complete HTTP/2 prefaces disable this deadline even if no valid HTTP/2 session follows: a client can bypass the deadline simply by sending the preface. There is deliberately no established-connection idle timeout, so quiet subscriptions are not disconnected; the connection cap can still be exhausted by idle or malicious established clients. Neither setting is an authentication or rate limit. Invalid values fail startup without printing the supplied value.

These limits apply only to the public proxy. Direct access to `INTERNAL_GRPC_URL` bypasses them completely. Use an external gateway and process supervision for additional connection, rate, and shutdown controls.

On SIGTERM or SIGINT, the proxy stops accepting connections and allows existing connections a fixed 10-second grace period before destroying remaining client and upstream sockets. It does not send HTTP/2 GOAWAY, so existing connections can carry streams during that period. Normal Nest shutdown starts afterwards and is not bounded by the proxy deadline. Forced disconnects interrupt subscriptions; resume from a safely processed checkpoint. An interrupted or unacknowledged write has an unknown outcome and may already have committed; reconcile before retrying according to the application's idempotency rules. See [proxy shutdown](docs/proxy-shutdown.md).

### Security and deployment

**The adapter does not implement client authentication, authorization, or TLS.** The user-management endpoints are stubs and do not protect data. Anyone who can reach either listener can access the adapter's operations, including destructive operations. The default external bind (`0.0.0.0`) exposes it on all interfaces; `INTERNAL_GRPC_URL` must remain loopback-only and protected from untrusted local processes. Do not expose either listener directly to the internet or an untrusted network.

For a controlled deployment, bind `GRPC_URL` to loopback or a private interface and restrict access to trusted workloads using network isolation and a separately managed, authenticated TLS gateway that supports gRPC/HTTP2. Protect the gateway-to-adapter hop as well; the adapter itself speaks plaintext. Restrict database connectivity, use database authentication and TLS appropriate to the backend, manage credentials outside the repository, and replace the development PostgreSQL defaults. These controls are external to this project and are not verified by the adapter. The MongoDB backend remains experimental; see [its limitations](docs/mongodb.md).

Logs may contain sensitive data. Trace logging can include request fields and event metadata. The proxy logs protocol and request metadata, not raw first-chunk previews, and omits unhandled HTTP request targets; other logs can include client-supplied identifiers. Credential-name redaction and metadata allowlisting do not guarantee that secrets are absent. Avoid trace logging with sensitive data, restrict and retain logs appropriately, and do not put secrets in URLs or client-controlled identifiers.

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
