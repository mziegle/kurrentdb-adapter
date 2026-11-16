# Current Architecture

This document describes the repository as it exists today. It is a status
snapshot, not a target architecture. The adapter implements a useful subset of
KurrentDB/EventStoreDB compatibility, but it is not a complete KurrentDB node.

## Purpose and scope

The application is a NestJS service that exposes KurrentDB-compatible protobuf
services so that the official `@kurrent/kurrentdb-client` can use another
persistence engine. PostgreSQL is the default backend. MongoDB is an opt-in,
experimental backend.

The production behavior lives primarily in:

- [`src/main.ts`](../src/main.ts), which boots the listeners and public proxy;
- [`src/app.module.ts`](../src/app.module.ts), which assembles the Nest modules;
- [`src/streams/streams.controller.ts`](../src/streams/streams.controller.ts),
  which adapts Streams RPCs to the backend contract;
- [`src/event-store/event-store-backend.ts`](../src/event-store/event-store-backend.ts),
  which defines that contract;
- [`src/postgres/postgres-event-store.service.ts`](../src/postgres/postgres-event-store.service.ts)
  and [`src/mongodb/mongodb-event-store.service.ts`](../src/mongodb/mongodb-event-store.service.ts),
  which implement it.

## Runtime topology

The process has two listeners. Clients normally connect to the public listener,
not directly to Nest's gRPC server.

```text
 Official client / Navigator / probe
                 |
                 v
 public TCP listener (GRPC_URL, default 0.0.0.0:2113)
                 |
        +--------+---------+
        |                  |
 known HTTP/1 probes   all other traffic
 answered directly         |
                           v
              internal Nest gRPC listener
              (INTERNAL_GRPC_URL, default
                    127.0.0.1:2213)
                           |
                  Nest gRPC controllers
                           |
                  EventStoreBackend token
                     /             \
              PostgreSQL         MongoDB
               default         experimental
```

The public listener is a small TCP protocol multiplexer implemented in
[`src/main.ts`](../src/main.ts). It directly answers `/health/live`, `/info`,
`/info/options`, `/gossip`, `/stats`, and `/ping`. Other traffic, including the
HTTP/2 prior-knowledge connection used by plaintext gRPC, is proxied to the
internal listener.

The proxy bounds accepted sockets with `PROXY_MAX_CONNECTIONS`, enforces a
configurable HTTP/2 preface deadline, and gives existing sockets a ten-second
grace period on SIGINT or SIGTERM. It is not a TLS terminator, authentication
layer, authorization layer, rate limiter, or full HTTP server. Direct access to
the internal listener bypasses its connection controls.

## Module and dependency structure

[`AppModule`](../src/app.module.ts) imports modules for Streams, Operations,
Monitoring, Gossip, Server Features, Persistent Subscriptions, and Users. The
name `PostgresModule` is now slightly misleading: it owns the backend provider
token and selects either implementation.

[`PostgresModule`](../src/postgres/postgres.module.ts) binds the
`EVENT_STORE_BACKEND` token as follows:

- unset or `EVENT_STORE_BACKEND=postgres`: construct
  `PostgresEventStoreService` from the PostgreSQL infrastructure, protocol, and
  subscription services;
- `EVENT_STORE_BACKEND=mongodb`: construct `MongoEventStoreService`;
- any other value: fail during startup.

Controllers depend on the `EventStoreBackend` interface rather than a concrete
database. The interface contains stats, append, batch append, read,
subscription, soft delete, tombstone, and scavenge operations. This is the main
architectural seam between protocol handling and storage.

There is still some backend knowledge above that seam. Feature advertisement
and persistent-subscription behavior inspect `EVENT_STORE_BACKEND` directly,
and the Streams controller recognizes MongoDB group-dispatch errors. The
backend abstraction is therefore useful but not completely opaque.

## Request flow

### Reads and subscriptions

`Streams.Read` is a server-streaming RPC. The controller chooses one of two
paths from the request options:

1. A finite read calls `EventStoreBackend.read()` and emits the returned
   responses.
2. A subscription consumes `EventStoreBackend.subscribeToStream()` as an async
   iterable until the client cancels.

Both named-stream and `$all` reads are supported, forward and backward for
finite reads. Filters apply to `$all`, not named streams. Backward subscriptions
and link resolution are not implemented.

### Append

`Streams.Append` uses Nest's `@GrpcStreamCall` and the raw grpc-js stream and
callback. Incoming messages are collected until the client stream ends, then
passed to the backend as one append. This direct implementation is intentional:
the earlier Nest buffered-observable path deadlocked with client-streaming
append calls.

Expected-version conflicts are protocol results, not ordinary thrown errors.
The backend returns `AppendResp.wrongExpectedVersion`, including the correct
presence encoding for revision zero, because that is the shape expected by the
official client. Protobuf 64-bit fields are normalized before comparison or SQL
use; callers must not assume they arrive as JavaScript numbers.

### Batch append

`Streams.BatchAppend` is a duplex RPC. `BatchAppendSession` groups fragments by
correlation ID, validates and limits buffered work, serializes response writes,
and tracks shutdown/cancellation. A completed group is handed to
`EventStoreBackend.batchAppend()` with a deadline-aware write context.

Cancellation is advisory. In particular, losing a connection or reaching a
deadline does not prove that a database transaction was rolled back. Callers
must reconcile an uncertain result before retrying.

### Delete and tombstone

Soft delete hides the old stream generation from named-stream reads but retains
its records in `$all`; the stream can later be recreated according to expected
revision rules. Tombstone permanently marks the stream deleted and rejects
later writes. Both operations check expected revisions transactionally.

## PostgreSQL backend

PostgreSQL is the default and more established implementation. On startup it
acquires a database advisory lock, creates or upgrades its schema, starts a
dedicated `LISTEN` connection, and then serves traffic. The main tables are:

| Table | Role |
| --- | --- |
| `stream_events` | Event payloads, metadata, stream generation/revision, and a `BIGSERIAL` global position |
| `stream_retention_policies` | Current generation/revision, soft-delete state, and `$maxAge`, `$maxCount`, `$tb` policy |
| `tombstoned_streams` | Permanently deleted streams and their last positions |

Writes run in transactions and take a per-stream PostgreSQL advisory transaction
lock. That serializes changes to the same stream across adapter processes while
allowing unrelated streams to progress independently. Appending the events and
updating stream/retention state commit together.

Subscriptions combine database reads with PostgreSQL `LISTEN`/`NOTIFY`:

- each committed write sends the stream name on a shared notification channel;
- the local subscription service increments in-memory versions for that stream
  and `$all`;
- subscribers read until caught up, wait for a version change, then query again;
- a short cancellation poll ensures idle subscribers can shut down.

Notifications are wake-up hints rather than the source of truth. Rows and
cursors in PostgreSQL determine what is delivered, so another adapter instance
can wake and continue a subscription against the shared database.

Retention metadata affects named-stream visibility immediately. Hidden events
remain stored and visible in `$all` until a scavenge deletes eligible records.
Scavenging is asynchronous, tracks active jobs in process memory, and preserves
the last event when truncation metadata alone would otherwise empty a stream.
Detailed semantics are in [`docs/scavenging.md`](../docs/scavenging.md).

PostgreSQL configuration comes from `POSTGRES_URL`, or from the individual
`POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_USER`, and
`POSTGRES_PASSWORD` variables.

## MongoDB backend

MongoDB is selected with `EVENT_STORE_BACKEND=mongodb` and is explicitly
experimental. It requires replica-set transactions and defaults to
`mongodb://localhost:27017/?replicaSet=rs0` and database
`kurrentdb_adapter`.

Its core consistency model differs from PostgreSQL:

- stream state, events, retention state, and global positions are updated in
  MongoDB transactions;
- a single transactional global counter serializes position allocation across
  instances;
- reads that need a consistent stream view use snapshot transactions;
- one database-level change stream wakes all local subscriptions when events,
  stream state, or retention state changes;
- positions and revisions are restricted to JavaScript safe integers, so full
  protobuf 64-bit range compatibility is not provided.

The change stream is a notification mechanism, not the source of event data or
subscription checkpoints. Named-stream and `$all` subscriptions retain their
revision/position cursors and query ordered committed rows after each wake-up.
The notifier is anchored to a MongoDB operation time during startup, and each
subscription compares a local notification version captured before its query
to avoid the query-to-wait race. This design uses one change stream per adapter
process rather than one per subscriber.

The client requests majority, journaled writes. This improves the requested
acknowledgement policy but does not make a single-member replica set redundant,
eliminate uncertain commit outcomes, or establish production readiness.

Experimental group commit exists behind
`MONGODB_EXPERIMENTAL_GROUP_COMMIT=true`. It also requires the explicit
single-adapter acknowledgement flag documented in
[`docs/mongodb.md`](../docs/mongodb.md). This path has a bounded dispatcher and
substantial unit/e2e experimentation, but it is not a safe multi-instance
production mode.

MongoDB scavenging is disabled and omitted from feature advertisement by
default. `MONGODB_UNSAFE_ENABLE_SCAVENGE=true` enables a destructive,
experimental implementation. The full safety limitations are documented in
[`docs/mongodb.md`](../docs/mongodb.md).

## Protocol surface

The current surface should be read in three categories:

| Area | Current state |
| --- | --- |
| Streams | Real implementations of append, batch append, named-stream and `$all` reads/subscriptions, delete, tombstone, metadata retention, filtering on `$all`, and scavenging support through Operations |
| Compatibility/observability | Gossip, server-feature negotiation, gRPC monitoring stats, and the public HTTP probe endpoints provide compatibility-oriented responses |
| Administrative APIs | Persistent subscriptions, Users, and several Operations methods are stubs rather than durable administrative implementations |

On PostgreSQL, persistent-subscription methods currently return synthetic or
empty responses and do not persist subscription groups. User-management calls
also return stub responses and provide no security. Operations such as shutdown,
merge indexes, resign node, set node priority, and restart persistent
subscriptions are no-ops. MongoDB feature negotiation omits these unsupported
areas; its persistent-subscription controller also rejects calls with
`UNIMPLEMENTED`.

Server-feature advertisement is for client compatibility, not proof that every
advertised method has complete KurrentDB semantics. The reported `24.0.0`
version is likewise a negotiation value, not a parity claim.

## Event and retention model

An event has a UUID, stream name, stream revision, global position, event
metadata, custom metadata, data, and creation time. The adapter maps a single
global database position into both KurrentDB commit and prepare positions. It
does not reproduce KurrentDB's full log-position model.

Metadata streams (`$$<stream>`) are ordinary event streams whose latest valid
`$metadata` event controls retention for the corresponding stream. Supported
policies are `$maxCount`, `$maxAge`, and `$tb` (truncate before). The important
visibility rule is:

```text
metadata update -> named-stream reads hide expired/truncated events immediately
                -> physical rows and $all history remain
                -> scavenge may permanently remove eligible history later
```

Soft deletion uses stream generations so a recreated stream does not expose its
old generation through named-stream reads. The historical rows still contribute
to `$all` until scavenged. Tombstoning is permanent.

## Observability and lifecycle

`AdapterStatsService` tracks operation counts/latencies and combines those with
backend statistics. It feeds both the gRPC Monitoring service and public
`/stats`. PostgreSQL reports pool and storage counts. MongoDB currently returns
no equivalent statistics and does not advertise Monitoring.

Logging uses Pino through [`src/shared/logger.ts`](../src/shared/logger.ts).
Hot paths log compact summaries, while trace logging can include request fields
and metadata. Trace logs therefore need the same access and retention care as
other potentially sensitive operational data.

The internal gRPC server caps each inbound message with
`GRPC_MAX_RECEIVE_MESSAGE_BYTES` (default and hard maximum: 4 MiB). This is a
per-message decoder limit, not a total-memory or whole-stream limit. Batch append
also has application-level buffering behavior described in
[`docs/batch-append-buffer-limits.md`](../docs/batch-append-buffer-limits.md).

## Security boundary

The adapter implements neither TLS nor client authentication/authorization.
The Users service is only a compatibility stub. Anyone who can reach either
listener can invoke available reads, writes, deletes, tombstones, and operations.

A deployment must therefore supply its own trusted network boundary and an
authenticated TLS-capable gRPC gateway, keep the internal listener private,
protect database credentials and connections, and isolate the process. The
default public bind is all interfaces and is a development convenience, not a
secure deployment default.

## Source layout

| Path | Responsibility |
| --- | --- |
| [`src/`](../src) | Adapter runtime, controllers, backend contract, and database implementations |
| [`proto/Grpc/`](../proto/Grpc) | Upstream-compatible gRPC protocol definitions loaded at runtime |
| [`src/interfaces/`](../src/interfaces) | Generated Nest/TypeScript protocol interfaces |
| [`generated/grpc/`](../generated/grpc) | Additional generated grpc-js types used by Operations tooling |
| [`test/contracts/`](../test/contracts) | Shared official-client behavior suites runnable against adapter backends and real KurrentDB |
| [`test/`](../test) | Backend, proxy, failover, concurrency, and integration e2e suites |
| [`cli/`](../cli) | Separate `kcli` package for smoke tests, reads/writes, comparison, tracing, benchmarks, and a TUI |
| [`docs/`](../docs) | Focused design, safety, and behavior notes |

Generated interfaces come from the checked-in protobuf files via `npm run
generate`. The Operations helper types have a separate generation command in
the root `package.json`.

## Testing strategy

The highest-value compatibility tests use only the official
`@kurrent/kurrentdb-client` for event behavior. Shared contract suites cover
append and expected revision handling, batch append, finite reads, `$all`,
metadata retention, subscriptions, delete, tombstone, and restart behavior.

The broader e2e layer uses disposable databases through Testcontainers and
covers multi-adapter PostgreSQL concurrency, public-proxy subscriptions, gRPC
receive limits, MongoDB transaction behavior, overload/cancellation, cursor
cleanup, group-commit experiments, and controlled replica-set elections. Docker
is required for these suites.

`kcli` is the preferred interactive tool for agent-led testing and protocol
comparison. In particular, `kcli trace` can sit between a client and a real
KurrentDB node when subtle HTTP/2, gRPC, Navigator, or bootstrap behavior needs
to be compared rather than guessed.

## Important current limitations

- This is partial protocol compatibility, not a drop-in KurrentDB replacement.
- Authentication, authorization, and TLS are absent.
- Several advertised PostgreSQL administrative services are stubs or no-ops.
- Persistent subscriptions are not actually implemented.
- Commit and prepare positions are the same simplified global sequence.
- Backward subscriptions, named-stream filters, link resolution, and filter
  checkpoints are unsupported.
- Write cancellation cannot guarantee rollback; interrupted results may be
  uncertain.
- PostgreSQL subscriptions depend on one dedicated `LISTEN` connection per
  adapter process; MongoDB subscriptions depend on one database change stream
  per adapter process.
- PostgreSQL scavenges and all MongoDB production characteristics need more
  failure, scale, and recovery validation.
- The MongoDB backend has additional safe-integer, global-counter,
  change-stream recovery, feature, and unsafe-scavenge limitations.

For planned hardening work rather than implemented architecture, see
[`docs/production-readiness-concept.md`](../docs/production-readiness-concept.md).
