# Agent Notes

## Current shape of the project

- This repo is a NestJS gRPC adapter that exposes the KurrentDB/EventStore `Streams` service.
- Persistence is backed by PostgreSQL in [src/postgres-event-store.service.ts](c:/Users/User/source/repos/kurrentdb-adapter/src/postgres-event-store.service.ts).
- E2E coverage is in [test/streams.e2e-spec.ts](c:/Users/User/source/repos/kurrentdb-adapter/test/streams.e2e-spec.ts) and uses Testcontainers with Docker.

## Important implementation lessons

- Do not assume numeric protobuf fields arrive as plain numbers.
- The Kurrent client sends some 64-bit values as protobuf `Long` objects.
- This already mattered for:
  expected stream revision in append
  read count / read revision values
- Normalize those values before comparing them or passing them into SQL.

- `Append` should be handled via `@GrpcStreamCall`, not the earlier buffered observable approach.
- Using Nest's `@GrpcStreamMethod` path caused a deadlock with client-stream append handling.
- The current direct stream/callback implementation in [src/streams.controller.ts](c:/Users/User/source/repos/kurrentdb-adapter/src/streams.controller.ts) is the stable version that passed e2e tests.

- Wrong expected version currently works by returning an `AppendResp.wrongExpectedVersion` payload, not by throwing a gRPC error.
- The Kurrent client expects that response shape for append failures.
- Be careful with revision `0`; presence/encoding mistakes around zero broke the client path earlier.

- Read-missing-stream behavior is implemented by returning `streamNotFound` in the read response stream.
- The Kurrent client converts that into `StreamNotFoundError`.

- Scavenging behavior is subtle and should follow the verified KurrentDB notes in
  [docs/scavenging.md](c:/Users/micha/Repos/kurrentdb-adapter/docs/scavenging.md).
- In particular, `$tb`, `$maxCount`, and `$maxAge` hide events from stream reads
  before scavenging, but KurrentDB keeps those records in storage and in `$all`
  until a scavenge runs.
- KurrentDB also keeps the last event in a stream rather than scavenging a
  stream to emptiness purely because of truncation metadata.

## Testing lessons

- Run e2e tests with Docker/Testcontainers available.
- Inside this environment, `npm run test:e2e -- --runInBand` had to run outside the sandbox to access Docker.
- Any code change should leave the repo lint-clean. Before finishing, run `npx eslint "src/**/*.ts" "test/**/*.ts"` and fix violations in touched code.

- Prefer asserting behavior only through the Kurrent client interface in e2e tests.
- Direct database assertions were removed on purpose.

- The current e2e suite covers:
  single-event append/read
  stale expected revision rejection
  missing stream read
  multi-event append ordering

## Good next tests

- backward reads return newest-first
- reads from a specific revision return the correct slice
- persistence still works after restarting the app against the same Postgres container
- eventually `Delete`, `Tombstone`, and `BatchAppend` once those RPCs exist

## High-risk areas

- Any change to append error mapping can silently break the Kurrent client.
- Any change to revision/count typing can reintroduce `Long` comparison bugs.
- Any attempt to "simplify" the append controller back to pure observable buffering should be treated skeptically unless reverified with the e2e suite.
