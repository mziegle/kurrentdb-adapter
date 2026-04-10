---
name: trace-kurrentdb-debug
description: Trace real KurrentDB traffic and compare it with the adapter in this repo. Use when Codex needs to inspect HTTP `/info` `/gossip` `/stats` traffic, watch gRPC and HTTP/2 communication through the repo tracer, compare real-node behavior against the adapter, debug Navigator/client compatibility issues, or quickly reproduce and analyze a request flow with `kcli trace`.
---

# Trace KurrentDB Debug

## Overview

Use this skill when the fastest path is to observe the real node on the wire instead of guessing. The main tool is the repo tracer at [`../../../scripts/trace-kurrentdb.js`](../../../scripts/trace-kurrentdb.js), and the preferred entry point is the repo CLI command `kcli trace`.

Read [`references/trace-workflow.md`](references/trace-workflow.md) for commands, environment variables, and interpretation details.

## Workflow

1. Start the real KurrentDB node on `127.0.0.1:2114`.
2. Start the trace proxy on `127.0.0.1:2115` with `kcli trace`.
3. Point the reproducer at the trace proxy instead of the real node.
4. Reproduce the exact behavior to capture.
5. Inspect the request order, payload shape, and transport details that matter.
6. Compare the real-node trace with the adapter on `127.0.0.1:2113`.
7. Patch the adapter, tests, or logs based on the observed difference.

## Quick Start

Use these commands from the repo root:

```powershell
npm run kurrentdb:up
npm run cli -- trace
```

Point the client or Navigator to:

```text
kurrentdb://127.0.0.1:2115?tls=false
```

Default wiring:

- real KurrentDB: `127.0.0.1:2114`
- trace proxy: `127.0.0.1:2115`
- adapter under test usually runs on: `127.0.0.1:2113`

For one-off CLI checks through the tracer:

```powershell
$env:KDB_ADAPTER_CONNECTION='kurrentdb://127.0.0.1:2115?tls=false'
node cli/dist/index.js ping
node cli/dist/index.js stream read some-stream --limit 10
```

## What To Look For

- For HTTP compatibility issues, compare `/info`, `/gossip`, `/stats`, and `/ping` request/response shape against the adapter.
- For Navigator startup issues, identify the first requests it sends, which endpoints repeat heavily, and what happens immediately before it fails or retries.
- For stream browsing issues, focus on `Streams.Read` request shape, direction, count, stream selector, and cursor semantics.
- For append issues, trace whether the client uses `Append` or `BatchAppend`, how often it reconnects, and whether responses complete cleanly.
- For noisy traffic, determine whether the traffic is expected behavior from the real node or a mismatch introduced by the adapter or proxy.

## Output Expectations

In `info` mode, prefer request-flow understanding over transport noise:

- repeated HTTP and HTTP/2 activity is deduplicated
- noisy endpoints such as `/gossip` and `/stats` are suppressed by default
- the output is good for quickly learning request order and endpoint mix

Use `debug` mode when you need transport-level detail:

- HTTP/2 client preface
- HEADERS, DATA, RST_STREAM, GOAWAY, SETTINGS, and related frames
- grpc payload previews or decoded payloads when method inference works
- connection resets and stream shutdown behavior

Do not stay in `debug` mode longer than necessary; it is much noisier and better for a focused capture.

## Preferred Commands

Use the CLI wrapper first:

```powershell
npm run cli -- trace
npm run cli -- trace --verbose info
npm run cli -- trace --verbose debug
npm run cli -- trace --suppress-http-paths /gossip,/stats
npm run cli -- trace --no-default-suppressions
```

Only fall back to `npm run kurrentdb:trace` or direct script execution when debugging the tracer itself or when the CLI wrapper is the suspected problem.

## Repository-Specific Notes

- This repo already used the tracer to find an HTTP compatibility gap in `/stats`, to diagnose backward `$all` cursor semantics, and to compare Navigator/Kurrent client behavior with the adapter.
- The CLI command `kcli trace` is now the fastest entry point because it wraps the tracer's environment-variable setup.
- Real KurrentDB traffic may still show normal connection teardown noise such as `ECONNRESET`; do not treat every reset as an adapter bug.
- The tracer is most reliable today for HTTP request order and HTTP/2/grpc transport inspection. Method-level grpc decoding can still be incomplete for some flows.
- The contract test for the backward `$all` exclusivity behavior lives in [`test/contracts/suites/read-all.contract.ts`](test/contracts/suites/read-all.contract.ts).
- The adapter logic for `$all` reads is in [`src/postgres-event-store.service.ts`](src/postgres-event-store.service.ts).

## Debug Loop

When tracing a compatibility issue:

1. Trace the real node first.
2. Record the exact endpoint order, repeated calls, and payload shape.
3. Reproduce the same scenario against the adapter.
4. Compare only the relevant differences; avoid broad speculation.
5. Patch the adapter or tests.
6. Re-run the trace if the behavior is subtle or cursor-related.

When tracing a CLI or client problem:

1. Point the client at `2115`.
2. Try a minimal command such as `ping`, `stream read`, or `stream append`.
3. Verify whether the failure is transport-level, request-shape-related, or just client-side formatting.
4. If the request succeeds against the real node but not the adapter, capture both and compare.

## When To Escalate

- If the trace shows the real node and adapter differ on HTTP payload shape, patch the HTTP stubs first.
- If the trace shows decoded `Streams.Read` semantics differ, add or update a contract test before patching the adapter.
- If the trace output is too noisy, adjust suppressions before collecting more logs.
- If the tracer shows only transport frames and not enough semantic detail, switch between `info` and `debug` deliberately and narrow the reproduction.
