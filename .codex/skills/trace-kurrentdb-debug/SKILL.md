---
name: trace-kurrentdb-debug
description: Debug KurrentDB adapter, Navigator, and protocol-compatibility issues in this repo with the repo tracer at `../../../scripts/trace-kurrentdb.js`. Use when Codex needs to inspect HTTP `/info` `/gossip` `/stats` traffic, decoded gRPC/protobuf payloads such as `Streams.Read`, compare real KurrentDB behavior against the adapter, or reduce trace noise while reproducing a bug.
---

# Trace KurrentDB Debug

## Overview

Use this skill when the repo's trace proxy is the fastest way to understand what Kurrent Navigator or the Kurrent client is doing on the wire. The main tool is the repo tracer at [`../../../scripts/trace-kurrentdb.js`](../../../scripts/trace-kurrentdb.js), which sits in front of a real KurrentDB node and prints parsed HTTP and decoded gRPC protobuf payloads.

Read [`references/trace-workflow.md`](references/trace-workflow.md) for commands, environment variables, and interpretation details.

## Workflow

1. Start the real KurrentDB node.
2. Start the trace proxy.
3. Reproduce the client or Navigator behavior against the trace proxy port.
4. Inspect parsed HTTP JSON and decoded protobuf request/response bodies.
5. Compare the traced real-node behavior with the adapter's behavior and patch the adapter or tests.

## Quick Start

Use these commands from the repo root:

```powershell
npm run kurrentdb:up
npm run kurrentdb:trace
```

Point the client or Navigator to:

```text
kurrentdb://127.0.0.1:2115?tls=false
```

Default wiring:

- real KurrentDB: `127.0.0.1:2114`
- trace proxy: `127.0.0.1:2115`
- adapter under test usually runs on: `127.0.0.1:2113`

## What To Look For

- For HTTP compatibility issues, focus on parsed `/info`, `/gossip`, and `/stats` bodies and whether the adapter shape matches the real node.
- For stream browsing issues, focus on decoded `event_store.client.streams.Streams.Read` requests and responses.
- For `$all` pagination issues, inspect whether backward reads from an explicit position behave as an exclusive cursor.
- For Navigator startup issues, confirm which requests happen before the UI fails and which payloads differ from the real node.

## Output Expectations

In `info` mode, prefer the parsed payloads, not low-level frame noise:

- HTTP request/response lines plus parsed JSON body
- decoded protobuf gRPC request/response bodies when the method can be inferred
- deduplicated summaries for repeated traffic

Use `debug` mode only when transport-level detail is required.

## Repository-Specific Notes

- This repo already used the tracer to find an HTTP compatibility gap in `/stats` and to diagnose backward `$all` cursor semantics.
- The contract test for the backward `$all` exclusivity behavior lives in [`test/contracts/suites/read-all.contract.ts`](test/contracts/suites/read-all.contract.ts).
- The adapter logic for `$all` reads is in [`src/postgres-event-store.service.ts`](src/postgres-event-store.service.ts).

## When To Escalate

- If the trace shows the real node and adapter differ on HTTP payload shape, patch the HTTP stubs first.
- If the trace shows decoded `Streams.Read` semantics differ, add or update a contract test before patching the adapter.
- If the trace output is too noisy, adjust suppressions before collecting more logs.
