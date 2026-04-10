# Production Readiness Concept

## Goal

This document collects the next stability and hardening experiments for the adapter so they can be implemented incrementally instead of rediscovered later.

The goal is not only protocol compatibility, but confidence that the adapter behaves predictably under restart, concurrency, long-running client sessions, and partial failure.

## Current Signals

Recent work already improved stability in a few important areas:

- load-balanced append behavior now passes end-to-end
- schema initialization no longer races across multiple adapter instances
- append completion no longer stalls because of pool starvation during `pg_notify`
- Navigator startup improved after adding `/info/options`
- the tracing workflow is now easier to use through `kcli trace`

These fixes are encouraging, but they also highlight the kinds of failures that matter for production readiness:

- startup races
- pool pressure and resource exhaustion
- compatibility gaps in HTTP bootstrap endpoints
- long-running client behavior that only appears after startup

## 1. Restart Durability

Verify that persisted data and stream behavior survive adapter restarts against the same PostgreSQL database.

Key checks:

- append events, restart adapter, read the same stream back
- verify `$all` still includes previously written events after restart
- verify stream metadata and retention-related behavior survives restart
- verify subscriptions can reconnect after restart without corrupting state

Why this matters:

- restart behavior is one of the highest-signal production checks
- it validates that the database is the source of truth rather than in-memory state

Suggested outcome:

- dedicated e2e coverage for restart persistence across stream reads, `$all`, and subscriptions

## 2. Same-Stream Concurrency Stress

Stress the adapter with many concurrent writers targeting the same stream.

Key checks:

- many concurrent appends with `any`
- many concurrent appends with explicit expected revision
- mixed append sizes, including larger batched appends
- multi-instance load-balanced writers against one shared PostgreSQL backend

Why this matters:

- this repo has already exposed concurrency bugs in append handling
- same-stream concurrency is where optimistic locking, transaction timing, and pool usage all interact

Suggested outcome:

- stress-oriented e2e test coverage that runs heavier than the current load-balanced suite
- clear expectations for success counts, wrong-expected-version failures, and final stream contents

## 3. Long-Running Navigator Soak

Leave Kurrent Navigator connected for an extended period and observe adapter behavior.

Key checks:

- request cadence over 30-60 minutes
- recurring `Gossip.Read` and `/info` traffic
- connection churn and reconnect behavior
- memory growth
- log volume and repeated warnings

Why this matters:

- startup success is necessary but not sufficient
- many UI/tooling problems only show up in long-running idle or semi-idle sessions

Suggested outcome:

- a soak checklist and optional scripted observation notes
- reduced noisy logging for expected background polling

## 4. Read Cursor and Backward-Read Semantics

Deepen coverage around cursor semantics that matter to Navigator and browser-style clients.

Key checks:

- backward reads return newest-first
- reads from a specific revision return the expected slice
- `$all` backward pagination from an explicit position behaves like real KurrentDB
- empty and missing-stream edge cases match client expectations

Why this matters:

- subtle cursor semantics can break UI clients without obvious server errors
- the repo already found a real `$all` backward-read discrepancy this way

Suggested outcome:

- expanded contract and e2e coverage around backward reads and explicit cursor positions

## 5. Subscription Stability

Exercise stream subscriptions and `$all` subscriptions under realistic lifecycle conditions.

Key checks:

- subscription behavior during concurrent writes
- subscription behavior after adapter restart
- idle subscriptions that remain connected for longer periods
- catch-up vs live transitions
- cancellation and teardown correctness

Why this matters:

- subscriptions are often stable in simple happy-path tests but fail under reconnect or lifecycle pressure

Suggested outcome:

- explicit subscription resilience tests
- better observability around subscription state transitions if needed

## 6. HTTP and Bootstrap Compatibility Sweep

Compare the adapter's probe/bootstrap endpoints with real KurrentDB using the tracer.

Key endpoints:

- `/info`
- `/info/options`
- `/gossip`
- `/stats`
- `/ping`

Why this matters:

- tooling often fails before stream operations begin
- Navigator already demonstrated that bootstrap compatibility can be the difference between repeated retries and healthy startup

Suggested outcome:

- a compatibility checklist for HTTP bootstrap endpoints
- trace-backed comparisons before changing endpoint shapes

## 7. Observability and Pool Health

Improve visibility into internal pressure signals that correlate with real incidents.

Key checks:

- PostgreSQL pool total/idle/waiting counts under load
- append latency and read latency
- notification behavior
- warning/error frequency

Why this matters:

- recent bugs were easier to understand once connection-pool behavior was considered
- production readiness depends on early detection of saturation and contention

Suggested outcome:

- focused metrics and logs for pool pressure and slow operations
- thresholds or heuristics for identifying unhealthy behavior during tests

## 8. Fault Injection

Test how the adapter behaves when parts of the system become unavailable or slow.

Key checks:

- PostgreSQL temporarily unavailable
- adapter restart during client reads or subscriptions
- deliberate delay in database operations
- client disconnects mid-stream

Why this matters:

- production incidents usually involve degraded dependencies, not clean happy paths

Suggested outcome:

- a small failure-mode test matrix
- documented expected behavior for transient failures

## Suggested Order

If these are tackled incrementally, this order gives the best signal early:

1. restart durability
2. same-stream concurrency stress
3. read cursor and backward-read semantics
4. subscription stability
5. long-running Navigator soak
6. HTTP and bootstrap compatibility sweep
7. observability and pool health improvements
8. fault injection

## Practical Next Step

If only one item is taken next, choose restart durability plus same-stream concurrency stress.

That combination gives the strongest confidence that:

- state is really durable
- concurrency handling stays correct under load
- the adapter can tolerate the two most likely early production failure modes

## Working Style Recommendation

For future work on this concept:

- add or update automated tests whenever a compatibility or stability issue is confirmed
- use `kcli trace` against real KurrentDB before patching subtle client-facing semantics
- prefer trace-backed comparisons over assumptions for Navigator and Kurrent client behavior
- keep an eye on resource usage, not just correctness
