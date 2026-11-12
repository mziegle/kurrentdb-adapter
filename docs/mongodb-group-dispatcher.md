# Experimental MongoDB group dispatcher

`GroupDispatcher` is an experimental queue-to-runner adapter. Supply a
runner such as `(batch) => runMongoGroup(client, batch, apply, retryLimits)`.
Each request specifies its event count, byte count, stream key (via the
constructor callback), and optionally an `AbortSignal` and absolute Unix epoch
deadline in milliseconds (`deadline`).

The first queued request starts a bounded coalescing window. A full request,
event, or byte budget seals immediately; otherwise the timer seals the batch.
One group runs at a time and each batch has distinct stream keys. Waiting behind
an active runner can exceed the coalescing window: it is not a transaction or
request deadline. A request that cannot be admitted is rejected with a
`GroupDispatchError` (`capacity`) rather than buffered without limit.

Cancellation or deadline expiry removes queued work without dispatching it.
After sealing, either settles only that caller as `unknown`: it does not cancel
the transaction, alter other callers, or establish rollback. A late commit remains
possible. Timers are subject to event-loop scheduling; without a signal or
deadline, a hung runner can still leave the caller pending. Shutdown cancels
queued requests, waits up to the supplied timeout for active groups, and
reports unresolved transaction outcomes as `unknown`. Unknown outcomes must
not be replayed automatically. A runner must settle the supplied batch and
retain/release its resources according to the existing coordinator contract.

Integration still needs broader proof against replica sets including concurrent
writes, failover, uncertain commits, and cleanup failures. Broader realistic-workload latency and throughput evidence is needed; the limited
comparison below is not a general performance claim.

## Test-only replica-set integration

`test/mongodb-group-dispatcher.e2e-spec.ts` connects the dispatcher to the real
runner and `appendGroupMember`, using a disposable single-node MongoDB 7 replica
set. It monitors the actual commit command for majority+journal concern and
checks ordered positions, per-stream results, tombstones, queued abort signals,
and draining shutdown. Run with:

```sh
npx jest --config test/jest-e2e.json --runInBand mongodb-group-dispatcher.e2e-spec.ts
```

The harness submits concurrent calls directly, not gRPC or the Kurrent client;
it does not establish production request wiring or production performance. It
models a queued deadline using `AbortSignal.timeout`. The single-node replica
set cannot establish replication durability
or multi-node failover. The integration test does not cover dispatcher-level
uncertain commits, timed-out shutdown, server failpoints or cross-process
contention; separate runner tests exercise some of those behaviors without the
dispatcher. No automatic replay of uncertain work is authorized.

## BatchAppend uncertainty at the RPC boundary

The test-only bridge in `test/mongodb-group-rpc.e2e-spec.ts` injects a lost
acknowledgement after a real MongoDB commit. A dispatcher `unknown` is returned
as a correlated `event_store.client.Unknown` BatchAppend status; the official
Node client rejects each affected append with `UnknownError`. This is not proof
of abort: both tested events were already committed. Reconcile by reading the
stream/event IDs before deciding how to proceed; do not blindly retry.

The Node client uses an infinite deadline on its shared BatchAppend stream and
sends request deadlines as message fields. The isolated bridge passes the real
request context, including its absolute deadline and cancellation signal, to the
dispatcher. A gated real Mongo commit test shows one sealed caller receives
`UnknownError` within a bounded interval while its peer succeeds; after release,
the timed-out event is readable and the caller settles only once. This does not
prove cancellation over an individual BatchAppend correlation: the official Node
client does not expose a per-append abort signal on that shared stream. Queued
cancellation is covered at dispatcher level, not through that client.
Its `UnknownError` does not retain
our explanatory status message, so callers must understand that an unknown
error can mean a committed write. A deadline can settle an individual caller
while a sealed transaction remains stalled, but it cannot establish rollback or
release transaction resources. Timers are subject to event-loop scheduling,
and this test does not establish bounds for arbitrary transport failures. The
per-correlation response preserves the stream for subsequent appends. Group
dispatch is opt-in as described below.

## Experimental application integration

The default is off. To experiment, set both
`MONGODB_EXPERIMENTAL_GROUP_COMMIT=true` and
`MONGODB_GROUP_COMMIT_ACKNOWLEDGE_SINGLE_ADAPTER=true` exactly. The latter is
an operator acknowledgement that only one adapter instance will access the
database, including instances using the ordinary write path. MongoDB startup
fails if group commit is opted in without that acknowledgement. PostgreSQL is
unaffected. The acknowledgement does **not** enforce a single instance, fence
other writers, prevent overlapping deployments, or establish production
readiness. **A single adapter is not sufficient to avoid contention:** within
one instance, eligible grouped appends and ordinary appends, soft deletes, and
hard deletes can run concurrent transactions against the shared counter. The
acknowledgement does not serialize them or prevent group-wide unknown outcomes.
Do not enable group commit on shared or production data. Controlled
mixed-instance tests intentionally violate the acknowledgement only on disposable
data to study the hazard. Any value other than `true` for the opt-in keeps
the existing append path. Only one-event ANY writes to non-system streams are
eligible. Other writes use the ordinary path. A group uses the same global
transactional counter as ordinary writers and majority+journal commit. The
tombstone reservation is reversed within the transaction while retaining the
counter fence. There is one active group, a 1 ms coalescing window, at most 32
requests/events and 1 MiB estimated JSON bytes per batch, and 4 MiB retained
bytes. These are fixed experimental limits, not heap or transport limits. An
oversized eligible request is rejected, not silently rerouted. The app drains
for one second on shutdown, then reports unresolved work unknown and closes
the Mongo client. A later outcome cannot be inferred from shutdown alone.

`test/mongodb-group-app.e2e-spec.ts` uses the official client against real
AppModule instances, including default-off behavior, capacity, tombstones,
lost acknowledgement, sealed deadlines and sequential mixed-instance ordering. The RPC bridge
tests below are still test-only. A short, instrumented
comparison reached 80% on one specific concurrent workload, not as a general
performance or production-readiness claim; see the measurement summary below.

The dispatcher waits for the runner promise, including session cleanup, unless
the individual caller cancels, its deadline expires, or shutdown times out.
An unknown client result does not release a live transaction or its retained
capacity; the runner must retain ownership until cleanup actually completes.
Shutdown does not stop the transaction; a later commit remains possible. The
queue counts caller-declared
bytes rather than actual retained heap, and the test bridge uses JSON size as a
proxy. Work failures affect the whole group, and no member replay is safe based
on the runner's `aborted` classification alone. Concurrent ordinary and grouped
transactions can conflict and yield an unknown error for the whole group; work
is deliberately not replayed without trustworthy abort evidence. Additional real-application
queued deadline, cancellation, shutdown, concurrent mixed-instance and broader grouped
failover tests remain necessary. This is not production-ready.

## Limited performance observation

In three short instrumented comparisons on 2026-11-12, the 1 ms opt-in grouped
path achieved 313.69, 262.43 and 295.19 operations/s versus 290.27, 310.04 and
305.48 operations/s for a separate KurrentDB reference: 108.07%, 84.64% and
96.63%, respectively. These are paired run ratios, not a general 80% guarantee.
The official Node client used six workers on distinct streams, each issuing 100
serial one-event ANY appends with a 1,024-character JSON payload, after 20 warmup
events. Official-client `$all` readback checked 1,200 measured events per target
across both settings in each pair, including revisions and payloads, but not
global ordering. Reports surfaced no client errors; the CLI stops on errors
and does not count them. Candidate p95 latency was higher than the 10 ms grouped
baseline in each comparison.

Each pair used fresh disposable pinned-image servers but reused them within the
pair. Run order, warmup, adapter restarts and time were confounded. Synchronous
profiling and command monitoring perturbed results. MongoDB used a single-node
replica set with majority+journal; KurrentDB used single-node defaults, with no
proven durability equivalence. Containers had 2 CPUs and 2 GiB each, while host
processes and shared disk were not isolated. The benchmark used Node 26, outside
this project's Node 24 target. There were no statistical confidence estimates,
power-loss tests or subscription-lag measurements. The original local raw
artifacts are not included in this checkpoint, so this summary alone is not a
reproducible benchmark or production-readiness evidence.

## Single-instance mixed-workload observation

On a disposable single-node replica set, one AppModule with grouping enabled ran
10 rounds, each launching two eligible ANY appends, one NO_STREAM append, one
soft delete and one tombstone on distinct streams concurrently. In two runs,
all 20 grouped appends returned unknown; all 10 ordinary appends, 10 deletes,
and 10 tombstones succeeded. Readback found the 10 acknowledged append IDs
exactly once and in increasing $all order. In the second run successful deletes
were checked by recreating the stream, and successful tombstones rejected a later
append. A
test-only serial admission of *all five operations* yielded 50 successes and
30/30 appended IDs in one run. This admission is not part of the adapter; it
also prevents grouping, and is not a performance comparison or a fix. Unknown
writes must be reconciled, not replayed. These small scheduler-dependent tests
have no server-side contention gate and do not prove a particular interleaving,
error rate, production readiness, or behavior under failures.

## Concurrent mixed-instance observations

The real-AppModule test runs 20 rounds of two eligible ANY writes against one
instance concurrently with one NO_STREAM write against another, all to distinct
streams on a disposable single-node replica set. Two runs observed 40/60 and
38/60 unknown results respectively; readback found 20 and 22 events. Every
acknowledged ID was present exactly once, no attempted ID appeared twice, and
$all subscription delivery matched read order. These are small, scheduler-
dependent observations, not an error-rate estimate or proof of safety under
failure. Subscription catch-up was tested after the writes, not live delivery.
A separate ten-round same-stream race checks exact revision 0 against a grouped
ANY write, per-stream IDs and contiguous revisions, and rejection of a stale
revision after a successful advance. The races are launched concurrently but
are not gated at the Mongo server; they do not deterministically force a specific
transaction interleaving or prove both commits actually conflicted. Rejected
race outcomes are reconciled by ID but not classified. A failed result can be
committed in other failure scenarios; do not replay unknowns without reconciliation.
No correctness fix is claimed from these tests. This failure frequency remains a
production-readiness blocker.

## Three-node election coverage

`test/mongodb-failover.e2e-spec.ts` uses Linux Docker host networking, three
MongoDB 7 nodes, the real AppModule with the experimental flag enabled, and the
Node Kurrent client. It pauses the primary and observes election of a different
node. Concurrent eligible writes before and after election each share one
monitored majority+journal commit; the test verifies acknowledged IDs and
contiguous returned/read positions after election. This is one controlled
failover scenario, not proof against every replication or storage failure.

A separate test intercepts the driver's return *after* a real successful commit,
pauses the original primary, waits for election, and then injects a lost
acknowledgement. Both client calls report unknown, and their IDs are each read
once from the new primary. This simulates loss between the driver and adapter;
it is not a network-level lost commit response or an election during an
unresolved server commit. It demonstrates why unknown does not mean aborted:
reconcile IDs before taking action, and do not blindly retry. In-flight commit
failures and other failure timings remain unproven.

Run with `npx jest --config test/jest-e2e.json --runInBand mongodb-failover.e2e-spec.ts`.
