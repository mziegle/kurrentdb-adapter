# Trace Workflow

## Standard Differential Debug Loop

1. Start real KurrentDB:

```powershell
npm run kurrentdb:up
```

2. Start the repo tracer:

```powershell
npm run cli -- trace
```

3. Point the reproducer at the trace proxy:

```text
kurrentdb://127.0.0.1:2115?tls=false
```

4. Reproduce the issue.

5. Compare what the real node does with what the adapter currently does on `2113`.

Preferred default:

- real KurrentDB: `127.0.0.1:2114`
- trace proxy: `127.0.0.1:2115`
- adapter under test: `127.0.0.1:2113`

For quick CLI probes through the tracer:

```powershell
$env:KDB_ADAPTER_CONNECTION='kurrentdb://127.0.0.1:2115?tls=false'
node cli/dist/index.js ping
node cli/dist/index.js stream read some-stream --limit 10
node cli/dist/index.js stream append some-stream --type test-event --data 1
```

## Useful Comparisons

- Navigator fails only against adapter:
  Trace the real node first to learn the expected request order and payload shape.
- Event shown twice in `$all`:
  Trace the real node's `Streams.Read` requests and responses, then compare cursor semantics in the adapter.
- Missing stream browser data:
  Look for `Streams.Read` payloads, especially `$all` vs named stream reads, direction, count, and position.
- Append hangs or times out:
  Check whether the client is using `Append` or `BatchAppend`, whether connections reset, and whether responses are returned before teardown.
- Gossip or info endpoints appear noisy:
  Confirm whether the real node shows the same polling pattern before changing adapter behavior.

## Preferred CLI Flags

- `--verbose info|debug`
  `info` is the default and should be preferred.
- `--no-default-suppressions`
  Disable default suppression if you need the full stream.
- `--suppress-http-paths /gossip,/stats`
  Suppress noisy HTTP endpoints.
- `--suppress-http2-frame-types settings,window_update,ping`
  Suppress low-value HTTP/2 frame types.
- `--suppress-http1-headers`
  Hide HTTP headers when only payloads matter.
- `--suppress-http1-bodies`
  Hide HTTP bodies when request/response lines are enough.
- `--proxy-port <port>` and `--upstream-port <port>`
  Useful when another local process already occupies `2115` or `2114`.

Example:

```powershell
npm run cli -- trace --verbose info --suppress-http-paths /gossip,/stats
npm run cli -- trace --verbose debug --no-default-suppressions
```

## Equivalent Environment Variables

Use these only when running the raw script directly:

- `TRACE_VERBOSITY=info|debug`
- `TRACE_USE_DEFAULT_SUPPRESSIONS=0|1`
- `TRACE_SUPPRESS_HTTP_PATHS=/gossip,/stats`
- `TRACE_SUPPRESS_HTTP2_FRAME_TYPES=settings,window_update,ping`
- `TRACE_SUPPRESS_HTTP1_HEADERS=1`
- `TRACE_SUPPRESS_HTTP1_BODIES=1`
- `TRACE_PROXY_PORT=2115`
- `TRACE_UPSTREAM_PORT=2114`

## Interpreting `info` vs `debug`

Use `info` to answer:

- Which endpoints are called?
- In what order?
- Which calls repeat heavily?
- Does the real node show the same high-level behavior as the adapter?

Use `debug` to answer:

- Does the client open HTTP/2 streams the way we expect?
- Are there `RST_STREAM`, `GOAWAY`, or connection-reset patterns?
- Is the payload actually reaching the wire?
- Is the tracer able to infer a grpc method or only show raw payload bytes?

## Interpreting Decoded `Streams.Read`

Typical request of interest:

```text
grpc method=event_store.client.streams.Streams.Read body=
{
  "options": {
    ...
  }
}
```

Key fields to inspect:

- `read_direction`
- `all` vs named stream selector
- `count`
- explicit `position` or `end`
- `resolve_links`
- `filter_option`

Typical response payloads:

- `content: "event"` with an embedded event
- `streamNotFound`
- `caughtUp`
- positional markers for empty system streams

If the tracer only shows HTTP/2 DATA frames or `grpc undecoded hex=...`, do not assume the request is wrong. It can mean the transport capture worked but grpc method inference was incomplete for that stream.

## Known Repo-Specific Example

Navigator duplicated the latest `$all` event because:

1. Real KurrentDB returned the newest event for `Read($all, backwards, end, count=1)`.
2. The follow-up `Read($all, backwards, position=<that event>, count=100)` treated the explicit position as exclusive.
3. The adapter originally treated it as inclusive and returned the same event again.

Use the trace to validate the real node semantics before patching the adapter.

Another useful repo-specific lesson:

- Real-node tracing is also a quick way to separate transport issues from CLI issues.
- Example: the CLI `--json` path failed on `BigInt` serialization even though the traced append/read requests themselves succeeded against KurrentDB.
