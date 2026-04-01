# Trace Workflow

## Standard Differential Debug Loop

1. Start real KurrentDB:

```powershell
npm run kurrentdb:up
```

2. Start the repo tracer:

```powershell
npm run kurrentdb:trace
```

3. Point the reproducer at the trace proxy:

```text
kurrentdb://127.0.0.1:2115?tls=false
```

4. Reproduce the issue.

5. Compare what the real node does with what the adapter currently does on `2113`.

## Useful Comparisons

- Navigator fails only against adapter:
  Trace the real node first to learn the expected request order and payload shape.
- Event shown twice in `$all`:
  Trace the real node's `Streams.Read` requests and responses, then compare cursor semantics in the adapter.
- Missing stream browser data:
  Look for `Streams.Read` payloads, especially `$all` vs named stream reads, direction, count, and position.

## Important Environment Variables

- `TRACE_VERBOSITY=info|debug`
  `info` is the default and should be preferred.
- `TRACE_USE_DEFAULT_SUPPRESSIONS=0|1`
  Disable default suppression if you need the full stream.
- `TRACE_SUPPRESS_HTTP_PATHS=/gossip,/stats`
  Suppress noisy HTTP endpoints.
- `TRACE_SUPPRESS_HTTP2_FRAME_TYPES=settings,window_update,ping`
  Suppress low-value HTTP/2 frame types.
- `TRACE_SUPPRESS_HTTP1_HEADERS=1`
  Hide HTTP headers when only payloads matter.
- `TRACE_SUPPRESS_HTTP1_BODIES=1`
  Hide HTTP bodies when request/response lines are enough.

Example:

```powershell
$env:TRACE_VERBOSITY='info'
$env:TRACE_SUPPRESS_HTTP_PATHS='/gossip,/stats'
$env:TRACE_SUPPRESS_HTTP2_FRAME_TYPES='settings,window_update,ping'
npm run kurrentdb:trace
```

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

## Known Repo-Specific Example

Navigator duplicated the latest `$all` event because:

1. Real KurrentDB returned the newest event for `Read($all, backwards, end, count=1)`.
2. The follow-up `Read($all, backwards, position=<that event>, count=100)` treated the explicit position as exclusive.
3. The adapter originally treated it as inclusive and returned the same event again.

Use the trace to validate the real node semantics before patching the adapter.
