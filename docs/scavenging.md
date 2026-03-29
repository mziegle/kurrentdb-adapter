# Scavenging Notes

These notes capture verified KurrentDB scavenging behavior from the checked-out
`../KurrentDB` source and docs so the adapter can stay aligned.

## High-level behavior

- Stream metadata such as `$tb`, `$maxCount`, and `$maxAge` affects stream reads
  immediately.
- Those hidden events are not removed from storage immediately.
- Before scavenging, the old events are still present in the database and still
  visible through `$all`.
- A scavenge is what physically removes those records and their stale index
  entries.

This matches KurrentDB's own documentation in
`../KurrentDB/docs/server/operations/scavenge.md`.

## Truncate-before behavior

- `$tb` is logical truncation first, physical deletion later.
- KurrentDB treats `$tb = N` as "discard events before event number `N`" during
  scavenging.
- KurrentDB does not use `$tb` to eagerly delete records when metadata is
  written.

Relevant code:

- `../KurrentDB/src/KurrentDB.Core/TransactionLog/Scavenging/Stages/StreamCalculator.cs`
- `../KurrentDB/src/KurrentDB.Core/TransactionLog/Scavenging/Stages/EventCalculator.cs`

## Important subtlety: keep the last event

- KurrentDB does not scavenge away the last event in a stream just because of
  `$tb`, `$maxCount`, or `$maxAge`.
- The scavenge path explicitly keeps the last event in the stream before
  applying those discard rules.

This is covered in KurrentDB's `EventCalculator` and in the truncate-before
scavenge tests.

## `$all` behavior

- Before scavenging, truncated or expired events still appear in `$all`.
- After scavenging, those old events stop appearing in `$all`.

That means parity with KurrentDB requires separating:

1. read visibility rules
2. physical cleanup rules

They are not the same operation.

## Metastream behavior

- Old metadata events in `$$stream` are also not removed immediately.
- When a newer metadata event supersedes an older one, the older metadata event
  becomes scavengeable.
- After scavenging, KurrentDB keeps only the newest relevant metadata event for
  that metastream in the tested scenarios.

## Implications for this adapter

- Do not physically delete truncation-hidden records at metadata-write time.
- Do not assume stream-read filtering implies `$all` filtering.
- Scavenging should be the operation that turns logically hidden records into
  physically removed records.
- Be careful not to diverge from KurrentDB's "keep the last event" rule when
  implementing truncation-based cleanup.
