# MongoDB admission review limitations

The experimental single-adapter group path is not production-ready. Admission
is process-local, not distributed fencing, and an unconfirmed cleanup keeps its
permit indefinitely. Driver session cleanup is not evidence of server-confirmed
transaction abort.

Request-size admission uses a bounded conservative estimate rather than JSON
serialization; it is not a V8 heap measurement or a transport receive limit.
The request has already been received and decoded, and stream-name decoding and
metadata-policy parsing can allocate before the estimator runs. In particular,
this does not protect against all oversized incoming requests. A transport-level
limit and earlier admission remain necessary.

Group and admitted ordinary writes charge the entire underlying allocation for every byte view,
including repeated or shared views across requests and sessions. This deliberately
overcounts shared protobuf or Node buffer pools and may reject fragmented or small
requests even when their actual combined backing memory would fit. The charge is
retained for dispatched groups until cleanup is confirmed; it is not a process-wide
limit on allocations held outside the group queue, or across adapter processes.
Without experimental admission, an ordinary started write still continues after a
deadline or cancellation is reported as unknown; this is not proof of rollback.
Shutdown tracks ordinary work and session cleanup independently of admission. If
work does not drain or cleanup is unconfirmed by the bounded deadline, it leaves
the MongoDB client open rather than closing it under live work. This fail-closed
choice can keep the process alive; shutdown closes the client later only if all
tracked local work drains. An operator may need to terminate the process, and started write
outcomes remain unknown.

The bounded shutdown wait covers both draining and client close. A definite close
rejection before the deadline rejects shutdown; a later rejection is logged and
sets a nonzero process exit status. A timeout is an unknown outcome, not a close
failure or proof that the client closed. The timeout
does not cancel MongoDB driver work or close network handles. A cleanup failure
after a write starts is logged and reported as unknown (correlated on
BatchAppend), not as a rollback; clients must reconcile before retrying. The
official-client regression uses a single-node replica set and disables the
single-permit admission for its concurrent healthy peer. With admission enabled,
unconfirmed cleanup deliberately retains the permit and may block peers.
It injects failure after the real session cleanup succeeds, so it does not prove
safe recovery from an actual driver or network cleanup failure.

Cursor cleanup here means the local cursor operation and driver close have
settled, not that MongoDB acknowledged destruction of a server-side cursor.
The MongoDB Node driver suppresses killCursors errors. A server cursor may remain
until server-side session or cursor expiry or other server cleanup; expiry is
not immediate or a bounded guarantee in this adapter. The failpoint regression
demonstrates a surviving server cursor after driver close resolves. No command
monitoring event is used as an authorization to release local work.

Regression coverage for session construction failure, pre-start capacity typing,
scavenge waiter cancellation, and oversized request estimation uses unit tests
with injected collaborators. The full existing MongoDB E2E suite passes, but
these four new failure scenarios have not been independently exercised through
the official client against MongoDB. Correlated capacity, cancellation, closed,
and post-start unknown behavior need explicit official-client regression tests;
scavenge cancellation also needs a real-cursor/hung-owner test. A stopped
scavenge cannot undo a transaction that already started; its outcome may remain
unknown and its permit remains held until cleanup completes.
