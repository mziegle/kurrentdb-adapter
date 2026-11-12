# Mongo group retry validation

Run `npx jest --config test/jest-e2e.json --runInBand test/mongodb-group-runner.e2e-spec.ts` with Docker. The fixture starts a disposable MongoDB 7 single-node replica set with test commands enabled; never enable failpoints on a production server.

The real-server transient-insert failpoint tests now assert no work replay even when `maxAttempts` is greater than one, including when ordinary abort cleanup can succeed. The runner currently does not support same-session work retry because it cannot establish trustworthy abort acknowledgement.

**Retry blocker:** when a staged write is followed by a failpoint-generated transient insert error and `abortTransaction` is rejected by a second failpoint, the driver reports command failure but its `abortTransaction()` promise resolves. The regression asserts that work is not replayed. A resolved driver abort promise or an untrusted, forgeable command-monitoring event alone must not authorize replay. Work retries remain disabled regardless of `maxAttempts`; do not enable them without an independently trustworthy cleanup/retry policy and validation.

The lost-reply tests wrap the real driver *after* MongoDB acknowledges commit; they simulate losing the acknowledgement to the runner, not a real network partition. The test checks same transaction commit retries, no callback replay, and real readback. The shutdown test gates the commit call and verifies unknown settlement and eventual persisted readback. These are single-node deterministic tests, not failover, multi-node durability, performance or statistical reliability evidence; they do not establish an 80% production reliability figure.
