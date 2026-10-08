# Protected buffered composition

Buffered component calls enroll the canonical invocation monotonically, including
when a later child is the first buffered component. Native attachment records a
logical component frame before enrollment, so earlier root appends are retained.
The engine records imperative child markers at invocation admission. Bindings
follow each frame's timeline in declaration order and repeated index order.
Retained sealed contexts dispatch under the nearest open ancestor. Neutral and
empty components still have logical frames.

A protected buffered invocation with several database owners supports only exact
native owners with locally owned transactions and no previously drained work.
Caller-owned/mixed transactions, custom owners and earlier native drains are
explicitly rejected before publishing a newly incompatible mutation capability.
Rejection is sticky even when a handler catches the child error. Local transactions
are cleaned up; caller-owned transactions stay usable and caller-owned. Existing
single-owner caller-transaction preparation and ordinary unbuffered paths retain
their lifecycle. This bounded admission is a compatibility limitation, not support
for general cross-owner external transactions.

The journal contains references to native operation records, never a second SQL or
payload queue. Closure joins engine resolution/registration and closes native
mutation admission. A single freeze validates exact owner/frame/operation
projections, operation identities and completed logical frames before publishing
runs. Failed or overlapping freeze cannot publish usable run authority.

Both root preparation boundaries consume the same fixed cursor through maximal
contiguous runs of one exact owner. Constructor-bound preparation consumes a
one-use drain permit tied to its invocation identity, frozen journal/run pointer,
exact owner and expected cursor. Foreign operations are native batching barriers;
adjacent eligible operations retain the native execution planner and SQLX executor.
There is no intermediate flush. An exhausted preparation is a no-op. Completion
requires exhaustion and verifies that every native record is executed/unreserved;
it cannot fall back to draining a missed suffix.

Transaction order is separate from journal order. A dedicated establishment gate
serializes successful native Begin and transaction/ordinal publication. Failed
Begin and repeated access/start create no ordinal; lazy drain startup joins at its
actual establishment. SQL does not hold the journal bookkeeping mutex. Native
lock order is execution serialization, establishment gate, owner state, sealed
identity state, then journal state, with bookkeeping locks released before native
callbacks or SQL. Root/frame bookkeeping never calls native execution under its
locks.

After successful preparation and all-owner guard checks, transactions complete
in creation order. Precommit failures roll back in reverse creation order. An
attempted commit is never repeated; failure stops commits and cleans up remaining
transactions in forward creation order. A successful commit followed by observer
failure retains its committed outcome. Owner Unit identities in public outcome
records retain the original unit order even when physical completion reverses.
Earlier commits can remain durable after a later commit fails: this provides no
distributed atomicity. Multiple native owners remain in the engine unit set and
remain ineligible for mutation retry.

Generic SQLite tests exercise real driver statements, triggers, batching and
transaction outcomes. Platform generated dispatch/CI_EVENT batch shape, MySQL
witnesses and application E2E/contract parity require separate root integration
verification before acceptance.
