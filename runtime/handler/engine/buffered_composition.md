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

## Explicit Read-Only Dependency Groups

Independent native reader inputs can opt into bounded concurrent resolution:

```text
#define($_ = $Audiences<*audience.AudienceOutput>(component/GET:/audiences)
    .Optional().WithReadOnlyGroup('subject'))
#define($_ = $Creatives<*creative.ActiveCreativesOutput>(component/POST:/creatives)
    .Optional().WithReadOnlyGroup('subject'))
```

`WithReadOnlyGroup('subject', 'Auth')` adds an explicit successful prerequisite
outside the group. `DependsOn('OtherMember')` orders a member behind another member
of that group. Unknown references and cycles fail compilation. Generated binding
tags carry `resolutionGroup=subject,resolutionMode=dependencies` plus the optional
`resolutionAfter` and `dependsOn` lists. The legacy `WithResolutionGroup` policy
retains its existing finite shape and security requirements.

Declare authorization inputs as explicit barriers rather than assuming that an
ungrouped input will run first. Required verified-JWT inputs and required inputs
declaring status 401/403 must be barriers; omission is rejected before member
execution. Application-specific authorization without that metadata must likewise
be named explicitly. Group authoring never grants an authorization bypass.

The shared Bindly scheduler defaults to four runnable members per invocation.
Waiting ancestors release their permit during input binding; native execution
and hooks reacquire or reuse the inherited permit. Nested bindings remain
sequential unless they explicitly declare a group. No HTTP method establishes
read-only authority: native POST readers are eligible; custom GET handlers and
writers are not. Unrelated components remain lazy. Eager, indexed, and LinkedOnly
loaders use the same effective metadata and runtime admission.

Each branch has separate input, binding, and output state. Providers and codecs
must still treat inherited values as read-only and be concurrency-safe. Child
error-aware and invoker finalizers finish before results become visible. Contracts
requiring success-only post-commit or MCP completion hooks are rejected, not run
early against an uncommitted transaction. Use the existing pre-completion
error-aware output finalizer for reader result construction.

Read-only authority covers nested calls and hooks for their complete binding
lifetime. Datly-managed DML, sequences, flushes, transaction startup, unchecked
transaction SQL, raw connector capabilities, and message-bus capabilities are
denied. Provider overrides, independent transactions and caller completion
callbacks are not admitted. Rejected effects remain failures even if caught;
retained child capabilities never acquire the parent's later write authority.
This is managed capability enforcement, not a sandbox for arbitrary Go code or
externally retained database/message-bus handles.

Inherited transactions and existing cursor locks are retained. Available worker
capacity never permits concurrent cursors on a transaction whose driver requires
serialization. A separate connector continues to resolve its own connection.
Optional means missing values are allowed, not that errors are ignored. Binding
errors are reported in declaration order. Authorization failures, cancellation,
transaction/cleanup failures and recovered panics cancel outstanding work. All
started work is joined before returning. Existing batching is unchanged.

MDP adoption requires explicit group declarations and regeneration. Its complete
Index response/publication comparison and scheduling-only latency A/B remain
application-level acceptance checks; unit overlap is not a latency claim.
