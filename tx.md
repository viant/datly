# Root-Owned Transaction and Shared Data Unit Plan

Date: 2026-07-21
Status: implemented across original Datly and Datly 1.0. Focused tests, race
tests, vet, and independent review pass. Repository-wide acceptance remains
limited by unrelated pre-existing failures described in section 20.

## 1. Scope

The primary implementation changes the original repositories:

- `/Users/awitas/go/src/github.com/viant/datly`
- `/Users/awitas/go/src/github.com/viant/xdatly`

The resulting behavior must also be supported by:

- `/Users/awitas/go/src/github.com/viant/datly_1`

through its own dispatcher/handler-engine adapter. Do not implement the feature
in `xdatly_1`; do not make the original implementation depend on either `_1`
repository. Original `xdatly` remains the public handler/SQLX contract used by
original Datly. Datly 1.0 must reproduce the same root-unit semantics in its
existing capability model.

The objective is one root-owned transaction and one ordered mutation data unit
for a component dependency chain using the same database. The data unit must be
passed explicitly through child-session dispatch, not rediscovered from a
global context and not recreated independently by each child executor.

The motivating case is:

1. a campaign component creates a campaign;
2. a component binding invokes a campaign-flight component;
3. the flight row has a foreign key to the campaign row;
4. child and parent DML are buffered; and
5. the final execution inserts campaign before flight in one transaction.

Passing only `*sql.Tx` is insufficient. The shared object must also own the
ordered DML buffer, sequencing, database identity, flush cursor, component
ordering, and completion state.

## 2. Required Invariants

1. The outermost Datly operation creates one root data unit.
2. Every nested component dispatch receives that unit through a dispatcher-owned
   invocation scope; handler code is not responsible for forwarding it.
3. Components using the same database share one database unit, one transaction,
   one sequencer, and one ordered DML journal.
4. Components using different databases never accidentally share state.
5. A locally opened transaction is completed only by the root operation.
6. A supplied transaction is never committed or rolled back by Datly.
7. Sequencing receives the exact transaction used for DML.
8. Parent/child semantic ordering is deterministic.
9. Table-specific flush cannot skip causal predecessors.
10. A parent failure rolls back child work already flushed into the local root
    transaction.
11. Existing `xdatly/handler.Session.Session(...state.Option)` callers and
    implementations continue to compile without a new required option.
12. Existing `state.WithSqlTx` remains supported during migration.
13. Original xdatly exposes no Datly concrete data-unit or transaction lifecycle
    API; existing `sqlx.WithSqlTx` remains the external transaction input.
14. Original Datly and Datly 1.0 use the same root-unit, database-unit, ordering,
    flush, sequencing, and completion semantics.

## 3. Current Original-Datly Behavior

### 3.1 Executor-local buffers

`service/executor/handler.Executor` currently owns:

```go
dataUnit  *expand.DataUnit
dataUnits map[string]*expand.DataUnit
tx        *sql.Tx
```

`NewExecutor` creates a new main `expand.DataUnit` for every executor. Named
connectors create additional executor-local units in `getDataUnit`.

This is appropriate for an isolated component, but nested component executors
do not share a single mutation journal. Passing a transaction to a child only
makes independently buffered statements execute against the same transaction;
it does not provide global statement ordering.

### 3.2 Existing transaction propagation

Original Datly propagates a supplied transaction on only some paths:

- `service/session.Options` stores `sqlTx` and exposes `WithSQLTx`;
- `xdatly/handler/state.Options` stores `sqlTx` and exposes `WithSqlTx`;
- `service/operator.Service` copies the parent session transaction in injector
  and MCP finalizer paths;
- `service/executor/handler.Executor.Session` adopts `session.Options.SqlTx`;
- `Executor.newSqlService` defaults SQLX options to `e.tx`; and
- redirect/session construction applies a transaction only when the redirect
  caller supplied `state.WithSqlTx`.

The ordinary component-binding dispatcher creates a new child session without
copying the parent transaction. It therefore does not currently share even the
transaction, much less the DML buffer. This path is a primary target of the
dispatcher-scope change.

This ownership rule is worth preserving: a transaction supplied by a caller is
parent-owned.

### 3.3 Existing local transaction behavior

`service/executor/lazyTx` lazily opens and completes a transaction only when it
opens that transaction itself. A transaction passed through `executor.WithTx`
uses a no-op closer, correctly leaving an externally supplied transaction open.

Original Datly's handler `Flush` has an additional defect: it calls
`handler.Service.Tx`, which opens and stores a transaction, and then passes that
transaction to the statement executor as though it were supplied externally.
The statement executor consequently does not commit it. Later executor
execution passes the same transaction again and also does not commit it. A
flush-opened original-Datly transaction can therefore remain open and its work
can be lost.

The root data unit fixes this leak by recording who actually opened the
transaction and completing it at the root boundary. Per-flush commit is not
described as existing original-Datly compatibility behavior.

### 3.4 Existing table flush behavior

`service/executor/handler.Service.Flush` calls:

```go
s.dataUnit.Statements.FilterByTableName(tableName)
```

and executes only the matching statements. This can skip an earlier parent
insert and execute a later child insert first.

Executed statements are marked individually, so later full execution does not
repair a foreign-key failure caused by the earlier selective flush.

`Executor.execData` currently marks an executable as executed before running
its SQL operation and does not restore the marker on failure. Raw SQL statements
are omitted entirely by `FilterByTableName`. The new reserved/executed/failed
state machine must correct both problems.

### 3.5 Existing sequencing behavior

Both:

- `service/executor/handler.Service.Allocate`; and
- `service/executor/expand.DataUnit.Allocate`

construct the sequencer with a raw `*sql.DB`. They do not pass the business
transaction.

The SQLX `PresetIDWithTransientTransaction` strategy also intentionally opens
a separate transaction. Therefore adding `tx` to an option list is not enough
unless the selected SQLX strategy honors it.

The existing `getDataUnit` path for an explicit `WithDb` creates a fresh unit
without registering it in `Executor.dataUnits`, so `Executor.Execute` does not
drain it. Database-unit registry work must close this loss path as well.

## 4. Why a Shared FIFO Is Not Enough

Datly resolves component-valued input before executing the parent mutation
body. A child component invoked through binding may queue flight DML before the
parent queues campaign DML:

```text
temporal append order:
    flight insert
    campaign insert
```

Executing one shared FIFO would preserve the wrong foreign-key order.

The shared data unit must distinguish:

1. **binding child**: invoked to produce parent input and logically executed
   after the parent's own mutation timeline; and
2. **imperative child**: invoked explicitly from a handler/session and executed
   at its exact call position.

Required semantic order:

```text
campaign component
├── campaign insert
├── imperative child marker, if invoked here
├── later campaign operations
└── binding children
    └── flight insert
```

Within each component, authored DML order remains unchanged. Binding siblings
retain deterministic resolver order. Imperative children retain call-position
order.

## 5. Ownership Boundary

### 5.1 The component dispatcher owns propagation

Original `xdatly` owns the public handler session contract:

```go
Session(ctx context.Context, route *http.Route, opts ...state.Option)
    (Session, error)
```

Handlers should not have to supply `WithDataUnit` when calling this method. The
session/redirect implementation already knows the active Datly executor and is
therefore the correct place to propagate its invocation scope.

The preferred design adds no public xdatly data-unit option. Instead, original
Datly owns an internal dispatcher scope:

```go
type invocationScope struct {
    unit  *rootDataUnit
    frame *componentFrame
}
```

The scope is installed at the root operation and propagated in two deliberate
ways:

1. synchronous component dispatch carries it in `context.Context`; and
2. returned `extension.Session` redirect/finalizer closures capture the scope so
   a later child-session call cannot lose it merely because it receives another
   derived context.

Context is a carrier, not a discovery fallback. Every dispatcher entry verifies
the scope and passes it to the child executor explicitly. A missing scope at a
root entry creates a root unit; a missing scope inside a known nested dispatch
is an error rather than permission to open an unrelated buffer.

Existing `xdatly/handler/state.WithSqlTx` remains the external transaction input
for original Datly. The root data unit adopts and validates it once. Nested
dispatch propagates the resulting unit, not the raw transaction as the primary
authority. Transaction propagation may remain temporarily for compatibility,
but it must agree with the unit.

If implementation inspection proves there is a public session handoff that
cannot preserve either context or the captured dispatcher closure, only then add
an additive, strongly typed original-xdatly option. That is a fallback design,
not the default plan.

### 5.2 Original Datly owns the concrete root data unit

The implementation belongs under Datly's executor packages and contains:

```text
rootDataUnit
├── root completion state
├── component frame tree
├── database-unit registry
│   ├── primary database
│   └── connector-specific databases
└── deterministic first-use ordering

databaseUnit
├── database identity and *sql.DB
├── supplied or locally opened *sql.Tx
├── ordered frame journals
├── SQLX services
├── transactional sequencer
├── execution cursor
├── failure state
└── commit observer
```

The Datly object is private to Datly. It does not implement a new public xdatly
lifecycle contract. Completion methods remain on private Datly types used only
by the root operator/executor.

### 5.3 Frame-scoped SQLX facade

Calls such as `Insert(table, value)` do not carry context, so component frame
identity cannot be inferred at append time. The existing
`Session.Db(...) -> Executor.newSqlService` path must resolve a private
frame-scoped facade that captures:

- root data unit;
- component frame;
- database/connector selection; and
- root, binding, or imperative invocation relation.

All handler SQLX methods use that facade through the existing
`xdatly/handler/sqlx.Sqlx` service boundary. Multiple facades share the same
underlying database unit and journal where their database identity matches.

### 5.4 Datly 1.0 adapter

Datly 1.0 applies the same model without `xdatly_1` changes:

- `Runtime.invokeComponent` receives an internal invocation option or explicit
  scope argument;
- root route/direct invocation creates the scope;
- `componentProvider` propagates it with relation `binding`;
- `scopedComponentInvoker` propagates it with relation `imperative`;
- `handlerengine.Request` receives the scope explicitly; and
- the engine projects frame-scoped `Data`, `DML`, `Sequencer`, and `Flusher`
  capabilities from the same database unit.

The original and 1.0 adapters differ only at dispatch/session integration. The
transaction and journal semantics defined by this plan must remain identical.

## 6. Complete Propagation Trace

The invocation scope must be passed at every place a child session or executor
is created. Transaction-only propagation must be upgraded to dispatcher-owned
scope propagation.

### 6.1 Root operation

`service/operator.Service.Operate` is the root ownership boundary when the
incoming `service/session.Session` has no data unit.

Root flow:

1. create a Datly root data unit and invocation scope;
2. attach the scope to the operation context and root executor;
3. create the root component frame;
4. perform binding, handler execution, and finalization;
5. seal the root frame;
6. on success, drain and complete locally owned database transactions;
7. on failure, roll back locally owned opened transactions; and
8. never complete a caller-supplied transaction.

If the operation context already carries a valid invocation scope, `Operate` is
nested and must not complete it.

Validity includes lifecycle state. An open scope may be adopted. A completed or
sealed scope reaching a new external root entry is replaced with a fresh root
scope. A nested dispatcher closure that still points at a completed scope fails
closed because using that session after root completion is invalid.

### 6.2 Public handler child session

The original public path is:

```go
handler.Session.Session(ctx, route, opts ...state.Option)
```

`service/executor/extension.Session.Session` delegates to the redirect callback.
The callback created by `service/executor/handler.Executor` must capture the
parent invocation scope. Its redirect passes that scope explicitly to the child
executor regardless of which public `state.Option` values the handler supplies.

It should also preserve `WithSqlTx` while compatibility requires it, but the
captured unit becomes the primary authority. The child executor adopts that
unit instead of creating an independent `expand.DataUnit`.

### 6.3 Component input binding

Component-kind lookup ultimately calls `service/operator.Service.Operate` with
a child `service/session.Session`. Every session construction path used by
component binding must forward the parent invocation context/scope before
`InitKinds` and binding begin.

This path is marked `binding` so its component frame is placed after the
parent's own mutation timeline during semantic flattening.

### 6.4 Imperative handler redirect

Calls made through `handler.Session.Session(...)` after handler execution starts
are marked `imperative`. The child frame is inserted at the exact call marker in
the parent's timeline.

The invocation relation should be a Datly-internal option or frame attribute;
it does not need to become a public `xdatly` transaction API.

### 6.5 Handler-kind parameter locator

`service/executor/handler/locator/handler.go` handles `state.KindHandler`
parameters by constructing a synthetic view, a new executor, and a handler
session during binding. Today that executor has an orphaned data unit and its
buffer is never drained.

This path must receive the parent invocation scope and a reserved binding frame.
The synthetic executor resolves its SQLX facade from the shared unit and does
not independently complete it. Add an audit test covering DML and sequencing
from a handler-kind parameter.

### 6.6 Injector finalizer

`service/operator.Service.finalize` creates child sessions inside the
`InjectorFinalizer` lookup. Its closure must capture/forward the invocation
scope and correct relation instead of relying on transaction-only propagation.

### 6.7 MCP finalizer

`service/operator.Service.finalizeMCPOutput` constructs child sessions for MCP
finalization. Its closure must propagate the same invocation scope. MCP must not
create or complete an independent transaction.

### 6.8 Public HandlerSession handoff

`datly.Service.HandlerSession` delegates to `service/operator.HandlerSession`
and returns a handler session to an external caller without entering `Operate`.
There is no root callback/finally boundary at which Datly can safely complete a
long-lived root unit.

The existing method therefore remains a standalone session boundary. SQLX
operations created through it use explicit flush-scoped completion: a flush
that opens a local transaction commits or rolls it back before returning. It
does not promise atomic buffering across separate external calls.

Add a callback-shaped API separately if callers need root atomicity, for
example:

```go
func (s *Service) UseHandlerSession(
    ctx context.Context,
    component *repository.Component,
    session *session.Session,
    fn func(xhandler.Session) error,
) error
```

That API can create a root scope, run `fn`, and complete in a guaranteed defer.
Do not attach an unfinishable invocation-scoped unit to the existing returned
session.

### 6.9 Executor handler session

`service/executor/handler.Executor.newSession` constructs the
`service/executor/extension.Session` and its `newSqlService` factory.

When an invocation scope exists:

- `newSqlService` resolves a frame-scoped SQLX facade from that unit;
- connector/DB/Tx options select or validate the database unit;
- it must not create an independent mutation buffer; and
- the returned SQLX service uses the shared journal and transaction.

### 6.10 HTTP and protocol entry points

Gateway and protocol adapters continue to call the ordinary Datly operation
entry point. They do not construct data units, choose commit policy, or inject
raw transactions except through an already supported caller-owned session.

### 6.11 Datly 1.0 dispatch

Datly 1.0 must cover the equivalent paths in one flow:

1. `ExecuteRoute` and public exact invocation create a root scope;
2. `Runtime.invokeComponent` accepts and forwards it;
3. binding `componentProvider` derives a binding child frame;
4. `scopedComponentInvoker` derives an imperative child frame;
5. `handlerengine.Request` receives the scope directly;
6. nested engine calls never complete it;
7. output finalization runs while the root scope remains open; and
8. the root engine completes it once after finalization.

Current Datly 1.0 completes its data scope before `finalize`. Phase 4 must
reverse that order so finalizers participate in the same unit and completion
errors are joined with finalization errors correctly.

Tests compare semantic traces from original Datly and Datly 1.0 rather than
allowing the two implementations to drift silently.

## 7. Component Frame and Journal Model

### 7.1 Frame state

Each frame records:

```text
componentFrame
├── stable frame ID
├── component/route identity
├── parent frame
├── relation: root | binding | imperative
├── own operation timeline
├── binding children in deterministic order
├── imperative child markers
└── state: open | sealed | failed
```

Original Datly resolves parameter groups and repeated parameters concurrently.
Binding-child order must therefore not be assigned when a child goroutine
happens to dispatch. Before starting a parameter group, the parent frame
pre-registers binding slots using stable parameter declaration indexes.
Repeated parameters extend that key with their authored repeated-item index.
The child dispatcher attaches to its reserved slot, and semantic flattening
orders slots by that declaration-index path.

An unexpected binding dispatch without a reserved slot is an error. This makes
ordering independent of goroutine scheduling while retaining concurrent value
resolution.

### 7.2 Operation state

Each journal operation records:

- monotonic operation ID;
- owning frame ID;
- database identity;
- kind: insert, update, delete, or raw SQL;
- table where applicable;
- typed payload or copied SQL arguments; and
- queued, reserved, executed, or failed state.

Typed payloads retain current reference semantics. Snapshotting mutable input is
out of scope unless separately approved.

### 7.3 Semantic flattening

Flatten one frame as follows:

1. traverse its own timeline in order;
2. emit ordinary DML operations directly;
3. recursively emit an imperative child at its marker;
4. after the own timeline, recursively emit binding children in reserved
   parameter declaration-index order;
5. batch only contiguous compatible inserts after flattening; and
6. treat update, delete, raw SQL, different table/type, and child boundaries as
   batching barriers unless equivalence is proven.

No foreign-key graph inference is performed. Ordering follows component
semantics and authored imperative call positions.

## 8. Flush Semantics

### 8.1 Causal-prefix flush

`Flush(ctx, table)` changes from table filtering to a causal barrier.

For a non-empty table:

1. compute currently eligible semantic order;
2. locate the last eligible unexecuted operation for the requested table;
3. reserve the complete unexecuted prefix through that operation;
4. execute the prefix in order; and
5. advance the cursor only after the prefix succeeds.

For an empty table, reserve and execute the complete currently eligible prefix.

If no matching table operation exists, return without opening a transaction.

### 8.2 Root-owned flush does not commit

Within a shared root data unit, `Flush` executes into the root transaction but
does not commit it. Only root completion commits or rolls back locally owned
transactions.

This ensures a parent failure after child flush still rolls back the child
work. In original Datly it also fixes the existing uncompleted flush-opened
transaction leak. In Datly 1.0 it removes the current per-flush durable
checkpoint from invocation mode.

### 8.3 Binding child flush

A binding child may request flush before its parent has queued future prerequisite
DML. An immediate child-only flush is unsafe, while silently deferring a method
whose contract promises execution is misleading.

The initial implementation fails closed with a typed ordering error when a
binding child flushes while a required ancestor frame is open.

A child requiring read-after-write must be invoked imperatively after parent
prerequisites are queued, or use a separately designed deferred operation API.

### 8.4 Failure

On execution failure:

- mark the database unit terminally failed;
- reject later queue and flush attempts;
- preserve journal metadata for diagnostics;
- roll back a locally owned transaction at root completion; and
- leave a supplied transaction untouched.

No automatic replay occurs after partial execution failure.

## 9. Transaction Lifecycle

### 9.1 Local transaction

A database unit opens its local transaction lazily on the first operation that
requires database access, including transactional sequence allocation.

Root success:

1. seal root frame;
2. drain all eligible journals;
3. ensure no unit failed;
4. commit locally owned transactions in deterministic first-use order; and
5. call commit observers only after their corresponding commit succeeds.

Root failure:

1. do not execute untouched buffered operations;
2. roll back all opened locally owned transactions; and
3. join rollback failures without hiding the primary error.

### 9.2 Supplied transaction

A supplied transaction:

- is stored by the matching database unit;
- is passed to DML and sequencing;
- receives explicitly flushed and root-drained statements;
- is not committed or rolled back by Datly; and
- does not trigger a Datly-owned commit observer.

### 9.3 Transaction conflicts

For one database identity:

| Existing unit | Child request | Result |
| --- | --- | --- |
| local | no tx | share |
| supplied tx A | no tx | share tx A |
| supplied tx A | tx A | share |
| supplied tx A | tx B | fail |
| local already opened | supplied tx | fail |
| supplied tx | incompatible database | fail |

Conflict detection occurs before sequence allocation or DML append.

### 9.4 Multiple databases

Each physical/logical database gets its own database unit and transaction.
Ordinary SQL transactions cannot make commits across databases atomic.

The first implementation should:

- roll back every locally owned unit if failure occurs before commit begins;
- commit in deterministic first-use order;
- report that a later commit failure may leave an earlier database committed;
- optionally reject multiple locally owned writable databases under a strict
  atomicity policy; and
- explicitly guarantee the campaign/flight case only when both use the same
  database identity.

## 10. Transactional Sequencing

### 10.1 Required change

Allocation must resolve through the database unit:

```go
func (u *databaseUnit) Allocate(
    ctx context.Context,
    table string,
    dest any,
    selector string,
) error {
    tx, err := u.transaction(ctx)
    if err != nil {
        return err
    }
    return u.sequencer.Allocate(ctx, tx, table, dest, selector)
}
```

SQLX must receive transaction authority explicitly:

```go
inserter.NextSequence(ctx, record, count, tx, strategy)
```

This ensures parent and child allocation share the root transaction and that a
supplied transaction reaches sequencing.

### 10.2 Transient SQLX strategy

`PresetIDWithTransientTransaction` explicitly begins another transaction.
Passing `tx` while retaining that strategy does not satisfy this design.

Required SQLX rule:

- when the data unit supplies a transaction, select or implement a strategy
  that performs sequence work through that transaction and never completes it;
- use transient reservation only when explicitly requested as an independent
  allocation policy; and
- add database-specific SQLX tests for every changed strategy.

Do not silently claim transaction propagation while a downstream strategy
ignores the supplied transaction.

### 10.3 Policy consequence

Transactional allocation can reuse identifiers following rollback, depending
on database behavior. Independent transient reservation can leave gaps. The
shared root data unit defaults to transaction-consistent allocation; independent
reservation remains explicit rather than hidden.

Using the root transaction can hold sequence-table or metadata locks until root
completion, materially longer than a transient allocator. SQLX implementation
and database-specific tests must measure this lifetime. Add two concurrent root
transactions allocating from the same sequence source and prove bounded
blocking, cancellation, and deadlock handling. Where a database's sequence
primitive is intentionally non-transactional, document that exception instead
of pretending the shared transaction controls it.

## 11. Database Identity and Resolution

The root data unit needs a comparable database identity before opening a local
transaction.

Resolution precedence:

1. explicitly selected connector identity plus resolved `*sql.DB`;
2. explicitly supplied `*sql.DB` identity;
3. transaction-associated database unit already registered by the parent; and
4. the component view's main connector.

The physical `*sql.DB` pointer is the final equality authority when available.
Connector name alone is insufficient because names can be scoped differently;
two connector names resolving to the same pool may share, while identical names
resolving to different pools must not.

An explicit `*sql.Tx` cannot reveal its originating `*sql.DB`. It must be
registered together with the database identity at root/session construction.

## 12. Concurrency

Required synchronization:

- frame creation and sealing;
- operation append and monotonic ID assignment;
- database-unit lookup/creation;
- transaction initialization;
- flush reservation;
- execution cursor publication;
- failure publication; and
- one-time root completion.

Do not hold the journal lock during SQL execution. Reserve an immutable prefix,
release the lock, execute serially per database unit, then atomically publish
success or failure.

Operations appended concurrently after prefix reservation remain queued for a
later flush/root drain. Concurrent append order is lock-acquisition order; no
stronger business ordering is promised across independent goroutines.

`expand.Statements` currently has no synchronization, and original component
locator construction uses a package-level mutable dispatcher fallback. The
implementation must route all statement mutation through the synchronized
journal and remove or synchronize the global dispatcher fallback; per-runtime
dispatcher authority is preferred to mutable package state.

Original allocation paths use `context.Background()` and the Velty-facing
`DataUnit.Allocate` method has no context parameter. A frame-scoped facade must
capture the active invocation context when created. Template allocation uses
that captured context for transaction start and SQLX calls. Explicit handler
methods continue to use their supplied context. Root cancellation terminates
new allocation/flush work; queued context-free DML is executed with the root
completion context. Do not replace a cancelled request with background context.

## 13. Backward Compatibility

### 13.1 No required xdatly session change

The preferred design leaves original `handler.Session.Session` and
`handler/state.Option` unchanged. Existing callers do not need to know that a
root data unit exists, and handlers cannot accidentally omit its propagation.

`state.WithSqlTx` remains supported as root input. During transition:

- supplied tx at a root: construct the root database unit around it;
- supplied tx at a child: validate it agrees with the propagated unit;
- no tx: lazily create a locally owned transaction; and
- nested dispatch: use the dispatcher-carried unit regardless of public options.

If a proven session handoff cannot retain dispatcher scope, an additive xdatly
option may be introduced in a separately reviewed fallback slice.

### 13.2 Standalone SQLX compatibility

Direct SQLX services outside a shared root unit retain their public signatures.
Their local flush lifecycle must be made explicit and correct: a flush that
opens a standalone local transaction commits on success and rolls back on
failure. This corrects original Datly's leaked flush transaction and matches the
already intended standalone/checkpoint behavior in Datly 1.0.

### 13.3 Explicit transaction mode

If both behaviors must coexist, use explicit terminology:

```go
type TransactionMode string

const (
    TransactionModeInvocation TransactionMode = "invocation"
    TransactionModeFlush      TransactionMode = "flush"
)
```

Invocation mode is the shared root unit. Flush mode preserves a transaction
completed by a flush that opened it. Modes cannot be mixed within one database
unit.

The mode is declared at root component/route registration or Datly service
configuration before the first frame or operation is created. It is not a
handler `Session.Db()` option and cannot be changed by a child. The standalone
`Service.HandlerSession` handoff uses flush mode because it has no root
completion owner; ordinary component operations default to invocation mode.

Do not use `legacy` in production names.

### 13.4 Intentional correction

In invocation mode, explicit child flush is not a durable commit. This is
necessary for root atomicity. Datly 1.0 applications depending on its current
durable checkpoints must select root-configured flush mode or split the work
into separate root invocations. Original Datly does not have a working durable
flush baseline to preserve; its currently leaked transaction is corrected.

## 14. Error Model and Observability

Use typed errors for:

- conflicting transactions for one database;
- unknown transaction/database association;
- invalid database identity;
- flush from an unsealed binding child;
- operation after terminal failure;
- repeated root completion;
- expired/captured invocation scope reuse;
- missing or duplicate reserved binding slot;
- transaction-mode conflict; and
- strict-atomicity multi-database rejection.

Errors include safe component, frame, database, operation, and table identities.
They must not log SQL argument values, credentials, or connection strings.

Tracing should make these events visible:

- root unit creation/adoption;
- child unit propagation;
- frame relation and ordering;
- database-unit reuse/creation;
- local versus supplied transaction;
- sequence allocation transaction identity;
- operation queue/reserve/execute; and
- root commit/rollback outcome.

## 15. Implementation Phases

### Phase 1: Original-Datly root data unit and ordered journal

Target original-Datly files/packages:

- `service/executor/expand/DataUnit` and statement storage, or a focused
  executor-owned replacement that reuses its operation model;
- `service/executor/handler` SQLX service;
- `service/executor` execution planner and transaction boundary; and
- new cohesive frame/journal files under the executor owner.

Work:

1. introduce root and database units;
2. add component frames and semantic flattening;
3. preserve contiguous compatible insert batching;
4. add causal-prefix flush;
5. separate flush from root completion; and
6. add terminal failure state.

Exit gate: focused executor tests pass without component dispatch changes.

### Phase 2: Transactional sequencing

Target files:

- `service/executor/sequencer/service.go`
- `service/executor/handler/sqlx.go`
- `service/executor/expand/data_unit.go`
- relevant original `viant/sqlx` sequence implementation

Work:

1. pass the data-unit transaction to sequence allocation;
2. remove implicit transient allocation in invocation mode;
3. implement/select a SQLX transaction-honoring strategy;
4. retain explicit transient standalone policy; and
5. test rollback/visibility behavior.

Exit gate: a test proves allocation and insert use the exact same transaction.

### Phase 3: Propagate through every original-Datly child-session path

Target files:

- `service/operator/service.go`
- `service.go` and `service/operator/executor.go` public `HandlerSession` path
- `service/session/option.go`
- `service/session/state.go` where child sessions clone options
- `service/executor/handler/executor.go`
- `service/executor/extension/session.go`
- `service/executor/handler/locator/handler.go`
- `repository/locator/component/dispatcher/disptacher.go`
- `repository/locator/component/component.go`

Work:

1. create/adopt the root invocation scope in `Operate`;
2. capture the scope in handler redirects;
3. propagate it through component binding;
4. propagate through injector finalizers;
5. propagate through MCP finalizers;
6. mark binding versus imperative relations;
7. pre-register deterministic binding slots before concurrent resolution;
8. adopt the unit in every child and handler-kind executor;
9. define standalone completion for public returned `HandlerSession` values;
10. remove/guard the package-global dispatcher fallback; and
11. complete only at the outermost owner.

Exit gate: root/child/grandchild identity tests cover every dispatch path.

### Phase 4: Datly 1.0 adapter

Target Datly 1.0 files:

- `runtime/dispatch.go`
- `runtime/runtime_facade.go`
- `runtime/component.go`
- `runtime/handler/engine/engine.go`
- `runtime/handler/engine/data_scope.go` or its focused replacement
- `sql/dml` transaction, journal, and sequencer integration

Work:

1. create the same root invocation scope at route/exact roots;
2. pass it explicitly through `Runtime.invokeComponent`;
3. distinguish binding and imperative child relations;
4. project frame-scoped existing handler capabilities;
5. implement the same causal-prefix and root-completion semantics;
6. pass the transaction to the Datly 1.0 sequencer; and
7. move root data completion after output finalization;
8. add parity trace tests against the original behavior contract.

Do not modify `xdatly_1` for this feature.

### Phase 5: Compatibility and cleanup

Work:

1. adapt tx-only child sessions;
2. retain standalone flush mode;
3. reject data-unit/tx conflicts;
4. remove transaction-only shortcuts that bypass unit propagation;
5. document the intentional child-flush behavior correction; and
6. add architecture tests preventing handler-visible completion controls.

### Phase 6: Independent review and verification

Run focused tests, full tests, vet, race tests, database-specific SQLX tests,
cross-implementation semantic trace tests, and a bounded review covering
correctness, ordering, transaction ownership, sequencing, dispatcher
propagation, compatibility, concurrency, and package ownership.

## 16. Test Plan

### 16.1 Dispatcher-scope tests

1. a root operation creates exactly one invocation scope;
2. nested `Operate` adopts it and never completes it;
3. handler redirect captures it without a public option;
4. component binding forwards it with relation `binding`;
5. imperative redirect forwards it with relation `imperative`;
6. handler-kind parameter lookup adopts it with relation `binding`;
7. finalizer and MCP finalizer closures retain it;
8. public returned `HandlerSession` uses defined standalone completion rather
   than an unfinishable root unit;
9. a completed scope at a new root creates a fresh scope;
10. a captured nested session used after completion fails closed;
11. missing scope inside a nested dispatch fails rather than opening a new unit;
12. existing xdatly session implementations require no new method; and
13. a root `state.WithSqlTx` is adopted and child transaction conflicts fail.

### 16.2 Journal unit tests

1. parent operations precede binding children;
2. imperative children remain at exact markers;
3. concurrently resolved binding siblings preserve declaration-index order;
4. nested binding and imperative frames flatten deterministically;
5. inserts batch only when contiguous and compatible;
6. update/delete/raw SQL remain ordered barriers;
7. table flush executes a causal prefix;
8. no-match flush does nothing;
9. repeated flush does not replay;
10. failed execution marks terminal state;
11. local completion commits once; and
12. local failure rolls back partial execution.

### 16.3 SQLite campaign/flight integration

Enable foreign keys and create:

```sql
CREATE TABLE campaign (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE campaign_flight (
    id INTEGER PRIMARY KEY,
    campaign_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    FOREIGN KEY (campaign_id) REFERENCES campaign(id)
);
```

Cases:

1. flight is invoked through campaign component binding;
2. flight handler runs before the parent mutation body;
3. both receive the exact same root data unit and database unit;
4. semantic execution inserts campaign before flight;
5. success commits both rows;
6. parent failure leaves both tables empty;
7. child failure leaves both tables empty;
8. explicit child flush does not commit independently;
9. unsafe binding-child flush returns the typed ordering error;
10. sequencing receives the root transaction;
11. allocation plus inserts roll back together;
12. two concurrent sequence allocators obey cancellation and do not deadlock;
13. raw SQL participates in causal-prefix order; and
14. an operation failing after reservation is not marked successfully executed.

### 16.4 Supplied transaction

1. root and child DML become visible inside the supplied transaction;
2. sequencing receives the exact supplied transaction;
3. Datly success leaves it open;
4. Datly failure leaves it open for owner rollback;
5. owner commit persists all work;
6. owner rollback removes all work; and
7. conflicting child transaction fails before allocation or append.

### 16.5 Dispatch coverage

Verify exact unit propagation through:

- root `Service.Operate`;
- component input binding;
- public handler `Session.Session` redirect;
- `state.KindHandler` parameter locator;
- public `datly.Service.HandlerSession` standalone handoff;
- injector finalizer lookup;
- MCP finalizer lookup;
- child/grandchild chains;
- connector-specific `Session.Db` calls; and
- incoming caller-supplied sessions.

Run the equivalent dispatch matrix in Datly 1.0 through route dispatch,
`componentProvider`, `scopedComponentInvoker`, bound input, and root exact
invocation.

### 16.6 Cross-implementation parity

Record a small semantic trace containing frame relation, operation kind/table,
database-unit identity class, flush boundary, transaction ownership, and
completion outcome. For equivalent component graphs, original Datly and Datly
1.0 must produce the same ordering and ownership trace.

### 16.7 Database isolation

1. same `*sql.DB` through two connectors resolves one database unit;
2. different `*sql.DB` values never share journal/transaction;
3. same database with conflicting supplied transactions fails;
4. tx without known database association fails closed;
5. transaction modes cannot conflict; and
6. multi-database completion follows configured atomicity policy.

### 16.8 Race coverage

Run focused `-race` tests for:

- concurrent append;
- append during reserved flush;
- concurrent binding siblings;
- concurrent flush attempts;
- failure racing with append; and
- repeated completion.

## 17. Acceptance Gates

Implementation is complete only when:

1. original Datly's dispatcher propagates one invocation scope through every
   child-session construction path without requiring a public xdatly option;
2. original xdatly session interfaces remain source-compatible;
3. campaign/flight succeeds with enforced SQLite foreign keys;
4. parent or child failure rolls back all same-database local work;
5. supplied transactions remain entirely caller-owned;
6. sequencing demonstrably receives the shared transaction;
7. table flush cannot skip causal predecessors;
8. standalone behavior remains covered;
9. existing public session method signatures remain unchanged;
10. Datly 1.0 supports the same flow without modifying `xdatly_1`;
11. cross-implementation ordering/ownership traces agree;
12. focused race tests pass;
13. full original Datly, original xdatly, and Datly 1.0 tests/vet pass;
14. required SQLX tests/vet pass; and
15. independent review has no unresolved correctness or architecture findings.

## 18. Non-Goals

- exposing root completion to handlers;
- adding handler commit or rollback;
- distributed transaction coordination;
- automatic replay after execution failure;
- arbitrary foreign-key graph inference;
- silently deferring explicit flush;
- replacing the existing handler session API; or
- modifying `xdatly_1`.

## 19. Independent Review Record

Review command: Claude CLI with `claude-fable-5`, high effort, and a bounded
prompt covering only this plan and its cited original Datly, original xdatly,
SQLX, and Datly 1.0 dispatch/sequencer files.

Initial implementation review result: `REQUEST_CHANGES`. Its findings covered
flush/completion races, post-completion access, identity-based discard,
transaction conflict/access rules, injector scope capture, declaration-order
reservation, stale-scope behavior, Datly 1.0 database identity, marker
reconciliation, and missing concurrency/integration coverage. Those findings
were corrected and covered by focused tests.

The first broad re-review CLI invocation stalled without a result and was
stopped after ten minutes. A bounded file-specific re-review then returned
`REQUEST_CHANGES` for three remaining issues:

- an imperative descendant could bypass the open-binding flush guard;
- original Executor maps had unlocked read paths; and
- `Reconcile` could race a reserved operation.

All three were corrected. The final bounded Claude Fable 5 re-review inspected
the affected original-Datly and Datly 1.0 files and returned `APPROVE`,
confirming the ancestor-aware binding guard, `unitMu` map snapshots/accessors,
and `flushMu`-serialized reconciliation without regressions.

## 20. Implementation Verification Record

Implemented on 2026-07-21:

- original Datly owns a private context-carried root unit under
  service/executor/uow, with component frames, binding/imperative ordering,
  per-database transactions, causal-prefix flush, terminal failure, contiguous
  batching, stale-scope rejection, and deterministic completion;
- original component binding, handler redirect, handler-kind lookup, injector
  finalizer, MCP finalizer, and public handler-session paths use that context
  carrier without an xdatly data-unit option; captured dispatcher closures
  transplant the private carrier onto caller contexts without losing caller
  cancellation;
- original sequencing uses the database-unit transaction and switches away
  from SQLx's independently completed transient strategy in invocation mode;
- Datly 1.0 extends its existing engine data scope with frame-scoped private
  data views, root completion after output finalization, transactional
  sequencing, same-database sharing, and separate root-owned database units;
- completion, explicit flush, sequencing, and reconciliation are serialized;
  imperative descendants of open binding components cannot bypass the typed
  flush-ordering error, and sealed frames reject late writes;
- neither original xdatly nor xdatly_1 public session interfaces gained a
  concrete data-unit or transaction completion API.

Passing focused verification:

- original executor, UOW, sequencing, session, operator, and component-locator
  tests;
- original focused UOW/expand/session race tests;
- Datly 1.0 SQL DML and handler-engine tests, including foreign keys,
  parent/binding/imperative order, child flush rollback, external transaction,
  finalizer failure, sequencing rollback, concurrent append, and
  multi-database completion;
- Datly 1.0 focused DML/handler-engine race tests;
- original xdatly's complete test suite, unchanged and source-compatible;
- final bounded Claude Fable 5 review: `APPROVE`.

Repository-wide limitations observed during this implementation:

- original Datly's full suite reaches unrelated existing failures in command
  transcription, shape generation, reader grouping, view collection, and
  column discovery; the changed executor/session packages pass;
- Datly 1.0's normal full build is blocked by incomplete untracked work in the
  sibling SQLx checkout (CompositeInRenderer, column scan metadata, and cache
  matcher fields). Focused verification temporarily excluded only those
  unrelated untracked files and restored them after each run.
