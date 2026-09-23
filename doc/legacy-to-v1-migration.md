# Migrating legacy Datly components to Datly 1.0

Use this guide when an application has legacy Datly handlers, handwritten SQL
services, or manually registered components and the target is a Datly 1.0
generated component architecture.

This guide applies to migrations with the following hard boundary:

- Every database read starts as a DQL graph and is generated with
  `datly transcribe get`.
- Every database mutation starts as a DQL graph and is generated with the
  matching `datly transcribe post`, `put`, or `patch` operation.
- Application-owned Go may implement supported generated hooks. It must not
  replace generated binding, Previous/current reads, presence, validation,
  sequencing, relation reconciliation, DML queueing, or transaction ownership.
- If the connected Datly build cannot express required behavior, preserve the
  application contract and close the gap in Datly's reader/writer
  transcription or shared runtime. Do not hide the gap behind raw SQL or a
  legacy custom handler.
- Every UPDATE action is sparse. PATCH, PUT, internal state transitions, and
  mixed insert/update graphs may persist only fields whose generated presence
  markers are true. A complete Go row, a loaded Previous row, or a server-side
  default never authorizes a full-row overwrite of an existing record.

Read the reader or writer skill and its contract references alongside this
guide. This document explains migration decisions; those references remain the
authority for the resulting component.

## Legacy shapes that must be removed

Treat each of these as a migration target rather than a reusable 1.0 pattern:

- `Exec(ctx, handler.Session) (any, error)` handlers.
- `Service.AddHandler`, `repository.WithContract`, reflection-based input/output
  registration, or a handwritten component `index.go`.
- `sess.Stater().Bind`, `sess.Db()`, handwritten `Insert`/`Update`/`Delete`
  loops, or direct `*sql.DB`/`*sql.Tx` access in application behavior.
- Handwritten body/current/Previous/output plumbing that transcription owns.
- HTTP handlers or finalizers that perform database reads or writes outside a
  generated component.
- A custom 1.0 `handler.Contract[I,O]` used merely to preserve a legacy DAO or
  handwritten mutation. A custom component may orchestrate external systems or
  invoke generated components, but under this migration policy it does not own
  database access.

Infrastructure that opens a connector, applies schema migrations, or seeds an
isolated test fixture is not product data access. Keep that boundary narrow and
do not let infrastructure helpers become application repositories.

## Inventory before redesign

Build an inventory of observable operations, not just SQL files. For every
route, scheduled job, MCP tool, and internal caller record:

- public method/path and input/output types;
- every table read or changed;
- authorization and tenant/parent scope;
- identity and complete composite keys;
- optional-field presence semantics;
- transaction and graph ordering boundary;
- generated IDs and parent/self links;
- validation and error/status behavior;
- post-commit work and external side effects;
- existing SQLite/runtime coverage.

Search for both raw SQL and legacy Datly APIs. Useful starting terms include
`database/sql`, `*sql.DB`, `*sql.Tx`, `QueryContext`, `ExecContext`,
`AddHandler`, `handler.Session`, `sess.Stater()`, and `sess.Db()`.

Do not migrate one SQL statement at a time when several statements implement
one business event. The migration unit is the complete read contract or atomic
mutation graph.

## Select the generated operation

| Existing behavior | Datly 1.0 target |
| --- | --- |
| Query, lookup, list, aggregate, or authorized projection | `transcribe get` reader |
| Insert-only contract | `transcribe post` writer |
| Update-only contract; missing row is not an insert | `transcribe put` writer |
| Mixed insert/update graph selected from authorized Previous state | `transcribe patch` writer |
| Explicit row deletion inside PATCH/PUT | Writer graph with an authored `delete_marker` |
| Read transformation | Generated row `OnFetch`/`OnRelation` hook |
| Mutation defaults or business validation | DQL-declared `lifecycle_type` and generated create-once lifecycle hook |
| Multi-step database business event | One generated writer graph with all writable relations and ordered entities |

Do not choose the operation from the HTTP verb alone. Preserve the existing
missing-row, sparse-update, deletion, and identity policy explicitly.

## Rebuild a legacy reader

1. Define the public input and output contract, including exact parameter
   sources, nullability, pagination, selectors, and authorized scope.
2. Author a separate reader DQL file with required `#package`, `input_type`,
   `output_type`, root `type(...)`, a named typed output holder, connector and
   route settings, and global `case_format('lc')`.
3. Make the projection field-driven: list the fields the caller contract
   consumes instead of selecting a physical row with `table.*`. A projection
   is an application contract, not a mirror of the current schema. This keeps
   generated shapes stable when unrelated columns are added, makes nullable
   and sensitive fields deliberate, and lets review prove which data leaves
   each relation. Use a whole-row projection only when the public contract
   intentionally is the whole row.
4. Put all joins, derived views, complete composite relations, and mandatory
   authorization restrictions in the declarative graph. Bind client values;
   never interpolate them.
5. Use typed predicates for caller-dependent authorization. Verified JWT input
   is explicit; query/header viewer IDs do not become authority by convention.
6. Move row transformations to `OnFetch`, and complete-relation transformations
   to `OnRelation`. Hooks do not query the database.
7. Run `datly transcribe get` against the application package. Inspect generated
   types, SQL resources, selectors, routes, and authored-code preservation.
8. Replace legacy callers with the generated component contract, then remove
   the old SQL service only after parity tests pass.

Aggregates assembled from several legacy service calls should normally become
one reader graph with related and DerivedView outputs. Do not preserve a
service-local fan-out of raw SELECTs merely because the public response is an
aggregate.

When several callers aggregate the same facts with different dimensions,
measures, or windows, prefer one authorized groupable reader with a derived
cube and, where needed, cube composition. Enable MCP cube/compose exposure only
when the source contract enforces the same identity and authorization policy
for those tools. A fixed dashboard graph can still compose cube results with
other relations; do not duplicate near-identical aggregate SQL in each
dashboard reader. Check the connected build's cube activation shape before
rewriting a correlated or joined aggregate: a reader that compiles normally
does not automatically qualify as a cube source.

## Rebuild a legacy writer

1. Identify the entire atomic business event. Include every insert, update, and
   explicit delete that must commit or roll back together.
2. Model the request aggregate as a typed DQL graph. Declare writable views and
   complete parent/child/self links. Parenthesize physical tables that are
   strictly auxiliary read state so transcription excludes them from DML.
3. Preserve authorized current/Previous scope in the DQL. Include every field
   required for identity resolution, invariant backfill, validation, and
   update-versus-insert decisions.
4. Declare `lifecycle_type` only for views that need application rules. Put
   defaults and marker-aware changes in generated lifecycle `Init`; put
   read-only business checks in `Validate`; use `AfterSequence` only for work
   requiring allocated identities; keep `AfterQueue` observational.
   Lifecycle code must use generated setters for intentional update changes;
   direct assignment does not create update presence.
5. Declare invariant groups, validation tags, concurrency tokens, and explicit
   deletion markers in DQL. Do not recreate these phases in a handler.
6. Select `post`, `put`, or `patch`, then run the matching `datly transcribe`
   command. The generator owns Original/presence capture, typed Previous reads,
   indexes, sequencing, diff, reconciliation, queueing, output, and outcome.
7. Verify that the generated graph retains the original transaction boundary
   and deterministic parent-before-child ordering (with required deletion
   ordering). Generated writer Current/Previous and auxiliary reads run after
   the managed transaction begins, so their evidence and queued writes share
   one database unit. A sequence of independent component calls is not
   equivalent to one atomic graph.
8. Replace the legacy route/caller and delete the old `sess.Db()` or raw SQL
   implementation only after SQLite and protocol parity pass.

For a legacy handler that manually indexes `Cur*` rows, model those rows as
authorized Previous or auxiliary views and use generated typed read indexes.
Do not carry forward unexported maps or repeated service-local SELECT loops.

For writer relations, declare every real equality key in the DQL join. Use
`AND 1=1` as the to-one shortcut only when the linked data is actually unique
for that parent; do not also declare `cardinality(child, 'One')`. Keep explicit
root cardinality where needed. If an account or learner can own multiple rows,
model a collection instead of forcing a to-one hint onto a broad foreign-key
join. A Current lookup returning multiple rows for a to-one holder is usually
evidence of an incomplete relation key or false cardinality, not a reason to
silence the reader.

When a new binding row references other new rows in the same atomic graph,
declare those producer rows as earlier writable siblings rather than nesting
them beneath the binding. A derived parent may project logical transient keys
for their typed relations while the physical foreign keys remain on the
binding. Resolve a request-supplied transient sibling key in generated input
`Init`, before Datly freezes Previous matching; entity `Init` is too late for
an existing child's parent-scope check. Keep the transient key non-DML and use
generated setters only for intentional working presence. Prove first insert,
idempotent re-import, unchanged sparse fields, and late-error rollback with a
runtime test before replacing the legacy transaction.

For an importer that must preserve operator-curated fields, compile a fresh
generated PATCH graph for each invocation and set only importer-owned columns.
Do not mark `status`, `created_at`, optional hints, or similar fields present
just to satisfy an insert. In the lifecycle hook, inspect typed Previous: use
a generated setter to supply a required value only when the row is new; for an
existing row, carry its Previous value into the working entity without setting
the presence marker when validation needs to see it. Otherwise a re-import can
reset a curated status or hint even when the authored manifest did not change.
Test insert and re-import after independently editing those fields, and assert
the exact keyed row counts across every writable relation.

### External work between database mutations

If a request calls an external provider, a durable lease can be its own generated
PATCH component. Acquire the lease before the provider call, save the prepared
provider result after it succeeds, and allow a failed call to release the lease
for retry. The final generated writer graph must include both the business row
and the lease's committed transition so they commit or roll back together. A
separate post-write lease update can leave a completed business row with a
retryable lease and repeat the provider call.

Check an existing request key before applying a target row's active-status
gate: an idempotent replay may arrive after the first request completed that
row. Return the saved response only after the generated read confirms the
viewer's scope. Recheck the target's mutable status in the final writer's
transaction so a slower provider result cannot overwrite a request that won
while the external call was in flight. Preserve the lease owner and expiry as
sparse update fields; do not reuse a prepared result from a different owner.

For reader relations, keep request predicates on the root whenever the child
is scoped by the declared `ON` keys. Datly loads relations in parent-key
batches; a child SQL source should therefore expose the link columns and let
the relation loader add its composite-key restriction. Do not repeat root path
parameters inside child SQL as a substitute for the typed relation. Preserve
per-parent ordering in the child source. When a per-parent top-N limit is
required, model it with a relation-safe ranked source or a generated read hook,
and prove multiple-parent batching so a global SQL `LIMIT` cannot truncate a
different parent's rows.

## Generated and authored file ownership

- Change DQL, imported authoritative Go shapes, or Datly itself; do not patch
  generated plumbing.
- Keep application behavior in create-once lifecycle/read hook files selected
  by the DQL contract.
- Preserve generated manifests/fingerprints and allow transcription to reject
  conflicts with authored edits.
- Use prefix-free default filenames unless collision or an explicit destination
  requires a declared override.
- Link selected generated packages through the Datly 1.0 project build policy.
  Do not restore per-component `AddHandler` registration or side-effectful
  `init()` functions.

## When Datly must be extended

Stop and record a framework gap when the required contract survives design but
the connected `transcribe` operation cannot generate or execute it. A parser
success, lower-level translation, or handwritten component is not a substitute.

Classify the gap before changing Datly:

- **Reader transcription:** missing typed relation, selector, predicate,
  DerivedView, hook, codec, or output support.
- **Writer transcription:** missing graph metadata, Previous projection,
  presence, identity, relation, deletion, validation, sequencing, output, or
  lifecycle generation.
- **Shared runtime:** generated metadata is correct but execution violates
  transaction, authorization, binding, validation, or output semantics.
- **Project build/linking:** generated packages compile but are not discovered,
  linked, exposed, or resource-backed according to the 1.0 build contract.

Fix the smallest authoritative layer. Add a native Datly fixture that proves the
generic capability, then keep an application SQLite regression proving the
application-style behavior. Do not add application-specific table names or
business rules to Datly.

### Composite snake_case Current keys

A generated PATCH Current query must apply `CompositeIn` using the SQL columns
projected by its derived table, not Go field names from the key helper. For an
identity such as `tenant_id,record_id`, criteria must address
`r.tenant_id,r.record_id`; `r.TenantId,r.RecordId` is invalid SQL. If binding
reports a missing CamelCase column in generated Current SQL, treat it as a
transcriber projection defect. Fix key projection/criteria generation and add a
native composite-key fixture; do not patch generated SQL or rename schema
columns to accommodate it.

### Body-key auxiliary state for a new mutation root

Some legacy transactions insert a new root while also inspecting or updating an
existing row selected by non-identity request fields. For example, a request may
insert a new record while transitioning the current record for the same
`tenant_id,owner_id` tuple. The new root identity cannot drive that Current read:
generated Current matches supplied root identities, and a child Current relation
normally derives its parent keys from matched Current parents.

Do not split this contract into an external reader call followed by a writer
call. That loses the original transaction boundary and permits the selected
current row to change between observation and mutation. The writer needs a
transactional auxiliary read keyed from captured body fields, with its evidence
available to lifecycle validation and with any intentionally supplied transition
row participating in the same sparse write graph.

Datly v1 supports this as an auxiliary root with writable descendants. The
auxiliary ancestor receives typed Current, presence, invariant and lifecycle
state but never emits DML; generated traversal continues into explicitly
writable child views. Lifecycle setters on an existing child create sparse
update presence after Current matching, while a missing child remains an insert.
This is materially stronger than a legacy read-then-write service split because
the auxiliary read, child classification, validation and DML share the writer's
managed transaction. If a connected older generator still propagates auxiliary
status to descendants or skips the auxiliary root traversal, classify that as a
writer/runtime version gap and preserve the legacy transaction.

Keep the auxiliary scope source distinct from any writable descendant that
targets the same physical table. If a derived auxiliary root is based on
`(record)` and the graph also joins writable `record records`, both views share
one physical table identity and Current classification can collapse them into
an incomplete or ambiguous writer row. Base the auxiliary scope on a different
authorizing table (or a purpose-built read-only scope) and join the writable
record as an ordinary descendant. Verify that transcription emits a separate
`Current<Record>` projection, then exercise the graph against SQLite. This is a
graph-modeling issue, not a reason to patch generated Current SQL or abandon the
single transaction.

### Replacement collections and explicit deletion

Legacy code often implements replacement semantics with `DELETE ... WHERE
parent_id = ?` followed by inserts. A PATCH graph must not infer deletion merely
because an existing child is absent from the request. Project a logical boolean
field, declare it with `delete_marker`, and have the root lifecycle append
identity-only deletion rows for Previous children that are no longer desired.
Use the generated deletion setter: it sets both the logical flag and its presence
marker. Directly assigning the boolean bypasses the generated mutation contract.
Deletion-marker fields are control metadata, not physical UPDATE columns.

### Transient relation keys

A derived writer projection may need a key only to connect an auxiliary scope to
a writable descendant. Declare that field `sqlx:"-"` and use it in the typed
relation `ON` mapping. Datly retains it in immutable writer metadata for relation
resolution and Current copying while excluding it from INSERT and UPDATE DML.
This permits a graph to reuse a projected lookup key without inventing a
physical column or reparsing tags at execution time. If writer registration says
that such a typed link does not resolve, update Datly rather than making the
projection key writable.

When the body may explicitly supply such a parent link, generated presence
metadata includes that transient relation field even though it is excluded from
DML. This lets Current lookup distinguish an authored request value from a
fallback copied from Previous without exposing the marker in public JSON.

The generated Current carrier maps that transient field to its projected SQL
alias while retaining mapping options such as `refTable`, `refColumn`, and
`required`. A tag like `sqlx:"-,refTable=parent,refColumn=id"` therefore becomes
`sqlx:"parent_scope,refTable=parent,refColumn=id"` on Current only. If SQL
returns the alias but read provenance reports the field as unloaded, treat it as
a Current-tag generation defect rather than removing the transient mapping.

Current projections commonly represent a nullable physical child key as a Go
pointer while an auxiliary request-derived parent key is a value. Previous graph
assembly compares their dereferenced typed values, so `string` and `*string`
representations of the same key still attach the child. Do not weaken the DQL
relation or make a transient parent field physical merely to force identical Go
pointer shapes.

A sparse update may also set a foreign key to a row inserted earlier in the same
ordered mutation graph—for example, closing an existing reservation with the ID
of a newly inserted event. Datly recognizes that exact earlier insert, avoids a
redundant pre-write reference lookup for only the proven update field, and leaves
the database foreign key to enforce the value inside the managed transaction.
This is not permission to suppress unrelated update validation or to reference a
later or unordered insert.

## Repeatable generation with an orchestrator

An application may use Endly or another task runner to regenerate many
components. The task runner orchestrates the same operation-based CLI; it does
not become a second generator or a place for database business logic.

- Enumerate one DQL source package per component and select `get`, `post`,
  `put`, or `patch` from its declared operation, not its directory name.
- Build or pin the Datly 1.0 CLI before regeneration. Use a disposable schema
  database for metadata discovery when possible; do not embed live credentials
  in generated files or task logs.
- Run with the merge generation policy so create-once authored hooks survive.
  A conflict in an owned shape is a migration failure to investigate, not a
  reason to overwrite the generated package wholesale.
- After generation, compile every generated package, run real component tests,
  and inspect any owned-file changes. A successful CLI exit alone does not
  prove authorization, sparse presence, or transaction behavior.

When newer generator metadata changes an owned relation tag, compare the DQL
proposal, the checked-in field, and the recorded generation manifest. Datly may
update its own join and JSON presentation tags only when the destination still
matches that trusted baseline; an authored relation edit remains protected.
Keep the corrected policy in Datly with a native regeneration test rather than
hand-editing every generated view field in the application.

Merge regeneration conservatively retains historical SQL resources for old
shape references. When a relation is intentionally retired, audit that package's
authored code and run a targeted `-generation-policy overwrite` regeneration
only after its replacement component passes parity tests. Overwrite removes
obsolete generated shapes and resources, but still rejects removal of a
resource whose bytes differ from its trusted fingerprint. Do not use it to
discard edited hooks or bypass an unexplained ownership conflict.

## Verification gates

A migrated component is complete only when all applicable gates pass:

- the high-level operation-based `transcribe` command succeeds;
- generated Go and embedded resources compile without legacy handler APIs;
- regeneration preserves authored hook code and produces no unmanaged diff;
- SQLite runtime tests exercise the generated component, not a direct SQL
  surrogate;
- readers cover empty/single/multiple rows, NULLs, authorization, complete
  composite keys, selectors, and relation batching where used;
- writers cover insert, update, mixed graphs, omitted versus explicit
  null/zero/false/empty, duplicate/partial identity, rollback, ordering,
  generated IDs/links, authorization failure, and repeat/idempotent behavior;
- update assertions prove that every omitted column remains byte-for-byte
  unchanged in storage, including PATCH, PUT, and server-driven transitions;
- HTTP and MCP tests assert the real generated route/tool status and body;
- repository searches find no product `database/sql`, legacy `AddHandler`,
  `handler.Session` execution, `sess.Stater()`, or `sess.Db()` path for the
  migrated slice.

Direct SQL is acceptable in tests only for schema setup, seed fixtures, or
narrow fixture inspection. It must not be the primary behavioral proof.

## Migration order

Prefer vertical slices that can remove a legacy path completely:

1. Establish a compiling Datly 1.0 application/toolchain baseline and one
   generated reader plus one generated writer canary.
2. Migrate simple leaf readers and insert/update components to validate project
   linking, route exposure, resources, and test harnesses.
3. Migrate authorization/current-state readers used by several writers.
4. Migrate atomic domain graphs, preserving their original transaction and
   side-effect boundaries.
5. Migrate aggregate/dashboard readers after their leaf contracts stabilize.
6. Remove legacy runtime registration, raw service wiring, and `*sql.DB`
   dependencies only after all callers have moved.

Keep both implementations only behind a temporary, explicit parity boundary.
Do not allow new features to land on the legacy side during migration.
