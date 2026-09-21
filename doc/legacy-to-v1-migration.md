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
3. Put all joins, derived views, complete composite relations, and mandatory
   authorization restrictions in the declarative graph. Bind client values;
   never interpolate them.
4. Use typed predicates for caller-dependent authorization. Verified JWT input
   is explicit; query/header viewer IDs do not become authority by convention.
5. Move row transformations to `OnFetch`, and complete-relation transformations
   to `OnRelation`. Hooks do not query the database.
6. Run `datly transcribe get` against the application package. Inspect generated
   types, SQL resources, selectors, routes, and authored-code preservation.
7. Replace legacy callers with the generated component contract, then remove
   the old SQL service only after parity tests pass.

Aggregates assembled from several legacy service calls should normally become
one reader graph with related and DerivedView outputs. Do not preserve a
service-local fan-out of raw SELECTs merely because the public response is an
aggregate.

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
