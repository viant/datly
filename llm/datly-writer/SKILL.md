---
name: datly-writer
description: Author Datly PATCH, POST and PUT object graphs (data graphs) with DQL and operation-based transcription to Go, preserving sparse presence, validation, business hooks and managed transactions.
---

# Datly Writer Components

Build a typed, data-driven mutation API. Declare the intended write graph and contract; put business customization in explicit hooks instead of editing regenerated orchestration.

## Write the object graph (data graph)

Object graph and data graph mean the same logical graph here. Declare its typed
root/relations, body, Current/auxiliary sources, connectors, sparse presence,
complete FK links and validation in a bounded generated writer. Native
orchestration owns writes and managed transaction completion; hooks own business
validation/normalization.
Compose genuinely separate operations through typed component invocation while
preserving authorization, constraints and the caller's transaction owner. Do not
replace this with application `sql.DB`/`sql.Tx` Query/Exec calls or transaction
wrappers. Read [release graph details](references/product/llm/datly-reader/references/object-graphs.md)
when a mutation needs related reads, revision selection or multiple writers.

## Audience and contract

This skill is for application developers, not Datly framework contributors. Explain DQL, Go shapes, tags, base types, interfaces, and behavior in application terms. Do not require knowledge of internal compilers, collectors, registries, or source directories.

The references describe the **required Datly 1.0 authoring contract**, including features under development. Those features remain part of the requested design. A capability missing from the connected build is an implementation gap, not permission to drop a requirement, invent syntax, or silently choose another architecture. Compile/validate against the connected developer server and report a missing capability precisely.

**Generation availability.** The v1 CLI exposes `datly transcribe get|patch|post|put` with Go output by default. Use the matching CLI first; inspect connected developer MCP targets when using a server.
Check the installed capability before generation. If operation-based `transcribe` with pure Go output is
missing, return the DQL and application hook contract and report that gap. Do not
substitute `translate`, lower-level transcription, or manual writer plumbing.

## Preserve the database during migration

A migration to Datly must preserve the existing database schema. Never propose a counter table, sequence ledger, reservation store, new column or other DDL as a migration prerequisite. Preserve MySQL's existing native auto-increment and the version-matched SQLX default transient allocation behavior; never replace internal auto-increment with external sequence storage.

Scoped non-identity numbering is a separate capability. Its optional ledger is not a migration default or a substitute for native identity allocation. When the existing schema and transient mechanism cannot express the requested scope, report that specific framework gap and preserve the current behavior. Do not silently add storage or DDL. A separately requested new allocator design must be evaluated as a separate change.

## Read what the task needs

- For a legacy migration, read [reverse-engineering.md](references/reverse-engineering.md) before authoring. Reconstruct the complete body, Current/auxiliary reads, writable graph, authorization, original ID mechanism, validation/error order and events. A route header or handler registration is not a migrated writer.

- For Go field renames, outer projection versus inner SQL aliases, SQLX/JSON
  naming, CAST/nullability and regeneration, read the
  [v1 shaping contract](references/product/datly/doc/shaping-contract.md).

- For migrating legacy `handler.Session`/`sess.Db()` writers or direct-SQL
  application mutations to generated Datly 1.0 graphs and hooks, read
  [legacy-to-v1-migration.md](references/product/datly/doc/legacy-to-v1-migration.md).

- For YAML/JSON instance constants (`-const` / `ConstURL`), typed defaults, DB-only identifier rendering and source preservation, read [constants and substitutions](references/constants-and-substitutions.md).

- For project init/build and deployment, read [project-build.md](references/project-build.md). Custom builds discover/link internally; no mandatory user init/Register/import list.
- Start with [concepts.md](references/concepts.md) for terminology, philosophy, base types, and authoring choices.
- Read [developer-mcp.md](references/developer-mcp.md) before using a developer MCP server. Its operations are conceptual capabilities, not assumed tool names.
- Use [dql-grammar.md](references/dql-grammar.md) and [dql.ebnf](references/dql.ebnf) for DQL, directives, options, declarative SQL graphs, CAST, and tag customization.
- Use [tags-and-interfaces.md](references/tags-and-interfaces.md) for Go shapes, binding tags, SQL mapping, predicates, validation, and public APIs.
- For JWT-based authorization, use the [explicit input and predicate pattern](references/tags-and-interfaces.md#jwt-input-and-authorization-predicates); preserve original certificate/public-key verification and do not inject ambient claims.
- For a typed remote authorization context, read the [auth-context contract](references/product/datly/doc/auth-context.md). Keep verification, dependency binding, outbound headers and SQL predicates explicit; confirm generic provider API availability before naming its syntax.
- For explicit row deletion and concurrency tokens, read [the mutation marker contract](references/writer-contract.md#explicit-deletion-and-token-validation). Omitted rows never imply deletion. Current v1 token writes use atomic IfMatch; see [mutation predicates](references/product/datly/doc/mutation-predicates.md) for general execution conditions.
- For affected-row losses, CAS winner adoption and bounded native writer replay, read [mutation recovery](references/product/datly/doc/mutation-recovery.md). Keep recovery decisions in the root lifecycle hook and verify transaction ownership.
- Read [writer-contract.md](references/writer-contract.md) for this component's behavior and decisions.
- Read the maintained [writable graph and generated body contract](references/product/datly/doc/mutations.md), then adapt [writer-examples.md](references/writer-examples.md), including its multiple writable collections and auxiliary lookups. Examples are patterns, not authorization to access a live database.
- For hook-injected message buses, commit-dependent publication or async job requests, read [mutation-messages.md](references/mutation-messages.md).
- For stable-ID/FK gaps, async, telemetry, YAML docs, static/MCP resources and standalone status, read [availability-and-operations.md](references/availability-and-operations.md). Separate current APIs from pending authoring/integration contracts.
- Use [acceptance.md](references/acceptance.md) to verify observable application behavior. Framework maintenance is outside this skill.

For explicitly requested per-scope non-identity numbering, read [scoped sequences](references/product/datly/doc/scoped-sequences.md). Its optional ledger requires a separate allocator design decision; never introduce it to migrate an existing database or replace MySQL auto-increment.

For opt-in idempotent leaf deletion, read [delete-not-found](references/product/datly/doc/delete-not-found.md). Strict deletion remains the default.

## Application-only scalar fields


Use `internal_transient(view.column)` for the exact combination `sqlx:"-" diff:"-" internal:"true" json:"-"`. It retains the generated scalar and presence marker while excluding SQL mapping, diffs and JSON. Keep public logical fields public; use `sqlx:"-"` alone when only persistence should be excluded. Place this annotation on the outer projection and retain separate CAST or validation declarations when needed.

## Directive formatting


Separate directive groups with one blank line: `#package`, consecutive `#import` declarations, consecutive `#setting`/`#settings` declarations, then `#define` parameters. Keep related declarations together and leave a blank line before the SQL graph. Preserve executable SQL and template expressions while formatting. Follow the original SQL-derived omission defaults instead of repeating `omitempty` on each column or adding a setting to reproduce an inferred default. Keep genuine wire exceptions and validation tags. Global `case_format` supplies ordinary names. Verify omission, explicit zero/null presence, and source response parity after regeneration; a missing inferred default is a framework gap to resolve, not a reason to inflate DQL.

## Preserve original JSON presence


For migrations, inspect the original generated Go type and explicit wire annotations before changing omission behavior. Preserve original Datly behavior: with `omitempty`, a scalar `bool(false)` is omitted, a nil `*bool` is omitted, and a nonnil `*bool(false)` is emitted as `false`. Pointer presence is distinct from the pointed-to zero value. Retain genuine source omission annotations and hooks; do not convert pointers to scalars, clear explicit false values, or add an encoder option to force absent fields. Verify disputed behavior against the pinned original serializer. Change an original expectation only with explicit authorization for that specific discrepancy.

## Simplify projections

Start with `view.*` for ordinary fields. Add outer declarations only for genuine shape or behavior configuration: type/holder names, actual type conversions, required/optional overrides, codecs, validation, relations or lifecycle policy. Do not enumerate columns beside a wildcard or repeat inferred types through CAST, WithColumnType or per-column JSON tags.

```sql
SELECT c.*, type(c,'Conversation'), optional(c.summary)
FROM conversation c
```

Infer column types, defaults, keys and FK annotations from the authoritative source schema. Keep generated constraints instead of restating them in DQL. Use `required(view.column)` or `optional(view.column)` only when the desired scalar/pointer differs from inference; these do not change database constraints or reject legitimate zero values. CAST is for a real type difference, such as integer width, decoded bytes or a structured value. A standalone DQL CAST is shape metadata; an SQL-aliased CAST is executable SQL.

Visibility metadata targets fields or views already in the graph. It does not require adding an explicit column or another `view.*` to the outer projection solely to hide it. Keep structural relation declarations when needed. Use global `case_format('lc')`; author JSON names only for real wire exceptions and tags only for actual visibility/omission policy.

Keep necessary aggregates, computed expressions and intentionally restricted projections inside named source SQL; use the source wildcard outside. Never broaden the original public shape through a wildcard. Verify names, types, nullability, presence, selectors, serialization and actual database behavior after native regeneration. If the connected build requires redundant declarations or projections, report that capability gap with a minimal compact contract instead of inflating the DQL to conceal it.

Keep SQL aliases short and independent from public field names; avoid database keywords. An explicit related holder name does not rename its SQL namespace.

## Keep view SQL in adjacent assets

For readers and writers, move substantial or nontrivial named-source SQL into the component's `sql/` subfolder. Keep parameters, predicates, defaults, the view graph and genuine Go-shape annotations in the main DQL. Use the supported `${embed:sql/records.sql}` resource syntax; assets belong beside their authoritative DQL, not in generated Go or flattened sibling files.

Graph fragment, retaining the component's existing declarations:

```sql
SELECT records.*, type(records,'Record')
FROM (${embed:sql/records.sql}) records
```

Preserve each query's SQL, template expressions, aliases, predicates, ordering and transaction capabilities. Share an asset only when its content and parameter/predicate context are identical. Keep embed tokens out of SQL comments and quoted strings: the raw resource scanner expands them there too.

For a cleanup sweep, inspect every reader and writer rather than only the named example. Record source/asset locations and any intentionally inline simple query. Verify stock transcription resolves the assets, preserves generated Go and runtime SQL semantics, and leaves no unresolved embed references; exercise the affected selectors and supported database dialects. Discovery must not bake its fixture dialect's quoting into portable persisted SQL.

## Authoring workflow

The standard workflow is **reader-like declarative DQL graph + explicit `transcribe`
operation (`get`, `patch`, `post`, `put`) → generated pure Go**. Declare auxiliary
tables in parentheses, explicit `lifecycle_type(view, 'package.Type')` hooks and
invariant tags in DQL. Omitting lifecycle declarations produces ordinary hookless
writes; never infer lifecycle struct names. `entity_hooks` is unsupported.
The generator owns binding, Previous reads, presence, validation and write orchestration; application
Go hooks own business rules. Existing linked Go types keep their authority.

- Establish operation, input/output shapes, writable tables, full identity tuples, parent links, auxiliary read-only joins, validation rules, and authorization/error policy.
- Sequence through version-matched native SQLX defaults resolved before root Data opens. MySQL uses its unchanged original transient transaction mechanism; its table allocator requires explicit `sequence_strategy('reservation')`. Canonical DQL accepts only `transient` or `reservation`; omission keeps native defaults. A child may explicitly name the same resolved default, but a differing child setting must fail without switching the root. Never recommend `maxid` or silently replace the original mechanism. Explain the original MySQL spare-connection, caller-lock and source-trigger constraints.
- Preserve the distinction between Original request facts, resolved/frozen identity, database Previous, and working Has markers. Input.Init can resolve identity using typed read indexes before the match freezes. An ID supplied as zero is still supplied; a sequenced ID never changes insert/update classification.
- Use the [typed read indexes](references/writer-contract.md#typed-read-indexes-for-application-hooks) in input/entity hooks: canonical key/link maps are eager; business GroupBy/IndexBy methods run on demand.
- Verify the generator supplies the canonical lifecycle: capture before input initialization; SyncPresence; invariant backfill; entity Init; declared concurrency-token checks; framework Go/database validation; custom Validate; begin/join transaction; Sequence; AfterSequence; Diff; Reconcile; Queue; AfterQueue; outcome-aware finalization.
- New entities get complete checks; sparse existing entities use Has-gated checks. Backfill does not mark client presence. Framework/database violations stop custom validation and mutation.
- Generate Go tags from authoritative constraints and refine them with tag(view.column, 'validate:...'). Do not manufacture constraints from missing metadata or make false/zero invalid merely because a column is NOT NULL.
- Keep business data fixed after validation. Identity/link reconciliation preserves the frozen resolved tuple and explicit relation producers. Verify graph structure before actions and queued values after observation hooks.
- Return the transformed request body with final IDs/links. Preserve authored status/message/error/violation payloads; do not publish commit-dependent messages on Queue or caller-pending work.
- Prove mixed inserts/updates, composite and zero identities, omitted/null/false values, rollback, shared transactions, hook order, and regeneration with SQLite before delivery.

## Focused lifecycle methods

Keep application lifecycle callbacks small and readable: each callback should show
its phase's business-rule order through calls to named private helpers. Split
distinct rules (authorization, normalization, validation, identity/link handling,
queue preparation or completion) into focused helpers in the existing phase files.
Apply this structure to existing large callbacks in the affected component as
well as new hooks; a wrapper alone does not improve the underlying method. Do
not expand a component change into unrelated lifecycle refactors.

Refactor within the current package and file layout. Preserve exported APIs,
callback signatures and order, sparse markers, identity authority, error/violation
order and payloads, and the native DML/transaction owner. Keep generated code
under canonical regeneration. Retire a no-op lifecycle declaration only after
proving native behavior and public Go callers remain compatible; preserve an
exported method when it is part of the source API. Retain hooks that implement an
actual business rule.

Add extensive unit tests for the affected component at its actual input/entity
callback or custom-handler boundaries, with scoped public-service fakes as needed;
helper-only tests are insufficient. Reuse the original business expectations and fixtures. Follow the
[lifecycle unit-test coverage](references/acceptance.md#lifecycle-unit-tests)
for applicable phases, then retain canonical generation, native execution with
hydrated runtime inputs, and the selected original Endly cases as separate gates.
Run the component and directly affected dependency tests; broaden only when the
change requires it or the user requests a wider suite.

## Non-obvious rules

- DQL plus Go-shape metadata is the component contract. HTTP method alone does not decide handler policy.
- Outer direct-column aliases in a named view graph rename Go fields and retain
  original SQLX mappings. Aliases inside view SQL remain vendor result aliases.
  Verify actual compiled SQL; do not inject outer field renames into it.
- Use full Go module/package identity and declared import aliases; create empty lifecycle methods only for explicitly named unresolved types in the generated destination package. Preserve known/imported hooks; foreign missing types and invalid signatures must fail.
- Has/presence bookkeeping is internal. Keep it out of client JSON, MCP schemas, examples of request bodies, and public error payloads.
- Internal physical columns still participate in SQL. A logical pseudo field that is not persisted is a different concept.
- Regenerate shapes from the current DQL and retain separate authored handlers/hooks. Direct edits inside generated files are overwritten and are the editor's responsibility.
- Use ordinary SQLX mapping and the framework's scoped services through their public surfaces; never advise a parallel raw-map row pipeline.
- Separate authoring-time developer MCP operations from runtime business MCP tools. Discover actual tool schemas; never invent a server URL, method, connector, or installed capability.

## Deliver

Return the component's purpose, public input/output contract, DQL/Go files, hook responsibilities, exposure choice, validation/error behavior, tests run, and any unresolved capability. Do not claim production registration or database mutation unless it actually occurred and was authorized.

## Graph naming

Use separate reader/writer DQL with required `#package`. Declare `input_type`,
`output_type`, outer `type(view,'Entity')` names, a typed main output holder
(`$Data<[]*Entity>(output/body)` for generated writes), and global
`#setting($_ = $case_format('lc'))`. Generated writer/body shapes leave
inferred JSON names empty so native caseFormat owns presentation; explicit
source names and omission options remain authoritative. Do not add per-column
lowerCamel JSON names or blanket `omitempty` to repair MCP discovery: naming,
omission and runtime validation are separate contracts. When regenerating a body
used by direct `encoding/json` or a custom marshaler, compare its saved-data and
wire behavior because native caseFormat does not transform custom-owned bytes.
Use `#setting($_ = $writer_omit_empty(true))` to opt a component into omitted
zero values without repetitive column tags; explicit JSON names/options remain
authoritative. Presence markers and internal/ignored fields retain their policy.
Use Structology casing rather than JSON tags
or holder `WithTag` solely for lowercasing; inner SQL aliases remain local.
Auxiliary `(TABLE)` sources are nonmutating, and outer `AND 1=1` marks a to-one
relation while retaining real equality links. See the grammar for CAST authority
over source-preserved SQL/CTEs and literal defaults.

## Filename controls

Emit generated support files only when they contain an implementation. Do not
add package-only phase placeholders; capture, Previous matching, validation and
action execution use the shared writer runtime. Filename overrides select where
an emitted role belongs, and do not force an empty artifact.

Use prefix-free default filenames with no `_gen` suffix. Application lifecycle
edits belong in create-once `lifecycle.go`; generated support remains separate.
Use explicit `$file_prefix('orders_')` only when requested or needed for chosen
same-package destinations. Exact per-file overrides win and are never prefixed.
Read [filename roles and override syntax](references/dql-grammar.md#generated-filenames-and-destinations)
for support files, split destinations, collision rules and safe regeneration.

## Application and runtime boundary

For ordered UPDATE-only persistence, see [explicit UPDATE commands](references/writer-contract.md#explicit-update-commands). Prefer ordinary PATCH matching unless the source contract specifically requires command semantics.

Application components, inputs and lifecycle hooks depend on Xdatly public interfaces; Datly supplies their implementations. Use `handler.ComponentInvoker` from `github.com/viant/xdatly/handler`; prefer the explicit lifecycle arguments described below for prerequisite and dependent work. Use `handler.WithInput` for an explicitly marked reader seed. A trusted typed `ComponentRequest.Input` is a bound input for readers and writers; it is not a raw request to bind again. Preserve generated presence markers. Reader selector side inputs remain separate; pass selector overrides through the existing invocation option instead of adding fields to the business Input. Provider overlays and completion callbacks retain the caller's canonical binding, authorization, shared buffered writes and transaction owner. Use Xdatly contracts for DML, sequencing, validation, differ, logger and mutation reporting. Do not instantiate Datly runtime implementations inside business hooks or expose runtime-only invocation controls in application contracts. Datly bootstrap/registration adapters may depend on Datly. The older Datly invocation interface remains supported for existing consumers; new application examples use Xdatly.

For binding reviews, check application DI fields and imports against Xdatly contracts, including `handler.MutationReporter` and `differ.Differ`. Preserve binding kinds and scoped behavior; regenerate generated Inputs/setters from DQL rather than replacing their Go imports by hand. Treat Datly-specific request options as runtime-adapter controls, not public Xdatly request fields.

## Generated Go compatibility

Minimize generated shape changes. Preserve existing exported type names, field
names and types, pointer/value semantics, setters, package paths and file
destinations unless the authoritative source contract requires a change. Do not
introduce naming, alias, wrapper or field-type churn as incidental cleanup.
Capture the current generated API before transcription, regenerate only affected
components and their genuine dependencies, and review the API diff separately
from tag/resource changes. Use existing DQL/native generation controls to retain
compatible shapes; never hand-edit generated Go. Check existing DQL controls,
public APIs and source-backed examples before proposing a framework change.
Compile affected consuming packages and relevant reverse dependents without
temporary overlays before integration, and retain relevant legacy consumer
compilation when changing an SDK binding interface. Record any necessary
incompatibility explicitly before publication; a successful generator package
check alone does not prove downstream compilation.

## Lifecycle composition

Prefer `Init(ctx context.Context, invoker handler.ComponentInvoker) error` for
input prerequisites and `Finalize(ctx context.Context, invoker handler.ComponentInvoker) error`
for output composition. These implement separate `handler.InvokerInitializer`
and `handler.InvokerFinalizer` interfaces; new input/output scaffolds use them.
Avoid adding an invoker field to Input solely for lifecycle calls. Existing plain
hooks, error/lookup finalizers and input injection remain supported; do not
rewrite existing hooks just to match the new scaffold. When migrating, remove a
field only after checking all its callers. Invoker-aware Finalize runs before
transaction completion, unlike plain success Finalize; keep commit-dependent
publication in outcome finalization. Await calls and never retain the supplied
invoker after its callback. See [contracts and lifecycle interfaces](references/tags-and-interfaces.md).
