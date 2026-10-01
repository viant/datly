---
name: datly-writer
description: Author Datly PATCH, POST, and PUT components with declarative DQL graphs and operation-based transcription to pure Go, preserving sparse presence, validation, application Go hooks, and transactions.
---

# Datly Writer Components

Build a typed, data-driven mutation API. Declare the intended write graph and contract; put business customization in explicit hooks instead of editing regenerated orchestration.

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
- Adapt [writer-examples.md](references/writer-examples.md); examples are patterns, not authorization to access a live database.
- For hook-injected message buses, commit-dependent publication or async job requests, read [mutation-messages.md](references/mutation-messages.md).
- For stable-ID/FK gaps, async, telemetry, YAML docs, static/MCP resources and standalone status, read [availability-and-operations.md](references/availability-and-operations.md). Separate current APIs from pending authoring/integration contracts.
- Use [acceptance.md](references/acceptance.md) to verify observable application behavior. Framework maintenance is outside this skill.

For explicitly requested per-scope non-identity numbering, read [scoped sequences](references/product/datly/doc/scoped-sequences.md). Its optional ledger requires a separate allocator design decision; never introduce it to migrate an existing database or replace MySQL auto-increment.

For opt-in idempotent leaf deletion, read [delete-not-found](references/product/datly/doc/delete-not-found.md). Strict deletion remains the default.

## Simplify projections

Prefer `view.*` for ordinary columns. Keep the outer SELECT for genuine Go-shape or behavior declarations: `type`, `required`, `optional`, codecs, validation, visibility, relations and lifecycle policy. Do not repeat every column alongside a wildcard.

```sql
SELECT c.*, type(c,'Conversation'), required(c.id), optional(c.summary)
FROM conversation c
```

Use inferred driver types. A string column already represented as Go `string` needs no CAST. Use `required(view.column)` for an inferred scalar value and `optional(view.column)` for an explicit pointer; these do not change database constraints or reject legitimate zero values. Keep CAST only for a genuine type mismatch, such as integer width, a textual timestamp, decoded bytes or a structured codec value. A standalone DQL CAST is Go-shape metadata; an SQL-aliased CAST remains executable SQL.

For aggregates or intentionally restricted shapes, keep the necessary SQL expressions/projection inside a named source and use its wildcard in the outer shape declaration. A wildcard must not broaden the contract. Verify generated field names, types, nullability, presence, JSON and relations, then run actual selectors and database reads/writes; compilation alone does not prove wildcard runtime support.

Use `case_format('lc')` for naming. JSON tags belong only to genuine wire exceptions or omission/visibility policy, never repetitive lowercasing. Keep SQL aliases short and independent from public field names; avoid database keywords. `type(u,'UsageView','Usage')` gives a related SQL view an explicit Go holder without renaming its SQL namespace.

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
`#setting($_ = $case_format('lc'))`. Generated writer JSON tags also honor this policy.
Use `#setting($_ = $writer_omit_empty(true))` to opt a component into omitted
zero values without repetitive column tags; explicit JSON names/options remain
authoritative. Presence markers and internal/ignored fields retain their policy.
Use Structology casing rather than JSON tags
or holder `WithTag` solely for lowercasing; inner SQL aliases remain local.
Auxiliary `(TABLE)` sources are nonmutating, and outer `AND 1=1` marks a to-one
relation while retaining real equality links. See the grammar for CAST authority
over source-preserved SQL/CTEs and literal defaults.

## Filename controls

Use prefix-free default filenames with no `_gen` suffix. Application lifecycle
edits belong in create-once `lifecycle.go`; generated support remains separate.
Use explicit `$file_prefix('orders_')` only when requested or needed for chosen
same-package destinations. Exact per-file overrides win and are never prefixed.
Read [filename roles and override syntax](references/dql-grammar.md#generated-filenames-and-destinations)
for support files, split destinations, collision rules and safe regeneration.
