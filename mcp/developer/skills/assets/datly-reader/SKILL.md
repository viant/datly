---
name: datly-reader
description: Author Datly readers with declarative DQL graphs and transcribe get to pure Go, linked shapes, typed relations, selectors, DerivedViews, cubes, and MCP exposure.
---

# Datly Reader Components

Build a typed, data-driven read API. A reader maps a declared query/view graph to a public Go-shaped response; it is not a handwritten DAO.

## Audience and contract

This skill is for application developers, not Datly framework contributors. Explain DQL, Go shapes, tags, base types, interfaces, and behavior in application terms. Do not require knowledge of internal compilers, collectors, registries, or source directories.

The references describe the **required Datly 1.0 authoring contract**, including features under development. Those features remain part of the requested design. A capability missing from the connected build is an implementation gap, not permission to drop a requirement, invent syntax, or silently choose another architecture. Compile/validate against the connected developer server and report a missing capability precisely.

**Generation availability.** The v1 CLI exposes `datly transcribe get|patch|post|put` with Go output by default. Use the matching CLI first; inspect connected developer MCP targets when using a server.
Check the installed capability before generation. If operation-based `transcribe` with pure Go output is
missing, return the DQL and application hook contract and report that gap. Do not
substitute `translate`, lower-level transcription, or manual writer plumbing.

Configured standalone/custom builds can expose report-enabled groupable readers
and opt-in cube composition from selected linked packages. Preserve source auth,
explicit SQL aliases and every warmed grouping dimension when reusing cube caches;
see [reports](references/product/datly/doc/reports.md) for declared configuration and authorization requirements.

## Preserve the database during migration

A migration to Datly must preserve the existing database schema. Never propose a counter table, sequence ledger, reservation store, new column or other DDL as a migration prerequisite. Preserve MySQL's existing native auto-increment and the version-matched SQLX default transient allocation behavior; never replace internal auto-increment with external sequence storage.

Scoped non-identity numbering is a separate capability. Its optional ledger is not a migration default or a substitute for native identity allocation. When the existing schema and transient mechanism cannot express the requested scope, report that specific framework gap and preserve the current behavior. Do not silently add storage or DDL. A separately requested new allocator design must be evaluated as a separate change.

## Read what the task needs

- For Go field renames, outer projection versus inner SQL aliases, SQLX/JSON
  naming, CAST/nullability and regeneration, read the
  [v1 shaping contract](references/product/datly/doc/shaping-contract.md).

- For migrating legacy Datly handlers or direct-SQL application reads to the
  operation-based generated contract, read
  [legacy-to-v1-migration.md](references/product/datly/doc/legacy-to-v1-migration.md).

- For YAML/JSON instance constants (`-const` / `ConstURL`), typed defaults, DB-only identifier rendering and source preservation, read [constants and substitutions](references/constants-and-substitutions.md).

- For project init/build and deployment, read [project-build.md](references/project-build.md). Custom builds discover/link internally; no mandatory user init/Register/import list.
- Start with [concepts.md](references/concepts.md) for terminology, philosophy, base types, and authoring choices.
- Read [developer-mcp.md](references/developer-mcp.md) before using a developer MCP server. Its operations are conceptual capabilities, not assumed tool names.
- Use [dql-grammar.md](references/dql-grammar.md) and [dql.ebnf](references/dql.ebnf) for DQL, directives, options, declarative SQL graphs, CAST, and tag customization.
- Use [tags-and-interfaces.md](references/tags-and-interfaces.md) for Go shapes, binding tags, SQL mapping, predicates, validation, and public APIs.
- For JWT-based authorization, use the [explicit input and predicate pattern](references/tags-and-interfaces.md#jwt-input-and-authorization-predicates); preserve original certificate/public-key verification and do not inject ambient claims.
- For a typed remote authorization context, read the [auth-context contract](references/product/datly/doc/auth-context.md). Keep verification, dependency binding, outbound headers and SQL predicates explicit; confirm generic provider API availability before naming its syntax.
- Read [reader-contract.md](references/reader-contract.md) for this component's behavior and decisions.
- For optional predicates and multiple AND/OR groups, read [reader predicates](references/reader-predicates.md), including custom SDK handlers, explicit verified JWT input and scoped component dependencies. Preserve required scope outside optional OR groups and retain presence markers and ordered bind arguments.
- Adapt [reader-examples.md](references/reader-examples.md); examples are patterns, not authorization to access a live database.
- For cache/warmup, multiview selectors, YAML docs, static/MCP resources, deployment, async and telemetry status, read [cache-and-operations.md](references/cache-and-operations.md). Separate current APIs from pending authoring/integration contracts.
- Use [acceptance.md](references/acceptance.md) to verify observable application behavior. Framework maintenance is outside this skill.

## Simplify projections

Start with `view.*` for ordinary fields. Add outer declarations only for genuine shape or behavior configuration: type/holder names, actual type conversions, required/optional overrides, codecs, validation, relations or lifecycle policy. Do not enumerate columns beside a wildcard or repeat inferred types through CAST, WithColumnType or per-column JSON tags.

```sql
SELECT c.*, type(c,'Conversation'), optional(c.summary)
FROM conversation c
```

Infer column types, defaults, keys and FK annotations from the authoritative source schema. Keep generated constraints instead of restating them in DQL. Use `required(view.column)` or `optional(view.column)` only when the desired scalar/pointer differs from inference; these do not change database constraints or reject legitimate zero values. CAST is for a real type difference, such as integer width, decoded bytes or a structured value. A standalone DQL CAST is shape metadata; an SQL-aliased CAST is executable SQL.

Visibility metadata targets fields or views already in the graph. It does not require adding an explicit column or another `view.*` to the outer projection solely to hide it. Keep structural relation declarations when needed. Use global `case_format('lc')`; author JSON names only for real wire exceptions and tags only for actual visibility/omission policy.

Keep necessary aggregates, computed expressions and intentionally restricted projections inside named source SQL; use the source wildcard outside. Never broaden the original public shape through a wildcard. Verify names, types, nullability, presence, selectors, serialization and actual database behavior after native regeneration. If the connected build requires redundant declarations or projections, report that capability gap with a minimal compact contract instead of inflating the DQL to conceal it.

## Preserve reader SQL during migration

Carry the original executable SQL closely into the named sources, ideally unchanged. Preserve the original DQL directory hierarchy and filenames; extract SQL into adjacent sql folders without reorganizing the component contracts. Preserve joins, predicates, vendor expressions, aggregates, grouping, ordering and summary semantics; change declaration/binding syntax separately. Simplification applies to shape declarations, not to rewriting the query for easier generation or tests. Document each unavoidable executable SQL change and prove source parity. If a runtime test dialect lacks a source function, identify the dialect gap or use an explicitly declared faithful compatibility tier rather than silently replacing product SQL.

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
tables in parentheses, entity hooks and invariant tags in DQL. Reader generation owns typed read contracts and resources; writer generation
owns Previous reads, presence, validation and write orchestration. Application
Go hooks own business rules. Existing linked Go types keep their authority.

- Establish the public result shape, parameter sources, connector names, identity/join keys, authorized filters, pagination, and selected package exposure.
- Author the DQL graph; link existing Go shapes or generate owned shapes as the project requires. Preserve explicitly requested Go-only or dynamic loading contracts and verify their support.
- Model ordinary relations, self references, and DerivedViews explicitly. For richer API fields use imported Go shapes and the required CAST/tag contract; keep physical backing columns internal but SQL-mapped.
- Use typed predicates and allowed selectors. Do not interpolate client values, column names, or arbitrary SQL.
- Define OnFetch transformations per row and OnRelation work after the complete relation is assembled. Configure batching, concurrency, partitions, cache, and retry policy deliberately.
- Generate with `datly transcribe get` (or a configured developer MCP generation target), inspect the emitted files, then exercise realistic SQLite fixtures and protocol exposure. Verify empty results, NULLs, composite joins, multiple batches, output slots, and hook counts.
- Persist with authored-code preservation and atomic generation replacement. Expose only the chosen components/packages; keep dependency components private unless explicitly selected.

## Non-obvious rules

- DQL plus Go-shape metadata is the component contract. HTTP method alone does not decide handler policy.
- Outer direct-column aliases in a named view graph rename Go fields and retain
  original SQLX mappings. Aliases inside view SQL remain vendor result aliases.
  Verify actual compiled SQL; do not inject outer field renames into it.
- Use full Go module/package identity and declared import aliases; never create a local empty substitute for an unresolved type.
- Has/presence bookkeeping is internal. Keep it out of client JSON, MCP schemas, examples of request bodies, and public error payloads.
- Internal physical columns still participate in SQL. A logical pseudo field that is not persisted is a different concept.
- Regenerate shapes from the current DQL and retain separate authored handlers/hooks. Direct edits inside generated files are overwritten and are the editor's responsibility.
- Use ordinary SQLX mapping and the framework's scoped services through their public surfaces; never advise a parallel raw-map row pipeline.
- Separate authoring-time developer MCP operations from runtime business MCP tools. Discover actual tool schemas; never invent a server URL, method, connector, or installed capability.

## Deliver

Return the component's purpose, public input/output contract, DQL/Go files, hook responsibilities, exposure choice, validation/error behavior, tests run, and any unresolved capability. Do not claim production registration or database mutation unless it actually occurred and was authorized.

## Graph naming

Use separate reader/writer DQL with required `#package`. Declare `input_type`,
`output_type` and outer `type(view,'Entity')` names; inner SQL aliases remain local.
Every complete reader example also declares a named, typed `(output/view)`
holder, with global `case_format('lc')` for lowerCamel output names. The holder's
row type must match the root `type(...)`; output type naming alone does not bind rows.
For public serialization renames, prefer `format:"name=CustomerName"` with global casing;
nonempty `json` names are exact overrides. With `lc`, the format name becomes `customerName`; the runtime encoder and
its wire schema use the same compiled naming policy.
Label small syntax/inner-SQL fragments and link a complete contract. Preserve
selectors, conditions, authorization, query resources, derived outputs and metadata.
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
