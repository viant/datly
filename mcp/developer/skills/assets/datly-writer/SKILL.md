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

## Read what the task needs

- For YAML/JSON instance constants (`-const` / `ConstURL`), typed defaults, DB-only identifier rendering and source preservation, read [constants and substitutions](references/constants-and-substitutions.md).

- For project init/build and deployment, read [project-build.md](references/project-build.md). Custom builds discover/link internally; no mandatory user init/Register/import list.
- Start with [concepts.md](references/concepts.md) for terminology, philosophy, base types, and authoring choices.
- Read [developer-mcp.md](references/developer-mcp.md) before using a developer MCP server. Its operations are conceptual capabilities, not assumed tool names.
- Use [dql-grammar.md](references/dql-grammar.md) and [dql.ebnf](references/dql.ebnf) for DQL, directives, options, declarative SQL graphs, CAST, and tag customization.
- Use [tags-and-interfaces.md](references/tags-and-interfaces.md) for Go shapes, binding tags, SQL mapping, predicates, validation, and public APIs.
- For JWT-based authorization, use the [explicit input and predicate pattern](references/tags-and-interfaces.md#jwt-input-and-authorization-predicates); preserve original certificate/public-key verification and do not inject ambient claims.
- Read [writer-contract.md](references/writer-contract.md) for this component's behavior and decisions.
- Adapt [writer-examples.md](references/writer-examples.md); examples are patterns, not authorization to access a live database.
- For hook-injected message buses, commit-dependent publication or async job requests, read [mutation-messages.md](references/mutation-messages.md).
- For stable-ID/FK gaps, async, telemetry, YAML docs, static/MCP resources and standalone status, read [availability-and-operations.md](references/availability-and-operations.md). Separate current APIs from pending authoring/integration contracts.
- Use [acceptance.md](references/acceptance.md) to verify observable application behavior. Framework maintenance is outside this skill.

## Authoring workflow

The standard workflow is **reader-like declarative DQL graph + explicit `transcribe`
operation (`get`, `patch`, `post`, `put`) → generated pure Go**. Declare auxiliary
tables in parentheses, explicit `lifecycle_type(view, 'package.Type')` hooks and
invariant tags in DQL. Omitting lifecycle declarations produces ordinary hookless
writes; never infer lifecycle struct names. `entity_hooks` is unsupported.
The generator owns binding, Previous reads, presence, validation and write orchestration; application
Go hooks own business rules. Existing linked Go types keep their authority.

- Establish operation, input/output shapes, writable tables, full identity tuples, parent links, auxiliary read-only joins, validation rules, and authorization/error policy.
- Preserve the distinction between Original request facts, resolved/frozen identity, database Previous, and working Has markers. Input.Init can resolve identity using typed read indexes before the match freezes. An ID supplied as zero is still supplied; a sequenced ID never changes insert/update classification.
- Use the [typed read indexes](references/writer-contract.md#typed-read-indexes-for-application-hooks) in input/entity hooks: canonical key/link maps are eager; business GroupBy/IndexBy methods run on demand.
- Verify the generator supplies the canonical lifecycle: capture before input initialization; SyncPresence; invariant backfill; entity Init; framework Go/database validation; custom Validate; begin/join transaction; Sequence; AfterSequence; Diff; Reconcile; Queue; AfterQueue; outcome-aware finalization.
- New entities get complete checks; sparse existing entities use Has-gated checks. Backfill does not mark client presence. Framework/database violations stop custom validation and mutation.
- Generate Go tags from authoritative constraints and refine them with tag(view.column, 'validate:...'). Do not manufacture constraints from missing metadata or make false/zero invalid merely because a column is NOT NULL.
- Keep business data fixed after validation. Identity/link reconciliation preserves the frozen resolved tuple and explicit relation producers. Verify graph structure before actions and queued values after observation hooks.
- Return the transformed request body with final IDs/links. Preserve authored status/message/error/violation payloads; do not publish commit-dependent messages on Queue or caller-pending work.
- Prove mixed inserts/updates, composite and zero identities, omitted/null/false values, rollback, shared transactions, hook order, and regeneration with SQLite before delivery.

## Non-obvious rules

- DQL plus Go-shape metadata is the component contract. HTTP method alone does not decide handler policy.
- Use full Go module/package identity and declared import aliases; create empty lifecycle methods only for explicitly named unresolved types in the generated destination package. Preserve known/imported hooks; foreign missing types and invalid signatures must fail.
- Has/presence bookkeeping is internal. Keep it out of client JSON, MCP schemas, examples of request bodies, and public error payloads.
- Internal physical columns still participate in SQL. A logical pseudo field that is not persisted is a different concept.
- Preserve existing field order, append new fields, and retain authored handlers/hooks. Do not overwrite edited generated output to make regeneration pass.
- Use ordinary SQLX mapping and the framework's scoped services through their public surfaces; never advise a parallel raw-map row pipeline.
- Separate authoring-time developer MCP operations from runtime business MCP tools. Discover actual tool schemas; never invent a server URL, method, connector, or installed capability.

## Deliver

Return the component's purpose, public input/output contract, DQL/Go files, hook responsibilities, exposure choice, validation/error behavior, tests run, and any unresolved capability. Do not claim production registration or database mutation unless it actually occurred and was authorized.

## Graph naming

Use separate reader/writer DQL with required `#package`. Declare `input_type`,
`output_type`, outer `type(view,'Entity')` names, a typed main output holder
(`$Data<[]*Entity>(output/body)` for generated writes), and global
`#setting($_ = $case_format('lc'))`. Use Structology casing rather than JSON tags
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
