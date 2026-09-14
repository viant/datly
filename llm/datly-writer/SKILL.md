---
name: datly-writer
description: Create and modify Datly PATCH, POST, and PUT components with DQL or Go shapes, sparse presence, database validation, typed hooks, transactions, and generated mutation policies.
---

# Datly Writer Components

Build a typed, data-driven mutation API. Declare the intended write graph and contract; put business customization in explicit hooks instead of editing regenerated orchestration.

## Audience and contract

This skill is for application developers, not Datly framework contributors. Explain DQL, Go shapes, tags, base types, interfaces, and behavior in application terms. Do not require knowledge of internal compilers, collectors, registries, or source directories.

The references describe the **required Datly 1.0 authoring contract**, including features under development. Those features remain part of the requested design. A capability missing from the connected build is an implementation gap, not permission to drop a requirement, invent syntax, or silently choose another architecture. Compile/validate against the connected developer server and report a missing capability precisely.

**Release validation in progress.** The local `v1` release copy enforces exact
authored names, explicit user-defined aliases and duplicate output-name errors.
Do not infer spelling variations. Check the connected build and the product
status guide for pending DQL destination and native recursive Velty work.

## Read what the task needs

- For project init/build and deployment, read [project-build.md](references/project-build.md). Custom builds discover/link internally; no mandatory user init/Register/import list.
- Start with [concepts.md](references/concepts.md) for terminology, philosophy, base types, and authoring choices.
- Read [developer-mcp.md](references/developer-mcp.md) before using a developer MCP server. Its operations are conceptual capabilities, not assumed tool names.
- Use [dql-grammar.md](references/dql-grammar.md) and [dql.ebnf](references/dql.ebnf) for DQL, directives, options, SQL/Velty boundaries, CAST, and tag customization.
- Use [tags-and-interfaces.md](references/tags-and-interfaces.md) for Go shapes, binding tags, SQL mapping, predicates, validation, and public APIs.
- For JWT-based authorization, use the [explicit input and predicate pattern](references/tags-and-interfaces.md#jwt-input-and-authorization-predicates); preserve original certificate/public-key verification and do not inject ambient claims.
- Read [writer-contract.md](references/writer-contract.md) for this component's behavior and decisions.
- Adapt [writer-examples.md](references/writer-examples.md); examples are patterns, not authorization to access a live database.
- For hook-injected message buses, commit-dependent publication or async job requests, read [mutation-messages.md](references/mutation-messages.md).
- For stable-ID/FK gaps, async, telemetry, YAML docs, static/MCP resources and standalone status, read [availability-and-operations.md](references/availability-and-operations.md). Separate current APIs from pending authoring/integration contracts.
- Use [acceptance.md](references/acceptance.md) to verify observable application behavior. Framework maintenance is outside this skill.

## Authoring workflow

- Establish operation, input/output shapes, writable tables, full identity tuples, parent links, auxiliary read-only joins, validation rules, and authorization/error policy.
- Preserve the distinction between original supplied identity, current values, database Previous, and Has markers. An ID supplied as zero is still supplied; a sequenced ID never changes insert/update classification.
- Use the canonical lifecycle: capture before input initialization; SyncPresence; invariant backfill; entity Init; framework Go/database validation; custom Validate; begin/join transaction; Sequence; AfterSequence; Diff; Reconcile; Queue; AfterQueue; outcome-aware finalization.
- New entities get complete checks; sparse existing entities use Has-gated checks. Backfill does not mark client presence. Framework/database violations stop custom validation and mutation.
- Generate Go tags from authoritative constraints and refine them with tag(view.column, 'validate:...'). Do not manufacture constraints from missing metadata or make false/zero invalid merely because a column is NOT NULL.
- Keep business data fixed after validation. Identity/link reconciliation follows the original tuple and explicit relation producers. Verify graph structure before actions and queued values after observation hooks.
- Return the transformed request body with final IDs/links. Preserve authored status/message/error/violation payloads; do not publish commit-dependent messages on Queue or caller-pending work.
- Prove mixed inserts/updates, composite and zero identities, omitted/null/false values, rollback, shared transactions, hook order, and regeneration with SQLite before delivery.

## Non-obvious rules

- DQL plus Go-shape metadata is the component contract. HTTP method alone does not decide handler policy.
- Use full Go module/package identity and declared import aliases; never create a local empty substitute for an unresolved type.
- Has/presence bookkeeping is internal. Keep it out of client JSON, MCP schemas, examples of request bodies, and public error payloads.
- Internal physical columns still participate in SQL. A logical pseudo field that is not persisted is a different concept.
- Preserve existing field order, append new fields, and retain authored handlers/hooks. Do not overwrite edited generated output to make regeneration pass.
- Use ordinary SQLX mapping and the framework's scoped services through their public surfaces; never advise a parallel raw-map row pipeline.
- Separate authoring-time developer MCP operations from runtime business MCP tools. Discover actual tool schemas; never invent a server URL, method, connector, or installed capability.

## Deliver

Return the component's purpose, public input/output contract, DQL/Go files, hook responsibilities, exposure choice, validation/error behavior, tests run, and any unresolved capability. Do not claim production registration or database mutation unless it actually occurred and was authorized.
