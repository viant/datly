---
name: datly-custom-component
description: Create Datly components with custom Go handlers, typed inputs and outputs, scoped capabilities, validation, component dependencies, and controlled HTTP/MCP responses.
---

# Datly Custom Components

Build a component whose application-owned handler orchestrates behavior that is not simply a generated reader or writer. Preserve the same typed contract and scoped services.

## Audience and contract

This skill is for application developers, not Datly framework contributors. Explain DQL, Go shapes, tags, base types, interfaces, and behavior in application terms. Do not require knowledge of internal compilers, collectors, registries, or source directories.

The references describe the **required Datly 1.0 authoring contract**, including features under development. Those features remain part of the requested design. A capability missing from the connected build is an implementation gap, not permission to drop a requirement, invent syntax, or silently choose another architecture. Compile/validate against the connected developer server and report a missing capability precisely.

**Generation availability for requested graph generation.** The v1 CLI exposes `datly transcribe get|patch|post|put` with Go output by default. Discover the connected developer server and installed CLI
capabilities before generation. If operation-based `transcribe` with pure Go output is
missing, return the DQL and application hook contract and report that gap. Do not
substitute `translate`, lower-level transcription, or manual writer plumbing.

Configured standalone/custom builds can expose report-enabled groupable readers
and opt-in cube composition from selected linked packages. Preserve source auth,
explicit SQL aliases and every warmed grouping dimension when reusing cube caches;
see [reports](references/product/datly/doc/reports.md) for declared configuration and authorization requirements.

## Read what the task needs

- For project init/build and deployment, read [project-build.md](references/project-build.md). Custom builds discover/link internally; no mandatory user init/Register/import list.
- Start with [concepts.md](references/concepts.md) for terminology, philosophy, base types, and authoring choices.
- Read [developer-mcp.md](references/developer-mcp.md) before using a developer MCP server. Its operations are conceptual capabilities, not assumed tool names.
- Use [dql-grammar.md](references/dql-grammar.md) and [dql.ebnf](references/dql.ebnf) for DQL, directives, options, declarative SQL graphs, CAST, and tag customization.
- Use [tags-and-interfaces.md](references/tags-and-interfaces.md) for Go shapes, binding tags, SQL mapping, predicates, validation, and public APIs.
- For JWT-based authorization, use the [explicit input and predicate pattern](references/tags-and-interfaces.md#jwt-input-and-authorization-predicates); preserve original certificate/public-key verification and do not inject ambient claims.
- Read [custom-contract.md](references/custom-contract.md) for this component's behavior and decisions.
- Adapt [custom-examples.md](references/custom-examples.md); examples are patterns, not authorization to access a live database.
- For direct bytes, conditional finalizers, YAML docs, static/MCP resources and operational services, read [output-and-operations.md](references/output-and-operations.md). Separate current APIs from pending authoring/integration contracts.
- Use [acceptance.md](references/acceptance.md) to verify observable application behavior. Framework maintenance is outside this skill.

## Authoring workflow

- Decide whether a standard reader or generated writer already expresses the task. Use custom orchestration when the user chooses it or application behavior requires it; do not silently substitute it for a requested generated policy.
- Declare input/output Go shapes and a named handler factory. Bind request values and dependencies through tags and the developer MCP server's supported registration workflow.
- Implement the public Contract[I,O] interface or an equivalent supported function adapter. Keep orchestration and business hooks in application-owned files.
- Request narrow capabilities (Validator, DML, Sequencer, logger, message bus, component binding) or the Data aggregate. Do not assume a raw DB/transaction or an ambient global session.
- Run framework/schema/database checks with complete versus sparse mode before custom business validation. Preserve typed violations and explicit public error payloads.
- Compose other components through supported scoped binding/invocation, preserving canonical input and shared transaction ownership. Private dependencies do not need public endpoints.
- For hook messaging or async work, use the injected-service and original-job-schema requirements in [custom-contract.md](references/custom-contract.md); dry-run reads do not prove mutation side-effect isolation.
- If writing, explicitly follow the appropriate identity/presence/transaction safety contract; custom code is not implicitly protected by every generated writer phase.
- Verify the actual orchestration with SQLite and HTTP/native MCP tests, including errors, cancellation, pending transactions, and side effects.

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

## Filename controls

Use prefix-free default filenames with no `_gen` suffix. Application lifecycle
edits belong in create-once `lifecycle.go`; generated support remains separate.
Use explicit `$file_prefix('orders_')` only when requested or needed for chosen
same-package destinations. Exact per-file overrides win and are never prefixed.
Read [filename roles and override syntax](references/dql-grammar.md#generated-filenames-and-destinations)
for support files, split destinations, collision rules and safe regeneration.
