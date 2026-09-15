# Developer MCP authoring workflow

## Operation-based generation to pure Go

**Generation availability.** Operation-based Go generation is available in the
`v1` source CLI. Check the installed CLI and connected developer server separately:
older releases and unconfigured servers may not expose high-level generation.
If unavailable, report that gap with the DQL and lifecycle contract; do not
substitute lower-level translation or manual writer plumbing.

For a matching `v1` CLI:

```sh
datly gen -op patch -lang go -dir /path/to/application example.com/application/records
```

Use `post`, `put` or `get` for the corresponding operation. Select one component
in a module-qualified package; flags precede the package. DQL/package metadata
owns destinations. Keep the authored route method consistent with the operation.
The graph supplies writable/auxiliary roles, complete relation keys, hooks and
invariants. Schema discovery uses an explicitly selected read-only connector.
Require generated pure Go and preview derived input/output, Previous lookups,
presence and validation policy before building. Go hooks are create-once
application files; regeneration preserves their edits. A transcribe capability
does not establish availability of this high-level operation.

## Datly validation command

`datly validate` uses the shared authoring Validator, with static checks by default and explicitly enabled schema discovery. Discover installed command help before using it; older builds may lack these options. The opt-in `mcp/developer` service exposes the same Validator and diagnostics through `datly.validate`; it does not add separate language rules. Discover the connected server's actual tools before calling it.

For an installed build with this command:

```sh
datly validate -dir /path/to/application -format json example.com/application/components/...
```

Use full module-qualified package patterns, not filesystem globs. The selected packages may contain DQL, Go-only components, or linked Go/DQL contracts. Repeat `-module-dir` for other local modules and `-exclude` to exclude component packages; private imported types/resources remain available. `-dir` identifies the application module used for prospective generated-artifact checks. Flags precede package patterns.

By default, the command checks source compilation, generation planning, resource references, component dependencies, route conflicts and prospective destination ownership without emitting files. JSON reports `valid`, component identities, completed stages, skipped stages and diagnostics; exit codes are 0 for a static pass, 1 for validation failure and 2 for invocation errors. Static mode does not compile Go, run hooks, check a live database, run fixtures or activate HTTP/MCP exposure. Read the skipped stages even when `valid` is true; static success is not runtime readiness or proof of full dialect SQL correctness.

Accept DQL sources or Go component packages, including DQL linked to existing shapes. Validate syntax, tags, imports/types, bindings, relations/DerivedViews, identity and presence policy, hook signatures, resources, dependencies, and selected HTTP/MCP exposure. Report unsupported capabilities explicitly. Return source-located diagnostics, a failing exit status on errors, and human-readable or structured JSON output. Distinguish checks performed from checks skipped; a static pass does not prove database behavior.

Default validation must not persist generated files, replace registry entries, activate components, invoke application hooks, execute application DML, or publish messages. Schema-backed checking requires an explicitly selected connector and read-only metadata access. Fixture execution is a separate explicit operation using an isolated database. Reuse existing project configuration and Go/DQL declarations; do not require a new YAML component contract.

## Opt-in schema discovery and developer validation

For a build exposing the schema options, use an explicitly selected connection:

```sh
datly validate -schema -connector main -driver sqlite3 \
  -dsn /absolute/path/schema.db -dir /path/to/application \
  -format json example.com/application/components/...
```

SQLite discovery opens an existing file read-only. Other registered drivers
require an appropriately restricted discovery connection. This inspects column
and explicit-table metadata through SQLX; it does not execute behavior fixtures
or prove every database constraint. Read each report's completed/skipped stages.
Unknown metadata stays unknown. Do not expand automatic UNIQUE discovery across
drivers; authored native UNIQUE tags remain the explicit validation contract.

An operator may register `mcp/developer.New` with named Validator targets and
mount it through the existing MCP server. The `datly.validate` tool accepts only
an exact configured target name, for example `{"target":"application"}`.
Project paths, package patterns and optional connector/refiner authority are
configured by the operator, not supplied by tool callers. Static and schema
targets can be configured separately. The response carries the same report in
text and structured content and marks unsuccessful validation as an error.
This capability does not activate components, emit files or execute application
hooks/DML. Native wire tests cover discovery and calls; operators still need to
configure and expose the service in their own environment.

## Skill metadata is not application configuration

The YAML frontmatter in `SKILL.md` identifies this skill. Optional `agents/openai.yaml` supplies assistant UI labels and a suggested prompt only. Neither file is a Datly component definition or a runtime dependency. Application developers author DQL and/or Go shapes, tags, and interfaces; they do not need to author this skill metadata.

## Tool discovery, not invented API calls

The target environment may provide a Datly developer MCP server. The combined candidate
exposes seven named
tools: `datly.transcribe`, `datly.validate`, `datly.run`, `datly.stop`,
`datly.components`, `datly.inspect` and `datly.reverseDQL`. Inspect the connected
server's actual tool list, URL, authentication, schema version and feature flags
before calling anything. If only validation is available, produce the authoring
files and record the missing workflow tools.

Map the available tools to these operations:

| Capability | Required information/result |
| --- | --- |
| Inspect project/package | Module path, package path, existing components, resources, generated versus authored files |
| Inspect connector/schema | Approved connector identity, dialect, tables, columns, nullability, defaults, PK/unique/FK constraints |
| Resolve Go shape | Full package/type identity, aliases, fields/tags, methods, collection/pointer shape |
| Inspect language capabilities | Grammar/version, supported directives, providers, codecs, predicates, generic writer and reload features |
| Parse/validate DQL | Source-located diagnostics and normalized authoring contract |
| Preview generation | Input/output shapes, physical/logical fields, routes/tools, dependencies, generated files and proposed diff |
| Validate behavior | Reader/writer plan, identity/presence policy, required constraints, hooks and parameter limits |
| Run fixtures | Isolated SQLite setup, invocation inputs, response/status and post-operation assertions |
| Create/update component | Exact target package, selected artifacts and revision-aware write result |
| Stage/activate generation | Atomic replacement of component/types/resources with expected prior revision |
| Inspect exposure | Effective public routes/tools/resources and private dependency closure |

If a needed tool is missing, produce the intended authoring files/contract and identify the missing capability. Do not claim creation, publication or tests occurred.

Gateway MCP folder resources and embedded application skills are candidate features.
The exact folder declaration is
`#setting($_ = $mcp_folder('docs', 'guide', 'skill://app-guide/'))`.
Generated bundles must be regenerated from the final canonical `llm` folders
with the implementation's skillpack tool at integration time. Do not edit
embedded generated copies or treat them as the source of truth.

## Request intake

Collect only information that materially changes the component: purpose; reader/writer/custom; operation; public shapes; connector; tables and constraints; parameter sources; identity tuples; relation links; allowed client selectors; business checks; authorization; error body/status; publication side effects; package exposure.

Use project conventions and existing shapes when clear. Ask about ambiguous write semantics or production effects; do not ask the user to supply Datly internal structures.

## Safe authoring loop

1. Inspect the current package, schema, shapes and tool capabilities.
2. Draft the declarative DQL graph and application Go hooks (or preserve an explicitly requested existing Go contract).
3. Parse and validate. Resolve unknown types/aliases/columns/providers instead of fabricating substitutes.
4. Discover and select the `gen` operation with pure Go output. Preview the resulting files, public schema, generated binding/Previous reads, dependencies and exposure. If `gen` is absent, report it and keep the authoring artifacts; do not substitute translation.
5. Exercise data-driven SQLite fixtures and inspect actual responses/DB state. For a vendor-sensitive feature also validate that vendor's dialect and limits.
6. Apply the approved development change using expected revision/hash where supported.
7. Re-read the resulting component and verify generation, schema and route/tool visibility.

A create/update request authorizes normal changes to the selected development component. It does not authorize live production DML, schema deletion, external message publication, credential changes, or exposing every dependency package. Use isolated fixtures; obtain direction when real external effects would be necessary.

## Required preview for readers

Show source view aliases, output fields/types, relation cardinalities and key links, DerivedView slots, hidden backing columns, parameter sources, selectors, predicates, codecs, pagination, cache policy and hook/batch semantics.

Check that internal fields are not in the business MCP schema or client field selector allowlist. They must still be fetched when transformations or joins need them.

## Required preview for writers

Show writable versus auxiliary tables, original identity tuples, insert/update policy, input/output types, Has behavior, previous-state query, framework/DB validation, invariant groups, custom hooks, sequence and link producers, ordered writes and finalization.

Require evidence that an explicit zero identity is preserved, missing identity is not inferred from zero, and a matched row's final IDs/links agree with the DB. An error must stop/roll back managed work while preserving its explicit public body.

## Dynamic generation and reload

For DQL stored in a DB or mutable resource store, stage the new DQL, shapes, handlers, resources and exposure as one generation. Validate before activation. Use an atomic revision-checked replacement; failed staging leaves the old generation active. In-flight requests must remain on one coherent generation. Never replace only the type name while old components still reference incompatible fields.

Persisted shapes retain existing field order and append new fields. Preserve authored hooks/handlers and decline to overwrite edited generated files. The tool should report conflicts, not silently discard edits.

## Side effects and retries

Authoring retries: retry idempotent inspection/validation when safe. For create/update/activate, use operation IDs or revision preconditions and inspect an uncertain result before repeating it.

Business read retries: use the framework/driver's supported retry policy, bounded by attempts/deadline and cancellation. Do not layer arbitrary sleep loops over existing driver retries. No duplicated rows, partial relation attachment, codec effects or hook calls.

Business writes: never blindly retry unknown completion. A caller-pending/unknown transaction is not a confirmed rollback or a safe replay signal.

## Deliverable checklist

Return the actual artifact paths and public component identity, authoring mode, DQL/Go shape, handler/hook interfaces, data dependencies, exposure, validation/error semantics, fixture results and unresolved requirements. Keep credentials, private diagnostics, internal Has markers and raw connection handles out of examples and generated public schemas.

## Candidate resources and declared skills

The combined candidate has seven developer tools: `datly.transcribe`,
`datly.validate`, `datly.run`, `datly.stop`, `datly.components`, `datly.inspect`
and `datly.reverseDQL`. These are authoring operations; runtime business tools
come from application exposure. Discover the actual connected schemas.

Resource folders use `$mcp_folder('docs','guide','skill://app-guide/')` or the
host's native folder configuration. Explicit `Folder.Skills` roots alone become
skills. A descendant `SKILL.md` is an ordinary supporting file unless declared.
Final SEP-2640 `skills/list` and `skills/get` describe compiler-sealed static
bytes; `resources/list` and `resources/read` must agree with their digest/size
inventory and authorization. Metadata JSON cannot replace native static authority.
Optional directory support is not advertised. Reading resources grants no tools.

Canonical links, including those inside references, are skill-root-relative.
Keep exact product imports in `packaging.json`; use the native skillpack generator
for both filesystem and embedded copies. Never hand-edit generated copies or
install raw source folders with unresolved product dependencies. The canonical
packaging profile records exact maintained imports and excludes implementation
and test-source trees. Generation availability is described at the start of this
reference; inspect the connected build independently.
