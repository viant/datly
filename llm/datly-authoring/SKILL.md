---
name: datly-authoring
description: Discover and apply the canonical Datly 1.0 reader, writer, and custom-component skills from a Datly v1 checkout or Git ref. Use for Datly 1.0 DQL, component architecture, project layout, transcribe/build workflows, runtime MCP exposure, cubes, composition, and skill resources. Do not use for legacy Datly repository/view/service APIs.
---

# Datly 1.0 Authoring

Before introducing a workaround, custom query-control path, or framework change,
apply [Datly native-first](../datly-native-first/SKILL.md). Prove the existing v1
alternative first; remove demonstrated duplication instead of preserving it because
it already has tests.

Use the selected Datly 1.0 source tree as authority. Never substitute legacy
Datly package conventions, DAO/service forwarding layers, route YAML, or the old
`repository`, `view`, `service/session`, or `service/operator` APIs.

## Understand the graph before changing it

The outer DQL SELECT/FROM/JOIN declares a typed data graph. Its views are nodes;
ON expressions supply relation keys, and declared cardinality supplies one-to-many
or one-to-one attachment. Outer JOIN/LEFT JOIN/CROSS JOIN spelling is not a SQL
execution strategy and does not supply SQL outer-join filtering semantics. SQL
inside a view remains ordinary executable SQL. Do not confuse these two layers.
Preserve complete relation keys and the native to-one marker where authored.

Trace the selected version's DQL → compiled graph → generated Go tags → runtime
consumer before explaining a failure or proposing a change. An error in a temporary
metadata supplier is not evidence of a Datly regression. Reproduce unchanged DQL
through the same authoring path and compare the existing native path first.
For regeneration diagnosis, read the field notes' “Regeneration evidence” section.

For MCP-only tasks, author projected field metadata with `tag(view.column, '...')`
and input metadata with the existing `.Tag(...)`/`.WithTag(...)` options. Regenerate
Go; preserve SQLX, REST JSON, graph cardinality, persistence ownership and envelopes.
Align matching reader/writer MCP names and enums without merging their components.
Do not invent a new fluent option when the existing tag mechanism owns the metadata.

## Discover the authoritative skills

Run:

```sh
bash scripts/discover.sh [datly-repository] [git-ref]
```

The script selects only a Datly 1.0 tree containing `spec`, `bootstrap`,
`transcribe`, `application`, and the three canonical `llm` skills. If the current
checkout is not v1, it prefers `origin/v1` without checking it out or changing a
dirty worktree.

Read the selected `SKILL.md` completely before acting:

- `llm/datly-reader/SKILL.md` for GET/read graphs, relations, selectors,
  predicates, reports/cubes, and ordinary data-backed components.
- `llm/datly-writer/SKILL.md` for generated PATCH/POST/PUT mutation graphs,
  presence, validation, transactions, sequencing, and lifecycle hooks.
- `llm/datly-custom-component/SKILL.md` for application-owned orchestration,
  component dependencies, external systems, or behavior not represented by a
  generated reader/writer.

For mixed work, read only the applicable skills, then read every reference those
skills route to for the requested features. Architecture and source-layout work
normally requires `references/concepts.md` and `references/project-build.md`.
DQL authoring requires the selected skill's complete DQL grammar plus the
relevant reader, writer, or custom examples. MCP, cube, resource, and skill work
also requires the linked operational/resource references.

When reading a Git ref, use `git show REF:path`. For compilation or commands that
need a filesystem tree, archive the ref into a temporary directory. Do not
checkout, reset, clean, or otherwise mutate the user's Datly repository.

Before changing project layout, schema discovery, embedded SQL, route variants,
or generated mutations, read [Datly 1.0 field notes](references/v1-gotchas.md).
It records verified integration behavior and connected-build gaps that are easy
to misdiagnose as DQL errors.

## Apply the Datly 1.0 model

- Model a backend capability as a typed component with input, output, data
  dependencies, behavior, and selected HTTP/MCP exposure.
- Do not recreate DAO/service/controller layers merely to forward a query or
  mutation. Generated readers and writers own framework plumbing; Go hooks own
  business rules; custom components own genuine orchestration.
- Discover authoring from the project root. Recursively inspect the configured
  DQL root, such as `project/dql`, and accept any nested source package rather
  than requiring the caller to supply one legacy package identity. Keep
  independent reader and writer components in separate folders so lower-level
  operation transcription remains unambiguous.
- Honor `project/dql/<packageName>/Component.dql` and require every component to
  declare `#package`. For dynamic DQL, this may be a stable virtual package that
  does not yet exist as Go source; it still supplies type/package authority. If
  the component is later transcribed, the same module-relative or
  module-qualified value becomes its generated destination and must then satisfy
  Go package/destination rules. Source discovery still starts at project root;
  users do not manually select this package to activate a component.
- Use `datly transcribe get|patch|post|put` for generated components. The
  operation must agree with route behavior. Do not infer writer policy from an
  HTTP verb alone.
- Use custom Go components only when the standard reader/writer contract is
  insufficient. Do not invent a DQL custom-handler directive.
- Preserve package/type identity, typed bindings, complete relation keys,
  authorization predicates, presence semantics, and transaction ownership.
- Separate authoring-time developer MCP capabilities from deployed business MCP
  tools and resources.
- Represent named route tools with Datly 1.0 MCP exposure metadata. Treat cube
  and cube-compose tools as report-derived components. Treat skills as explicit
  validated resource-folder roots backed by the generation resource store.

## Generated writers and lifecycle hooks

For database save, create, archive, patch and delete behavior, use the operation-based Datly v1 writer and typed lifecycle hooks. Put the complete writable graph, Current reads, auxiliary reads, identity, presence, relation and exposure contract in DQL. Generate with `transcribe post|patch|put`; hooks contain business validation, authorization, normalization and declared lifecycle behavior.

When the user requires writers with lifecycle hooks, do not replace them with custom handlers, manually constructed writer structs, a repository/DAO, or a transaction wrapper. `transcribe handler` only generating endpoint registration does not satisfy writer migration. Application production code must not issue `database/sql` Query/QueryRow/Exec calls, use a raw `*sql.DB`/`*sql.Tx` for persistence, or obtain a `TransactionSQLProvider` to execute SQL. Reads belong in transcribed readers or the writer's declared Current/auxiliary graph; DML and transaction completion belong to the generated writer runtime.

Before claiming completion, audit production sources separately from test setup/assertions and generated SQL assets. Verify actual writer generation, stable regeneration, lifecycle validation/authorization, sparse presence, cross-owner rejection, rollback and persisted outcomes with local fixtures. A compiled route, a handler wrapper, or a passing mocked service test is not writer proof. If the required behavior is not expressible in the selected v1 build, identify the precise framework gap rather than reverting to direct SQL or a custom-handler substitute.

## Verify against the selected build

### Native reader capability check — required before implementation

For filtering, sorting, pagination, projection, joins or aggregation, inspect the
selected Datly 1.0 reader contract and an executable example before writing
application logic. Verify predicate spelling in that version's registry and tests;
do not infer names from UI operators, another Datly version or memory.

Use the native reader predicate for database-backed text filtering. Do not fetch
rows and filter names in Go (`strings.Contains`, regular expressions or equivalent),
or duplicate a supported predicate with handwritten wildcard SQL. Keep business
authorization checks; they are not a substitute for query filtering. Apply the
predicate before pagination/counting and expensive relation expansion.

Verified v1 example: the registry key is `contains` (not `contain`), declared as
`.WithPredicate(1, 'contains', 'r', 'name')`. Recheck it against the selected build:
`runtime/predicate/velty/registry.go`, `doc/reader-predicates.md`, and
`runtime/handler/reader/sql_template_test.go`. For typed components, read the
selected reader's predicate-tag contract rather than inventing a tag.

Trace raw input through UI, datasource projection, typed input, predicate and SQL
bindings. Exactly one layer owns wildcard construction. Test the actual native
datasource and reader path with present/absent search, relevant wildcard/collation
semantics, filtering before paging, and unauthorized rows. A transport stub or
render-only test cannot prove this contract. If native support is missing, document
the exact version, lookup and minimal failing native test before proposing a
framework change; do not silently implement a Go filtering fallback.

Build or use the selected Datly 1.0 CLI, inspect its actual help, then validate
or transcribe. Do not assume an MCP developer server exists merely because the
source contains MCP packages.

Prefer project-root commands when the selected Datly 1.0 build exposes them:

```sh
datly validate -dir PROJECT -format json
datly transcribe get|patch|post|put -dir PROJECT [schema options]
datly build -dir PROJECT -o OUTPUT
```

If the connected v1 CLI still requires module-qualified package arguments,
identify that as a project-root discovery gap. Tooling may enumerate discovered
packages internally as a temporary bridge, but user-facing Studio APIs and Forge
must remain project-root based and must not expose legacy package selection.

Use schema discovery only with an authorized connector and database. A syntax
parse is not proof of type, provider, resource, schema, or runtime validity.
Report missing Datly 1.0 capability precisely instead of using legacy APIs or
inventing syntax.

## Deliver

State which Datly 1.0 skill/ref was selected, component boundaries, source and
destination packages, DQL/Go artifacts, exposure choices, commands run, and any
connected-build gap. Distinguish verified behavior from proposed architecture.
