# Datly 1.0 field notes

These notes capture observed Datly 1.0 behavior. Recheck them against the
selected v1 commit; a later implementation may close a recorded gap.

## Authority and version selection

- Fetch the requested v1 ref before claiming it is latest. Record the commit.
- Do not inspect the legacy default branch as Datly 1.0 authority.
- A dirty Datly checkout is not permission to switch branches. Use `git show` or
  an isolated `git archive` tree for builds.
- Re-read canonical `llm/*/SKILL.md` files after updating the v1 ref; those files
  evolve with behavior.

## Project-root discovery versus CLI package arguments

- Keep authoring and instance discovery separate. `transcribe` scans Go/DQL
  source with AST metadata. `run`/`start` resolve `GoBootstrap.Packages` only
  from linked runtime reflection (`xunsafe.PackageTypes`) for components,
  predicate handlers, codecs, and embedded-resource capabilities.
- `bootstrap/index.Builder` is the project/bootstrap index. It scans selected
  packages, indexes route and MCP identities, fingerprints DQL/Go/resource
  sources, expands report-derived cube/compose entries, and supports lazy
  generation materialization.
- The current index still requires nonempty `Config.Include` patterns. A host can
  provide one recursive project-root pattern; users need not select individual
  packages.
- The current `datly validate` and `datly transcribe` CLI still require explicit
  module-qualified package arguments. Treat this as a CLI surface limitation,
  not a reason to expose package selection in Studio or another product UI.
- `transcribe` still expects exactly one component in the selected source
  package. Separate reader and writer source folders when using this command.

## Project initialization

- Datly v1 uses a user-owned blank-import link, empty generated component
  `init()` functions, component-holder metadata/resources, ephemeral CLI
  generation, and package-scan enforcement.

- `datly init -dir PROJECT` is the Datly 1.0 project-root initializer.
- For an existing Go module that already requires `github.com/viant/datly`, do
  not pass a conflicting `-module` or dependency pin. Init preserves existing
  files and creates missing files with exclusive create semantics.
- The scaffold adds `cmd/datly`, `datly.yaml`, the user-owned
  `internal/datlylink/link.go`, and `generated`, `hooks`, and `resources` roots.
  It does not transcribe DQL.
- New modules require an exact Datly pin or an explicit local mapping. Mutable
  versions such as `latest` are intentionally rejected.
- Init is not a dependency updater after initialization. Use explicit Go module
  operations for later dependency changes.
- `internal/datlylink` is explicit application selection policy. It blank-imports
  selected component packages and contains an empty `init()`; `cmd/datly`
  blank-imports only this link package. There is no `UseDefaultImports` call or
  central holder list. Generated component package `init()` functions are empty.
- `datly build` never rewrites the user link package and does not persist a
  `link_gen.go`, checksum, registry, or build sidecar. Authored DQL must be
  transcribed or materialized through the project index separately.
- Bootstrap still scans only `GoBootstrap.Packages`. Blank imports supply linked
  Go type/resource authority and do not register contracts. Generated components
  and lifecycle types expose collision-free `reflect.Type` anchors so runtime
  package scanning survives linker dead-code elimination.
  `x.Registry` remains a dynamic-struct/factory fallback, not linked Go authority.
- Generated holders expose assets with `EmbedFS() *embed.FS` and
  `EmbedNamespace() string`; resource authority must be propagated through both
  initial index loading and lazy transcription/materialization.

## Generation-owned resources

- Datly v1 commit `ab47118d` adds `application.Build.Shutdown` for stage-local
  external resources such as database handles. The application manager invokes
  it for rejected stages, retired generations after their request leases drain,
  and the active generation during manager shutdown.
- A host that opens connectors while compiling a generation must return their
  closer through this callback. Do not retain all historical handles in one
  process-global slice or close them immediately after publication.

## Universal writer generation

- Standard POST, PUT, and PATCH transcription emits shapes, input setters,
  typed read indexes, component metadata/resources, and optional lifecycle
  hooks. It does not emit component-private frames, actions, invariants,
  snapshots, layout, validation, or mutation-program files.
- `runtime/handler/writer` compiles the component/tag graph once into immutable
  metadata and creates one invocation-owned `Program` per request. Invariant
  groups, sparse presence, Previous matching, typed hook roles, batching,
  sequencing, links, validation, DML, output, and finalization are interpreted
  there.
- Root body fields carry canonical `view` metadata for writer table and lifecycle
  authority. Regeneration may update that generated `view` tag while still
  rejecting unrelated authored tag changes.
- External lifecycle types are linked by collision-free type anchors in the
  generated component file. Local generated lifecycle types own equivalent
  anchors in `lifecycle.go`. No `init` registration is used.

## Required package directive

- Every authored DQL component requires `#package` under the current authoring
  contract.
- Dynamic DQL may use a stable virtual package that has no existing Go files.
  The package still supplies type authority. If later transcribed to Go, it must
  satisfy generated destination and Go package rules.
- Generate dynamic package values server-side from immutable owner/report keys.
  Raw mutable usernames are not stable identities; normalize a readable prefix
  and append a hash to prevent collisions.
- Datly v1 commit `5d054cb4` makes the current `#package`/source package scope
  shadow same-named imported types for both package and transcribe authority.
  An unqualified `Input` therefore resolves to the component package even when
  an imported auth component also declares `Input`; use a qualified import only
  when intentionally selecting an external contract.

## Component boundaries and names

- A Datly component owns the DAO and service behavior for one capability. Do not
  recreate a forwarding DAO/service/controller stack.
- Prefer a reader component plus a PATCH writer component per aggregate.
- Go package names should be short lowercase words. `reader` and `writer` match
  Datly terminology better than redundant filenames such as
  `connector_read.dql` inside an already named package.
- Keep DQL and SQL filenames lowercase snake case.

## WithURI route expansion

- `WithURI('/{id}')` on a transport input is reader route activation.
- `bootstrap/routes.Compiler` materializes the base collection route and an
  alternate path route. It appends `By<Fields>` to copied named MCP tool
  exposures, for example `read` and `readByName`.
- The activated identity parameter can be required because it is inactive on the
  base route.
- Do not create a separate get/list component when one reader plus `WithURI`
  expresses the contract.
- Do not apply `WithURI` merely to combine unrelated writer operations.

## Stateless reader builder

- `authoring/readerbuilder` edits complete DQL source without server-side editing
  sessions. It supports fields, predicates, projection functions, settings, and
  views, and returns source-preserving spans plus compiled structure.
- `readerbuilder.Config` supplies component Scope/Name and available connector,
  predicate, and cache catalogs.
- Reader-builder inspection is not schema discovery or runtime execution.
- `setSetting` covers connector, MCP, cube, and cube-compose settings. Latest
  cube-compose DQL accepts enabled plus optional MCP flag, max cubes, max limit,
  and timeout.
- Generated reader inputs retain parallel `Has` markers for transport fields.
  Predicate activation must use suppliedness: omission does not activate a
  predicate, while an explicitly supplied empty string, zero, or false does.
  Component tests must exercise both states.

## SQL resource decomposition

- Datly 1.0 supports `${embed:sql/file.sql}`. Keep route/binding/view metadata in
  DQL and database SQL in a sibling `sql/` folder when this improves ownership.
- Supply `Source.Path` and a resource store/default filesystem rooted at the DQL
  package when compiling outside normal discovery.
- The bootstrap index fingerprints declared resources; failed/missing resources
  must fail staging.
- Datly v1 commit `720cbcce` removes unresolved `${predicate...}` expansions
  only from the zero-row SQLX metadata query while preserving authored/runtime
  SQL. This lets transcribe generate complete Go reader shapes for embedded
  predicate SQL. On an earlier build, an “unrecognized token: $” diagnostic is
  a schema-discovery gap; do not remove runtime predicates as a workaround.

## Schema discovery and drivers

- Static DQL parsing does not prove primary keys, nullability, constraints, or
  database types. Generated PATCH/PUT needs authoritative primary keys.
- Datly's stock v1 CLI links `sqlite3`; it does not automatically link MySQL.
  MySQL-based rule generation requires an application/custom command that imports
  the MySQL driver, or a custom-built Datly CLI.
- A stock Datly binary cannot start an application configuration that names an
  unlinked driver. For example, Studio's `datly.yaml` intentionally uses
  `sqlite` from `modernc.org/sqlite`; launch it with the application's
  `cmd/datly` binary, which blank-imports its dependency link and database
  drivers, rather than the standalone Datly CLI.
- For Datly SQLite schema-refinement tests, use `github.com/mattn/go-sqlite3`
  with driver name `sqlite3`. The `modernc` driver name `sqlite` did not provide
  Datly's expected primary-key metadata in observed tests.
- Keep SQLite for fast unit behavior and MySQL as E2E/rule-build authority when
  that is the product requirement.

## JWT authorization predicates

- Configure `JWTValidator`, declare the required Authorization header, and bind
  `JwtClaim`; that authenticates the request. Do not attach a row predicate
  handler to the JWT parameter merely to require a valid token.
- In DQL, `JwtClaim` requires distinct source and destination types. Author
  `Jwt<string,*jwt.Claims>(header/Authorization).Required().WithCodec(JwtClaim).WithStatusCode(401)`;
  `Jwt<*jwt.Claims>` incorrectly makes the codec source type claims instead of
  the bearer string.
- Resolve a separate user/auth context component when tables need subject,
  owner, tenant, role, or ACL constraints. Use that dependency directly in SQL.
  Attach a typed handler predicate to the auth context only for genuinely
  complex authorization that cannot be represented safely in SQL.
- Predicate handlers and codecs must be ordinary linked Go types. Instance
  `GoBootstrap.Packages` discovers all linked package types by reflection;
  applications do not register them manually.
- Datly 1.0 handler predicate metadata names the concrete exported handler type,
  for example `WithPredicate(3,'handler','authorization.ReportRead')`. Do not
  carry the legacy leading `*`; the runtime creates the pointer receiver after
  resolving the linked concrete type.
- The v1 standalone configuration supports `JWTValidator` and native MCP
  authorization policy metadata. It does not currently expose Platform's legacy
  `OAuth2ConfigURL`, `IssuerURL`, or `AuthorizerMode` fields; provide OAuth MCP
  transport integration in the host until the standalone configuration grows
  equivalent settings.

## Writer key/reference authority

- Readers do not require primary-key metadata. Generated PATCH/PUT writers do.
- On Datly v1 commit `5d3bf366`, column
  refinement reuses SQLX table and foreign-key metadata. A parser-proven direct
  physical source supplies PK, auto-increment, unique, required/default, and FK
  facts; FK facts are emitted as native SQLX `refDb`/`refTable`/`refColumn`
  metadata.
- The writer fallback is also native SQLX authoring. Use
  `tag(view.column, 'sqlx:"physical_name,primaryKey"')`; use
  `refTable`/`refColumn` and optional `refDb` for an authored reference. Writer
  generation promotes SQLX key/constraint options into canonical mutation
  planning. Do not invent a standalone `primaryKey` DQL tag.
- Database identifier spelling and quoting remain unchanged. Only generated Go
  shape fields and helper aliases use the shared Tagly/typecatalog UpperCamel
  conversion (`owner_id` -> `OwnerId`); their SQLX tags retain `owner_id`.
- Resolved resource SQL can recover one physical source through SQL parser
  lineage/table APIs. If a projection is too opaque or ambiguous to prove
  lineage, keep explicit SQLX tags rather than guessing schema authority.
- Prefer authoritative schema lineage. If resource decomposition prevents key
  recovery, fix or extend the Datly column-refinement path rather than patching
  private plan state in the consumer.

## Sparse PATCH, delete, and concurrency

- Select operation `patch` explicitly; HTTP PATCH alone does not select writer
  policy.
- Generated PATCH supplies Has/presence semantics. Omitted differs from supplied
  zero, false, empty, or null.
- `delete_marker(view.column)` designates one logical boolean projected field.
  Only explicit true plus a complete authorized existing identity deletes.
  Missing rows, omitted collections, and false never imply deletion.
- `concurrency_token(view.column)` validates the captured expected token before
  update validation. It does not advance the token, add an atomic SQL predicate,
  lock a row, or prevent races by itself.
- Delete markers and concurrency tokens require generated PATCH/PUT. Readers and
  POST reject them.

## Module-root generation edge case

- On the observed v1 build, destination import validation walked a module root
  whose module basename contained a hyphen (`datly-studio`) and attempted to
  treat that basename as a Go package, failing with “invalid Go package name.”
- This can occur even when the authored destination ends in valid package names.
  Do not rewrite valid DQL packages or rename the product module as a silent
  workaround. Fix the project import-validation walk to skip directories with no
  Go package, or use a verified later v1 build where the issue is closed.

## MCP, cubes, and skills

- Explicit route tools use `spec.MCPExposure`. Alternate `WithURI` routes copy
  tool exposure with a derived `By<Fields>` suffix unless path MCP is disabled.
- Report-enabled, groupable readers derive cube components. Cube-compose derives
  another bounded component. Their current tool names derive from component
  identity unless Datly adds explicit derived-name settings.
- Skills are explicit `spec.ResourceFolder` skill roots backed by registered
  files. Validate `SKILL.md`, paths, URI prefixes, duplicate roots, and root
  escapes. A runtime exposing skills requires resource authorization.

## Reader-builder relation types

- Treat the compiled `spec.Relation` as authoring metadata authority. Its
  `Kind` distinguishes ordinary `subview` relations from `derived` outputs;
  `Cardinality`, `MatchStrategy`, `On`, and child `View` supply the rest
  of the graph contract. UI clients must not classify a relation by parsing SQL.
- Outer DQL `JOIN`/`LEFT JOIN` is decomposed graph syntax: the root and child
  views execute independently and attach through compiled links. The modifier
  does not currently change runtime attachment semantics. Express required-child
  filtering in root SQL (for example with `EXISTS`) or an explicit future
  required-relation policy; do not present join kind as a behavioral UI choice.
- `authoring/readerbuilder.ViewMutation.Kind` accepts `subview` or `derived`.
  A subview requires parent, join, SQL, and complete ON keys. A derived view is
  a root-attached `output/derived` declaration, rejects join/ON metadata, and
  receives an UpperCamel type expression through Tagly when none is supplied.
- Use the same `addView`, `updateView`, and `removeView` operations for both
  relation kinds. Keep source-preserving DQL edits inside Datly's reader-builder
  API; consumers such as Studio send typed operations and render the returned
  compiled graph.
- Use `createReader` for an empty source when an authoring client needs the
  initial root graph. Its typed payload supplies package, connector, route,
  root name, and SQL; Datly owns canonical DQL layout, Tagly UpperCamel type
  naming, output-holder defaults, compilation, and validation. Do not assemble
  the initial DQL string in a browser client.
- `updateField` and `removeField` use declaration head and option source spans.
  Updating type/source/required/query-selector preserves unrelated predicate and
  codec options. Rename is atomic and token-aware: executable `$Parameter`
  references change with the declaration, identifier boundaries are honored,
  quoted literals remain untouched, and collisions roll back. Clients must not
  emulate rename with remove/add. Removal recompiles the complete candidate
  before persistence.
- `updateRelation` patches only the SQL parser's authored `ON` span. The typed
  command names the child, intended parent, and complete equality expression;
  candidate compilation must resolve the same child under that parent before
  persistence. Use it for key changes and re-parenting rather than remove/add.
- `setColumnRole` owns cube dimension/measure authoring. It uses Tagly to set
  `groupable:"true"` for dimensions and explicit `groupable:"false"` for
  measures while preserving every unrelated struct tag. Keep the explicit
  false tag when a wildcard-projected column would otherwise disappear from
  static authoring metadata.
- Reader-builder `AvailableConnectors` is the caller's authorized active
  connector catalog, not merely the component's default connector. Validate
  typed connector edits against exact catalog names. For execution, collect
  required names from `Settings.DefaultConnector`, each view's
  `Source.Bindings.Connector`, and legacy `use_connector` occurrences while
  importing. Supply the same named handles to `column.Connections` and register
  them on one `sql.SQLComponent`; never fall back to the default DB when a named
  connector is missing.
- `use_connector(view, name)` remains valid compiled view-binding syntax, but a
  Studio/import workflow must resolve `name` to a real active connector. Do not
  recreate the legacy per-view connector configuration store or invent a
  placeholder connector to make compilation pass.
- Dynamic multi-user authoring needs server-owned package identity. In Studio,
  derive the owner segment from verified JWT `sub` (text before `@`, lowercase,
  non-alphanumeric characters removed) and keep business namespace as separate
  filter metadata. Use Reader Builder `setPackage` with an expected current path
  to rehome existing DQL; do not replace `#package` in consumer code.
- A shared runtime MCP listener requires globally unique tool names. Studio base
  tools use the same owner prefix and check other readers before persisting a
  revision. Cube/compose tools inherit owner-scoped component identity.
- Dynamic resources are versioned generation inputs. Materialize resource rows
  as a scoped default Bindly filesystem for each component and one shared named
  namespace authority for the active generation. Explicit folder rows become
  `mcpresource.Folder` plans; explicit skill-root rows must resolve a `SKILL.md`
  under that folder and pass Datly's native skill compiler during validation and
  reload. Saving a file, folder, or skill root invalidates the reader revision.

## Verification discipline

- Test source compilation, route expansion, schema refinement, generation,
  SQLite behavior, and MySQL rule/E2E behavior as separate stages.
- Log the selected Datly commit and driver. Report skipped stages.
- Never convert a connected-build gap into invented DQL syntax or a legacy API
  fallback.

## Verified migration authoring details (v1 071eeab5)

- Independent auxiliary view parameter `.WithLimit(0)` emits `view:"limit=0"`
  and executes SQL `LIMIT 0`; it is not unlimited enumeration. An empty lookup
  can therefore produce a misleading application existence/authorization error.
  For unbounded auxiliary reads, verified declaration SQL uses
  `selector_no_limit(TABLE,true)`, producing `selectorNoLimit=true`. Verify the
  emitted tag and a fixture beyond 100 rows; merely removing a zero limit does
  not prove absence of a default cap.
- Generated writer prior-state discovery currently chooses the first matching
  same-table read declaration. When both a complete Current read and a subset
  business lookup exist, put the genuine complete identity-bearing Current
  declaration first. Missing key/projection provenance can fail DELETE with
  `delete requires a matched complete identity`. Do not manufacture Previous
  rows or remove matching checks to repair this. Prefer an unambiguous authority;
  declaration-order dependence is an observed current limitation.
- A custom orchestration capability can keep DQL as sole contract authority with
  two native generation phases: generate shape-only input/output contracts into
  a separate contract package, then generate one handler registration using a
  native factory that explicitly reuses those genuine contracts. Verify the
  final recursive bootstrap inventory: one parent route, no contract-holder
  route, no dummy SQL. The generated holder's `DatlyHandler` method is detected
  by Go bootstrap and invoked by standalone source compilation; separate registry
  function registration is an alternative for explicit registry hosts, not an
  automatic requirement for a linked Go-bootstrap host.
- Component/application providers cannot supply the protected runtime logger
  kind: registering `provider.Static(LoggerCapabilityKey, logger)` in
  `command.Service.Providers` compiles and starts but fails actual requests with
  `component provider cannot supply protected runtime kind "logger"`.
  The trusted injection channel is `RuntimeCapabilities.Invocation.Logger`.
  A bootstrap diagnostics/observation logger does not automatically populate it.
  Datly v1 `12541091` adds typed standalone/command `InvocationLogger` options,
  verified with actual HTTP reader input/output, child inheritance and writer
  lifecycle/persistence tests. Nil preserves missing-capability failures; a
  spoofed application-provider logger remains rejected. Check the selected build
  before using the new option.
  Preserve protected-kind checks and avoid context/global logger getters.
  Static injection alone does not certify correlation or original span parity.
- Encrypted original key resources still require their existing Scy KMS provider
  to be linked (for example `github.com/viant/scy/kms/blowfish`). Missing provider
  startup is an application linkage issue. Preserve the verifier/signer/key
  configuration rather than replacing it with unverified claims or plaintext
  key material.

## Generated-only Steward migration (2026-10-18)

Verified authoring checkout: Datly `v1` commit `1912e5140c044344ca80b1ad456f4a3f3ee267bd`. Re-run discovery for later work rather than treating this hash as a permanent pin. The maintained writer references are `llm/datly-writer/references/writer-contract.md`, `writer-examples.md`, `reverse-engineering.md`, `dql-grammar.md`, and `doc/mutations.md`; use `doc/legacy-to-v1-migration.md` for typed composition and transaction ownership.

A source header naming a custom Factory plus `transcribe handler` generates route registration, not a native writer. Under the user's generated-writer policy, do not accept this as migration even if it borrows a Datly transaction. Remove direct SQL Query/Exec calls and TransactionSQLProvider access from business code; use generated writer graphs and lifecycle hooks, with declared Current/auxiliary reader graphs. Keep persistence SQL in DQL/adjacent assets. Tests may use database fixtures/assertions, but must invoke the real generated writer to establish behavior.

The initial Steward audit found candidate direct SQL/transaction usage beyond Pixel backfills: recommendation and site-list helpers, CTV/audio specification helpers, and Advanced Reporting patch code. Audit the whole production tree; classify framework bootstrap/schema-discovery code separately, and do not hide existing violations by checking only newly added packages.

## Regeneration evidence

Before attributing a failure to the framework, identify the exact failing phase:
DQL parsing, resource expansion, schema facts, mutation planning, Go generation,
build, or runtime execution. Record the author binary's linked Datly revision
(`go version -m`) separately from the checkout and consumer dependency revisions.

Reproduce with unchanged committed DQL. A prepared-column failure does not establish
an ordinary database-discovery regression, and passing existing generated runtime
tests does not establish that regeneration works. Compare the same input through
the relevant native owner using a local fixture; do not query production merely
to diagnose authoring.

When supplying existing Go/SQLX schema facts, satisfy the native metadata contract
completely. In the inspected v1 column refiner, result columns carry both Name and
Source, as well as types and constraints. The writer carries Source into relation
key plans and generated Current reads. Missing Source in a consumer-supplied column
can therefore cause “link column is required” even when the authored ON keys are
correct. Compare against native discovery output before changing a relation or
adding framework fallback logic. Do not guess physical lineage for derived fields.

Native discovery already parses runtime tuple predicates for metadata analysis.
Reuse its parser-owned helper when that path is needed; preserve executable SQL,
validate expression placement, and do not evaluate generated runtime methods during
schema analysis. Do not replace parsing with substring/regex recognition or infer
metadata from naming conventions. Framework extensions still require the applicable
approval and a minimal failing native reproduction.

A writer's `(TABLE)` marker, actual `(SELECT ...)` subquery and outer graph relation
are different constructs. Before changing a mutation marker, trace generated DML,
lifecycle calls, nested component writes and transaction ownership. A metadata-only
MCP change does not authorize moving persistence ownership or adding skip policies.

Review regenerated diffs against the intended scope. Preserve existing SQLX/JSON
facts through authoritative DQL tags when needed; do not hand-edit generated Go.
Verify public schema and actual input/output through the transport, including an
authorized visible-field case and the existing denied case. If a field is absent,
inspect producer authorization and projection before blaming serialization.
