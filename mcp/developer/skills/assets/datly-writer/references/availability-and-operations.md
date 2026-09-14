# Mutation availability and operational boundaries

Use this reference when a writer needs produced IDs/FKs, async execution,
completion, telemetry or published documentation. Keep
[writer-contract.md](references/writer-contract.md), [writer-examples.md](references/writer-examples.md)
and the explicit [JWT pattern](references/tags-and-interfaces.md#jwt-input-and-authorization-predicates)
as the authored baseline. Required behavior is not automatically available in
every connected build.

## Select high-level generation

Use a reader-like DQL graph, auxiliary tables in parentheses, DQL entity hooks
and invariant tags, and explicit `gen` operation with pure Go output. The
implementation is delivered in an isolated review worktree and is under review;
release CLI availability is not established. Discover the connected capability
using [developer-mcp.md](references/developer-mcp.md). Missing `gen` is a reported
gap, not a reason to author body/output/Current plumbing or use translation.

## Stable IDs and relation-produced validation

Sequence allocation supplies stable IDs before Queue. It does not change original
insert/update classification or original presence. Pending supplied IDs must be
visible to sequencing. Complete original tuples determine identity; do not match
Previous by a prefix, current zero values or child position.

Verify relation-produced validation on the connected build with normal, self and
composite parent/child cases. Metadata acceptance or local sequence support alone
does not prove that generated children receive valid foreign keys before Queue.

Preserve the required contract: only an originally absent field on an exact
captured authorized INSERT edge may await production, with verified parent INSERT,
matching native target/columns/values, same transaction and correct queue order.
Supplied nil/zero, UPDATE, unrelated roots or same-Go-type roles must not inherit
that permission. Final Go/NULL/UNIQUE/reference checks see reconciled values before
Queue. Do not disable database FK checks, flush early or broaden deferral to make
a fixture pass. Use [mutation status](references/writer-contract.md).

Schema metadata absence means unknown. NOT NULL support does not establish full
UNIQUE/reference discovery. Authored native UNIQUE tags remain the explicit
validation contract; do not pursue unrequested automatic UNIQUE reconstruction.

## Completion, messaging and async

Use the existing [message-hook reference](references/mutation-messages.md) for typed bus
injection and `outcome.CommitConfirmed()`. Queue and caller-owned flush do not
confirm commit; a bus error after commit is not rollback. Assemble event IDs only
after their sequence/reconciliation phase when needed. No exactly-once/outbox
claim follows from publishing after commit.

Programmatic jobs preserve the original 34-column `DATLY_JOBS`, canonical State,
AFS publication/dispatch and native cache/metrics. A mandatory authorizer must
refresh current access. Do not invent a table, scheduler, binder or stored read
provenance. The candidate includes explicit HTTP controls, canonical source replay with fresh
Current/Previous read provenance, and native differ/output integration. Standalone
declarative async still requires an explicit authorization integration. Pending or
unknown completion retains RUNNING/event state; never advise blind replay.

Read [async](references/mutation-messages.md#async-and-dry-run) for enablement, defaults, publication
failure, terminal write-back failure and shutdown. Reader dry run is not a mutation
or external-side-effect sandbox.

## Supporting services and publication

- [Configuration](references/project-build.md): linked Go factories and
  contracts must be compiled into the executable. Preserve exact accepted CLI
  flags. Current standalone service configuration includes CORS, APIKeys,
  warmup admin, OpenAPI startup exports and Observation/OTel. Parent custom-build
  real-TCP acceptance passes; collector-drain qualification remains separate.
- [Observability](references/product/datly/doc/observability.md): native capture exists;
  optional OTel has an application-owned bounded exporter. No performance numbers
  or zero-cost claim without workload measurements.
- [Cache/warmup](references/product/llm/datly-reader/references/cache-and-operations.md): independent AFS/Aerospike
  choice and explicit TTL; generated mutations do not own read-cache invalidation.
  Parent race acceptance passes AFS and live Docker Aerospike narrowing, regular/cube, groups, pagination and table-drop replay. Earlier author sandbox denial is not a capability blocker; no production-scale qualification is implied.
- [API documentation](references/product/datly/doc/api-documentation.md): preserve explicit
  required/error metadata. Shared global YAML dictionary plus per-rule override
  publication to OpenAPI/MCP is present in the candidate, pending release integration. Settings
  are `$DocGlobalURLs(...)`, `$DocURL(...)`, `$DocURLs(...)` and `$DocBaseURL(...)`;
  YAML sections are `Columns`, `Filter`, `Parameters`, `Paths` and `Responses`.
- [Custom output/finalizers](references/product/llm/datly-custom-component/references/output-and-operations.md): candidate SDK
  direct HTTP bytes have independently authored media/schema documentation. Current
  SDK/runtime injector finalizer uses `handler.InjectorFinalizer`,
  `handler.InjectorLookup`, `kind=caller_output` child inputs and destination
  `bind` tags. Static serving requires explicit root/resource authority and policy; a
  `ContentURL` string cannot grant filesystem authority.

Use [developer MCP](references/developer-mcp.md) for the seven candidate tools,
business exposure, folders and declared Final SEP-2640 static skills. Regenerate
embedded/filesystem bundles from canonical sources and exact product imports.
Use [project build](references/project-build.md) for automatic traversal and linking,
without mandatory user init/Register/import lists. Keep source-backed deployment
inputs and the pinned release dependency graph.

Reader/writer regeneration must propagate CAST pointer/value changes, generated
column removal and `AND 1=1` Many-to-One and One-to-Many holder changes while
preserving authored hooks, tags, methods and order. See [generation](references/developer-mcp.md#operation-based-generation-to-pure-go).
The release copy enforces exact names, user-defined aliases only,
and duplicate output-column errors. Never invent an alias to repair a collision.

Return tested authored artifacts and exact connected-build gaps. Final release
regression remains required. Use [formats](references/product/datly/doc/selectors-and-formats.md)
for JSON/CSV/XML/XLSX, singleton/body and raw-response boundaries.
