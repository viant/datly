# Implementation and release status

[All guides](README.md)

**Published-dependency baseline verified.** The integrated reader, writer, cache,
output, resource, documentation and MCP implementation has passed the complete
repository suite against published dependencies. The release module pins the
published xdatly SDK and native versions in `go.mod`, without local dependency
replacements. Subsequent explicit lifecycle naming, output naming and documentation
corrections are integrated; their final regression checks are described below.
No Datly `v1.0.0` tag or completed release is claimed.

| Surface | Integrated behavior / acceptance boundary |
| --- | --- |
| Project init/build | Automatic Go-selected traversal and internal linking are present; parent real-TCP endpoints, SQLite read/mutation and add/remove rebuild checks pass. Runtime remains source-backed. |
| Reader/writer generation | CAST/imported types, pointer changes, owned column drop and `AND 1=1` to-one transitions are documented with protected regeneration. |
| Selector names and duplicate columns | Exact authored names and explicit mappings are enforced; duplicate output names are errors. The reviewed naming, marker and cube corrections are integrated, with focused main and live-cache tests passing. |
| Multiview formats | JSON/CSV/XML/tabular/XLSX projection and singleton corrections are integrated; affected main race tests pass. |
| Singleton/body output | Scoped approval covers direct, named `output/view` and named `output/body`, including CSV/tabular and JSON envelopes. |
| AFS/Aerospike | Independent authored backends and native wiring are integrated. Bounded AFS and live Docker Aerospike race tests pass, including groups, pagination, empty results and table-drop replay with the naming corrections. |
| Warmed full projection | Ordinary narrowing and cube measure subsets retain all grouped dimensions. Parent AFS/Aerospike race acceptance covers these regular/cube narrowing cases; older sandbox failures are not current capability blockers. |
| Async | Scoped rebase approval covers capture/replay/results/differ, current authorization and completion ownership. Standalone Jobs/AFS/watch/HTTP configuration uses a mandatory linked host authorizer; absent policy rejects startup. See [configured async](../standalone/ASYNC.md) for settings and authorization. Complete standalone/config race tests pass, including network tests. |
| CORS/services/static | Configured policy, native services and corrected static-root authority are integrated. A configured path cannot grant filesystem authority to itself. |
| Shared YAML docs/embeddings | Global/rule dictionaries, schema resources and OpenAPI/MCP enrichment are integrated; generated embedded resource proofs are distinct from custom-build deployment. |
| Developer/business MCP | Seven developer tools, business exposure, resource folders and native Final SEP-2640 static skills are composed. The authoring-root provenance correction has bounded parent approval. Declared roots alone become skills. Optional directory support is not advertised. |
| Metadata resolution | The reviewed catalog/resolver optimization is integrated; race checks preserve detached public descriptors. |
| Native observations/optional async OTel | Native capture and bounded default-off export remain separate; no zero-overhead or production-capacity promise. |
| Mutation/schema boundary | Resolved identities, immutable original presence, scoped Previous matching, typed indexes, recursive DTOs, sequencing and final validation are integrated. Stable keys from an authorized existing parent can supply a new child's declared link; pending-reference receipts retain their narrower INSERT requirements. See [mutation lifecycle](mutations.md). Universal constraint discovery is not promised. |
| DQL package destinations | `#package` selects the generated component package. DQL controls individual shapes and files; generated entity methods follow their owning package. Plain filenames are the default, with explicit optional prefixes and per-file overrides. Split-package and protected-regeneration tests pass. |

## Dependency tooling

`go mod tidy` and `go mod verify` succeed with the pinned public modules. The
former dependency-test import of a removed Datly package has been corrected in
the published dependency. Development workspaces may use local replacements;
release module manifests use published dependency versions.

## Verification meaning

Source links and existing harness cases identify owners and practical examples.
The separate delivery audit records exact snapshots, commands, results and patch
hashes. Public guides do not navigate host paths or temporary implementation
reports. Focused tests are evidence for their exercised behavior, not certification
of all drivers, cloud deployments, cache combinations or production performance.

## Latest authoring corrections and verification

Lifecycle type selection is explicit through the outer DQL
`lifecycle_type(view, 'TypeName')` declaration. This change replaces the older
`entity_hooks` spelling and removes inferred lifecycle struct names. Focused
generation, regeneration, hookless-operation and unsupported-target checks pass;
the full affected-owner regression run also passes. Reader request initialization remains on the declared input
contract; reader row hooks and output finalization are separate concerns.

Complete reader examples explicitly declare output holders and global casing.
The published Structology correction applies global casing to `format` names
while preserving exact nonempty JSON names; output and wire-schema tests pass.
Skill packaging now validates links relative to their containing documents.

Grouped/custom predicate examples and imported handler aliases are integrated.
The comprehensive grammar reference and EBNF are integrated, with the DQL +
Velty extension documented separately at the end. Per-instance constant expansion
from YAML/JSON files is integrated for discovery, execution and configured resource
paths, with immutable authored sources. CLI and generated-code regression checks
pass. Unquoted `${project}.dataset.table` parsing still awaits the published native
parser update; supported quoted forms and `$project.dataset.table` are documented
in [instance constants](constants-and-substitutions.md). Release publication and
version tags remain separate from local commits and verification.

## Production review follow-up

The independent production review identified additional work beyond the earlier
suite baseline. The gateway now defaults to noncredentialed CORS, requires
operator opt-in for diagnostic metrics, classifies external binding failures by
typed errors, hides unknown internal error details, retains private panic
information, and applies header/idle timeouts. API-key checks use a shared
constant-time comparison. Published Bindly and Structology updates supply the
native error and custom-serialization fixes; HTTP/MCP/output race checks pass.

Pagination rejects negative values and overflowing page/limit products before SQL
execution. OpenAPI supports custom JSON fields conservatively and text-serialized
fields where their wire representation is known, without invoking user codecs.

Sequencer dialect/concurrency corrections are integrated against the published SQLX
dependency. MySQL retains the unchanged original transient-transaction mechanism by
default; the table-based allocator is an explicit option. Generated delete-marker and
token-validation policies remain a separate release gate with their SDK dependency.

## Numeric sequencing

MySQL defaults to the original SQLX transient transaction mechanism. The three
original implementation files remain unchanged; the allocator table is explicit
opt-in. The canonical `sequence_strategy` DQL setting is carried through generated
metadata to the root invocation Data source. PostgreSQL 10+ exact values and
SQLite native reservations retain their native defaults. See
[numeric sequencing strategies](sequencing.md) for options and observed limits.
