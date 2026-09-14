# Implementation and release status

[All guides](README.md)

**Release validation in progress.** The reader, writer, cache, output, resource,
documentation and MCP changes described here are integrated into the development
tree. The source baseline was reported with passing main and SDK full suites. The remaining
corrections below still need acceptance and regression checks before release. The local `v1` release copy pins the published xdatly SDK
`v0.5.4-0.20260914204318-08752d9972c1` and the native versions in `go.mod`,
without local dependency replacements. No published Datly 1.0 version is claimed.

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
| Mutation/schema boundary | Integrated relation-produced FK deferral is restricted to captured parent INSERTs and validates final values before Queue. Main SQLite ordinary/self/composite acceptance passes. Direct recursive Velty DTO registration exposes a native selector-expansion bug still being corrected; adapter-based acceptance does not prove that direct path. Complete schema constraint discovery and automatic UNIQUE inference are not promised. |
| DQL package destinations | Project root remains the external destination. The extension that lets existing DQL directives control separate component/shape packages is under correction: generated entity methods must follow their owning shape package. It is not yet integrated or accepted. |

## Verification meaning

Source links and existing harness cases identify owners and practical examples.
The separate delivery audit records exact snapshots, commands, results and patch
hashes. Public guides do not navigate host paths or temporary implementation
reports. Focused tests are evidence for their exercised behavior, not certification
of all drivers, cloud deployments, cache combinations or production performance.

The documentation author's sandbox denied TCP and Aerospike access, but parent
acceptance ran successfully in an environment that permits them. Preserve that
provenance: do not claim the author executed those runs or present the earlier
sandbox restrictions as missing implementation. Parent live cache/TCP acceptance
does not establish production scale or source-free deployment.

Remaining release work includes DQL destination support for generated entity
methods, native recursive Velty selectors, and final regression and documentation checks
against the pinned published dependencies. The SQLX scanner update is already
pinned to its published version in this release copy. Regenerate the canonical skill bundle from its source
inputs after those corrections. No hand-edited embedded copies are authoritative.
