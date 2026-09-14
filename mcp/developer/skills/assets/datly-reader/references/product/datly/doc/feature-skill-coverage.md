# Feature and skill coverage

[All guides](references/product/datly/doc/README.md) · [Status](references/product/datly/doc/status.md)

This is coverage of the documentation contract, not a claim that every feature
is released or that every backend was exercised. Exact audit commands/results
are delivered separately from public product prose.

| Requested feature | Guide | Canonical skill reference | Acceptance boundary |
| --- | --- | --- | --- |
| Typed readers and writer shape regeneration | [authoring.md](references/product/datly/doc/authoring.md) | reader-contract.md / writer-contract.md | CASTOverride, CASTPointerConsumer, ProjectionRemoval harness; preserve field order and authored edits |
| AND 1=1 to-one both directions | [authoring.md](references/product/datly/doc/authoring.md) | dql-grammar.md / writer-contract.md | JoinToOneHintReaderWriterRegenerationSQLite; Many→One and One→Many |
| Declared JWT and bound predicates | [security.md](references/product/datly/doc/security.md) | tags-and-interfaces.md | JWT input only; verifier plus predicate; zero protected reads on failure |
| Hooks, injector finalizers and messaging | [custom-handlers.md](references/product/datly/doc/custom-handlers.md) | custom-contract.md / mutation-messages.md | Canonical DI; both conditional branches; commit-confirmed messages only |
| Stable IDs, FK validation and sparse writes | [mutations.md](references/product/datly/doc/mutations.md) | writer-contract.md | Original presence/keys; relation-produced validation remains bounded |
| Async schema, filesystem, replay, dryrun | [async.md](references/product/datly/doc/async.md) | availability-and-operations.md | 34-column DATLY_JOBS, canonical source replay, current auth, pending completion, reader-only dryrun |
| Native spans and optional async OTel | [observability.md](references/product/datly/doc/observability.md) | cache-and-operations.md / output-and-operations.md | Native capture independent of default-off bounded exporter; overflow/drain/privacy |
| CORS and standalone services | [configuration.md](references/product/datly/doc/configuration.md) | output-and-operations.md | Explicit empty/default/disabled policy; startup exports and native lifetimes |
| Static local and embedded content | [static-content.md](references/product/datly/doc/static-content.md) | output-and-operations.md | Explicit roots, symlink rejection, embedded assets and reload |
| YAML dictionaries and schema embeddings | [api-documentation.md](references/product/datly/doc/api-documentation.md) | cache-and-operations.md | Global then rule overlays; canonical resources; OpenAPI and MCP |
| Independent AFS/Aerospike TTL | [cache-and-warmup.md](references/product/datly/doc/cache-and-warmup.md) | cache-and-operations.md | Explicit provider and TTL; no backend switching; parent live Docker AFS/Aerospike race acceptance passes; production scale unqualified |
| Warmup connector, fallback and statistics | [cache-and-warmup.md](references/product/datly/doc/cache-and-warmup.md) | cache-and-operations.md | Exact/indexed identity, dedicated connector, native lazy fill, actual stats and expiry |
| Full projection → narrower ordinary/cube measures | [cache-and-warmup.md](references/product/datly/doc/cache-and-warmup.md) | cache-and-operations.md | Parent AFS/live Aerospike race proof; regular/cube/groups/pagination/table-drop replay; all cube dimensions retained |
| View-specific selectors and pagination/filter/order | [selectors-and-formats.md](references/product/datly/doc/selectors-and-formats.md) | reader-contract.md / cache-and-operations.md | One-argument QuerySelector(view); independent policies; bound criteria |
| Exact names, user aliases, duplicate-column errors | [selectors-and-formats.md](references/product/datly/doc/selectors-and-formats.md) | all SKILL.md / dql-grammar.md | Integrated exact-name and duplicate-output corrections; final release regression required |
| JSON/CSV/XML/tabular/XLSX projection | [selectors-and-formats.md](references/product/datly/doc/selectors-and-formats.md) | cache-and-operations.md | v4 typed codec proof; null/zero/relation and concurrency limits |
| Singleton output/view, output/body and raw bytes | [selectors-and-formats.md](references/product/datly/doc/selectors-and-formats.md) | output-and-operations.md | Scoped singleton/body approval; raw authored media/schema without payload inference |
| Developer versus business MCP | [protocols.md](references/product/datly/doc/protocols.md) | developer-mcp.md | Seven developer tools; exposed business components; native auth and reload |
| Resource folders and Final SEP-2640 skills/list/get | [protocols.md](references/product/datly/doc/protocols.md) | developer-mcp.md | Declared roots only; sealed bytes; list/get/read digests; no optional directory advertising |
| datly init/build automatic traversal | [project-build.md](references/product/datly/doc/project-build.md) | project-build.md | Parent real-TCP read/mutation and add/remove rebuild pass; no user registration list |
| Source-backed deployment and release pins | [project-build.md](references/product/datly/doc/project-build.md) | project-build.md | Published SDK/native graph, local Datly mapping; source/resource runtime needs |
| Reproducible canonical skill bundle | [authoring-skills.md](references/product/datly/doc/authoring-skills.md) | developer-mcp.md | Exact packaging.json imports, root-relative links, generated-only embedded copies |

Configured standalone report discovery now uses the canonical report compiler and
registration path. See [reports](references/product/datly/doc/reports.md) for linked contract requirements,
source authorization, explicit aliases, grouping-safe cache reuse, and bounded
HTTP/native MCP evidence.
