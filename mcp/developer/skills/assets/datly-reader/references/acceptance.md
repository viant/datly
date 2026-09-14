# Application acceptance checklist

Choose fixtures relevant to the component; this is a behavioral checklist, not permission to mutate production.

## Generation contract

Start from a reader-like DQL graph with no hand-authored body/current/output
plumbing. Select each requested `gen` operation with pure Go output after
capability discovery. Inspect the generated artifact, not just parsed DQL:
authorized tuple-restricted Previous reads, auxiliary exclusion, Has/SyncPresence,
DQL hook/invariant metadata and preserved application Go hook files. If `gen` is
missing, retain the graph and hook contract and report that gap; translation
success does not satisfy this check. For dates, change only End and verify Start
backfill without Has, valid intervals, reversed dates, missing Previous evidence
and new-row completeness.

## Shared contract

- Correct public names/types, aliases resolved to the intended full package.
- Input values come from declared sources; request/header/body/path precedence is tested.
- Required, nullable, defaulted, explicit false/zero/empty/null and omitted cases differ correctly.
- Internal fields/Has markers do not appear in JSON/MCP schemas; public error bodies contain no private cause.
- Selected packages/components are public; cross-package dependencies work without becoming public.
- Existing fields retain order on regeneration, new fields append, authored hook/handler edits survive.
- Dynamic reload succeeds atomically; failed activation leaves old generation usable; in-flight requests do not mix generations.

## Reader

Use empty, singleton and multi-row fixtures, NULLs, duplicate/nonmatching keys, composite keys, nested/self relations, multiple DerivedViews and empty-root aggregates. Test field/order/filter allowlists, criteria parameters, limit/offset/page, declared SQL LIMIT versus view pagination, batching/concurrency/partition ordering, and cancellation.

OnRelation must see all fetched records for that relation, not a single batch. Count callbacks to catch duplicates on retry. Verify typed codec/null behavior and cache replay equivalence. Validate rich pseudo shape transformation and that required internal backing columns are fetched but not exposed.

For cube/compose, test multiple frames, per-frame selectors and bindings, SQL wrapper safety, native parameter budgets, configured cube/limit/time budgets and HTTP/native MCP exposure. A configurable operational limit is not an architectural fixed number of cube slots.

## Writer

Use at least: update existing, insert missing, mixed insert/update, supplied zero identity, omitted identity, partial composite identity, same ID/different tenant, reordered sparse children, missing parent links, nullable values, false/zero/empty updates, and omitted fields remaining unchanged.

- Original identity determines matching despite later initialization/sequencing.
- Full checks for inserts; Has-gated field checks for sparse updates.
- Required/unique/reference/DB rules run before custom validation.
- Invariant backfill activates only affected groups, preserves Has, and fails atomically when required previous data is unknown.
- Existing object/holder addresses remain stable when hooks depend on prepared entity references.
- Business/marker data is fixed after validation; graph mutation cannot silently escape the prepared set.
- Parent and self links are correct; mutation uses full composite identities and native dialect binding.
- Queue follows intended graph order; no DB change is assumed before completion.
- Repeated allocations in one shared transaction do not overlap; explicit supplied zero is not reallocated.
- Validation/sequence/queue/hook failure and cancellation do not commit managed work.
- Confirmed commit, rollback, caller-pending and unknown outcomes are distinguished.
- Finalization runs once; explicit finalizer override does not double-call root hooks.
- Writer output is the transformed request body with final IDs/links, not the full previous dataset.
- Explicit status/message/error/violations survive wrapping and finalization. Check 401 when requested, and default validation behavior separately.

## Custom handler

Test narrow capability injection, typed input access, dependent-component binding, missing providers, handler-returned errors and intentional output/status. A custom handler does not automatically acquire generated mutation safeguards: prove its own identity, validation, sequencing and commit rules.

## Transport and realistic execution

Use SQLite through shared project fixture utilities, not mocks alone. For MCP use viant/mcp client/server integration where the project supplies it. Inspect actual tool lists/calls and structured errors, not just generated schemas. HTTP tests assert both status and body. Never reinterpret an MCP tool error as an HTTP transport-authentication failure.

Record what actually ran. A parser pass is not proof of DB semantics; generated source compilation is not proof of route registration; one happy-path fixture is not full parity.

## Operational and pending-feature acceptance

Use [cache-and-operations.md](references/cache-and-operations.md) for the relevant cache, async, output, documentation and lifecycle checks. Record source inspection, tests actually run and pending integration separately. Do not count a tag/SDK construction test as end-to-end acceptance.

## Naming and release boundary

Test exact authored names, rejected inferred spelling variants, explicit mappings
and duplicate output-column errors. These rules are integrated; preserve them
when regenerating readers, writers and multi-view selectors. Test
`datly init/build` discovery after adding/removing a package without changing
registration code, and preserve source-backed deployment inputs. Verify canonical
skill-root links and reproducible declared product imports before bundling.
