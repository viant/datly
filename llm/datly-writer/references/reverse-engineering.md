# Reverse engineer a complete writer

Use this workflow for a legacy writer migration. The short examples teach syntax;
they do not establish a complete migration contract. The original application,
its schema and its independent expected results determine the target behavior.

## Reconstruct the operation before authoring

Read the original DQL, Input/body/entity/output structs, binding tags, SQL assets,
input initialization, authorization, validation, handler, child hooks, sequencing,
transaction completion and tests. A JSON route/handler comment contains routing
metadata, not the operation specification. Preserve the original DQL directory
and public filename; extract substantial SQL to the adjacent `sql/` folder.

Produce a source-to-contract table with these columns:

| Source fact | Original evidence | DQL declaration or generated responsibility | Application hook | Independent acceptance case |
|---|---|---|---|---|
| Body envelope and each nested field | Struct, tags, decoder and original request | Generated body row/relation, type/nullability/presence and exact wire exception | Business conversion only | Omitted/null/zero/false/empty and malformed values |
| Header/query/path/default | Binding tags and original Init | Explicit parameter/provider/codec/status/default | None for binding | Actual HTTP and typed MCP binding |
| Current and auxiliary reads | Every SQL asset and call | Scoped generated Current plus declared read-only views | Typed indexes and business decisions | Cross-owner keys and complete physical state |
| Insert/update classification | Original request/Previous tests | Native identity policy, complete keys and original suppliedness | Supported identity resolution | Missing, supplied zero, existing and mixed records |
| Parent/child links and deletion | Original loop and FK schema | Complete relation equalities and explicit delete marker | Business deletion decision only | New parent IDs, reordered children, omitted children |
| IDs/sequence mechanism | Original sequencer, source DDL and dialect | Same native allocator, schema-inferred identity/FK tags | Observe IDs after sequencing | Real database IDs, links, rollback and concurrency |
| Validation/error order | Original validation and error wrapping | Generated schema checks and lifecycle declarations | Original business rules and safe response policy | Exact status/body/violations and no writes on failure |
| Events/external effects | Original event creation, insertion and publish | Writable event graph or typed generated component dependency | Event business payload, outcome-aware publication | Event ordering, commit/rollback, trusted publisher fake |

Do not label the writer migrated while any source input, read, write, relation,
validation rule, ID mechanism or side effect remains unmapped. A generated
registration or a successful schema compile alone does not close this table.

## Make the body visible in DQL

For an ordinary database writer, declare its full reader-like mutation graph and
use `transcribe patch|post|put`. Generation derives the typed body and Current
bindings from that graph. The named root and child types are the body contract;
Has/presence markers stay generated and private. Do not construct a handwritten
body wrapper to avoid describing its records and relations in DQL.

Reuse a Go type only when it is genuinely authoritative or shared business data.
Name the exact package/type and explain why it is reused. A new struct created
solely to carry this endpoint's body is not evidence of a reverse-engineered
contract. A shared structured value such as grid preferences can be reused inside
a generated record, with its storage conversion explicitly accounted for.

Some original endpoints only transform input and invoke another writer. In that
case distinguish the logical public body from the child persistence graph:
public DQL owns its complete binding/body/output contract; the child writer DQL
owns tables, Current, IDs and DML. Keep the dependency explicit and typed. Do not
invent a parent SQL query or allocator table to obtain a shape. Use native `handler_factory` settings for custom registration, and verify
connected-build support before choosing that generation path. If
it is missing, retain the complete declarative contract, report the precise gap,
and obtain the project's required framework authorization before extending it.

## Model every table role

For each physical table classify it as writable root, writable child,
read-only auxiliary, Current lookup, derived output or persisted event. A normal
join declares a writable relation; parenthesized physical tables such as
`(PRODUCTS)` are auxiliary and must never enter sequencing or DML. SQL resources
with no route are query assets, not additional components.

Include every part of a composite parent/FK relation. Keep independent child
collections separate. Declare delete intent explicitly; absence from a request
never means delete. Preserve original append-only/update/delete event policy;
do not treat event rows as an ordinary upsert collection without source evidence.

Keep the original ID mechanism per component and database. In a migration that
forbids new tables, do not provision reservation/scoped sequence tables. For
original MySQL transient allocation, preserve AUTO_INCREMENT and native transient
transaction/lock/trigger behavior. Fixture sequence publications are test inputs,
not permission to change the product allocator.

## Preserve SQL and keep DQL compact

Start the outer graph with each necessary `view.*`. Only add genuine type,
required/optional, conversion, invariant, lifecycle, relation or validation
configuration. Do not repeat inferred types or enumerate fields beside a
wildcard. Visibility alone does not require another projection. Preserve original
source SQL predicates, joins, expressions and ordering when extracting assets.
A computed logical column is justified by actual application behavior, not by
an effort to make a test or discovery tier pass.

## Verify the whole operation

Discover shapes from the complete authoritative DDL on the required source
dialect. In a MySQL-source migration, use MySQL discovery and keep its inferred
FK/default/nullability/identity metadata; SQLite exercises those same generated
shapes as a hydrated runtime tier. Never infer the production shape from a
reduced SQLite fixture.

Regenerate twice from the same source and compare all artifacts. Then exercise
the actual generated writer with independent expected responses and complete
persisted-state guards: mixed inserts/updates, sparse changes, child IDs/links,
explicit deletion, cross-owner access, failed validation, late DML failure,
rollback, transaction ownership and relevant event/publication outcomes.
Original end-to-end cases remain required. Publish evidence separately for
contract compilation, generation stability, runtime behavior and full acceptance.

## Maintained Datly 1.0 references

Read [writable graph/body inference](product/datly/doc/mutations.md),
[the complete writer lifecycle](writer-contract.md), and
[request-keyed auxiliary scope with writable descendants](product/datly/doc/legacy-to-v1-migration.md#body-key-auxiliary-state-for-a-new-mutation-root).
These maintained contracts cover more than the introductory example. Validate
capabilities against the connected build; documentation describes required
contracts and does not itself prove a particular migrated component works.
