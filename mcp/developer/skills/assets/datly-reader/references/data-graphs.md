# Data graph authoring

**Data graph** and **object graph** name the same logical graph here: its typed
root/relations and the DQL sources, connectors, predicates and loading that
supply them. They are not separate models or components; readers and writers
operate on that same graph. Neither means Go map joins or fetch-and-filter.
This guide covers general graph structure; see [release graph details](object-graphs.md)
for release/revision selection, selector transport names and writer ownership.

Datly's outer query is the **view graph**. SQL inside each named source is the
**database query** for that view. Keeping these two levels distinct lets one
reader assemble related data without a separate endpoint or application loop for
every table. The full syntax remains in [DQL grammar](dql-grammar.md); use the
selected build's native compiler rather than treating this guide as a parser.

## Choose the graph before the component count

| Requirement | Native starting point |
| --- | --- |
| Root records with related collections | Outer JOIN with complete identity keys; retain many cardinality |
| A single dictionary label per record | Outer to-one relation, explicit keys and `AND 1=1` |
| A label stored in another database | Related view with its own `use_connector`; do not copy the dictionary into fact SQL |
| A prerequisite such as an authoritative timezone | Typed input `view` / `data_view`; not an application-side preliminary query |
| Full-match count or bounds alongside a page | DerivedView based on the intended non-window scope; not the visible page count |
| Several projections over the same authorized facts | Existing selectors and groupable reader/cube composition, preserving aggregate semantics |
| Unrelated fact grains or distinct authorization | Separate internal components where necessary; compose through an explicit supported contract |

## Avoid statement-sized components

A SQL statement is not automatically a component boundary. A root query, child
query, dictionary query, prerequisite and total can serve one authorized result.
Declare them as native views and relations in that result's graph. Wrapping each
query in a public GET or a custom component, then looping over those components
in Go, duplicates binding and assembly responsibilities and can create N+1
queries. Calling direct SQL from a component wrapper does not make it a native
reader. Keep executable SQL in named DQL sources or adjacent SQL assets.

Separate components are appropriate for independently reusable contracts,
different authorization policies, independent operations or unrelated fact grains.
A prerequisite with its own established service/authorization boundary may remain
a typed component dependency. Preserve that boundary instead of copying its SQL
or broadening access. For ordinary database prerequisites within the same use
case, start with declared input `view` / `data_view` reads. Keep dependency
components private unless their public exposure is independently required.

Do not consolidate solely to reduce file count. Keep independent writer and
reader operations separate, and do not force several fact collections into one
physical join. Use typed invocation for legitimate composition, with the caller's
authorization and transaction ownership intact.

## A complete related-view pattern

Illustrative schema: invoice IDs and customer IDs are unique only within a
tenant. `ledger` and `directory` are configured connectors, never client-supplied
connection strings. Validate the schema and component with the selected v1 build.

```sql
#package('example.com/billing/invoices/read')
#setting($_ = $input_type('InvoicesInput'))
#setting($_ = $output_type('InvoicesOutput'))
#setting($_ = $case_format('lc'))
#setting($_ = $route('/v1/invoices', 'GET'))

#define($_ = $TenantID<int>(query/tenantId).Required().WithPredicate(0,'equal','i','tenant_id'))
#define($_ = $Invoices<[]*Invoice>(output/view))

SELECT invoices.*, lines.*, customer.*,
       type(invoices, 'Invoice'), type(lines, 'InvoiceLine'),
       type(customer, 'Customer'),
       use_connector(invoices, 'ledger'),
       use_connector(lines, 'ledger'),
       use_connector(customer, 'directory'),
       set_limit(invoices, 100)
FROM (
    SELECT i.id, i.tenant_id, i.customer_id, i.total
    FROM invoices i
    ${predicate.Builder().CombineAnd($predicate.FilterGroup(0,"AND")).Build("WHERE")}
) invoices
LEFT JOIN (
    SELECT tenant_id, invoice_id, id, amount FROM invoice_lines
) lines ON lines.tenant_id = invoices.tenant_id
       AND lines.invoice_id = invoices.id
LEFT JOIN (
    SELECT tenant_id, id, name FROM customers
) customer ON customer.tenant_id = invoices.tenant_id
          AND customer.id = invoices.customer_id AND 1=1
```

The graph attaches a collection of lines and one customer to each invoice. The
outer JOIN does not require either database to execute a cross-database join.
The equality links supply relation identity; connector controls belong in the
outer projection. Do not move these controls into the inner database SQL.

The example's required tenant filter is **not authentication**. In a real
application, bind and verify the caller through the existing authorization
component/predicate and constrain the root accordingly. A supplied tenant ID,
a dictionary relation or a UI selection never grants access. Keep tenant keys
in every relation where IDs are not globally unique. Related-view authorization
must reflect its own data contract when root admission alone is insufficient.

## Preserve grain and aggregate meaning

Declare measure aggregation in SQL: `groupable:"false"` supplies a role, not an
implicit SUM. For preaggregated sources, an authored outer SUM can regroup an
unchanged inner deduplication query. Preserve nonadditive raw grain separately
from explicit cube projection; an all-fields selector still requests projection.

For example, retain an authorized deduplication query as the inner source and
declare its additive rollup explicitly:

```sql
SELECT d.category, SUM(d.amount) AS amount
FROM (
    SELECT category, amount, COUNT(*) AS copies
    FROM authorized_facts
    GROUP BY category, amount
) d
GROUP BY d.category, d.amount, d.copies
```

No scalar selector preserves the authored raw grain; an explicit
category/amount projection regroups the deduplicated values. Do not apply this
SUM pattern to distinct reach or other nonadditive snapshots.

A trusted Go adapter that preserves raw scalar grain can select row relations
independently with `ReaderOptions.Relations = exec.NewRelationSelection(target,
"records", "Label")`. Names resolve against the exact compiled component route
and canonical view. An explicit empty list skips row relations; omission retains
ordinary loading. This invocation control does not alter authorization, select
scalar fields, suppress output relations, or become an HTTP/MCP input.

Aggregate a fact at its authorized grain before attaching a to-one dictionary.
Do not join a many-valued label table into database fact SQL and then sum the
duplicated rows. A to-one marker is a claim to verify, not a way to conceal
duplicates: test duplicate keys and enforce the actual unique identity.

Adding a groupable dimension can change a cached cube's grain. Prove existing
projections before and after that change, including uneven subgroup sizes and
cache reuse. `AVG` of subgroup averages is not generally the original average;
`COUNT`, `COUNT DISTINCT`, `SUM`, `MAX` and ratios have different regrouping rules.
Do not replace them with one convenient aggregate to make a test pass.

Keep current/prior windows, authorization predicates and ordinary UI filters at
their source-defined stages. A scalar MAX-date input can have a different scope
from the visible rows; declare that intentionally without reading outside the
authorized domain. A related label must not change the fact's admitted row set.

## Performance follows the loading contract

One component is not a promise of one SQL statement. Native related loading
uses view queries and batches; prerequisites and DerivedViews can add queries.
The benefit is declared loading and shared graph semantics, not an invented
constant query count or guaranteed latency improvement.

- A per-parent lookup can issue another query for each parent. Native `batch`
  groups parent keys; composite keys and other arguments consume database bind
  limits. Inspect actual query counts across multiple batches rather than
  asserting that a relation always runs once.
- `batchConcurrency`, `relationalConcurrency` and partition `concurrency` control
  different work. More parallel work can increase connection and database load;
  tune with the configured pool and real workload. See [reader loading](reader-contract.md#batching-concurrency-and-partitioning).
- Fetch-and-filter transfers and decodes rows that native predicates could reject.
  Filtering or sorting a page in Go also changes full-result paging/count
  semantics. Push the intended scope into the query before paging and expansion.
- Use allowed Query Selectors for projection and relation selection while retaining
  internal keys. A different projection alone does not require duplicated source
  SQL, caches or components. Verify selected-away fields and required hooks.
- A full-match DerivedView must use the intended non-window scope. It may perform
  additional database work; it is not a free count of the returned page.
- Cache reuse depends on prepared view/query identity, bindings, projection and
  grouping compatibility. Preserve authorization for cold and warm paths, and
  declare invalidation after writes; related read caches are not automatically
  invalidated. Follow [cache and operations](cache-and-operations.md).

Compare query count, bound arguments, returned rows, database plans and latency
with representative authorized fixtures and selector/cache cases. Report measured
conditions and results; do not claim production gains from syntax validation or
small fixtures. Cross-connector assembly does not guarantee a common snapshot.

## Verify the graph through native execution

Use the existing component test and project workflow. Check the emitted typed
holders and native queries, not just balanced syntax or a successful import.
Cover two tenants sharing an ID, absent labels, one-to-many cardinality, empty
root pages, all requested relations and full-match totals. Verify field selection
and paging do not duplicate or lose root rows and that hidden fields stay hidden.
For cross-connector reads, inspect actual native execution to establish which
query uses which connector; do not infer this from an outer JOIN's spelling.

Exercise stock validation/transcription and regeneration, then native execution
with complete bindings. Test prerequisite failure, unauthorized scope, repeated
IDs across tenants/revisions, duplicate to-one keys, multiple batches, selector
variations, empty pages with totals and cold/warm cache behavior. Verify the
supported target database as well as local SQLite fixtures: SQLite or mocked
execution alone cannot prove MySQL source syntax, bind limits or query plans.
For writer reuse, additionally prove that auxiliary tables receive no DML and
that real writable entities still validate, persist and roll back correctly.

When a graph cannot express the requested behavior, record the precise missing
native capability before adding a custom handler. A per-row Go lookup or several
public readers stitched together is not a default substitute for the graph.
