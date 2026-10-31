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

## Verify the graph through native execution

Use the existing component test and project workflow. Check the emitted typed
holders and native queries, not just balanced syntax or a successful import.
Cover two tenants sharing an ID, absent labels, one-to-many cardinality, empty
root pages, all requested relations and full-match totals. Verify field selection
and paging do not duplicate or lose root rows and that hidden fields stay hidden.
For cross-connector reads, inspect actual native execution to establish which
query uses which connector; do not infer this from an outer JOIN's spelling.

When a graph cannot express the requested behavior, record the precise missing
native capability before adding a custom handler. A per-row Go lookup or several
public readers stitched together is not a default substitute for the graph.
