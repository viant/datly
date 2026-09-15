# Reader authoring examples

## Explicit output holder and JSON shape

Name the output field as well as the output type. `output_type` names a Go
contract; `output/view` binds the main root-view result into that contract.
This declaration fragment belongs in the
[complete parameterized reader](reader-examples.md#parameterized-dql-reader):

~~~~sql
#setting($_ = $input_type('OrdersInput'))
#setting($_ = $output_type('OrdersOutput'))
#setting($_ = $case_format('lc'))
#define($_ = $Orders<[]*Order>(output/view))
~~~~

In the complete DQL, declare `#package`, route and connector, and name the root
row with `type(orders, 'Order')` in the outer SELECT. The generated public shape
is `OrdersOutput.Orders []*Order`; `case_format('lc')` makes the envelope key
`orders` and uniformly shapes nested field names using the Structology JSON
marshaler. Row columns come from the selected projection. Do not add JSON tags
merely to obtain lower-camel casing. Do not imply that the output type setting
alone specifies a holder, or that reader and writer contracts are automatically
combined. Request initialization belongs to the explicitly named input contract;
row `OnFetch` processing and output finalization are separate lifecycle points.


These are application patterns. The developer server supplies actual connector/schema/type authority and validates the finished component. Names/tables are illustrative.

## Parameterized DQL reader

~~~~sql
#package('example.com/app/records/read')
#setting($_ = $input_type('RecordsInput'))
#setting($_ = $output_type('RecordsOutput'))
#setting($_ = $case_format('lc'))
#setting($_ = $route('/v1/records', 'GET'))
#setting($_ = $connector('main'))
#setting($_ = $mcp('records.list', 'List records in a tenant'))
#define($_ = $TenantID<int>(query/tenantId).Required())
#define($_ = $Limit<int>(query/limit).Optional().QuerySelector('Records'))
#define($_ = $Records<[]*Record>(output/view))
SELECT records.*, type(records, 'Record'),
       set_limit(records, 100),
       allowed_order_by_columns(records, 'id,name')
FROM (
    SELECT r.id, r.tenant_id, r.name FROM records r
    WHERE r.tenant_id = :TenantID
) records
~~~~

Save as `source/read/Records.dql` in the existing `example.com/app` module:

```sh
datly transcribe get -dir "$PROJECT" \
  -schema -connector main -driver sqlite3 -dsn "$PROJECT/schema.db" \
  example.com/app/source/read
```

`#package` selects the destination, input/output settings name contracts, and
`type(records,'Record')` names the row. The `Records` output/view holder binds
the root to `RecordsOutput.Records []*Record`, serialized as `records`. `records` is the outer view alias; `r`
stays local to its SQL. Configure selector permissions and verify the canonical
selector view identity `Records` in the generated component. Author the writer
separately. Default filenames are plain; [exact overrides and optional prefixes](developer-mcp.md#operation-based-generation-to-pure-go)
control their destinations.

## Go-shape reader

~~~~go
package records

import xdatly "github.com/viant/xdatly"

type Components struct {
    List xdatly.Component[Input, Output] `component:"List,path=/v1/records,method=GET,connector=main,view=Records" caseFormat:"lc" desc:"List records in a tenant"`
}
type Input struct {
    TenantID int `parameter:"TenantID,kind=query,in=tenantId,required=true"`
}
type Output struct {
    Data []*Record `parameter:"Data,kind=output,in=view" view:"Records,table=records" sql:"SELECT id,tenant_id,name FROM records WHERE tenant_id=:TenantID"`
}
type Record struct {
    ID       int    `sqlx:"id,primaryKey"`
    TenantID int    `sqlx:"tenant_id"`
    Name     string `sqlx:"name"`
}
~~~~

The component-level `caseFormat:"lc"` applies lowerCamel output names to the
envelope and nested fields without routine JSON tags.

No DAO forwarding layer is required. Add custom orchestration only when chosen or needed.

## Nested and derived data

Use explicit relation keys, including all composite parts. A DerivedView supplies a query-derived output alongside the root rows. **Attach it with `parameter:"...,kind=output,in=derived"` in Go, or `(output/derived)` in DQL.** Naming a field `Count` or adding a normal input view does not attach it to the response.

### Complete Go-shape pattern: one-row pages, full-match count and bounds

This example uses a configured SQLite connector `main` and `records(tenant_id INTEGER,id INTEGER,name TEXT)`. `Records` is the root view identity; `Offset` targets that same view. The aggregates wrap its filtered, non-windowed query, retaining the tenant argument while excluding view pagination.

~~~~go
package records

import xdatly "github.com/viant/xdatly"

type Components struct {
    Records xdatly.Component[Input, Output] `component:"Records,path=/records,method=GET,connector=main,view=Records" caseFormat:"lc"`
}
type Input struct {
    TenantID int `parameter:"TenantID,kind=query,in=tenantId,required=true"`
    Offset int `parameter:"Offset,kind=query,in=offset" querySelector:"Records"`
}
type Record struct {
    ID int `sqlx:"id"`
    Name string `sqlx:"name"`
}
type Totals struct {
    Count int `sqlx:"count"`
}
type Bounds struct {
    Minimum *int `sqlx:"minimum"`
    Maximum *int `sqlx:"maximum"`
}
type Output struct {
    Data []*Record `parameter:"Data,kind=output,in=view" view:"Records,limit=1,selectorOffset=true" sql:"SELECT id,name FROM records WHERE tenant_id=:TenantID ORDER BY id"`
    Totals *Totals `parameter:"Totals,kind=output,in=derived" view:"Totals" sql:"SELECT COUNT(*) AS count FROM ($View.Records.NonWindowSQL) parent"`
    Bounds *Bounds `parameter:"Bounds,kind=output,in=derived" view:"Bounds,allowNulls=true" sql:"SELECT MIN(id) AS minimum,MAX(id) AS maximum FROM ($View.Records.NonWindowSQL) parent"`
}
~~~~

The three output holders are `Data`, `Totals`, and `Bounds`. The root **view** is `Records`; do not replace `$View.Records.NonWindowSQL` with `$View.Data.NonWindowSQL` merely because the JSON collection is called `data`. `parent` is only the SQL wrapper alias in each aggregate.

`limit=1` supplies a positive page size; `selectorOffset=true` authorizes the query-bound offset. An offset without a positive limit is invalid. `allowNulls=true` preserves SQL MIN/MAX NULLs when nothing matches.

For tenant 7 with IDs 1, 2, 3, these are the verified results:

| Request query | Root row IDs | Totals.Count | Bounds.Minimum / Maximum |
| --- | --- | --- | --- |
| `tenantId=7&offset=0` | 1 | 3 | 1 / 3 |
| `tenantId=7&offset=1` | 2 | 3 | 1 / 3 |
| `tenantId=7&offset=10` | no rows | 3 | 1 / 3 |
| `tenantId=9&offset=0` with no tenant 9 records | no rows | 0 | NULL / NULL |

The tenant condition is an example filter, not proof that an arbitrary client-supplied tenant is authorized. Bind/enforce the application's trusted tenant policy separately.

### Equivalent DQL attachment with the linked shapes above

When the component uses the same linked `Input`/`Output` shapes, this DQL declares the two output-derived queries explicitly:

~~~~sql
#package('example.com/app/records')
#setting($_ = $input_type('Input'))
#setting($_ = $output_type('Output'))
#setting($_ = $case_format('lc'))
#setting($_ = $route('/records','GET'))
#setting($_ = $connector('main'))
#define($_ = $TenantID<int>(query/tenantId).Required())
#define($_ = $Offset<int>(query/offset).Optional().QuerySelector('Records'))
#define($_ = $Data<[]*Record>(output/view))
#define($_ = $Totals<Totals>(output/derived) /* SELECT COUNT(*) AS count FROM ($View.Records.NonWindowSQL) parent */)
#define($_ = $Bounds<Bounds>(output/derived) /* SELECT MIN(id) AS minimum,MAX(id) AS maximum,allow_nulls(parent) FROM ($View.Records.NonWindowSQL) parent */)
SELECT r.id,r.name,type(r,'Record'),set_limit(r,1)
FROM records r
WHERE r.tenant_id=:TenantID
ORDER BY r.id
~~~~

The explicit `Data<[]*Record>(output/view)` binds root rows to `Output.Data`
under JSON key `data`; `type(r,'Record')` matches the linked row type. The
`Totals` and `Bounds` declarations retain their separate derived-output bindings.
Use component name/root view `Records`, and retain the linked root's `selectorOffset=true` permission. `r` is the SQL namespace for `set_limit`; it is not the selector/view identity. The DQL specifies its own limit and aggregate NULL policy because authored DQL can override the corresponding Go SQL settings. Do not assume the overwritten Go query still supplies these controls.

`NonWindowSQL` removes **view pagination**, not an explicit business `LIMIT` already inside the authored SQL source. If the base query itself says `LIMIT 1`, a derived count may legitimately count only that limited source. Use a view limit for page-size behavior when totals must count all matches.

Both authoring variants have SQLite data-driven evidence for first/second/empty pages, a different tenant, no matching rows, and unchanged persisted data. These examples do not establish every dynamic/nested declaration variant: nested output-holder authoring and runtime-only type generation must still be verified against the selected build rather than inferred from the two top-level slots shown here.

For multi-batch relations, OnRelation sees the complete collection. A reducer sees the completed collection according to its contract; concurrent partition arrival does not establish business order.

## Rich public shape

Required target pattern:

~~~~sql
#package('example.com/app/configurations/read')
#import('model', 'example.com/app/model')
#setting($_ = $input_type('ConfigurationsInput'))
#setting($_ = $output_type('ConfigurationsOutput'))
#setting($_ = $case_format('lc'))
#setting($_ = $route('/v1/configurations', 'GET'))
#setting($_ = $connector('main'))
#define($_ = $Configurations<[]*Configuration>(output/view))
SELECT configurations.*, type(configurations, 'Configuration'),
       CAST(configurations.bounds AS model.Bounds),
       tag(configurations.bounds, 'sqlx:"-"'),
       tag(configurations.BOUND_UNIT, 'internal:"true"'),
       tag(configurations.BOUND_CAP, 'internal:"true"')
FROM (
    SELECT r.id, r.BOUND_UNIT, r.BOUND_CAP, '' AS bounds FROM configurations r
) configurations
~~~~

The root binds to `ConfigurationsOutput.Configurations []*Configuration`,
serialized under `configurations`. The result must contain the `bounds` output.
The outer CAST supplies its Go type
even when driver type metadata is empty; inner CTE/computed SQL need not have
literal provenance. Without CAST, simple `''`/`0` projections default to
`string`/`int`. Missing/duplicate outputs and undeclared unknown types fail.
`sqlx:"-"` makes this hook-built logical value non-SQL/non-DML; Verify it is not sent as an unsupported physical field, and backing dependencies are fetched. Do not infer transient behavior for every physical rich/JSON cast.

The imported model.Bounds may contain Unit and Cap. OnFetch builds it from the internal columns. Reuse the imported type; do not emit a duplicate local Bounds.

## Cube/compose

Declare cube/report input layout and enable composition. Callers supply a frame list and validated wrapper using $CubeSQL1 ... $CubeSQLN. Preserve frame parameters and selector rules. Inspect configured cube/limit/timeout budgets: the design is list-based, not fixed two/three-cube slots, but deployed operational limits apply.

For a declared Spend report with these SQL output aliases, a two-frame request
can compare web and store totals:

```json
{
  "cubes": [
    {"filters": {"accountIDs": "1,2", "tenant": "acme", "region": "EU", "channel": "web", "status": "active"}},
    {"inheritFrom": 1, "filters": {"channel": "store"}}
  ],
  "sql": "SELECT t1.AccountID, t1.TotalSpend AS web, COALESCE(t2.TotalSpend, 0) AS store FROM $CubeSQL1 AS t1 LEFT JOIN $CubeSQL2 AS t2 ON t1.AccountID = t2.AccountID ORDER BY t1.AccountID LIMIT 8"
}
```

This requires that application contract; it is not a call against an installed
demo. `inheritFrom` is one-based and refers to a prior frame. Each frame retains
its authorized inputs and ordered SQL bindings; wrapper SQL cannot name arbitrary
source tables. Configured defaults are 8 cubes, result limit 100 and timeout
30000 ms; zero selects defaults. Check the connected configuration and total
placeholder budget. Multi-key composition joins need their own parser acceptance.
Verify inherited/omitted filters, nullable joins, aliases, exceeded budgets and
HTTP/MCP denial. Automatic linking of JWT embedded types needs its own build
acceptance beyond manually linked runtime tests.

## Dynamic source

For mutable DQL/resources, validate and atomically activate the matching component/types/resources. Failed staging leaves the previous generation active. Persisted shapes retain field order and append new fields; authored hooks survive regeneration.
