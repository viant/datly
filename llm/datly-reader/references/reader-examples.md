# Reader authoring examples

These are application patterns. The developer server supplies actual connector/schema/type authority and validates the finished component. Names/tables are illustrative.

## Parameterized DQL reader

~~~~sql
#package('example.com/app/records')
#setting($_ = $route('/v1/records', 'GET'))
#setting($_ = $connector('main'))
#setting($_ = $mcp('records.list', 'List records in a tenant'))
#define($_ = $TenantID<int>(query/tenantId).Required())
#define($_ = $Limit<int>(query/limit).Optional().QuerySelector('Records'))
SELECT r.id, r.tenant_id, r.name,
       set_limit(r, 100),
       allowed_order_by_columns(r, 'id,name')
FROM records r
WHERE r.tenant_id = :TenantID
~~~~

Configure selector permissions and row type. Verify that the selector view identity is Records in the compiled component; r is the SQL namespace.

## Go-shape reader

~~~~go
package records

import xdatly "github.com/viant/xdatly"

type Components struct {
    List xdatly.Component[Input, Output] `component:"List,path=/v1/records,method=GET,connector=main,view=Records" desc:"List records in a tenant"`
}
type Input struct {
    TenantID int `parameter:"TenantID,kind=query,in=tenantId,required=true"`
}
type Output struct {
    Data []*Record `view:"Records,table=records" sql:"SELECT id,tenant_id,name FROM records WHERE tenant_id=:TenantID"`
}
type Record struct {
    ID       int    `json:"id" sqlx:"id,primaryKey"`
    TenantID int    `json:"tenantId" sqlx:"tenant_id"`
    Name     string `json:"name" sqlx:"name"`
}
~~~~

No DAO forwarding layer is required. Add custom orchestration only when chosen or needed.

## Nested and derived data

Use explicit relation keys, including all composite parts. A DerivedView supplies a query-derived output alongside the root rows. **Attach it with `parameter:"...,kind=output,in=derived"` in Go, or `(output/derived)` in DQL.** Naming a field `Count` or adding a normal input view does not attach it to the response.

### Complete Go-shape pattern: one-row pages, full-match count and bounds

This example uses a configured SQLite connector `main` and `records(tenant_id INTEGER,id INTEGER,name TEXT)`. `Records` is the root view identity; `Offset` targets that same view. The aggregates wrap its filtered, non-windowed query, retaining the tenant argument while excluding view pagination.

~~~~go
package records

import xdatly "github.com/viant/xdatly"

type Components struct {
    Records xdatly.Component[Input, Output] `component:"Records,path=/records,method=GET,connector=main,view=Records"`
}
type Input struct {
    TenantID int `parameter:"TenantID,kind=query,in=tenantId,required=true"`
    Offset int `parameter:"Offset,kind=query,in=offset" querySelector:"Records"`
}
type Record struct {
    ID int `json:"id" sqlx:"id"`
    Name string `json:"name" sqlx:"name"`
}
type Totals struct {
    Count int `json:"count" sqlx:"count"`
}
type Bounds struct {
    Minimum *int `json:"minimum" sqlx:"minimum"`
    Maximum *int `json:"maximum" sqlx:"maximum"`
}
type Output struct {
    Data []*Record `json:"data" parameter:"Data,kind=output,in=view" view:"Records,limit=1,selectorOffset=true" sql:"SELECT id,name FROM records WHERE tenant_id=:TenantID ORDER BY id"`
    Totals *Totals `json:"totals" parameter:"Totals,kind=output,in=derived" view:"Totals" sql:"SELECT COUNT(*) AS count FROM ($View.Records.NonWindowSQL) parent"`
    Bounds *Bounds `json:"bounds" parameter:"Bounds,kind=output,in=derived" view:"Bounds,allowNulls=true" sql:"SELECT MIN(id) AS minimum,MAX(id) AS maximum FROM ($View.Records.NonWindowSQL) parent"`
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
#setting($_ = $route('/records','GET'))
#setting($_ = $connector('main'))
#define($_ = $TenantID<int>(query/tenantId).Required())
#define($_ = $Offset<int>(query/offset).Optional().QuerySelector('Records'))
#define($_ = $Data<?>(output/view))
#define($_ = $Totals<Totals>(output/derived) /* SELECT COUNT(*) AS count FROM ($View.Records.NonWindowSQL) parent */)
#define($_ = $Bounds<Bounds>(output/derived) /* SELECT MIN(id) AS minimum,MAX(id) AS maximum,allow_nulls(parent) FROM ($View.Records.NonWindowSQL) parent */)
SELECT r.id,r.name,set_limit(r,1)
FROM records r
WHERE r.tenant_id=:TenantID
ORDER BY r.id
~~~~

Use component name/root view `Records`, and retain the linked root's `selectorOffset=true` permission. `r` is the SQL namespace for `set_limit`; it is not the selector/view identity. The DQL specifies its own limit and aggregate NULL policy because authored DQL can override the corresponding Go SQL settings. Do not assume the overwritten Go query still supplies these controls.

`NonWindowSQL` removes **view pagination**, not an explicit business `LIMIT` already inside the authored SQL source. If the base query itself says `LIMIT 1`, a derived count may legitimately count only that limited source. Use a view limit for page-size behavior when totals must count all matches.

Both authoring variants have SQLite data-driven evidence for first/second/empty pages, a different tenant, no matching rows, and unchanged persisted data. These examples do not establish every dynamic/nested declaration variant: nested output-holder authoring and runtime-only type generation must still be verified against the selected build rather than inferred from the two top-level slots shown here.

For multi-batch relations, OnRelation sees the complete collection. A reducer sees the completed collection according to its contract; concurrent partition arrival does not establish business order.

## Rich public shape

Required target pattern:

~~~~sql
#import('model', 'example.com/app/model')
#setting($_ = $route('/v1/configurations', 'GET'))
SELECT r.id, r.BOUND_UNIT, r.BOUND_CAP, NULL AS bounds,
       CAST(r.bounds AS model.Bounds),
       tag(r.bounds, 'sqlx:"-"'),
       tag(r.BOUND_UNIT, 'internal:"true"'),
       tag(r.BOUND_CAP, 'internal:"true"')
FROM configurations r
~~~~

The resolved view must identify bounds as a logical non-DML projection. Verify it is not sent as an unsupported physical field, and backing dependencies are fetched. Do not infer transient behavior for every physical rich/JSON cast.

The imported model.Bounds may contain Unit and Cap. OnFetch builds it from the internal columns. Reuse the imported type; do not emit a duplicate local Bounds.

## Cube/compose

Declare cube/report input layout and enable composition. Callers supply a frame list and validated wrapper using $CubeSQL1 ... $CubeSQLN. Preserve frame parameters and selector rules. Inspect configured cube/limit/timeout budgets: the design is list-based, not fixed two/three-cube slots, but deployed operational limits apply.

## Dynamic source

For mutable DQL/resources, validate and atomically activate the matching component/types/resources. Failed staging leaves the previous generation active. Persisted shapes retain field order and append new fields; authored hooks survive regeneration.
