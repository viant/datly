# Cache, warmup and reader operations

Read for operational reader choices. The candidate and the final exact-name
contract have different acceptance states; consult [status](references/product/datly/doc/status.md).

## Cache and warmup

AFS and Aerospike are independent explicit choices. Use the actual native SQLX
cache service for a prepared view; no backend switching threshold, Datly row-map
cache or handler invalidation layer. Authored AFS uses a location and positive
TTL; candidate Aerospike uses its provider URL, namespace, set and positive
whole-second TTL. TTL and timeToLiveMs must agree when both are present. Native
client ownership must outlive consumers and drain before close.

```sql
#setting($_ = $cache('records', '5m').WithProvider('afs').WithLocation('cache/records'))
```

Configure the query and connector too. For a fixed `WHERE id=:ID`, warm an exact
case with `$cache_warmup('', 'ID=1')`. Indexed warmup clears IndexParameter to
compute group identity, so its SQL must support an unfiltered form. Preserve
other filters and authorization; never combine fixed-ID SQL with an incompatible
indexed identity.

`$cache_warmup('order_id','Connector=bq_metrics_prewarm','IndexParameter=OrderId','Period=today,yesterday')`
uses declared canonical parameters and an explicit dedicated warmup connector.
Both connectors must address equivalent authorized data and the same intended
native cache. Bound case products, timeout and MaxCases. Warmup HTTP requires
explicit administration policy and a server-owned lifetime.

Warm a full ordinary projection, then narrow compatible regular fields. Cubes
must retain every warmed dimension and may select a subset of measures. Dropping
a dimension requires reaggregation. Parent release-candidate race tests pass on
AFS and live Docker Aerospike for full-projection narrowing, regular/cube requests,
indexed groups, pagination, empty cases and replay after table removal. Earlier
native corrected-stream regressions also pass. Attribute those runs to the
parent, not the documentation author; the author's sandbox restriction is not
missing capability acceptance. No production-scale/vendor qualification follows.
The exact-name correction still awaits final code delivery and review.

A warmup miss can use the selected native service's lazy/exact-query DB fill.
This is not fallback between backends. Inspect native query/warmup/marker identity,
hit/miss/error and expiry statistics; unknown physical record counts stay unknown.
Prove warm hit without DB, unwarmed fill then hit, expiry and connector execution.
See [cache and warmup](references/product/datly/doc/cache-and-warmup.md).

## Selectors and output

Use one-argument `.QuerySelector('inventory')` or `querySelector:"inventory"`.
Each view has its own projection, pagination, filter and order controls; binding
locations and allowed columns/methods are authored. Keep relation keys internally
and presentation request-local. **Draft correction:** exact names, only user-defined
aliases, duplicate output column errors; no inferred case/snake/camel variants.

JSON/CSV/XML/tabular/XLSX selection and direct/named singleton output have bounded
candidate proof. Native CSV expands relation slices; XML uses result/row and
explicit selected null holders; XLSX null cells are blank. Named `output/body`
and `output/view` identify the same existing row-holder authority. Do not infer
a child collection as root after selection. Read [selectors and formats](references/product/datly/doc/selectors-and-formats.md)
for null, envelope and format-specific limitations.

## Other operational tasks

- [Project build](references/project-build.md): automatic discovery/linking, the pinned release graph and source-backed deployment.
- [JWT and predicates](references/tags-and-interfaces.md#jwt-input-and-authorization-predicates): verified declared input and bound values, never ambient claims.
- [Async](references/product/datly/doc/async.md): original 34-column job schema, AFS events, canonical replay/current authorization, explicit HTTP controls, cache-only result inspection and limited reader dryrun.
- [Observability](references/product/datly/doc/observability.md): native capture plus optional default-off bounded async OTel export; no zero-cost claim.
- [API documentation](references/product/datly/doc/api-documentation.md): `$DocGlobalURLs`, `$DocURL`, `$DocURLs`, `$DocBaseURL`; global then rule YAML, explicit annotation precedence and shared embedded OpenAPI/MCP schemas.
- [Static content](references/product/datly/doc/static-content.md): `$static_resource('site','public')` or `$static_content('content-url','root')`, resource manifests, explicit filesystem authority and CORS.
- [Developer MCP](references/developer-mcp.md): seven authoring tools versus business tools, folder publication, declared Final SEP-2640 skills/list/get and canonical bundle generation.
