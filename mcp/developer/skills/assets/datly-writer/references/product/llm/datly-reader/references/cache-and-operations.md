# Cache, warmup and reader operations

Read for operational reader choices. The candidate and the final exact-name
contract have different acceptance states; consult [status](developer-mcp.md#operation-based-generation-to-pure-go).

## Cache and warmup

AFS and Aerospike are independent explicit choices. Use the actual native SQLX
cache service for a prepared view; no backend switching threshold, Datly row-map
cache or handler invalidation layer. Authored AFS uses a location and positive
TTL; candidate Aerospike uses its provider URL, namespace, set and positive
whole-second TTL. TTL and timeToLiveMs must agree when both are present. Native
client ownership must outlive consumers and drain before close.

Declaration fragment; adapt to the [complete reader contract](reader-examples.md#parameterized-dql-reader)
with the target view identity and existing input/output bindings.

```sql
#setting($_ = $cache('records', '5m').WithProvider('afs').WithLocation('cache/records'))
```

For an explicitly selected Aerospike backend, the candidate's authored form is:

Declaration fragment; adapt to the [complete reader contract](reader-examples.md#parameterized-dql-reader)
with the target view identity and existing input/output bindings.

```sql
#setting($_ = $cache('records').WithProvider('aerospike://127.0.0.1:3000/test').WithLocation('records').WithTimeToLiveMs(60000))
```

Use the application's configured endpoint/namespace/set. Health or port checks
alone do not establish native cache, TTL or shutdown behavior. No capacity or
production cluster qualification follows from bounded fixtures.

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
Verify exact names and explicit aliases on the connected build.

A warmup miss can use the selected native service's lazy/exact-query DB fill.
This is not fallback between backends. Inspect native query/warmup/marker identity,
hit/miss/error and expiry statistics; unknown physical record counts stay unknown.
Prove warm hit without DB, unwarmed fill then hit, expiry and connector execution.
Read-cache TTL differs from async job retention. Writes do not automatically
invalidate related read caches. Inspect SQL/arguments, projection, prepared view,
namespace and index identity for misses. Warmup administration uses eligible GET
routes beneath `Meta.CacheWarmURI` (default `/v1/api/cache/warmup`), with explicit
admin authorization and a positive bounded timeout. Bind the target's declared
credentials too. Accepted work uses the server lifetime and reports actual
completion; client disconnection is not a success signal.
See [cache and warmup](../../../datly/doc/cache-and-warmup.md).

## Selectors and output

Use one-argument `.QuerySelector('inventory')` or `querySelector:"inventory"`.
Each view has its own projection, pagination, filter and order controls; binding
locations and allowed columns/methods are authored. Keep relation keys internally
and presentation request-local. Use exact names, only user-defined
aliases, duplicate output column errors; no inferred case/snake/camel variants.

JSON/CSV/XML/tabular/XLSX selection and direct/named singleton output have bounded
candidate proof. Native CSV expands relation slices; XML uses result/row and
explicit selected null holders; XLSX null cells are blank. Named `output/body`
and `output/view` identify the same existing row-holder authority. Do not infer
a child collection as root after selection. Read [selectors and formats](../../../datly/doc/selectors-and-formats.md)
for null, envelope and format-specific limitations.

## Other operational tasks

- [Project build](project-build.md): automatic discovery/linking, the pinned release graph and source-backed deployment.
- [JWT and predicates](tags-and-interfaces.md#jwt-input-and-authorization-predicates): verified declared input and bound values, never ambient claims.
- [Async](../../../../mutation-messages.md#async-and-dry-run): original 34-column job schema, AFS events, canonical replay/current authorization, explicit HTTP controls, cache-only result inspection and limited reader dryrun.
- [Observability](../../../datly/doc/observability.md): native capture plus optional default-off bounded async OTel export; no zero-cost claim.
- [API documentation](../../../datly/doc/api-documentation.md): `$DocGlobalURLs`, `$DocURL`, `$DocURLs`, `$DocBaseURL`; global then rule YAML, explicit annotation precedence and shared embedded OpenAPI/MCP schemas.
- [Static content](../../../datly/doc/static-content.md): `$static_resource('site','public')` or `$static_content('content-url','root')`, resource manifests, explicit filesystem authority and CORS.
- [Developer MCP](developer-mcp.md): seven authoring tools versus business tools, folder publication, declared Final SEP-2640 skills/list/get and canonical bundle generation.

## Configured reports

Selected linked packages can enable `report=true` on groupable GET readers and
`reportCompose=true` for composition. Keep the source reader and its declared
JWT/non-query inputs. Explicit cube filters override source values; omitted cube
filters retain source binding. Composition masks omitted frame filters so they
cannot inherit unrelated outer request values. Composition uses declared SQL output aliases or explicit authored mappings,
not inferred Go/JSON name variants. Cache reuse must retain every warmed grouping
dimension and may narrow measures. Source/URI reload publishes the complete report
set atomically. API-key-only HTTP routes must set `reportMCPTool=false` and
`reportComposeMCPTool=false`, or declare an MCP-compatible authorization policy.
See [composition examples](reader-examples.md#cubecompose) for frame
inheritance and budgets. Verify actual configured HTTP and native MCP execution; metadata discovery alone
is not runtime proof.
