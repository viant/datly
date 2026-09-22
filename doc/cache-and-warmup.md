# Native read caches and warmup

[All guides](README.md) · [Reports](reports.md) · [Configuration](configuration.md)

Use a read cache to reuse SQLX query results, and warmup to populate known query
cases before ordinary requests arrive. Datly attaches the actual native SQLX
`cache.Cache` service to each prepared view. Normal reads and warmup use that
same service. Datly adds shared generation controls for administrative invalidation;
row storage, scanning, TTL and publication remain native SQLX responsibilities.

## Choose a backend explicitly

| Backend | How it is selected in this snapshot | Verification boundary |
| --- | --- | --- |
| AFS | Authored `provider=afs` (or empty provider), a location and positive TTL; or a directly supplied native cache. | Existing SQLite/native AFS cases cover typed read/cache slices. Remote AFS providers need their own credentials and acceptance. |
| Aerospike | Authored Aerospike provider URL, namespace, set/location and positive TTL; or direct native injection through `ReaderRuntimeConfig.ReadCaches`. | Authored provider URLs and pooled native clients are present in the candidate; parent release-candidate live Docker race tests pass the bounded AFS/Aerospike matrix. Dependency publication/main integration remain separate from that acceptance. |

AFS and Aerospike are independent user-selected mechanisms. There is **no automatic
switch**, million-row threshold or fallback chain between them. Choose from
application workload, storage topology and measured capacity. TTL is required
for authored caches, including the requested Aerospike configuration; indefinite
retention must not be silently substituted.

## Enable AFS

A component-level DQL declaration can carry current AFS settings:

Declaration fragment; adapt to the [complete reader contract](../llm/datly-reader/references/reader-examples.md#parameterized-dql-reader)
with the target view identity and existing input/output bindings.

```sql
#setting($_ = $cache('records', '5m').WithProvider('afs').WithLocation('cache/records'))
```

This is a declaration fragment to add to a component with a real query and
configured connector. It is not a standalone cache service. For multi-view named
configuration, select the named cache in view metadata (for example
`use_cache(r, 'records')`) and supply matching host settings:

```go
// Embedding fragment. configuredSQL and artifact are already built by the host.
reader, err := artifact.ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{
    SQL: configuredSQL,
    CacheSettings: map[string]*spec.CacheSettings{
        "records": {Enabled: true, Name: "records", Provider: "afs",
            Location: "cache/records", TTL: "5m"},
    },
})
// Handle err and attach reader to the component's existing registration.
```

Imports are `github.com/viant/datly/bootstrap` and `github.com/viant/datly/spec`.
Do not discard registration errors or register a second reader to hide them.
`TTL` is a positive Go duration; `TimeToLiveMs` is the numeric alternative. If
both are supplied they must agree. A cache name without enabled settings or a
supplied native service fails registration. Per-view identity includes component,
prepared path and connector so unrelated views cannot collide just because SQL
text matches.

Direct native services use `ReadCaches`, keyed by the exact resolvable view
identity/path, **not** by a provider name or arbitrary cache name. The registration
owner resolves these to exact prepared `*data.View` keys. The host retains the
native service's lifecycle.

## Aerospike configuration in the candidate

The candidate accepts this authored form, preserved from
original Datly syntax:

Declaration fragment; adapt to the [complete reader contract](../llm/datly-reader/references/reader-examples.md#parameterized-dql-reader)
with the target view identity and existing input/output bindings.

```sql
-- Candidate syntax; configure and verify the selected backend before deployment.
#setting($_ = $cache('records').WithProvider('aerospike://127.0.0.1:3000/test').WithLocation('records').WithTimeToLiveMs(60000))
```

The native pool validates endpoint, namespace, set and a positive
whole-second TTL; it owns clients for an explicit service lifetime. Its reviewed
health-accounting correction counts native record operations rather than
duplicating wrapper failures. Retry, operation timeout and failure-probing options
are still not current standalone fields. Zero retry/timeout values must not be
advertised as "disable retries" or "bounded operation"; the native defaults
differ by setting.

Integration requires the native revision, main wiring, lifetime shutdown,
publication and real cache/TTL acceptance together. A server-port check or
successful metadata roundtrip does not establish that path. No fixed row capacity,
throughput, supported production cluster configuration or current vendor-version
qualification is claimed here. See [status](status.md) for the release boundary.

## Warmup query cases

The DQL parser supports a dedicated connector, an index column/parameter and case
values:

Declaration fragment; adapt to the [complete reader contract](../llm/datly-reader/references/reader-examples.md#parameterized-dql-reader)
with the target view identity and existing input/output bindings.

```sql
#setting($_ = $cache_warmup('order_id', 'Connector=bq_metrics_prewarm', 'IndexParameter=OrderId', 'Period=today,yesterday', 'Granularity=hour,day'))
```

This fragment requires those canonical input parameters, index column and both
connectors to exist. The `Period`/`Granularity` values expand into combinations;
select limits deliberately rather than accidentally warming an unbounded product.
Configured `CacheWarmupSettings` additionally exposes `Name`, `Priority`,
`Limit`, `MaxCases`, `FieldNames`, `IndexColumn`, `IndexParameter`, `IndexMeta`,
`Connector`, `CaseRefs` and `Cases`. Per-case field names and excluded defaults
belong to the typed settings; do not invent DQL options for every Go field.

A cache may declare several warmups: the singular `Warmup` plus an ordered
plural `Warmups` list, with named reusable case sets in `SharedCases`. The
singular contract is unchanged and executes first; plural entries follow in
declaration order and each warmup owns its cases, connector, limits, projection
and index settings. `CaseRefs` expand per warmup ahead of inline cases with no
cartesian product across indexes and no shared mutable case slices. Duplicate
effective warmup names or index identities (an absent name derives from
`IndexParameter`, then `IndexColumn`) fail initialization, and an empty plural
list never shadows a valid singular warmup. A regular request selects the most
restrictive supplied index by explicit `Priority`; equal priorities let the
later, more specific declaration win. Required index inputs are omitted only
for the warmup that owns them.

`IndexMeta` selects related output queries for warmup too. Each target needs its
native cache service. Limits and counts describe completed warmup work, not a
promise that every later query shape can reuse it. Partial failure can leave
already-populated entries; warmup is not a cross-query cache transaction.

## Choose the warmup identity to match the SQL

For a query that always filters one ID, warm the concrete parameter case without
an index column:

```sql
#package('example.com/app/records/warmup')
#setting($_ = $input_type('RecordsInput'))
#setting($_ = $output_type('RecordsOutput'))
#setting($_ = $case_format('lc'))
#setting($_ = $route('/records', 'GET'))
#setting($_ = $connector('main'))
#define($_ = $ID<int>(query/id).Required())
#define($_ = $Records<[]*Record>(output/view))
#setting($_ = $cache_warmup('', 'ID=1'))
SELECT r.id, r.name, type(r, 'Record') FROM records r WHERE r.id=:ID
```

Configure `main`, `records(id,name)` and the native read cache for this reader.
`RecordsOutput.Records []*Record` receives the root rows as JSON `records`.
The explicit `ID` input supplies the exact warmup SQL/argument pair;
another ID can fill its own lazy cache entry. A successful warmup response alone
does not prove later reuse: verify the read after making the database unavailable.

Indexed warmup needs an all-row form of the query. Ordinary lookup clears the
configured index parameter when computing the warmup identity, then retrieves
its requested groups. The authored predicate must support that cleared-input
form, and warmup must use the same identity. Combining fixed `id=:ID` SQL with
`IndexParameter=ID` and a concrete `ID=1` warmup case produces different identities.
Use per-query warmup for that SQL, or author an optional predicate and warm its
whole-group form. Other inputs, such as tenant authorization and period, remain
part of the identity and must match.

## A separate connector for prewarming

Configure ordinary reads with their normal connector and set `Warmup.Connector`
to the dedicated warmup connector. The [reader warmup owner](../sql/reader/warmup.go)
resolves that connector for DB execution, then writes through the same native
cache already assigned to the prepared view. With no override, it uses the view's
normal connector. Unknown connector names fail explicitly.

For BigQuery, an application might assign the warmup workload to a reserved
capacity configuration and keep ordinary requests on an on-demand configuration.
This is a **deployment choice** implemented by your connector/project/job policy.
Datly does not create a reservation, infer billing mode from a name, or switch it
automatically. Both connectors must query equivalent authorized data and populate/
read the intended cache namespace. Verify connector selection with query evidence;
a parsed string alone does not prove a dedicated DB path or cache hit.

## Full projection and narrower regular requests

The requested reuse contract distinguishes two cases:

| Warmed result | Required regular-query compatibility |
| --- | --- |
| Ordinary row projection | Warm a full projection; a narrower request may reuse compatible stored fields through the native cache/mapping owner. |
| Grouped cube projection | Keep **all warmed dimensions**; select a subset of measures. Dropping dimensions changes grouping and cannot be treated as column hiding. |

Parent release-candidate race tests pass for both AFS and a real Docker-backed
Aerospike service: full-projection narrowing, ordinary and cube requests, indexed
groups, per-group pagination, missing/duplicate groups, empty results and replay
after source tables are removed. Earlier native corrected-stream regression tests
also pass. These are executed parent acceptance results, not tests rerun by the
documentation author. The author's earlier sandbox connection denial does not
make these implemented and tested capabilities blocked.

Acceptance is bounded to those fixtures and the tested configuration; it does not
qualify production scale, arbitrary clusters or vendor versions. Cubes still retain
all warmed dimensions. The reviewed naming and duplicate-column corrections
are integrated and included in the bounded live-cache acceptance.

## Invoke warmup and secure its HTTP surface

The runtime exposes a warmup operation for eligible reader targets. HTTP adds
POST administration under `Meta.CacheWarmURI` (default
`/v1/api/cache/warmup`), mapping eligible exposed GET route paths beneath it.
It requires a server-owned lifetime, positive timeout and explicit administrator
authorizer. Merely setting a URI does not install those services.

The operation still binds the target's declared credentials/parameters. Its accepted work uses the server lifetime
with a bounded timeout; client disconnection does not define success or abandon
completion accounting. Use the `Completed` callback for the actual result/error.
Configured defaults and case budgets must be authorized as deliberately as an
ordinary request. [HTTP warmup tests](../gateway/http/warmup_policy_test.go) cover
partial results, limits and failed setup; [JWT warmup tests](../gateway/http/warmup_jwt_sqlite_test.go)
cover declared verified credentials. Standalone services expose warmup admin configuration. Startup warmup and each actual backend/connector combination need their own acceptance.

### Detect warmup in a custom authorization predicate

Custom predicates receive immutable invocation metadata through both their
`context.Context` and the reserved `invocation` binding. Both access paths return
the same `xdatly/handler/exec.InvocationInfo`. Warmup preparation and cache
filling have distinct phases:

```go
type AuthorizationPredicate struct {
    Input      *SearchInput                    `bind:"kind=input,required"`
    Invocation *handlerexec.InvocationInfo     `bind:"kind=invocation,required"`
}

func (p *AuthorizationPredicate) Compute(ctx context.Context, value any) (*predicate.Criteria, error) {
    info := p.Invocation
    if info.MayBypassRowAuthorization() {
        return nil, nil
    }
    return authorizedCriteria(p.Input, value)
}
```

Use `MayBypassRowAuthorization`, rather than inferring permission from
`IsCacheWarmup`. The capability is installed only by the server-owned warmup
operation and is available during both `WarmupPhasePrepare` and
`WarmupPhaseFill`.

This capability only controls application predicate behavior. It does not skip
HTTP warmup administrator authorization, API-key checks, required input binding,
credential codecs, authored warmup cases or case budgets. A required JWT still
needs a valid credential even when a predicate elects to omit its row filter.
Before returning no authorization criteria, ensure the warmed cache identity and
ordinary read path cannot expose a broad entry across tenants or principals.

## Expiry, writes and diagnosis

Read-cache TTL is independent of async job TTL, event retention and application
message delivery. Handler DML does not automatically invalidate every related
read cache. Define freshness/refresh expectations before enabling caching.

For an unexpected miss, compare actual native SQL/arguments, selected projection,
prepared view identity, namespace, TTL and index metadata. For wrong rows, test
complete composite keys and authorization cases. Measure cold read, hit, warmup,
expiry and failure separately. [Native observability](observability.md) supplies
native cache metadata, persisted creation/expiry timestamps and publication counters. Run bounded
realistic workloads before making memory or latency claims.

## Miss fallback and statistics

A native warmup miss may fall back to the normal database read and populate the
native lazy/exact-query entry. This is fallback within the selected SQLX cache
service, not switching from Aerospike to AFS. Completed async result inspection
uses cache-only lookup unless an authorized explicit refresh is requested; it
must not silently query a database to recreate an expired result.

Inspect native cache hit/miss, query key, warmup/marker identity, expiry and error
statistics. A refresh conflict stays an error; it is not a successful hit.
Physical cache record counts can remain unknown; do not substitute returned rows
for storage counts. Prove a warmed hit with the source unavailable, an unwarmed
lazy fill then hit, TTL expiry and the dedicated connector actually used.

## Cache provider location expansion

Named provider settings and component settings expand `${View.Name}`,
`${View.Alias}`, and `${View.Table}` before application constants are resolved.
`Name` is the canonical view name, `Alias` is its SQL namespace, and `Table` is
`ViewSource.Table`. An absent alias or table expands to an empty string. Escaped-dot
forms such as `${View\.Name}` and unbraced `$View.Name` are accepted as well.

```go
"records": {
    Enabled: true, Provider: "afs", TTL: "5m",
    Location: "cache/${View.Name}/${View.Alias}/${View.Table}",
}
```

Each prepared view still receives its own component/path/connector namespace.
Two views can share a provider location without sharing query entries.

## Invalidate lazy and prewarmed caches

Enable the HTTP control with an explicit administrator policy:

```go
httpConfig.CacheInvalidation = &http.CacheInvalidationConfig{
    Timeout: 10 * time.Second,
    Authorize: authorizeCacheAdministrator,
}
httpConfig.Meta.CacheInvalidateURI = "/v1/api/cache/invalidate"
```

Here `http` is `github.com/viant/datly/gateway/http`. The authorization callback
receives `(context.Context, *net/http.Request, exec.ComponentTarget)` and must
validate administrative access to the exact server-selected target. Enabling this
control is independent of configuring HTTP warmup. It includes lazy-only views.
Existing subnet, route API-key, application authorization and CORS policies apply.

For a GET route `/v1/api/orders`, using `APIPrefix: "/v1/api"`:

```http
POST /v1/api/cache/invalidate/orders
Content-Type: application/json

{"view":"items","scope":"all"}
```

`view` selects a prepared view using its canonical name or registered alias;
unknown/ambiguous selections are rejected. Omit it to invalidate every cached
view in the component, including children. Path parameters select the component
route; they do not narrow invalidation to one query-argument value.

`scope` is `all` (default), `lazy`, or `warmup`. The response reports each selected
view and its new generation. Warmup-only invalidation preserves existing lazy
entries, and lazy-only invalidation preserves warmup publications. To refresh all
cached data, use `all`, then invoke the existing warmup endpoint if desired.
Invalidation itself does not execute SQL. Programmatic callers can use
`Runtime.InvalidateCache(ctx, target, view, scope)` on a server-owned target.
Directly supplied cache services must implement scoped invalidation; unsupported
services produce an explicit error before changes are made.

Generation controls live in the same shared backend as the data. Readers obtain
the current generation on each lookup. Requests already in flight may finish;
older writers can only publish into their retired generation. Other instances
sharing the same backend observe invalidation without process-local broadcasts.
Separate local disks/memory stores are separate cache domains. Multi-view
invalidation is not transactional: storage failures return per-view results and
can leave a partially completed operation. Retrying is safe.

Keep generation controls outside external TTL/eviction cleanup. Removing them
while retaining payloads can make old generations reachable. Aerospike control
records do not expire. AFS stores controls below `.datly-generations`; data TTL
prevents serving expired payloads but is not a background file garbage collector.
Storage lifecycle rules may clean expired payloads while retaining the controls.

The generation namespace makes pre-upgrade entries cold. Normal reads and warmup
refill the new namespace. Exact-query refresh retires the selected exact entry
and matching warmup publication, preserving unrelated authored cases. Use
component/view invalidation for a broader refresh.

## Creation timestamps and counters

Completed SQL metrics expose `cacheStats.createdTime` and `expiryTime`.
Creation time is persisted by SQLX, remains stable on hits, and changes when a
new entry is published. Legacy entries without timestamps omit `createdTime`.
Authorized diagnostic headers retain these fields; OpenTelemetry spans export
`cache.created_unix_nano` and `cache.expiry_unix_nano`.

Per-view native counters include `cache:created`, `cache:lazy_created`, and
`cache:warmup_created`. They count successful publications, including empty-result
entries and index markers, excluding overflow chunks. Hits, failed writes,
rollbacks and reused AFS warmups do not increment them. `cache:miss_write` remains
the separate attempt counter. Invocation-local warmup observers preserve metric
ownership across concurrent reader executions and reloads.

Standalone JSON/YAML configuration can enable the same control without embedding
an authorization callback:

```yaml
CacheInvalidation:
  TimeoutMs: 10000
  Admin:
    APIKeyHeader: X-Cache-Admin
    APIKeyValue: replace-with-your-admin-secret
Meta:
  CacheInvalidateURI: /v1/api/cache/invalidate
```

Configure the administrator credential through your deployment's configuration
handling. Standalone preloads GET component metadata when this control is enabled
so caches declared on nested Go output types are available before the first read.
Only components with actual cached views receive invalidation routes.
