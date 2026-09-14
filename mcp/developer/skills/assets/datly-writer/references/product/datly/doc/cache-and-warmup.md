# Native read caches and warmup

[All guides](references/product/datly/doc/README.md) · [Reports](references/product/datly/doc/reports.md) · [Configuration](references/product/datly/doc/configuration.md)

Use a read cache to reuse SQLX query results, and warmup to populate known query
cases before ordinary requests arrive. Datly attaches the actual native SQLX
`cache.Cache` service to each prepared view. Normal reads and warmup use that
same service; there is no Datly row replay cache or handler invalidation layer.

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
qualification is claimed here. See [status](references/product/datly/doc/status.md) for the release boundary.

## Warmup query cases

The DQL parser supports a dedicated connector, an index column/parameter and case
values:

```sql
#setting($_ = $cache_warmup('order_id', 'Connector=bq_metrics_prewarm', 'IndexParameter=OrderId', 'Period=today,yesterday', 'Granularity=hour,day'))
```

This fragment requires those canonical input parameters, index column and both
connectors to exist. The `Period`/`Granularity` values expand into combinations;
select limits deliberately rather than accidentally warming an unbounded product.
Configured `CacheWarmupSettings` additionally exposes `Limit`, `MaxCases`,
`FieldNames`, `IndexColumn`, `IndexParameter`, `IndexMeta`, `Connector` and `Cases`.
Per-case field names and excluded defaults belong to the typed settings; do not
invent DQL options for every Go field.

`IndexMeta` selects related output queries for warmup too. Each target needs its
native cache service. Limits and counts describe completed warmup work, not a
promise that every later query shape can reuse it. Partial failure can leave
already-populated entries; warmup is not a cross-query cache transaction.

## Choose the warmup identity to match the SQL

For a query that always filters one ID, warm the concrete parameter case without
an index column:

```sql
#setting($_ = $cache_warmup('', 'ID=1'))
SELECT id, name FROM records WHERE id=:ID
```

This fragment assumes a declared `ID` input. It warms that SQL/argument pair;
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
to the dedicated warmup connector. The [reader warmup owner](references/product/datly/sql/reader/warmup.go.txt)
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

The operation still binds the target's declared credentials/parameters and
preserves authorization predicates. Its accepted work uses the server lifetime
with a bounded timeout; client disconnection does not define success or abandon
completion accounting. Use the `Completed` callback for the actual result/error.
Configured defaults and case budgets must be authorized as deliberately as an
ordinary request. [HTTP warmup tests](references/product/datly/gateway/http/warmup_policy_test.go.txt) cover
partial results, limits and failed setup; [JWT warmup tests](references/product/datly/gateway/http/warmup_jwt_sqlite_test.go.txt)
cover declared verified credentials. Standalone services expose warmup admin configuration. Startup warmup and each actual backend/connector combination need their own acceptance.

## Expiry, writes and diagnosis

Read-cache TTL is independent of async job TTL, event retention and application
message delivery. Handler DML does not automatically invalidate every related
read cache. Define freshness/refresh expectations before enabling caching.

For an unexpected miss, compare actual native SQL/arguments, selected projection,
prepared view identity, namespace, TTL and index metadata. For wrong rows, test
complete composite keys and authorization cases. Measure cold read, hit, warmup,
expiry and failure separately. [Native observability](references/product/datly/doc/observability.md) supplies
existing cache metadata without inventing a second key scheme. Run bounded
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
