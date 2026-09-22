# Cache management

Datly uses SQLX v0.26.0 and adds persistent, view-scoped generations to native
cache identities. Both request-populated (lazy) and prewarmed entries remain
subject to their configured TTL.

## Invalidate a component or view

For a GET component at `/v1/api/orders`, Datly registers:

```http
POST /v1/api/cache/invalidate/orders
Content-Type: application/json

{"scope":"all"}
```

The path follows the same component-prefix mapping as the existing warmup route.
`Meta.CacheInvalidateURI` configures the prefix, defaulting to
`/v1/api/cache/invalidate`.

- `all` (default) retires lazy entries and warmup publications.
- `lazy` retires request-populated entries only. Warmup hits remain eligible.
- `warmup` retires prewarmed entries only. Existing lazy hits remain eligible.
- `view` optionally selects a view by name, including a nested view. Omitting it
  selects every cached view reachable through the component's `With` relations,
  including views without a warmup configuration.

```json
{"view":"order_items","scope":"lazy"}
```

An empty request body defaults to `all`. The response reports each view's scope
and new generation. Unknown views return 404; invalid JSON, unknown properties,
and invalid scopes return 400. Storage failures return 500 with per-view results;
a multi-view request is not a transaction, so some views can succeed while others
fail. Retrying invalidation is safe.

The route inherits the component's API-key requirements and the gateway's subnet
policy. Invalidating caches requires the same deployment access controls as other
administrative routes. Rejected API keys stop execution before any mutation.

## Repopulate after invalidation

Invalidation does not query the database. The next request fills a lazy entry.
To repopulate configured warmups, call the existing route after invalidation:

```http
POST /v1/api/cache/warmup/orders
```

Use `all` before warming if previously populated lazy results must also be
retired. `warmup` alone intentionally preserves lazy results. Invalidating a
warmup retires every configured warmup case for the selected view.

A forced reader cache refresh retires both generations for the view before
reading. This is intentionally broader than an exact-query refresh, and prevents
Aerospike from satisfying the refresh with an older warmup publication.

## Storage and concurrency

A cache owner is derived from the resource identity, view name, and connector
configuration. Resource paths below `/routes/` are made route-relative so replicas
with different deployment roots agree. Views with the same owner intentionally
share their cache; invalidation affects that shared view wherever it is used.
Standalone resources outside `/routes/` need consistent resource URLs across
replicas. Read SQL is unchanged. Warmup queries receive a cache-identity SQL
comment; query semantics and bound arguments are unchanged.

AFS stores generation files under `<expanded location>/.datly-generations/<owner>`.
Local generation changes use atomic file replacement. Object-storage deployments
must provide atomic object replacement and read-after-write consistency for these
control objects. Aerospike stores a permanent control record per owner, with
independent `all`, `lazy`, and `warmup` bins. Concurrent scope changes do not
replace one another's bins.

Readers obtain the shared generation on every lookup. Operations already in
flight may finish using their captured generation. A writer that started before
invalidation can only publish into its old generation; later lookups cannot use
it. All cache instances must use the same underlying storage to share invalidation;
separate local disks and per-process memory caches are independently managed.

Generation controls must be retained and excluded from external cache cleanup or
Aerospike eviction. Removing control state while retaining payloads can make old
generations reachable. Invalidation is logical, not a storage purge. Aerospike
reclaims old payloads through its TTL. AFS does not run a background garbage
collector: retain your storage lifecycle/cleanup policy for expired JSON payloads,
excluding `.datly-generations`.

Upgrading introduces a new cache-key namespace, so existing cache entries are
cold until lazily refilled or prewarmed. Existing payloads need normal cleanup.

## TTL and location configuration

`TimeToLiveMs` must be positive and fit in a Go duration. For Aerospike, it must
be an exact number of seconds; subsecond/fractional-second values and reserved
server TTL values are rejected instead of truncated. For example, use `1000` or
`60000`, not `500` or `1500`. AFS supports millisecond TTLs.

Cache-provider locations support view expansion:

```yaml
CacheProviders:
  - Name: local
    Location: /cache/${View.Name}
    TimeToLiveMs: 60000
```

`${View.Alias}` and `${View.Table}` are also supported. The escaped-dot forms
(`${View\.Name}`, `${View\.Alias}`, `${View\.Table}`) are accepted too. For Aerospike the expanded
location is the set name and must satisfy Aerospike's set-name rules.

## Verification

The managed-cache tests exercise native AFS/SQLite cache reads and writes, TTL
expiry, both warmup forms, scope isolation, owner isolation, refresh, persistence
across cache instances, and invalidation during lazy/warmup writes. HTTP tests
cover nested lazy-only views, input errors, and API-key rejection. Generation-store
tests cover concurrent file publication and Aerospike's independent bins and
nonexpiring control-record write policy.

A live Aerospike test is available against a dedicated test server:

```sh
DATLY_TEST_AEROSPIKE=aerospike://127.0.0.1:3000/test \
  go test ./internal/cache/managed -run TestManagedAerospikeIntegration -count=1 -v
```

The existing `e2e/local/system.yaml` uses Aerospike `ce-6.2.0.2` at that address.
The test uses unique owner identities in `datly_cache_test`, short payload TTLs,
and removes its own generation records. It does not truncate a namespace or set.
It is skipped when `DATLY_TEST_AEROSPIKE` is unset.
