    # Generalized requirement: authorization-aware hierarchical cube warmup

## Goal

Datly 1.0 must support warmup-cache reuse for an authorized cube whose callers
may select entities at different levels of an ordered hierarchy, while preserving
row-level security, exact cache identity, and correct grouping semantics.

This requirement is intentionally domain-neutral. Example hierarchy levels are:

```text
tenant > collection > group > item
```

Applications supply their own level names, ownership relations, SQL columns, and
authorization policy.

## Problem statement

A cube may be queried using any one of several hierarchical dimensions. The
request can also contain multiple levels. The system must choose the most
specific supplied level, validate its relationship to any broader supplied
levels, expand the selection through an authorized SQL lookup when necessary,
and produce a parameterized row predicate.

Warmup caching adds a second constraint: the cache index used for the request
must correspond to the selected hierarchy level. A cache built under one index
dimension cannot be treated as if it were indexed by another. Query SQL,
arguments, grouping grain, time bounds, and selected index strategy must all
participate in deterministic cache identity.

## Functional requirements

### 1. Declarative hierarchy metadata

A cube component must be able to declare an ordered set of scope levels. Each
level defines:

- public parameter name;
- physical result/index column;
- priority or parent/child order;
- value type;
- optional aliases;
- expansion/ownership resolver;
- maximum accepted and expanded ID counts.

Selection uses suppliedness/presence, not language zero values. The most specific
present level wins.

### 2. Scope consistency

When a request supplies more than one level, the selected specific entities must
belong to every supplied broader scope. Broader values are consistency constraints,
not alternate OR branches.

An inconsistent hierarchy must return a typed authorization or validation error.
It must not silently discard a narrower selector or fall back to a broader one.

### 3. SQL-backed expansion

The scope resolver must be able to use a server-configured SQL connector. It must
not accept a connector name or SQL source from the client.

The resolver returns canonical typed values and trusted predicate metadata. All
runtime values are emitted as ordered bind arguments:

```go
&predicate.Criteria{
    Expression:   "resource_item_id IN (?,?,?)",
    Placeholders: []any{11, 12, 13},
}
```

Values must be validated, deduplicated, and stably sorted. Expansion must be
bounded and cancellation-aware. Lookup errors fail closed.

### 4. Separation of normalization and predicate rendering

Datly must distinguish:

1. scope normalization, which may update canonical component input and cache-key
   bindings before query identity is built;
2. predicate rendering, which converts the normalized scope to SQL plus ordered
   bind values.

A predicate-only API is insufficient when cache matching depends on the
normalized values or selected index strategy. Normalization must run before the
prepared query/cache matcher is constructed.

### 5. Singular and plural warmups

A warmed view must support both the existing singular warmup and an ordered
collection of warmups. The singular contract remains valid; plural support is
additive.

Conceptually:

```go
type Cache struct {
    Warmup  *Warmup
    Warmups []*Warmup
}
```

Each `Warmup` retains the complete existing configuration, including index
column, index parameter, cases, fields, connector, limit, and case budget.

Normalization rules are:

- singular only: execute the existing path unchanged;
- plural only: execute all plural items in order;
- both: singular first, then plural;
- reject duplicate effective warmup identities;
- never let an empty plural value shadow a valid singular value.

The runtime should expose one canonical `EffectiveWarmups()` accessor while
retaining existing singular accessors for source compatibility.

#### Case targeting

Cases are scoped to an individual warmup/index by default. The runtime iterates
warmups and generates only the cases owned by each warmup. It must not create a
global Cartesian product between every declared case and every declared index.

Some cases apply to several indexes. Datly 1.0 should allow named case sets to be
referenced by one or more warmups:

```go
type Cache struct {
    Warmup        *Warmup
    Warmups       []*Warmup
    SharedCases map[string][]*CacheParameters
}

type Warmup struct {
    Name     string
    CaseRefs []string
    Cases    []*CacheParameters
    // existing warmup fields
}
```

References are expanded during initialization into immutable per-warmup case
lists. Inline cases are appended after referenced cases. Duplicate cases within
one warmup are removed using canonical typed parameter/value identity. The same
case on two different warmups is not a duplicate because index strategy is part
of cache identity.

Named sets are optional authoring syntax; duplicating a case in two declarations
is semantically equivalent. Implementations must not share mutable case slices
between warmups.

#### Metadata compatibility with original Datly

Datly 1.0 must not invent a separate warmup wire model. Its authoring pipeline
must compile to the same JSON/YAML metadata accepted by original Datly:

```go
type Cache struct {
    Warmup         *Warmup
    Warmups        []*Warmup
    SharedCases map[string][]*CacheParameters
}

type Warmup struct {
    Name           string
    Priority       int
    IndexColumn    string
    IndexParameter string
    IndexMeta      bool
    Limit          *int
    MaxCases       *int
    FieldNames     []string
    Connector      *Connector
    CaseRefs       []string
    Cases          []*CacheParameters
}

type CacheParameters struct {
    Set        []*ParamValue
    FieldNames []string
}

type ParamValue struct {
    Name           string
    Values         []interface{}
    ExcludeDefault bool
}
```

Serialized field names use the exported names above with the existing
`omitempty` behavior. Existing singular fields retain their original meaning;
plural warmups and reusable case sets are additive.

Both implementations must use identical normalization:

- execution order is singular first, then plural; request selection uses explicit
  priority and lets the later broad-to-specific declaration win equal-priority
  ties so the most restrictive supplied scope is selected;
- absent `Name` derives from `IndexParameter`, then `IndexColumn`;
- duplicate effective names or index identities are rejected;
- case references expand before inline cases;
- case values use the target parameter's type and date format;
- canonical case values and unordered ID lists are stably sorted;
- `Period` and `Granularity` are ordinary case parameters;
- canonical fingerprints use the same fields, ordering, and zero-value rules.

The preferred implementation is a shared low-level metadata package consumed by
both authoring pipelines. If repository boundaries prevent that, mirrored structs
must be guarded by cross-repository golden fixtures and fingerprint tests. A
fixture emitted by either runtime must load in the other without adapters and
produce the same effective warmups.

For a regular request Datly must:

1. select the effective warmup corresponding to the normalized most-specific
   scope;
2. capture the canonical actual values;
3. clear only that parameter and its presence marker in a cloned state;
4. build the base query identity with every other filter retained;
5. match the indexed cache using the strategy column and canonical values.

Datly must execute every effective warmup by invoking the existing SQLX warmup
API once per warmup. Generated work entries carry their
originating warmup so connector, cases, projection, limits, and index settings
cannot accidentally fall back to the singular field. Datly supplies each
warmup's index column, SQL, ordered arguments, and matcher through the existing
SQLX `IndexBy` contract.

The underlying `viant/sqlx` implementation is unchanged. Its `IndexBy`, lazy
fill, marker identity, cache record format, providers, and hit/miss semantics are
a working dependency. Cross-index isolation must be proven against that existing
API. If a configuration is not safely representable, Datly must use separate
prepared-view/cache metadata or reject it; it must not alter SQLX behavior.

Authoring must support one declaration exactly as before. Repeated warmup
declarations are additive and preserve declaration order; they must not overwrite
the previous warmup. Structured configuration may use `Warmup`, `Warmups`, or
both under the normalization rules above.

### 6. Authorization and warmup invocation modes

Datly must expose explicit invocation metadata to predicates and normalization
hooks:

- regular request;
- warmup prepare;
- warmup fill.

Regular requests always enforce principal-specific row authorization.

A server-owned warmup may bypass only principal-specific row filtering, because
the resulting cache is populated before a particular end-user request. The
bypass must require an explicit trusted warmup invocation and must not remove:

- tenant/static isolation;
- authored warmup case filters;
- component invariants;
- connector restrictions;
- warmup endpoint administration policy.

Client input must never be able to set the invocation mode or bypass flag.

### 7. Connector access

Normalization and authorization resolvers must receive a scoped connector/DB
capability in both regular and warmup preparation paths. The capability is
selected by component configuration and is not ambient arbitrary database
access.

Warmup SQL preparation must not depend on an HTTP handler session being present.
The same resolver contract must work for HTTP, MCP, internal component calls,
and scheduled warmup.

### 8. Canonical time windows

The cube input must support:

- explicit `from` and `to` bounds;
- named `period` values;
- `granularity`, at minimum `day` and `hour`.

Every request is normalized to an explicit half-open interval `[from, to)` before
SQL generation and cache matching.

Rules:

- explicit bounds take precedence over a named period;
- named periods resolve through an injected clock and declared timezone;
- relative database expressions such as `CURRENT_DATE` are not used in cached
  query identity;
- day granularity normalizes to day boundaries;
- hour granularity normalizes to hour boundaries;
- a date-only upper bound denotes the complete date and is converted to midnight
  of the following date;
- a timestamp upper bound is exclusive;
- canonical literal values are used as SQL bind arguments and cache-key inputs;
- normalization changes bindings only and does not rewrite stored source data.

The configured date layout and timezone must be explicit. Invalid or ambiguous
bounds fail validation.

### 9. Cube grouping compatibility

A warmed cube result is reusable only if the request retains all dimensions in
the warmed grouping set. The request may select fewer measures.

Dropping a warmed dimension requires reaggregation. Datly must either perform an
explicit supported reaggregation or report a cache miss and execute an appropriate
query. It must never satisfy the request by hiding the dropped dimension while
returning aggregates at the old grain.

Granularity is part of grouping and cache identity. Day and hour caches are
distinct even when sourced from the same physical records.

### 10. Deterministic identity

The following must participate in cache identity:

- component/view and source generation;
- selected index strategy;
- canonical index values;
- canonical time bounds and granularity;
- retained filters and ordered arguments;
- complete warmed dimension set;
- stored projection metadata needed to validate compatible narrowing;
- tenant/static authorization scope.

Equivalent semantic requests must normalize to the same identity. Different
scope levels, time grains, tenant scopes, or grouping sets must not collide.

## Suggested authoring contract

The exact DQL syntax remains an implementation decision, but it must compile to
typed metadata. A conceptual declaration is:

```sql
$cache_warmup('resource_cube', 'tenant_id',     'IndexParameter=tenant_id',     'Period=today,yesterday', 'Granularity=day,hour')
$cache_warmup('resource_cube', 'collection_id', 'IndexParameter=collection_id', 'Period=today,yesterday', 'Granularity=day,hour')
$cache_warmup('resource_cube', 'group_id',      'IndexParameter=group_id',      'Period=today,yesterday', 'Granularity=day,hour')
$cache_warmup('resource_cube', 'item_id',       'IndexParameter=item_id',       'Period=today,yesterday', 'Granularity=day,hour')
```

This is illustrative, not prescribed parser syntax. The compiled model must
retain an ordered `[]*Warmup`. One call must compile to the same singular behavior
as today; multiple calls must remain distinct typed warmups.

A richer authoring form may declare reusable sets and target them per warmup:

```sql
$cache_warmup_cases('recent', 'Period=today,yesterday', 'Granularity=day,hour')
$cache_warmup_cases('broad', 'Period=week,month', 'Granularity=day')

$cache_warmup('resource_cube', 'tenant_id',     'IndexParameter=tenant_id',     'CaseRefs=recent,broad')
$cache_warmup('resource_cube', 'collection_id', 'IndexParameter=collection_id', 'CaseRefs=recent,broad')
$cache_warmup('resource_cube', 'group_id',      'IndexParameter=group_id',      'CaseRefs=recent')
$cache_warmup('resource_cube', 'item_id',       'IndexParameter=item_id',       'CaseRefs=recent')
```

The syntax is illustrative. The contract requires index-specific cases and an
optional many-to-many reuse mechanism.

## Error behavior

The component must fail closed for:

- missing required identity/authorization;
- no authorized intersection;
- hierarchy mismatch;
- unknown scope level;
- expansion over configured limits;
- unavailable authorization connector;
- invalid period, bound, timezone, or granularity;
- missing warmup strategy for the selected scope;
- incompatible warmed grouping/projection;
- Datly warmup/index configuration that cannot be represented safely by the
  existing SQLX contract.

Errors must not expose private SQL, credentials, token contents, or unauthorized
entity IDs.

## Observability

Warmup and request cache metrics should include bounded labels for:

- component/view;
- invocation phase;
- selected index strategy;
- granularity;
- hit, miss, fill, expiry, denial, and error;
- number of cases and groups written;
- resolver latency and expanded ID count in numeric metrics, not unbounded labels.

Logs may record canonical counts and strategy names. They should not log complete
authorization ID lists or credentials.

## Acceptance criteria

- The most specific supplied level is selected deterministically from presence
  metadata.
- Multiple supplied levels are relationship-validated.
- SQL expansion returns stable, unique, bounded values and ordered placeholders.
- Missing authorization and resolver errors execute zero protected-source reads.
- Regular requests cannot activate warmup authorization bypass.
- Warmup prepare/fill are distinguishable and work without an HTTP session.
- Every declared hierarchy level can be warmed and subsequently read with the
  protected source unavailable.
- Existing singular warmup fixtures and APIs remain compatible.
- Plural-only and mixed singular-plus-plural configurations execute every
  effective warmup in deterministic order.
- Duplicate warmup/index identities fail initialization instead of overwriting
  or ambiguously matching.
- Index-specific cases execute only for their owning warmup.
- Shared case sets expand independently for every referenced warmup.
- No undeclared case/index Cartesian product is generated.
- Original Datly and Datly 1.0 deserialize and normalize the same singular,
  plural, mixed, shared-case, date/hour, connector, and limit fixtures.
- Both implementations produce an identical canonical metadata fingerprint for
  every shared fixture.
- Equal numeric IDs at different hierarchy levels remain isolated when Datly
  invokes the existing SQLX contract for each warmup.
- Equivalent period and explicit-bound requests normalize to one cache identity.
- Date-only upper bounds include the entire requested day through an exclusive
  next-midnight bound.
- Day and hour requests do not collide.
- Requests retaining all warmed dimensions may narrow measures and hit cache.
- Requests dropping a warmed dimension do not reuse the incompatible grouping.
- Concurrent warmup and regular reads pass race tests and do not publish partial
  index generations.
- Failed warmup publication leaves the previous complete generation readable.

## Non-goals

- Client-defined SQL, hierarchy metadata, connector names, or cache providers.
- Automatic authorization bypass because an invocation lacks a principal.
- Treating cache contents as authorization evidence.
- Rewriting source timestamps or other stored values to improve cache matching.
- Assuming projection masking can replace cube reaggregation.
- Changing SQLX warmup, lazy-cache, `IndexBy`, marker, record, or provider
  implementation.

## Deliverables

1. Backward-compatible singular and plural warmup metadata with canonical
   normalization.
2. Pre-query normalization hook with scoped SQL connector access.
3. Explicit regular/prepare/fill invocation contract.
4. Deterministic Datly matcher construction over the unchanged SQLX cache API.
5. Canonical period/bounds/granularity normalizer.
6. Grouping/projection compatibility checks.
7. DQL compiler support and diagnostics.
8. Unit, integration, race, and source-unavailable replay tests.

No implementation is included in this requirement document.
