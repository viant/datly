# Warmup-friendly hierarchical authorization for the performance cube

## Scope

This document targets the original Datly implementation in this repository. It
analyzes how an authorized, groupable cube can reuse an indexed warmup cache when
callers may scope a request at different levels of an entity hierarchy.

The application motivating the design has this hierarchy, from broadest to most
specific:

1. advertiser
2. campaign
3. order
4. audience/line

The same design applies to any ordered entity hierarchy. The application-specific
names are retained here because this document is the implementation plan for the
platform performance cube.

## Executive conclusion

The performance cube is not warmup-ready as currently authored. Adding only an
authorization predicate is insufficient.

Original Datly currently gives a prepared view exactly one singular warmup,
containing one index column and one matching request parameter:

```go
type Warmup struct {
    IndexColumn    string
    IndexParameter string
    // ...
}
```

At request time Datly removes that one parameter from the cloned query state,
builds the base SQL identity, and sets `matcher.By` and `matcher.In` from the
configured index column and the actual request values. This is what allows one
large warmup query to be split into indexed cache groups.

Consequently, a predicate that dynamically chooses advertiser, campaign, order,
or audience can produce correct SQL, but it cannot make the cache matcher switch
to a different index. A single view cannot safely support four dynamic index
strategies with the current model.

The existing singular warmup and the underlying SQLX cache behavior work and
must remain supported unchanged. The natural
extension is a collection of complete warmup definitions—not a collection of
index fields embedded inside one warmup. Each warmup remains independently
coherent: it owns its index column, index parameter, connector, cases, fields,
limits, and existing SQLX identity inputs.

The safe implementation choices are:

1. Short term: expose one internal prepared view/cache per hierarchy level. Each
   view has a fixed `IndexColumn`/`IndexParameter`. Route selection chooses the
   most specific supplied level.
2. Framework enhancement: add plural `Warmups` alongside singular `Warmup` and
   select one effective warmup before SQL identity and cache matching are built.

The second option preserves one public cube contract and is the preferred final
architecture. The first option can be shipped without weakening cache isolation.

## Current implementation findings

### Performance DQL

The current performance DQL:

- enables cube and cube composition;
- uses the `bq_performance` source and the `aerospike` cache;
- is groupable at an hourly source grain;
- exposes advertiser, campaign, order, and line identifiers;
- has no `cache_warmup(...)` declaration;
- requires `campaign_id`, which prevents advertiser-level or unfiltered warmup;
- binds `*authorization.Campaign` to the auth component;
- accepts `from` and `to` as direct date predicates.

The existing campaign authorization handler is not reusable for this cube. It
calls `campaign.GetInput(ctx)`, whose implementation uses a direct type assertion
to `*campaign.CampaignInput`. The performance cube has a different input type, so
that handler can panic rather than safely deny. It also emits campaign-page SQL
aliases and endpoint-specific rules.

The performance cube needs its own authorization/scope predicate.

### Predicate behavior

An original-Datly custom predicate implements:

```go
Compute(context.Context, any) (*codec.Criteria, error)
```

It can return trusted SQL and ordered placeholders. Predicate expansion preserves
the placeholder order and appends the values to the query data unit. This is the
correct place to generate an `IN (?,...)` criterion from a normalized list.

It is not currently a state-normalization hook. It cannot replace the selected
input parameter, select another warmup index, or publish a canonical cache-key
binding. Those operations must happen before matcher construction.

### Warmup behavior

Warmup performs two conceptual phases:

1. prepare: generate parameter cases and build parameterized SQL;
2. fill: execute the SQL and call the SQLX cache `IndexBy` operation.

The original repository does not mark these phases in the invocation context.
The sibling implementation in `viant/datly_1/datly` adds `WithCacheWarmup` for
prepare and fill and tests `MayBypassRowAuthorization`. That distinction should
be ported to original Datly before an authorization-aware cube is warmed.

The bypass must be narrowly defined. It may suppress per-user row authorization
during a server-owned warmup, but it must not suppress tenant boundaries, static
data constraints, requested warmup cases, or administration authorization for
the warmup endpoint.

Warmup SQL is currently built without an application handler session. Therefore
a custom predicate cannot assume that `session.Db(...)` is available while the
warmup query is prepared. A SQL-backed scope resolver needs an explicit,
invocation-scoped connector/DB resolver supplied to predicate execution, or its
expansion must occur in a pre-query normalization stage that has connector access.

### Cube cache compatibility

A warmed cube can be reused only when the request retains every dimension in the
warmed grouping. Measures may be narrowed. Dropping a warmed dimension requires
reaggregation and cannot be implemented by projection masking alone.

The warmup `FieldNames` must therefore describe the exact reusable cube grain.
For this source, hourly and daily caches are distinct grains. They must not share
an identity merely because their source rows originate from the same table.

## Required request contract

The cube should accept:

- `advertiser_id []int`
- `campaign_id []int`
- `order_id []int`
- `audience_id []int` (with `line_id` accepted only as a documented alias if
  backward compatibility requires it)
- `from string`
- `to string`
- `period string`
- `granularity string`, restricted to `day` or `hour`

`campaign_id` must no longer be globally required. Scope validation requires at
least one authorized hierarchy selector unless an explicitly authorized
account-level use case is supported.

When more than one hierarchy selector is present, choose the most specific one:

```text
audience > order > campaign > advertiser
```

Broader supplied selectors are not ignored. They are consistency constraints.
The SQL-backed resolver must verify that every selected specific entity belongs
to the supplied broader entities and to the caller's authorized scope. A mismatch
must deny the request; it must not silently broaden or reinterpret it.

## Scope resolution and authorization

Introduce a performance-specific resolver with two separate responsibilities:

1. normalize the selected hierarchy scope into a canonical scope;
2. enforce the caller's row authorization for that canonical scope.

A useful internal result is:

```go
type ResolvedScope struct {
    Kind       ScopeKind // advertiser, campaign, order, audience
    IDs        []int     // sorted, unique, positive canonical IDs
    AdvertiserIDs []int  // expanded ownership set when needed
    CampaignIDs   []int
    OrderIDs      []int
    AudienceIDs   []int
}
```

The resolver should use the `ci_ads` connector through an explicit `*sql.DB` (or
connector service) supplied by the invocation. It should query ownership links in
bounded batches, deduplicate IDs, sort every canonical list, and reject missing
or cross-scope entities.

The predicate must always use placeholders:

```sql
m.line_id IN (?,?,?)
```

with:

```go
[]any{101, 102, 103}
```

It must never interpolate entity IDs into SQL text. Stable sorting is required
because SQL plus ordered arguments participates in cache identity.

The normal request path must fail closed for a missing auth component, missing
auth context, DB expansion error, empty authorized intersection, or invalid
hierarchy. Server-owned warmup is the only path that may bypass user-specific row
authorization, and only when the invocation context explicitly says it is in an
approved warmup phase.

## Cache/index architecture

### Safe short-term design

Create four internal prepared views that share the same physical query contract
but have fixed warmup identities:

| View | Index column | Index parameter |
| --- | --- | --- |
| advertiser | `advertiser_id` | `advertiser_id` |
| campaign | `campaign_id` | `campaign_id` |
| order | `order_id` | `order_id` |
| audience | `line_id` | `audience_id` |

The public component resolves and authorizes the scope, then delegates to the
view matching the most specific selected level. Each view clears exactly its
configured index parameter while creating the base cache matcher. All other
filters—including canonical time bounds and granularity—remain in the base SQL
identity.

This may be implemented as generated internal variants or as four views within a
custom component. Do not duplicate business SQL manually; use one shared SQL
resource with explicit aliases and variant metadata.

### Preferred original-Datly enhancement: singular and plural warmups

Do not replace or redefine the existing `Warmup`. Add a plural collection to the
cache:

```go
type Cache struct {
    // Existing contract. It remains valid and unchanged.
    Warmup *Warmup `json:",omitempty" yaml:",omitempty"`

    // Additional warmups. Each item has the existing Warmup shape.
    Warmups []*Warmup `json:",omitempty" yaml:",omitempty"`
}
```

Compatibility rules:

- `Warmup` only: behavior is byte-for-byte compatible with the current single
  warmup path.
- `Warmups` only: execute and match every item in declaration order.
- both present: the singular item is first, followed by the plural items.
- duplicate effective identities (`IndexColumn` + `IndexParameter`, preferably
  plus an explicit name) are rejected during initialization.
- an empty plural collection does not disable a valid singular warmup.

### Index-specific and shared cases

Cases belong to a warmup/index, not automatically to the entire cache. This is
important because some parameter combinations are meaningful or affordable only
for one hierarchy level. For example, a broad date window may be acceptable for
an advertiser index but too large for an audience index.

The existing form remains naturally index-specific because each `Warmup` owns
its own `Cases`:

```go
type Warmup struct {
    Name           string
    IndexColumn    string
    IndexParameter string
    Cases          []*CacheParameters
    // ...
}
```

The plural runtime must never compute a global Cartesian product of all cases and
all warmups. It iterates effective warmups, then generates only that warmup's
cases.

Some cases legitimately apply to more than one index. Support that without
changing cache semantics by allowing named reusable case sets:

```go
type Cache struct {
    Warmup  *Warmup
    Warmups []*Warmup

    // Optional authoring-time reusable definitions.
    SharedCases map[string][]*CacheParameters
}

type Warmup struct {
    Name     string
    CaseRefs []string
    Cases    []*CacheParameters
    // existing fields...
}
```

Initialization expands `CaseRefs` into each warmup independently and appends its
inline `Cases`. The same named set can be referenced by advertiser and campaign
warmups while an audience-only set is referenced only by the audience warmup.
After expansion, runtime entries retain exactly one originating warmup/index.

Named case sets are optional authoring sugar. A first implementation may repeat
the same case in two warmup declarations, provided the compiled model still
stores separate per-warmup cases. Reuse must not introduce shared mutable slices.

Within one warmup, identical expanded cases should be deduplicated by canonical
parameter name and canonical typed value. Across different warmups they are not
duplicates because the index strategy is part of identity.

### Shared metadata contract with Datly 1.0

Original Datly and Datly 1.0 must use the same serialized warmup metadata. Their
DQL/front-end syntax may differ, but both compilers must produce this
version-neutral shape and semantics:

```go
type Cache struct {
    // Existing fields are omitted here.
    Warmup          *Warmup                       `json:",omitempty" yaml:",omitempty"`
    Warmups         []*Warmup                     `json:",omitempty" yaml:",omitempty"`
    SharedCases      map[string][]*CacheParameters `json:",omitempty" yaml:",omitempty"`
}

type Warmup struct {
    Name           string             `json:",omitempty" yaml:",omitempty"`
    Priority       int                `json:",omitempty" yaml:",omitempty"`
    IndexColumn    string             `json:",omitempty" yaml:",omitempty"`
    IndexParameter string             `json:",omitempty" yaml:",omitempty"`
    IndexMeta      bool               `json:",omitempty" yaml:",omitempty"`
    Limit          *int               `json:",omitempty" yaml:",omitempty"`
    MaxCases       *int               `json:",omitempty" yaml:",omitempty"`
    FieldNames     []string           `json:",omitempty" yaml:",omitempty"`
    Connector      *Connector         `json:",omitempty" yaml:",omitempty"`
    CaseRefs       []string           `json:",omitempty" yaml:",omitempty"`
    Cases          []*CacheParameters `json:",omitempty" yaml:",omitempty"`
}

type CacheParameters struct {
    Set        []*ParamValue `json:",omitempty" yaml:",omitempty"`
    FieldNames []string      `json:",omitempty" yaml:",omitempty"`
}

type ParamValue struct {
    Name           string        `json:",omitempty" yaml:",omitempty"`
    Values         []interface{} `json:",omitempty" yaml:",omitempty"`
    ExcludeDefault bool          `json:",omitempty" yaml:",omitempty"`
}
```

The shared model should live in the lowest package both compilation paths can
depend on. If repository boundaries prevent that, both implementations must keep
strict wire-compatible mirrors. Do not introduce a 1.0-only spelling for a field
that has an original-Datly equivalent.

Compatibility rules are normative:

- preserve every existing singular `Warmup` field name and meaning;
- `Warmups`, `Name`, `Priority`, `CaseRefs`, and `SharedCases` are additive;
- absent `Name` is canonicalized from `IndexParameter`, falling back to
  `IndexColumn`;
- absent `Priority` uses declaration order; when several supplied parameters
  have equal priority, the later declaration wins so broad-to-specific warmup
  declarations select the most restrictive scope;
- JSON/YAML omission and zero-value behavior are identical in both runtimes;
- both use the same singular-plus-plural normalization, duplicate detection,
  case-reference expansion, typed conversion, canonical sorting, and fingerprint
  algorithm;
- `Period` and `Granularity` remain ordinary named `ParamValue` entries, not
  version-specific fields;
- connector references and `FieldNames` retain their existing wire shape.

Both repositories must carry the same golden metadata fixtures: singular only,
plural only, mixed singular/plural, shared and index-specific cases, duplicate
identity rejection, date/hour values, explicit connectors, and JSON/YAML round
trips. A fixture produced by either DQL compiler must load in both runtimes
without semantic translation and yield the same canonical metadata fingerprint.

Expose one canonical accessor and migrate runtime consumers to it:

```go
func (c *Cache) EffectiveWarmups() []*Warmup
```

The accessor returns a defensive ordered slice containing singular then plural.
The old `View.Warmup()` accessor remains for source compatibility; add
`View.Warmups()` for new code.

At request time:

1. inspect presence markers, not zero values;
2. select the most specific matching effective warmup using explicit priority,
   then the later broad-to-specific declaration as the tie breaker;
3. normalize its value list;
4. clone state and clear only that strategy's parameter and marker;
5. build the base SQL/cache identity with all remaining filters;
6. set `matcher.By` to the selected column and `matcher.In` to the canonical
   actual values.

The Datly warmup engine must invoke the existing SQLX `IndexBy` API for every
effective warmup. Each generated
entry must carry the exact `*Warmup` that produced it; DB/connector selection,
field projection, case labels, index column, and limits must no longer dereference
the global `Cache.Warmup`. Datly passes that warmup's index column, SQL, ordered
arguments, and matcher exactly as the singular path does.

No changes are required in `viant/sqlx`. Do not change `IndexBy`, lazy fill,
marker construction, cache records, providers, or hit/miss behavior. Tests must
prove that multiple Datly warmups remain distinct through the existing SQLX API.
If a configuration cannot be represented safely by that API, Datly must isolate
it with separate prepared-view/cache metadata or reject it; it must not change
SQLX semantics.

Repeated `cache_warmup(...)` calls currently overwrite the singular object. Make
the function additive: the first declaration may continue to populate `Warmup`;
later declarations append to `Warmups`. A collection form may also be added for
structured configuration. For example:

```sql
cache_warmup(performance, 'advertiser_id', 'IndexParameter=advertiser_id', 'Period=today,yesterday', 'Granularity=day,hour'),
cache_warmup(performance, 'campaign_id',   'IndexParameter=campaign_id',   'Period=today,yesterday', 'Granularity=day,hour'),
cache_warmup(performance, 'order_id',      'IndexParameter=order_id',      'Period=today,yesterday', 'Granularity=day,hour'),
cache_warmup(performance, 'line_id',       'IndexParameter=audience_id',   'Period=today,yesterday', 'Granularity=day,hour')
```

An optional named-case authoring form could reduce repetition:

```sql
cache_warmup_cases('recent', 'Period=today,yesterday', 'Granularity=day,hour'),
cache_warmup_cases('long_range', 'Period=week,month', 'Granularity=day'),
cache_warmup_cases('audience_recent', 'Period=today', 'Granularity=hour'),

cache_warmup(performance, 'advertiser_id', 'IndexParameter=advertiser_id', 'CaseRefs=recent,long_range'),
cache_warmup(performance, 'campaign_id',   'IndexParameter=campaign_id',   'CaseRefs=recent,long_range'),
cache_warmup(performance, 'order_id',      'IndexParameter=order_id',      'CaseRefs=recent'),
cache_warmup(performance, 'line_id',       'IndexParameter=audience_id',   'CaseRefs=recent,audience_recent')
```

This syntax is illustrative. The required behavior is per-index ownership plus
optional many-to-many references between named case sets and warmups.

One call continues to work exactly as it does today. Multiple calls produce a
collection. The translator must not silently overwrite an earlier declaration.

Runtime migration points include all direct `Cache.Warmup` reads. In particular:

- warmup discovery must use `len(EffectiveWarmups()) > 0`;
- `GenerateCacheInput` remains as the singular compatibility API;
- a new plural generator returns inputs paired with their originating warmup;
- `warmupEntry` carries that warmup and uses its connector/index configuration;
- request matching resolves one effective warmup from supplied parameter
  presence;
- cache inheritance and cloning deep-copy both singular and plural forms;
- gateway route eligibility and repository warmup metadata recognize either
  form.

## Time-window normalization

Do not use `CURRENT_DATE()` or `CURRENT_TIMESTAMP()` in cached query identity for
named periods. Resolve every period to explicit literal bounds before SQL and
cache matching.

Use one canonical half-open interval:

```text
[from, to)
```

Rules:

- `granularity=day`: normalize `from` to the start of its day and `to` to the
  start of the day after the requested inclusive end date.
- `granularity=hour`: normalize bounds to exact hour timestamps. The upper bound
  remains exclusive.
- a date-only upper bound always means the full day and becomes `< midnight of
  the next day` (equivalent to end-of-day without precision loss);
- an explicitly timestamped upper bound remains an exclusive timestamp;
- `period` is allowed only when explicit `from`/`to` are absent;
- named periods are resolved once using an injected clock and declared timezone;
- normalized literals, not relative names or database clock expressions, are
  used in SQL arguments and cache identity.

Example:

```text
to=2026-09-21, granularity=day
=> canonical to=2026-09-22 00:00:00
=> predicate advertiser_time < ?
```

Keep the physical partition predicate on `event_date`, but derive it from the
same canonical bounds. This changes query bindings only; it does not rewrite or
reformat stored event data.

The existing Steward predicates demonstrate date/hour parsing and period
vocabulary, but date-only `to` should be changed from `<= DATE(?)` to the same
exclusive-next-midnight representation used for timestamp bounds. This produces
one cache-safe semantic model.

## Proposed delivery sequence

1. Add backward-compatible plural warmups and migrate runtime reads through
   `EffectiveWarmups()`.
2. Port explicit warmup prepare/fill invocation markers to original Datly and
   define the narrow authorization-bypass contract.
3. Add tests proving normal requests never receive the bypass and warmup phases
   do.
4. Add a connector/DB resolver to the predicate normalization context, without
   exposing arbitrary client-selected connectors.
5. Implement canonical time-window normalization and presence-aware hierarchy
   resolution in the platform component.
6. Add the performance cube's four additive warmup declarations.
7. Remove the unconditional
   campaign requirement, and replace `authorization.Campaign` with the dedicated
   performance authorization predicate.
8. Verify warm hit after disabling the source database, unwarmed lazy fill,
   expiry, denied scopes, projection compatibility, and day/hour separation.

## Acceptance criteria

- Missing or invalid authorization performs zero reads against the performance
  source.
- The most specific supplied hierarchy level is chosen using presence markers.
- Broader supplied IDs are validated as consistency constraints.
- Expanded IDs are positive, unique, sorted, bounded, and bound as placeholders.
- A DB lookup failure fails closed and does not fall back to a broader scope.
- Warmup bypass is available only in explicit server-owned warmup phases.
- Every cache lookup uses an index matching the selected hierarchy level.
- A cache configured with only singular `Warmup` passes all existing behavior
  tests unchanged.
- A cache configured with only plural `Warmups` executes every item.
- A cache configured with both executes singular first and every plural item,
  rejecting duplicates.
- Index-specific cases execute only for their owning warmup.
- A shared case set may target multiple warmups and produces one independently
  indexed execution per target.
- Case expansion does not create an unintended global Cartesian product.
- Original Datly and Datly 1.0 produce the same normalized metadata fingerprint
  for shared golden fixtures.
- Metadata emitted by either compiler loads in the other runtime without loss of
  warmup, case, connector, field, limit, or index semantics.
- Existing SQLX index/marker behavior remains unchanged and multiple Datly
  warmups are proven distinct through the existing `IndexBy` contract.
- Requests with equivalent periods and explicit bounds produce identical
  canonical SQL arguments and cache identity.
- A date-only upper bound includes the complete day via an exclusive next-day
  boundary.
- Day and hour granularities never collide in cache identity.
- Cube cache reuse retains every warmed dimension; dropping a dimension causes a
  miss/reaggregation path rather than returning incorrectly grouped data.
- Warmed entries can be read with the source database unavailable.
- Tests cover admin, scoped user, denied user, empty intersection, cross-hierarchy
  mismatch, each hierarchy level, multiple IDs, and concurrent warmup/read.

## Files implicated in original Datly

- `view/cache.go`: warmup model, case generation, canonical parameter conversion
- `internal/translator/function/cache_warmup.go`: DQL warmup parsing
- `warmup/cache.go`: prepare/fill lifecycle and `IndexBy` execution
- `service/reader/service.go`: request-side warmup matcher and index parameter
- `service/executor/expand/predicate.go`: custom predicate execution and ordered
  placeholder propagation

The SQLX cache implementation is explicitly out of scope; it is an unchanged
dependency exercised by compatibility tests.

No runtime code is changed by this document.
