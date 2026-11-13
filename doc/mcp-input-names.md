# MCP field names

Native MCP members use lowerCamel by default: `CampaignId` becomes `campaignId`
and `MetricID` becomes `metricId`. Explicit MCP/JSON names and authored case
overrides retain their precedence. HTTP naming is independent.

To override implicit member names, set `caseFormat` on the route's MCP
exposure. This is independent of HTTP. For example, `"lc"` exposes
`AccountID` as `accountId`; `"lu"` exposes it as `account_id`.

In DQL, the fourth optional MCP argument selects the member format:

```sql
#setting($_ = $case_format('lc'))
#setting($_ = $mcp('Orders', 'Manage orders', '', 'lu'))
```

The existing description and description-path arguments retain their positions.
`mcp_route('POST:/orders', 'Orders', 'Manage orders', '', 'lu')` selects the same
policy for one route. Omitting the final argument selects the lowerCamel default.

The policy applies to nested ordinary JSON objects and flattened anonymous bodies
during discovery and invocation. Top-level named parameters use the public naming policy while retaining their
canonical logical binding paths. Derived Cube and CubeCompose tools inherit the source exposure's
input format. No per-field JSON name is needed to repeat the format policy.

Explicit `mcp:"AccountingId"` names take precedence over explicit JSON names.
An explicit `json:"account_id,omitempty"` name takes precedence over the MCP
format. `json:",omitempty"` retains format-based naming. An unset MCP
format selects lowerCamel for implicit names. Use `mcp:"-"` to exclude a member from
MCP discovery; internal fields and presence markers remain hidden.

The MCP format also applies to native output schemas and result encoding.
Explicit JSON names retain their spelling; use an explicit MCP name when the
public MCP spelling must differ. HTTP retains its existing output contract.

A native output field tagged `mcp:"name=mode"` uses `mode` in the MCP schema
and encoded result while retaining its original HTTP JSON name.

A native output field tagged `mcp:"-"` is omitted from the MCP output schema
and JSON result, including nested fields and typed error payloads. The HTTP
JSON contract retains its own visibility. Caller-selected Contract metadata
is preserved before applying the MCP exclusions. Complete custom JSON owners
and recursive edits retain the dynamic-contract boundaries.

Formatting does not replace a custom JSON decoder's wire contract or an explicit
whole-argument wire representation. Such owners retain their existing field
spelling. Nested scalar wire-schema metadata stays associated with its original
member when the surrounding ordinary JSON body uses formatted names.

Tool compilation rejects ambiguous public spellings. Supplying multiple spellings
for one member also remains an invocation error. Formatting does not infer field
presence, add runtime required checks, or change authorization.

## Scalar admission for integer query and path lists

Use `mcp:"scalar=true"` to admit a signed integer as one element of a native
integer query or path slice. Declare it with existing input metadata in DQL:

```sql
#define($_ = $AdvertiserId<[]int>(query/advertiserId).Optional().Tag('mcp:"aliases=advertiser_id,scalar=true"'))
```

Discovery advertises the integer alternative first, the native array second,
and null for optional bindings. Both public spellings use the same compiled
input plan. Supplying multiple spellings remains an error. Zero becomes `[0]`;
omission and query null retain native defaults and presence semantics. Arrays,
including empty arrays, retain the existing contract. Fractional numbers,
nonempty strings, booleans, objects and integer overflows remain invalid. The
inherited empty query string behavior is preserved: `""`, like query null,
means omission at binding time. Discovery advertises the typed input contract
and does not advertise this legacy empty-string omission. Required bindings
still reject omission, including null and the empty string, through their native
required-input checks.

Path scalars bind as the same singleton array as a one-element array. Path null
remains invalid; consolidated route selection also rejects an empty identity
instead of falling back to the collection route. URI and REST binding retain
their existing behavior.

The opt-in is limited to signed integer query or path slices without an explicit wire
representation or custom JSON/text decoder on the slice or its element (value
or pointer receiver); unsupported contracts fail tool compilation.
`scalar=false` retains ordinary admission. REST query binding, application Go
types, selectors, SQL predicates and authorization retain their existing owners.

## Required discovery for native array bodies

For an optional whole-body native slice, an explicitly authored `required:"true"`
tag marks its named MCP argument as required in discovery without changing runtime
omission or null handling. For example, retain a bare HTTP array and expose `Ids`:

```sql
#define($_ = $Body<*associate.CreativeIdsBody>(body/).Optional().WithTag('mcp:"name=Ids" required:"true"'))
```

Here `CreativeIdsBody` is a named `[]int` with no custom JSON methods. Its MCP
schema retains array/null types and integer items. The schema tag does not make
the runtime binding required. This explicit declaration applies only to a direct
whole-body native slice with matching source and destination types and no wire
override. Untagged arrays, query parameters, named body members, and nested-member
requiredness keep their existing behavior.

### Native integer input alternatives

For native body fields, `jsonx:"boolToInt,null=omit"` on an `int` or
`*int` declares boolean-to-integer input conversion and null omission in the
native body provider. MCP input projection retains integer/null and adds boolean;
it does not restrict integer values to 0 or 1. Untagged fields and output schemas
retain their existing types. Internal and MCP-hidden members remain excluded.
Explicit wire-schema declarations remain authoritative. This requires a body
provider version implementing the same policy; schema metadata alone does not
perform conversion. Custom decoder ownership and transcribed component contracts
must be validated before adopting the policy in an application.

OpenAPI input discovery represents the same native integer policy with its
existing `anyOf` schema: nullable integer or boolean. Response schemas and
untagged fields retain their existing representation.

### Native query text admission

`mcp:"text=true"` opts a signed-integer query slice into integer-or-string MCP
array items. Canonical Go source/destination types remain unchanged. Text passes
unchanged to the existing native query parser, including its declared list codec;
the MCP adapter does not parse numeric text or introduce another normalizer.
Optional query null follows the existing omission behavior. `scalar=true` can
add singleton integer/string input alongside the array.

Without this tag, ordinary integer-array admission is unchanged. Unsupported
source kinds, element types, custom decoders and conflicting wire contracts
fail compilation. Strict unknown-argument handling remains independently opt-in.
