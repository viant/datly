# MCP input field names

MCP input names preserve legacy Go/JSON spelling by default: `CampaignId` stays
`CampaignId`, and `MetricID` stays `MetricID`. A component's HTTP `caseFormat`
does not rename MCP inputs.

To customize implicit input member names, set `caseFormat` on the route's MCP
exposure. This is opt-in and independent of HTTP. For example, `"lc"` exposes
`AccountID` as `accountId`; `"lu"` exposes it as `account_id`.

In DQL, the fourth optional MCP argument selects the input format:

```sql
#setting($_ = $case_format('lc'))
#setting($_ = $mcp('Orders', 'Manage orders', '', 'lu'))
```

The existing description and description-path arguments retain their positions.
`mcp_route('POST:/orders', 'Orders', 'Manage orders', '', 'lu')` selects the same
policy for one route. Omitting the final argument preserves legacy names.

The policy applies to nested ordinary JSON objects and flattened anonymous bodies
during discovery and invocation. Top-level named parameters retain their logical
binding names. Derived Cube and CubeCompose tools inherit the source exposure's
input format. No per-field JSON name is needed to repeat the format policy.

Explicit `mcp:"AccountingId"` names take precedence over explicit JSON names.
An explicit `json:"account_id,omitempty"` name takes precedence over the MCP
format. `json:",omitempty"` retains format-based naming. An unset MCP
format preserves existing Go/JSON names. Use `mcp:"-"` to exclude a member from
MCP discovery; internal fields and presence markers remain hidden.

This setting controls inputs only. Output schemas and result encoding continue
to use the component's output contract and HTTP case format together.

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
