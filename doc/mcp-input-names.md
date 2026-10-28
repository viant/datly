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

Formatting does not replace a custom JSON decoder's wire contract or an explicit
whole-argument wire representation. Such owners retain their existing field
spelling. Nested scalar wire-schema metadata stays associated with its original
member when the surrounding ordinary JSON body uses formatted names.

Tool compilation rejects ambiguous public spellings. Supplying multiple spellings
for one member also remains an invocation error. Formatting does not infer field
presence, add runtime required checks, or change authorization.
