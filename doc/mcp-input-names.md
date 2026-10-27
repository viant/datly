# MCP input field names

MCP tools use a component's existing `caseFormat` setting for implicit JSON body
field names. For example, `caseFormat:"lc"` exposes `AccountID` as `accountId`;
`caseFormat:"lu"` exposes it as `account_id`. The same policy applies to nested
ordinary JSON objects and flattened anonymous bodies during both discovery and
invocation. No per-field JSON name is needed solely to repeat that policy.

Explicit `mcp:"AccountingId"` names take precedence over explicit JSON names.
An explicit `json:"account_id,omitempty"` name takes precedence over the component
format. `json:",omitempty"` retains format-based naming. An unset component
format preserves existing Go/JSON names. Use `mcp:"-"` to exclude a member from
MCP discovery; internal presence markers remain hidden.

Formatting does not replace a custom JSON decoder's wire contract or an explicit
whole-argument wire representation. Such owners retain their existing field
spelling. Nested scalar wire-schema metadata stays associated with its original
member when the surrounding ordinary JSON body uses formatted names.

Tool compilation rejects ambiguous public spellings. Supplying multiple spellings
for one member also remains an invocation error. Formatting does not infer field
presence, add runtime required checks, or change authorization.
