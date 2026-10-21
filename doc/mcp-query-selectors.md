# MCP query selectors

MCP tool discovery and execution use the same selector parameter projection.
Root selectors have public names such as `Limit`, `Offset`, `Page`, `Fields`
and `OrderBy`. Child selectors use the output relation holder as their prefix:
`MatchLimit`, `MatchOffset`, `MatchPage`, `MatchFields`, `MatchOrderBy`.
Nested holders include the full path, for example `MatchDetailsLimit`.

Public names do not replace HTTP bindings. A declared child parameter bound to
`match_limit` still reaches `match_limit`; supplying `MatchLimit` does not also
send the parent's `limit` or `_limit`. Custom bindings and URI templates retain
their declared names. Both typed string slices and JSON-decoded arrays encode
query field lists as CSV.

The bridge exposes enabled implicit root selectors because the HTTP session
already consumes those bindings. A child requires its actual declared selector
binding; the bridge does not invent a query parameter the reader cannot consume.
Shared HTTP bindings remain a single control and cannot offer independent child
pagination. Definitions remain unchanged during MCP discovery.

This applies to the main-line gateway. Datly `v1` uses its separate native typed
`QuerySelector` contracts; its MCP and HTTP transports must agree on the declared
per-view bindings rather than importing the main-line gateway implementation.
