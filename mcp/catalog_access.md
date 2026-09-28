# MCP catalog authorization

`Config.AuthorizeCatalogTool` receives the server-owned component target and an
action (`discover` or `describe`). `tools/list` includes a tool only when both
actions permit its metadata. Tool invocation continues to use `AuthorizeTool`.

`Config.AuthorizeCatalogResource` receives the resource URI and action. Resource
and skill lists filter metadata before skill pagination. Skill manifests also
require access to their supporting-file inventory. Both native skills operations
and the `skills/list` / `skills/get` compatibility tools use these checks.
Filtered skill cursors become invalid when the visible catalog changes.

`AuthorizeResource` now also runs before the protocol adapter reads sealed static
skill files. Static byte authority, digests, and generation ownership remain
unchanged. Authorization callbacks must use the deployment's verified identity
and server-owned resource policy. Skill frontmatter is descriptive content.

Nil catalog callbacks preserve public metadata behavior. Hosts exposing protected
components must configure catalog callbacks independently of execution guards.
Callbacks apply to each pinned generation and never mutate its registry.
