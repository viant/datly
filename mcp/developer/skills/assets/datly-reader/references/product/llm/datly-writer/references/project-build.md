# Project builds and deployment

Use `datly init` to scaffold and `datly build` to discover Go-selected component
packages, reachable types and typed factories. Discovery/linking is internal;
do not require application `init()`, `Register()`, blank-import lists or aggregate
registration functions. Ordinary imports express real code dependencies.

Operation-based `transcribe` to pure Go precedes building generated components; first
verify the connected generation capability in [developer-mcp.md](references/product/llm/datly-writer/references/developer-mcp.md).
Preserve authored hooks, resource manifests, Go workspace/tags/target settings
and existing module choices. New packages are found by normal traversal; runtime
exposure is a separate policy. Never infer aliases to repair discovery errors.

The main Datly module remains a local `v1` checkout. The release copy pins the
published SDK `v0.5.4-0.20260914204318-08752d9972c1` and native dependencies.
Use a `-local` mapping for Datly and exact published `-pin` values for dependencies.
`v0.0.0` is only a local requirement placeholder paired with a replacement, not
a published release. Dependency replacements are not inherited by Go: supply the
complete selected graph in the application module/workspace.

These binaries remain source-backed: deploy the corresponding metadata/source
and resource files at the recorded paths, or rebuild for the deployment layout.
An embedded SQL/static/doc/MCP resource proof does not establish source-free
custom-project deployment. Do not promise that copying only the binary works.

For a build advertising these commands:

```sh
datly init -dir .
datly build -dir . -o bin/app
./bin/app run -conf datly.yaml
```

Build selects actual Go files using workspace, tags, GOOS, GOARCH and CGO. Factories
are exported zero-argument functions returning `handler.Contract[I,O]`, a supported
mutation definition or typed handler, optionally with error. Discovery does not
execute factories. Add/remove packages and rebuild to refresh internal linking;
private imported types do not expose routes. Preserve the previous executable on
build failure and report conflicts with edited generated files.

Parent real-TCP acceptance passes custom-build read/mutation endpoints and
add/remove package rebuilds. Do not present the documentation author's sandbox
listener restriction as a current implementation or parent-acceptance gap.
The source-backed deployment requirements above still apply.

## Linked host configuration

`run -conf` and `start -c` run in the foreground; discover command help before use.
They need application exports linked by the custom build. Preserve the existing
configuration; these application placeholders show the relevant host fields:

```json
{
  "BaseDir": "/absolute/path/app",
  "Endpoint": {"Address": "127.0.0.1:8080", "ReadTimeoutMs": 30000},
  "GoBootstrap": {"Packages": ["example.com/app/api"]},
  "Connector": "main",
  "DependencyURL": "connections",
  "Info": {"title": "Application API", "version": "application-defined"}
}
```

Connector documents use `Connectors` entries with exact Name, Driver, DSN and
pool settings. Other database drivers must be linked by the application. JSON
and YAML configuration resolve relative resource locations against their URL;
DSNs/secrets are not rewritten as file paths. Remote configuration still needs
local BaseDir for linked source; ModuleDirs locates additional modules.

Preserve CORS/DisableCors, APIKeys, APIPrefix/Meta, JWTValidator trust configuration,
MCP listener authorization, warmup administration, OpenAPI startup exports and
Observation/OTel settings. API-key longest URI-prefix matching is distinct from
JWT declared-input verification. Use either Info or OpenAPI; document-access
policy is separate from business authorization. Endpoint Address conflicts with
nonzero Port; zero HTTP Port defaults to 8080, while explicit `127.0.0.1:0`
requests allocation. Explicit MCP Port zero also requests allocation. Do not
assume every zero timeout/pool setting has the same semantics.

Resources use canonical namespaces and immutable generation publication. Failed
reload retains the previous generation; missing files fail staging. Shutdown
stops admission and drains accepted work and externally owned services. A deadline
return does not justify closing services still in use.
