# Linked Standalone Applications

Standalone starts trusted, linked Go packages from authored source configuration
through `application.Manager`. It is not complete standalone deployment parity.

`cmd/datly` accepts `run` and `start` as foreground commands with `-conf` or `-c`.
The stock binary contains no application exports: arbitrary source-only Go/DQL
packages cannot become executable merely by naming them in configuration. Use `datly init` and `datly build` for automatic application discovery and linking;
see [project builds](../doc/project-build.md). For application-owned embedding,
build using `cmd/command.Service{Registry: exports}` and supply
the input/output and referenced types in the existing `x.Registry` and typed
factory bridges through the existing `custom.Factory` or mutation factory owner.
`testdata/app/cmd` is a complete linked executable example. It supplies no Build,
prebuilt component, route registry, or alternative runtime.

The sources must correspond to the linked executable. Go declaration and method
changes require rebuilding it; standalone does not verify compiled-source hashes,
compile Go on demand, load plugins, or hot-replace compiled methods. Canonical
source discovery still supplies package selection, route metadata, DQL overlays,
private type dependencies and resource snapshots. DQL changes requiring new
runtime contract types or generated handlers are rejected. An explicit linked
`input_type` / `output_type` can retain authored contract authority in an overlay.

Report-enabled groupable GET readers register their source, POST `/cube`, and
opt-in `/cube/compose` endpoints in the same generation. Runtime MCP exposes the
derived tools when enabled. [Report guidance](../doc/reports.md) covers linked
contracts, SQL aliases, source authorization and cache grouping. URI/embed SQL
resources are resolved by the reader compiler and reload atomically with reports.

Example configuration, beside the local application's `go.mod`:

```json
{
  "Endpoint": {"Port": 8080, "ReadTimeoutMs": 30000},
  "GoBootstrap": {"Packages": ["example.com/app/api"]},
  "Connector": "main",
  "DependencyURL": "connections",
  "Info": {"title": "Application API", "version": "1"}
}
```

`connections/main.yaml`:

```yaml
Connectors:
  - Name: main
    Driver: sqlite3
    DSN: /absolute/path/application.db
```

JSON, `.yaml` and `.yml` documents load through AFS. Relative location fields
resolve against the config URL, not process cwd. `BaseDir` and `ModuleDirs` are
explicit local-module location extensions; a remote configuration must identify
a local BaseDir. DependencyURL accepts a dependency document or a flat directory
of JSON/YAML connector and cache documents. Inline `Connectors` is an additional supported
configuration form. DSNs and Scy secret settings retain their content and are
not treated as location fields. SQLite is linked by the command; other database
drivers must be linked by the application.

`GoBootstrap.Packages`, `BaseDir`, and `ModuleDirs` also define the bootstrap
component index. Startup scans selected package authority and route metadata;
normal component compilation is deferred until the first matching HTTP or MCP
use and cached until reload. This default creates no `paths.yaml` or persistent
cache. Set `GoBootstrap.EagerComponents` only when the deployment explicitly
wants every component compiled during startup.

Named reader caches use `Caches` either inline or in a `DependencyURL` document:

```yaml
Caches:
  aerospike:
    Enabled: true
    Provider: aerospike://localhost:3000/ns_memory
    Location: steward_${View.Name}
    TimeToLiveMs: 14400000
```

Standalone forwards these definitions to the existing reader runtime, including
nested relations declaring `view:",cache=aerospike"`. A cache name is independent
of its provider; `Provider: afs` with an absolute directory is also supported.
Use a positive `TTL` duration or `TimeToLiveMs`; if both are supplied they must
agree. Native definitions require `Enabled: true`. An unused disabled definition
is allowed, but a referenced disabled or missing cache fails component publication.

Legacy `CacheProviders` lists are accepted in both locations, with `Name` on each
entry and optional document `ModTime` in dependency files. They default to enabled
when `Enabled` is omitted; explicit false is preserved. Lists are normalized to
the same named settings map, not a second resource assembly path. Repeated names
must have identical normalized settings; conflicting definitions fail instead of
being overridden by document order. Map keys and any explicit `Name` must agree.
Duplicate object keys are rejected before decoding, including repeated `Caches`
blocks and duplicate names within a single map. Struct-field aliases such as
`Caches`/`caches` and `Enabled`/`enabled` also conflict in JSON and YAML; ordinary
map keys (including cache names) remain case-sensitive. Identical definitions in
separate dependency documents remain allowed.

`Provider` uses instance constants from `ConstURL`. Cache locations retain reader
ownership: `${View.Name}` is expanded for each view, followed by instance/authored
constant expansion. Cache locations are not rebased against the config URL.
Services use the existing SQLX implementations and standalone-owned Aerospike pool.
Definitions are fixed for a server lifetime; route reload does not reload them.

HTTP policy is the existing `gateway/http.Config`: CORS defaults and presence,
DisableCors, APIPrefix, Meta and OpenAPI retain their owner. `Info` selects
OpenAPI generation; configuring both Info and OpenAPI fails. JWTValidator
configures the existing declared-input JwtClaim codec, never an ambient
principal. MCP accepts Port or Address plus the native Authorization policy and
uses `mcp/server` with Manager as its generation source. Old MCP OAuth/BFF option
conversion is not implemented.

Endpoint.Port zero or absent retains the original 8080 default. Explicit
Endpoint.Address is an extension (including `127.0.0.1:0`). MCP.Port is a pointer,
and explicit zero remains an allocated port. Nonpositive original HTTP and SQL
pool limits retain their standard-library disabling/default semantics. Endpoint
ShutdownTimeoutMs is an extension, defaulting to five seconds.

All endpoints bind before the initial Manager publication and begin serving only
after compilation succeeds. SIGINT and SIGTERM cancel foreground execution.
Transport shutdown rejects new admission and drains accepted requests before
closing Manager services and application-owned database handles. Deadline return
does not release resources beneath active handlers: background cleanup continues,
and another Shutdown call joins it. Reload is an embedding API over Manager;
there is no CLI file watcher or SIGHUP reload yet.

Unsupported config fields fail instead of being ignored. Remaining original
contracts include plugin build/load/checksum/ABI policy, RouteURL/ContentURL
deployment, DQLBootstrap mode policies, watched dependencies, unlinked CLI application authorization, original logging policy,
auth signer/provider/secret-to-environment options and extended metadata endpoints. [Configured async](ASYNC.md) starts existing application job services
with an explicit linked `Options.Async.Authorize` policy. Without it, Jobs
configuration fails closed; no permissive CLI default is installed.

Configured warmup administration, APIKeys, HTTP documentation/startup exports and
optional async OTLP/HTTP export are described in SERVICES.md. Their original vs
added settings and shutdown/export limits are explicit there.

Tests use the shared SQLite and generated-module harness. Socket-free tests prove
source-to-Manager read/mutation, resource snapshots, DQL overlays, JWT, OpenAPI,
CORS, MCP metadata, failed publication and shutdown retention. The real CLI TCP
and process-signal tests remain mandatory; a sandbox that forbids binds cannot
establish those claims.

Configured report tests also prove native MCP over stdio, required non-query
filters, independent view providers, JWT/API-key denial, and native AFS cache
replay with all warmed grouping dimensions retained. They do not establish TCP
listener acceptance in a sandbox that denies binds.
