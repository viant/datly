# Configuration and linked applications

[All guides](README.md) · [Quickstart](quickstart.md) · [Standalone services](../standalone/SERVICES.md)

The linked standalone host loads trusted authored packages through
`application.Manager`. It supplies service lifetime and transport ownership;
your executable supplies compiled application types and factories. The current
stock executable has no application exports.

## Actual command surface

```text
datly init -dir project-directory
datly build -dir project-directory -o bin/app
datly run -conf configuration-URL
datly start -c configuration-URL
datly validate [-dir project-directory] [-format text|json] module/package [...]
```

`run` and `start` both run in the foreground. `-conf` and `-c` are aliases.
SIGINT/SIGTERM cancel foreground service. There is no daemon install, arbitrary
source compilation, plugin loading, watch, SIGHUP reload or deploy command here.
Use [project build](project-build.md) to discover and link application types and factories internally. Use `validate -h` for the actual
schema flags and repeatable package/module selection options.

## Configuration example

For an application whose linked source module is `example.com/app`:

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

`connections/main.yaml`:

```yaml
Connectors:
  - Name: main
    Driver: sqlite3
    DSN: /absolute/path/application.db
    MaxOpenConns: 8
    MaxIdleConns: 4
```

These are application placeholders; use the README setup for a runnable version.
JSON, `.yaml` and `.yml` load through AFS. Relative location fields resolve against
the configuration URL, not the shell working directory. Remote configuration
still needs an explicit local `BaseDir` for linked source. `ModuleDirs` locates
additional local modules. `DependencyURL` accepts a connector document or a flat
directory of JSON/YAML connector documents. Inline `Connectors` is also supported.
DSNs and Scy secret content are not rewritten as relative file locations.

## Services and policy

| Setting | Current meaning |
| --- | --- |
| `GoBootstrap.Packages` / `Exclude` | Canonical component package selection. Bootstrap scans these packages once into an in-memory route/MCP/source index; no `paths.yaml` or other sidecar is required. Executable components load on first use and are cached for that generation. |
| `GoBootstrap.EagerComponents` | Explicitly compile every selected component at startup. The default is `false`; use this only when an operator intentionally accepts eager startup work. |
| `Connector` | Exact default connector name. Unknown names fail. |
| `Connectors` | Named DB configuration: driver, DSN, optional Scy secret and SQL pool settings. SQLite is linked by the command; other drivers must be linked by the app. |
| `Endpoint.Port` | Zero/absent defaults to 8080. |
| `Endpoint.Address` | Explicit override, mutually exclusive with nonzero Port; `127.0.0.1:0` requests an allocated port. |
| `Endpoint.ReadHeaderTimeoutMs` / `IdleTimeoutMs` | Zero/absent defaults to 10s / 120s on HTTP and MCP; negative explicitly disables the respective deadline. |
| `Endpoint.ReadTimeoutMs` / `WriteTimeoutMs` | Remain unchanged: zero adds no active request/response deadline. Positive configured durations are preserved on HTTP. |
| `Endpoint.ShutdownTimeoutMs` | Defaults to five seconds. Deadline return does not discard active cleanup. |
| `JWTValidator` | Existing Scy verifier configuration for explicitly declared `JwtClaim` inputs. |
| `CORS`, `DisableCors`, `APIPrefix`, `Meta` | Absent CORS allows noncredentialed cross-origin reads. Credentialed access requires explicit origins; wildcard credentials fail staging. Empty CORS disables cross-origin exposure. |
| `Info` or `OpenAPI` | Opt into OpenAPI publication; configuring both fails. Document-access policy is separate from component authorization. |
| `MCP.Address` or `MCP.Port` | Native MCP listener, with explicit authorization policy. Explicit MCP Port zero requests allocation. |
| `APIKeys` | Current configured component-key policy. Longest raw URI prefix wins; duplicate prefixes fail. |
| `Warmup` | Current HTTP warmup administration bridge with required admin key and positive timeout. Startup warmup URI execution remains separate. |
| `OpenAPI.StartupExports` | Current JSON/YAML file snapshot export before listener admission. It is not a file watcher or multi-file transaction. |
| `Metrics` | Absent disables diagnostic response headers. `{}` enables SQL-redacted metrics; `AllowSQL: true` enables SQL/arguments for debug requests. |
| `Observation` | Current native capture summaries and optional bounded OTLP/HTTP export. Export failure is telemetry loss, not business failure. |

Ordinary route lookup, API-key checks, CORS, and MCP identity lookup use the
bootstrap index without loading executable components. A first HTTP or direct
MCP call loads only its owner. MCP tool listing loads MCP-exposed owners so it
can return the same complete input schemas as eager compilation. OpenAPI loads
component contracts when the document is first requested; startup exports are
an explicit startup use of that document. Reload builds and atomically publishes
a fresh index, and source fingerprints prevent an older indexed source from
materializing after it changes.

Read/write timeouts and SQL pool limits preserve their existing nonpositive
semantics. Read-header and idle timeouts now have safe zero defaults; use negative
values only for an intentional disabled deadline.
Inspect the [configuration type](../standalone/config/config.go) and
[connector owner](../bootstrap/connector/config.go) for exact fields. The
accepted CORS/API-key/warmup/OpenAPI/Observation settings and limits are also
listed in [standalone/SERVICES.md](../standalone/SERVICES.md).

## Security defaults migration

See [gateway security migration](gateway-security.md) for original Datly behavior,
explicit credentialed CORS examples, diagnostic policy and streaming timeouts.

## Resources and reload

Package SQL/template/document resources use the shared Bindly resource store
and standard `fs.FS`/`embed.FS`. A named package store needs explicit namespace
references, not whichever store registered first. Missing declared SQL resources
fail closed. File edits do not mutate an already published resource snapshot.

`Server.Reload(ctx, revision)` and `Manager.Reload` are embedding APIs. Supply a
new monotonic revision and a complete validated source set. Failure leaves the
last generation serving; accepted work stays pinned. Linked Go declarations,
methods and handlers require rebuild. This host cannot hot-replace them from
source text or generate new runtime contracts just because DQL asks for them.

## Shutdown

Listeners bind before initial publication and serve only after compilation
succeeds. Shutdown stops admission and drains transports before closing Manager
services and application-owned database handles. Cleanup may continue after a
caller deadline; another shutdown call joins the same completion. Borrowed
services remain the external owner's responsibility.

## Configuration that is not available here yet

The candidate host rejects `RouteURL` and `PluginsURL` deployment.
Selected linked packages support [report/cube and composition](reports.md)
derivation through the same configured publication as ordinary readers. Do not copy unsupported fields from original Datly and assume they
took effect.

Programmatic and explicitly wired HTTP [async](async.md) are present in the
candidate. [Standalone async configuration](../standalone/ASYNC.md) wires `Jobs`
connector/table/notification/retention, `JobURL`, `FailedJobURL`, `MaxJobs` and
HTTP `Async` routes into the same application owners. It requires the linked
`Options.Async.Authorize` host policy; stock CLI configuration alone is denied. Authored Aerospike, static `ContentURL` and resource-folder publication
are candidate features with the bounded acceptance in [status](status.md).
Custom builds still need their recorded sources and resource paths at runtime.
