# Standalone HTTP and Observation Services

These settings work in the linked executable described in README.md. They feed
the existing Manager, HTTP, Scy/Bindly, SQLX and observation owners. They do not
compile plugins or create a second runtime. [Configured async jobs](references/product/datly/standalone/ASYNC.md)
use the application owners with explicit linked host authorization.

```yaml
GoBootstrap:
  Packages: [example.com/app/api]
Connector: main
DependencyURL: connections
Endpoint:
  Port: 8080
  ShutdownTimeoutMs: 15000
CORS:
  AllowOrigins: [https://client.example]
  AllowMethods: [GET, HEAD, POST]
  AllowHeaders: [Content-Type, Authorization, X-Read, X-Admin, X-Document]
  ExposeHeaders: [Content-Type]
  AllowCredentials: true
  MaxAge: 600
APIKeys:
  - URI: /records
    Header: X-Read
    Value: replace-with-a-secret
Meta:
  CacheWarmURI: /warm
  OpenApiURI: /schema
  DocURI: /docs
Warmup:
  TimeoutMs: 2000
  Admin:
    APIKeyHeader: X-Admin
    APIKeyValue: replace-with-an-administrator-secret
OpenAPI:
  Info:
    title: Application API
    version: '1'
  AggregateAccess:
    APIKeyHeader: X-Document
    APIKeyValue: replace-with-a-document-secret
  RouteAccess:
    APIKeyHeader: X-Document
    APIKeyValue: replace-with-a-path-document-secret
  StartupExports:
    - URL: schema.json
      Format: json
    - URL: records.yaml
      Path: /records/{id}
      Format: yaml
Observation:
  LogSummaries: true
  OTel:
    Enabled: true
    QueueSize: 256
    BatchSize: 64
    MaxSpans: 2048
    BatchTimeoutMs: 5
    ExportTimeoutMs: 1000
    ServiceName: application
    ServiceVersion: '1'
    IncludeSQL: false
    HTTP:
      EndpointURL: https://collector.example/v1/traces
      Insecure: false
      Headers:
        Authorization: replace-with-a-collector-credential
```

## Original Options

CORS retains original optional arrays/booleans, MaxAge, global/route inheritance,
DisableCors, and the credentialed-wildcard parent restriction. Empty arrays and
false values are not treated as absent. The existing HTTP owner handles OPTIONS
and preflight without requiring component credentials on the preflight itself.

APIKeys retains URI, Header, Value and optional Scy Secret. The longest raw URI
prefix wins, including the original empty-prefix fallback; matching configured
keys override authored route keys. Duplicate prefixes fail validation. Secrets
are resolved during generation staging. The resulting keys apply to components
and their warmup routes, not to an inferred ambient principal. Document access
remains separate, consistent with original OpenAPI assembly.

Meta.OpenApiURI defaults to /v1/api/meta/openapi. When OpenAPI is enabled,
Meta.DocURI now retains the original empty-to-/v1/api/meta/doc default. Explicit
whitespace disables either mount. APIPrefix and AllowedSubnet retain their
existing HTTP owner semantics. Info is an alternative to OpenAPI.Info, not a
second simultaneous document configuration.

JWTValidator continues to configure Scy's verifier only for declared JwtClaim
inputs. Warmup prepares that same input through Bindly. A valid JWT is not an
administrator grant and does not replace a configured component API key.

## Added Options

Warmup is the public configuration bridge to gateway/http.WarmupConfig. Presence
requires a positive TimeoutMs and valid Admin key policy. Component keys, admin
authorization and declared JWT input checks are all enforced. Cache contents,
cases, indexing and query execution remain authored source and SQLX concerns.
Safe completion summaries go to the command diagnostic writer (stderr by
default), including failures and caller disconnects. Raw errors, credentials,
SQL and arguments are not included in those completion messages. Configuring
Warmup while disabling its Meta URI is rejected. Startup warmup URI execution
is a separate remaining contract.

OpenAPI.AggregateAccess and RouteAccess reuse the existing HTTP document owner.
The UI shares aggregate access. Nil access policies retain public documents;
credentials are not embedded in generated documents. StartupExports is new:
JSON/YAML snapshots are rendered through Manager.ExportOpenAPI after initial
publication and before listener admission. Destinations are local files, relative
to the configuration URL when relative. Parent directories must already exist.
All renders complete before writes; each file is atomically replaced with mode
0600, but several destination writes are not one transaction. Exports are startup
snapshots, not a file watcher; successful later source reloads update HTTP
documents, not those files. Unknown paths or write failures abort startup.

Observation.LogSummaries opts into the existing Recorder's structured summaries
using one standard-library slog diagnostic sink. It is not an alias for original
Logging.EnableAudit, EnableTracing or IncludeSQL. Those broader logging policies
remain unsupported rather than being mapped to different behavior.

OTel is optional and off by default, independently of native capture. Its finite
queue, batching, span cap, capture detachment, drops and shutdown are unchanged
in the reviewed adapter. Zero numeric values retain adapter defaults. This
configuration frontend permits QueueSize, BatchSize and MaxSpans up to 65536;
negative values and duration overflow fail before queue allocation. The official
OTLP/HTTP exporter performs transport only, with explicit endpoint, headers,
TLS verification and timeout. HTTP requires Insecure:true; HTTPS requires false.
No userinfo/query credentials or conflicting transport headers are accepted.
Exporter retry is disabled; the configured export timeout bounds each call.
There is no second SDK batching processor and no global provider installation.

IncludeSQL is opt-in only for the exporter and may expose authored literals.
Arguments, bodies and credential headers are not exported. This is not a
zero-overhead or no-delay promise: native capture and bounded snapshot admission
remain on the caller path, and export is lossy on overflow, span-limit rejection,
transport failure or timeout. Exporter work runs asynchronously after capture.

HTTP policy is frozen per published generation. The observer/exporter lifetime
is fixed for the Manager and shared across pinned/reloaded generations. Invalid
reloads retain the active generation and do not stop its exporter. Services
configuration changes currently require a new server lifetime. Manager shutdown
joins producers and exporter drain before the server releases its DB handles.
A Shutdown caller deadline bounds waiting; embedding callers can join cleanup
again. A CLI process that exits after its shutdown deadline cannot promise that
background cleanup or optional export survives process termination.

## Verification Boundary

The linked executable and real OTLP collector tests use internal/testharness and
remain unskipped. They require local TCP. Socket-free tests cover configured
SQLite requests, CORS/preflight, all warmup credentials, documentation/export
policy, native capture with export disabled, invalid config and rejected reloads.
Normal collector drain, queue overflow and signal exit require the TCP-capable
parent run. Dynamic plugin/DQLBootstrap deployment, unlinked CLI async authorization,
richer auth adapters, custom collector TLS credentials and complete original
logging policy remain separate work; full production parity is not claimed.
