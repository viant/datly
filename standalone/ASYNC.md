# Configured asynchronous jobs

Standalone wires the existing `application.WithAsync`, `bootstrap.JobStoreConfig`,
SQLX job store, canonical Bindly replay and AFS publisher/watcher. It does not add
another binder, scheduler, job schema or HTTP loopback executor.

## Configuration and linked host

`config.Loader` reads JSON or YAML. This fragment assumes linked authored routes
and their input/output contracts, `GoBootstrap`, `Connectors` and `JWTValidator`
are configured as in the [standalone guide](README.md):

```json
{
  "Jobs": {
    "Connector": "jobs",
    "Table": "DATLY_JOBS",
    "DisableTableCreation": true,
    "Notification": {"Method": "Storage", "Destination": "events"},
    "TTLSeconds": 3600,
    "ErrorTTLSeconds": 10,
    "PollIntervalMs": 100
  },
  "JobURL": "events",
  "FailedJobURL": "failed-events",
  "MaxJobs": 4,
  "Async": [
    {"Route": {"Method": "POST", "Path": "/orders"},
     "MatchKey": "Key", "SyncFlag": "Sync"},
    {"Route": {"Method": "GET", "Path": "/order-jobs/{jobid}"},
     "Inspect": {"JobID": "JobID",
                 "Target": {"Method": "POST", "Path": "/orders"}}}
  ]
}
```

`Jobs.Connector` must explicitly name a configured connector; it need not be the
component's data connector. `Table` defaults to `DATLY_JOBS`; optional `Dataset`
uses the native SQL dialect's identifier rules. With `DisableTableCreation=true`,
the original 34-column table must already exist and be readable. Otherwise the
existing SQLX schema owner creates a missing table and leaves existing tables
unchanged. This is the existing application behavior, not a new portable schema
migration system. Deployment-managed schema is recommended.

`Notification.Method` currently supports only `Storage`. `Destination` selects
publication independently of watcher discovery. A directory destination receives
original-compatible `<view>/<unique>.job` files. The publisher also supports its
existing `${unixUs}`, `${unixMs}`, `${viewName}`, `${jobHash}` templates ending in
`.job`; no additional template syntax is introduced here. Choose a destination
under `JobURL` for this process to consume its publications. Loader-relative
`JobURL`, `FailedJobURL` and `Notification.Destination` resolve against the config
file URL, including remote AFS URLs. Programmatically supplied config is already
resolved; `New` does not resolve its paths against `Config.URL`.

An empty `JobURL` disables polling, while configured HTTP scheduling and
`Server.DispatchStorageEvent(ctx, object)` remain available. In that mode omit
`FailedJobURL`, `MaxJobs` and `PollIntervalMs`. Polling requires distinct job and
failed roots. `MaxJobs=0` is unlimited admission; a positive value bounds this
watcher. Zero poll interval means 100 ms. Zero success/error TTL means one hour /
ten seconds. Negative and overflowing durations and negative job limits fail.
Retention applies to durable terminal rows; it does not set cache TTL or promise
recovery. An `Async` route without `Jobs` fails validation.

Configuration cannot express application-specific current authorization. A
linked host supplies exactly one focused option:

```go
server, err := standalone.New(ctx, standalone.Options{
    Config: cfg,
    Registry: exports,
    Async: &standalone.AsyncOptions{
        Authorize: hostPolicy.AuthorizeJob, // jobs.Authorizer; required
        FS: fs,                           // optional afs.Service
    },
})
```

`hostPolicy.AuthorizeJob(context.Context, jobs.Access) error` is implemented by
the deployment, using its own trusted authorization owner. There is no built-in
allow policy, JWT-subject shortcut, serialized policy callback or ambient
principal. **Missing `Async.Authorize` rejects startup before opening any
connector.** The stock unlinked CLI therefore cannot enable jobs by configuration
alone. Supplying linked Async options without Jobs also fails.

The optional FS is the lifetime-wide AFS service for publication, download,
watching and acknowledgement; nil selects `afs.New()`. The host registers cloud
providers and credentials through AFS. `config.Loader.FS` only loads config and
is not implicitly the job filesystem. `AsyncOptions.Notify` receives terminal
jobs after persistence. `WatchError` handles discovery, dispatch and archival
errors. Without it, Diagnostics receives a generic error event that omits job
state, credentials and URLs. Callbacks must support concurrency; the host keeps
them and its borrowed FS valid until shutdown finishes.

## Authorization contract

- `Submit` receives canonical declared inputs after their codecs, before Init or
  handler execution. Check current identity, tenant/resource access and the
  exact operation. Successful JWT verification alone is not permission.
- `Replay` receives fresh canonical input with raw declared JWT sources verified
  again. Recheck current access and refresh application authorization inputs.
  Queued credentials can expire and permissions can be revoked. JSON claims,
  stored UserID/email and event State cannot replace verification or current
  policy. The existing replay owner rejects fabricated claims.
- HTTP `Inspect` receives the status route's freshly bound declared inputs and
  the actual durable job identity. Validate current requester access to that
  job. Result routes also use the existing target route checks. Ordinary route
  APIKeys and declared JWT requirements remain in force.
- Storage dispatch first calls `Inspect` with **nil Input**, including duplicate
  terminal events. There is no HTTP principal at this point. The host must check
  its own current worker/service grant for the exact durable job; it must not
  infer authorization from stored principal fields. A pending job then goes
  through the separate verified `Replay` check before being claimed/executed.
  Hosts unable to establish worker authority should deny this call. This also
  applies to programmatic inspection without request input.

HTTP controls name canonical fields, whose tags define their source. Declare a
string MatchKey and optional boolean SyncFlag on scheduling input, a string
JobID on the authored inspection route, and any required JWT input on each route.
Output fields use the existing `kind=async` job/jobinfo projection. No universal
`async=true` query parameter is installed. See [the async guide](../doc/async.md)
for status/result codes, match reuse and supported reader result behavior.

## Lifetime and evidence

The watcher starts on first successful Manager publication (`Reload`, or initial
publication inside `Serve`). Reload retains the store and host policy; dispatch
uses current component metadata. An external storage adapter calls
`Server.DispatchStorageEvent` and owns its acknowledgement. Polling owns deletion
on success and archival on failure. The durable row supplies identity/route/state,
so altering an event cannot replace that authority.

Standalone shutdown drains transports, then stops/cancels/joins Manager jobs,
notifications and watcher post-processing before closing its caches/connectors.
A caller timeout bounds waiting; subsequent shutdown calls join continuing
cleanup. The host-owned FS remains borrowed.

`standalone/async_test.go` proves JSON-loaded linked contracts, real SQLite
mutation and reader scheduling, AFS dispatch, HTTP status/JWT/APIKeys, explicit
submission/worker/replay/status denial, queued expiry across restart, forged
claims rejection, native table selection/creation and shutdown with active
execution and AFS acknowledgement. HTTP uses the actual Server handler without
TCP; listener/process signal acceptance requires an environment that allows
binding sockets. Cloud providers and non-SQLite databases are not qualified by
these tests. Destination-table output, distributed delivery guarantees and
RUNNING-job reconciliation remain outside this wiring.
