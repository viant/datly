# Developer MCP authoring service

`developer.New` creates an explicitly configured developer service using the
existing native MCP transport and canonical Datly authoring/runtime owners.
It exposes seven tools, separately from application business components:

| Tool | Behavior and authority |
| --- | --- |
| `datly.validate` | Validate a configured target with the shared Validator; static by default, optional operator-owned schema authority. No file emission or application execution. |
| `datly.transcribe` | Transcribe supplied source using the target's configured source context, destination and generation options; preserve authored-file ownership. |
| `datly.run` | Start a configured linked application on an allowed loopback address/port; return a server-owned instance ID after publication and readiness. |
| `datly.stop` | Cancel/drain an owned instance by ID; no arbitrary PID, port or shell process control. |
| `datly.components` | List canonical component identities and project hash or owned application revision without invoking handlers. |
| `datly.inspect` | Inspect component contracts, routes, views, selectors, cache and declared authorization; large metadata can use immutable resources. |
| `datly.reverseDQL` | Read retained authored DQL or bounded canonical reconstruction with explicit limitations; no handler invocation or source rewrite. |

Discover each tool's actual schema. The validation call accepts only an exact
configured target, for example `{"target":"application"}`. That restriction is
specific to validation: transcription also accepts `source`; run has allowed
`address`/`port` choices; inspection uses `component` and optional `instanceId`;
stop requires its owned `instanceId`. There are no client-selected DSNs, output
paths, shell commands or compiler overrides.

## Operator configuration

`Config.Targets` fixes `transcribe.Validator` BaseDir, ModuleDirs, Include/Exclude
patterns and optional Connector/ColumnRefiner authority. `Config.Authoring` fixes
the corresponding transcription request and output destination. `Applications`
contains trusted standalone options with allowed loopback ports/addresses;
`MaxInstances` bounds owned application instances. Authorization remains explicit.
Missing per-target authoring/application capability is not permission to infer one.

For high-level DQL generation, set the authoring request's `Generation.Operation`
to `get`, `patch`, `post`, or `put`. Go is the default language. This uses the same
generator as `datly transcribe <operation>`: GET produces reader code, while write operations produce
mutation code and create-once Lifecycle placeholders. The DQL must declare its
destination with `#package`. Configure separate targets when exposing different
operations; clients submit only the target name and DQL source.

Tool metadata exposes `datly.authoringTargets` with each target's enabled state,
mode, operation, and language. The transcription result reports the selected mode
and generated files. Source discovery uses the configured project and schema
authority, including local package imports. Invalid operations and combinations
of high-level generation with low-level options or linked contracts fail before
file emission. An empty `Generation` retains the configured transcription path.

The service copies target configuration and package-selection slices. Native
refiners/connections and authorization objects remain operator-owned and must be
stable and concurrency-safe. Use the ordinary MCP server configuration to select
stdio or its supported HTTP transport. Constructing the service does not itself
start a listener. Close the developer service after its transport stops admission.

The approved parent authoring-root correction uses the same resolved root for
destination containment and generated-file reporting. Reported generated files
are valid relative paths under that root, including a configured symlinked root;
host-absolute or escaping paths are not returned. Resolving authoring roots does
not require validation-only targets to exist at construction, and it does not
mutate the caller's configuration. This is bounded parent correction approval,
not a claim that this documentation patch applies runtime code.

## Reports, resources and lifecycle

Validation returns the canonical report as JSON text and structured content.
Invalid source retains diagnostics with `isError=true`; invalid arguments produce
an MCP invalid-params error. Read `completed` and `skipped` even when `valid=true`.
Static validation does not prove runtime/schema behavior. Transcribe and run have
their own explicit write/execution effects; do not describe the whole service as
read-only or validation-only.

The three authoring skills use the canonical generated bundle, native Final
SEP-2640 `skills/list` and `skills/get`, and consistent static resource list/read
inventory. Only explicitly declared skill roots are skills; supporting documents
do not activate tools. Keep resource and skill authorization aligned with the
server policy. See [protocols](../../doc/protocols.md) and
[canonical packaging](../../doc/authoring-skills.md).

Custom project discovery/linking is handled internally by `datly init/build`;
no mandatory user init/Register/import list is needed. Parent custom-build tests
pass real TCP reader/mutation endpoints and add/remove rebuilds. This evidence
is distinct from testing every developer-tool workflow, and custom executables
remain [source-backed](../../doc/project-build.md#deployment-contract).
Exact-name and duplicate-column correction still awaits final code delivery/review;
never infer spelling equivalences or aliases not authored by the user.
