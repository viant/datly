# Custom handlers, direct bytes and finalizers

[All guides](README.md) · [Mutations](mutations.md)

Use an application-owned Go or Velty handler when the component needs explicit
orchestration. The handler keeps the same typed input/output contract and asks
for invocation capabilities rather than opening a second runtime or request
binder.

## A typed handler

The public Go contract is `handler.Contract[I, O]`; `Exec` receives context,
`handler.Session`, `*I` and `*O`. A named factory belongs in component metadata.
The custom project build discovers and links its typed factory bridge and input/output types internally.

The [greeting example](../llm/datly-custom-component/references/custom-examples.md#typed-application-owned-handler)
shows complete Go source, explicit validator injection and error handling. The
[project fixture](../project/build/testdata/app/hooks/write.go) shows a DML handler
that the custom build discovers internally. Choose narrow `Validator`, `DML`, `Sequencer` or
other capabilities, or the public `handler.Data` aggregate when needed. A bind
tag requests a configured service; it does not install one.

Keep request-local dependencies in local values or scoped hook instances.
Shared static services must be concurrency-safe. Velty programs use the same
validation, StructQL, sequence and buffered-DML services; they do not gain a
separate patch engine or raw transaction facade.

## Compose components

Bind a declared component dependency through the existing component provider,
or explicitly invoke a typed target through the scoped `handler.ComponentInvoker`.
The [forwarding example](../llm/datly-custom-component/references/custom-examples.md#explicit-typed-forwarding-and-trusted-service-registration)
shows a public POST forwarding an exact typed input to a private reader. It
preserves internal visibility, authorization and the root transaction owner.
Supplied zero remains supplied, and arbitrary body fields cannot replace a
registered service.

To call an external HTTP endpoint or MCP tool without writing transport code,
declare a `remote.Config` constant and use the ordinary
[remote handlers](remote-handlers.md). They use the same handler DI injection
and lifecycle as application handlers; their typed output composes like any
other component result.

A configured named connector-provider capability is an opt-in exception for
specialized integrations. It returns a borrowed DB for an exact configured name,
not a managed transaction. Do not close it or assume direct work silently joins
Data's transaction; it is not client-bindable or installed by default.

## One transaction for a native mutation

The first endpoint invocation owns transaction completion. Its first generic
writer starts the transaction. Invoke child readers and writers through the
scoped `handler.ComponentInvoker`; they share the database unit and buffered DML.
By default, child calls retain their operations in the caller's journal; the
owning root prepares and completes it. No `component_call_policy` setting is
needed for buffered calls. Omitted child policies inherit their parent, including
calls through a retained scoped invoker with a replacement context. Do not flush
between these calls.
Use `component_call_policy('imperative')` explicitly on components that need
their imperative child calls to flush a queued prefix before returning, so a
later child reader sees the write. A buffered ancestor cannot be overridden
by an imperative or independent child. The separately configured source-less
`independent_child_transactions` mode retains its independent completion policy.
The endpoint's final error rolls back the shared unit, including any work
already prepared in its transaction.

Do not construct another `Runtime` with a fresh `*sql.Tx` inside a handler for
the same database. Datly rejects that conflicting owner. For immediate SQL
that is not a reader or writer component, resolve the connector through
`handler.TransactionSQLCapabilityKey`; its capability has no commit or rollback
method. A child success finalizer runs after the root completes. If a parent
needs data from a child before then, read the child's typed view or call a pure
projection method rather than depending on a success finalizer's output fields.

## Return already-shaped bytes

The canonical SDK transport contract is `response.Response`: body reader,
status, headers and size. `response.Buffered` is its in-memory implementation.
There is no additional Datly `responseBuffer` abstraction to invent.

This self-contained SDK function constructs a direct CSV response:

```go
package download

import "github.com/viant/xdatly/response"

func CSV() response.Response {
    return response.NewBuffered(
        response.WithStatusCode(200),
        response.WithHeader("Content-Type", "text/csv; charset=utf-8"),
        response.WithHeader("Content-Disposition", "attachment; filename=records.csv"),
        response.WithBytes([]byte("id,name\n1,first\n")),
    )
}
```

`WithBuffer` accepts a `*bytes.Buffer` when the application already has one.
The caller must not mutate shared backing bytes after transferring the response.

For a typed custom component whose HTTP response must set cookies or redirect,
use an output struct embedding `*response.Buffered`. The custom handler fills
that field with `response.NewBuffered` and `response.WithStatusCode`,
`response.WithHeader("Location", target)` and one `response.WithHeader("Set-Cookie", value)`
per cookie. The HTTP adapter preserves repeated `Set-Cookie` values. Declare
request data as ordinary typed input fields, for example `kind=cookie` for an
opaque session ID, `kind=query` for OAuth code/state and `kind=header` for
Origin. Do not read an ambient `http.Request` or write to `http.ResponseWriter`
from the component handler. The adapter regression test
`gateway/http/custom_response_cookie_test.go` verifies these bindings and the
redirect/cookie response together.

The custom handler can orchestrate a trusted OAuth service, but encrypted token
persistence should still use a declared Datly writer component (and reader for
lookup). A custom handler does not turn a separate raw SQL store or another
runtime into the endpoint's managed transaction.

For exact execution counts from invoked generated writers, bind the read-only
`handler.MutationReporter` capability from `github.com/viant/xdatly/handler`
with `bind:"kind=mutationReporter,required"`.
Its detached report distinguishes affected rows from queued or intended writes,
including a zero-row update suppressed by a database trigger. Compare the
executed result list before and after an imperative component call; validate
the operation/table and count before using the evidence. This capability
exposes no database handle, write method, or transaction completion method.
Affected rows inside a parent transaction still await that parent's outcome;
use outcome-aware finalization for commit-dependent effects.

For already-compressed output, use the response's explicit compression metadata.
The [HTTP adapter](../gateway/http/handler.go) recognizes `response.Response`
before ordinary encoding; [writeResponse](../gateway/http/response.go) copies
its body with `io.Copy`. It does not need to marshal a buffer into a Go entity.

**Scope:** this function constructs an SDK response. The candidate additionally
supports authored generic output/documentation metadata; use its explicit
contract rather than assuming any arbitrary interface can become a standalone
linked output. Author media type, status, headers, schema/reference and examples
through [documentation metadata](api-documentation.md#shared-yaml-dictionary-and-response-schemas).
Do not infer a schema by inspecting runtime bytes or executing the handler.

## Finalization and commit evidence

| Contract | Timing/meaning |
| --- | --- |
| `Finalize(ctx, invoker handler.ComponentInvoker) error` | Explicit child composition before transaction completion; returned or child errors prevent owned commit. Also runs before a native writer's separate outcome callback. |
| `Finalize(ctx, invoker handler.ComponentInvoker, operationErr error) error` | Error-aware child composition in the same pre-completion phase; the current error is retained even when the callback returns nil. |
| `Finalize(ctx, err) error` | Error-aware output finalization before root completion; returned error can prevent locally owned commit. Original cause errors remain preserved. |
| `Finalize(ctx) error` | Success-path output finalization after root completion; does not run again when the error-aware contract was selected. |
| Mutation outcome finalization | Explicit result-aware `handler.Outcome` for confirmed commit versus pending/failed/unknown work. |
| `FinalizeMCP` | Runs with the explicit MCP context according to the SDK interface after shared success finalization. |

Custom contracts implementing runtime `handler.OutcomeFinalizer` retain that
opt-in through `custom.New` and `custom.Factory`. Their callback replaces the
ordinary output finalizers and runs once after owning-root completion, including
for nested calls. It receives the result and resolved outcome for compensation
or confirmed publication; early input failures may supply a nil result. A new
callback failure after commit is a finalization error, not a database rollback.
See [mutation finalizer selection](mutations.md#understand-finalizer-selection).

A plain success hook is not proof that a caller-owned transaction committed.
Use outcome evidence for commit-dependent messages. A post-commit finalizer
failure cannot undo completed database work. Do not finalize twice or retain an
invocation Data capability for use after its scope completes.

## Explicit invoker lifecycle arguments

Prefer separate `handler.InvokerInitializer` and `handler.InvokerFinalizer`
contracts for component composition:

```go
func (i *Input) Init(ctx context.Context, invoker handler.ComponentInvoker) error
func (o *Output) Finalize(ctx context.Context, invoker handler.ComponentInvoker) error
```

Each is a single-method interface; implement either independently. Input Init
runs after binding and snapshot capture, before handler execution. Output
Finalize runs after execution and output binding, before root completion. It
runs when an output is available even if the handler returned an error; that
error is preserved. Early binding/initialization failures do not invoke it.
Native writer outcome finalization remains a separate later phase.

For conditional composition after a successful operation, implement
`handler.InvokerErrorFinalizer` with
`Finalize(ctx, invoker, operationErr) error`. Return before child calls when
`operationErr` is non-nil. The error includes handler, output-binding and any
preparation failure encountered before this callback. It cannot report future
drain/commit errors or early failures without an output. Existing two-argument
finalizers keep their unconditional output-plus-error behavior. Protected roots
retain their existing preparation order; verify timing before replacing an
error-aware or post-completion finalizer. Regeneration preserves all these
supported authored signatures.


Call `invoker.Invoke(ctx, handler.ComponentRequest{...})` with an exact trusted
target and the child's declared input type. The argument uses the same scoped
runtime authority as field injection. Await child calls and do not retain it
after the callback. Child failures cannot be swallowed to permit owned commit.
For commit-dependent publication, use actual outcome evidence.

New Go input/output lifecycle scaffolds use the invoker-aware signatures.
Regeneration preserves user-owned lifecycle files and accepts the old forms.
Existing `Init(ctx)`, output finalizers, and invoker injection into Input remain
supported. New authoring should prefer the explicit arguments instead of storing
an invocation capability in request data. Static handler binding is unchanged.
This is additive: existing applications do not have to migrate. Go does not
allow method overloading, so each input/output chooses one signature per method.
Do not migrate post-completion publication from `Finalize(ctx)` into the new
pre-completion hook without preserving its transaction semantics.

## Existing injector-aware finalization

The candidate supports the public SDK injector finalizer contract:

```go
func (o *OrderOutput) Finalize(ctx context.Context, lookup handler.InjectorLookup) error
```

This form remains supported for existing output-to-input binding workflows.
For new explicit child composition, prefer `Finalize(ctx, invoker)` above. The
lookup receives a `handler.Route` with a local path, optional query string, scope
and name, then returns a binder. A child input can declare
`parameter:"ID,kind=caller_output,in=ID,required"`, and the caller output uses
destination `bind` tags such as `bind:"kind=param,in=Data"`.

The first `Bind` or `Lookup(ctx, handler.ResultKey)` invokes the selected
component through the runtime's canonical component invoker. Later operations on
that binder reuse the same typed result. External URLs, client-selected component
keys, copied internal session objects and saved background binders are not
supported.

SQLite/auth/concurrency/runtime tests cover conditional execution, false-branch
no-lookup behavior, error retention, cancellation, child success ordering and
transcribed Go output discovery. Keep examples tied to
`xdatly/handler/injector_finalizer.go` and
`xdatly/handler/injector_finalizer.md`; do not infer additional signatures.

### Reusing a generated writer input for preparation

An explicitly selected custom handler can reuse a generated input with body
reconciliation metadata when the component has no mutation operation or root
view. The body remains handler data; its writer reconciliation policy is not
attached to a nonexistent mutation root. Independent Current views, predicates,
codecs and component dependencies still bind through the normal input plan.
Every route must explicitly select a handler. Mixed mutation declarations retain
the mutation-root checks.

This does not suppress input `Init` or `Validate`. A preparation handler that
requires binding alone must use an input without those methods and verify that
constraint. Entity writer hooks and DML require an actual writer handler; reusing
its input type does not execute them. `Replay.Only` is not a substitute for
Current reads, since it prepares selected replay sources rather than the full
dependency plan.

### Native validation in an application-owned buffered writer

A trusted typed custom contract may resolve the protected
`handler.FrameworkValidatorKey` through its invocation binder. Use the native
validator rather than exposing validation policy as request data. Updates need
detached typed Previous values, actual `ReadMetadata` provenance and explicit
coverage. Keep validation coverage separate from persistence presence markers.

When preserving a source that validates complete new records in Go but applies
SQL reference/unique checks only to supplied fields, use the existing options in
two passes: full Go/required coverage with `CheckRef` and `CheckUnique` false,
then `HonorPresence` with explicit `Fields` and both database checks enabled.
Deduplicate repeated diagnostics without dropping distinct violations. Collect
business and schema violations before allocation or writes. This split is a
source-specific policy, not a default for every custom writer.

An INSERT child may reference an unflushed parent only when the trusted owner
has successfully queued the exact typed parent INSERT to the exact table through
the same invocation-scoped DML owner, before that child. Native `MatchReference`
checks the exact constraint and captured key values in the active transaction;
it does not prove write policy or order. Preserve those bindings until root
completion. Only that proven INSERT reference may be supplied through
`SatisfiedReferences`; validate all remaining required, null, unique and FK
constraints with complete final insert coverage. A failed, later or mismatched
parent write cannot supply that fact, and an existing parent UPDATE is not a
pending INSERT. `DeferredFields` remains restricted to compiled producer
authority and is not an application shortcut.

Reuse the existing shared buffer. Do not also execute automatic writer DML for
those rows, flush from a child, create a second action journal or replace the
allocator. Database FK enforcement and root-owned rollback still apply. A
successful queue operation is not proof of commit.
