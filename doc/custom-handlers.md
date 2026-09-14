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
or explicitly invoke a typed target through the scoped `exec.ComponentInvoker`.
The [forwarding example](../llm/datly-custom-component/references/custom-examples.md#explicit-typed-forwarding-and-trusted-service-registration)
shows a public POST forwarding an exact typed input to a private reader. It
preserves internal visibility, authorization and the root transaction owner.
Supplied zero remains supplied, and arbitrary body fields cannot replace a
registered service.

A configured named connector-provider capability is an opt-in exception for
specialized integrations. It returns a borrowed DB for an exact configured name,
not a managed transaction. Do not close it or assume direct work silently joins
Data's transaction; it is not client-bindable or installed by default.

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

| Existing contract | Timing/meaning |
| --- | --- |
| `Finalize(ctx, err) error` | Error-aware output finalization before root completion; returned error can prevent locally owned commit. Original cause errors remain preserved. |
| `Finalize(ctx) error` | Success-path output finalization after root completion; does not run again when the error-aware contract was selected. |
| Mutation outcome finalization | Explicit result-aware `handler.Outcome` for confirmed commit versus pending/failed/unknown work. |
| `FinalizeMCP` | Runs with the explicit MCP context according to the SDK interface after shared success finalization. |

A plain success hook is not proof that a caller-owned transaction committed.
Use outcome evidence for commit-dependent messages. A post-commit finalizer
failure cannot undo completed database work. Do not finalize twice or retain an
invocation Data capability for use after its scope completes.

## Injector-aware finalization

The candidate supports the public SDK injector finalizer contract:

```go
func (o *OrderOutput) Finalize(ctx context.Context, lookup handler.InjectorLookup) error
```

Use it when an output conditionally needs another registered component. The
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
