# Author a custom Datly component

For current enablement and pending integration boundaries, consult [output-and-operations.md](references/output-and-operations.md) when using operational or extension features below. Required behavior is not a claim of connected-build availability.

## When to choose custom behavior

Use a custom component for business orchestration, external integrations, specialized transformations, or an explicitly application-owned workflow. A custom component still has typed input/output, parameter bindings, dependencies, validation and exposure. It does not require a parallel DAO/controller layer.

Choose Go when behavior benefits from typed services and normal Go methods/tests. Choose Velty for a supported data-oriented template workflow. Generated readers/writers remain the simpler choice when they already express the task. Never downgrade a requested generated writer to custom code without saying why.

## Go workflow

1. Define or reuse Input and Output.
2. Declare the component and named factory in the application package.
3. Implement handler.Contract[Input,Output].
4. Bind canonical input and needed services through public tags/Binder.
5. Validate framework/schema/DB rules, then custom business rules.
6. Invoke other components or perform buffered writes through focused services.
7. Set the intended output and error/status contract.
8. Test the actual HTTP and MCP behavior and side effects.

Constructor names are package-scoped identities. Use full import paths to resolve dependencies across projects; do not expose every referenced package.

## Custom writes are explicit

A custom handler can orchestrate differently from the generic writer, but must not accidentally inherit only half of its safety model. If it implements sparse mutation, preserve original presence/identity, classify tuples correctly, use full versus sparse validation, allocate only genuinely new/unassigned identities, repair parent links and distinguish queued from committed work.

Do not allocate every row whose current ID is zero. An original supplied zero can be a valid identity. Do not match sparse children by position. SQLX/StructQL capabilities should own tuple matching/binding, not string concatenation or maps used as the ordinary row model.

## Component dependencies

A bound field such as bind:"kind=component,in=GET:/internal-records,required" consumes an existing component through the declared interface. Use the actual server-advertised identity scheme for cross-project/private components. Canonical input remains available to predicates and hooks; helper-target binding must not impersonate the component input.

This declarative binding inherits the caller's request/provider scope; it does **not** automatically pass the parent's typed Input object. If a POST body must become a private GET component's query-shaped input, explicitly map the approved fields into that dependency's exact Go input type. The existing exported `exec.ComponentInvoker` accepts a trusted `ComponentRequest.Input` for this purpose. Never let a client choose the component key, route, providers, or input type. See the [verified forwarding and registration example](references/custom-examples.md#explicit-typed-forwarding-and-trusted-service-registration).

Application service injection is also explicit: the embedding host can attach a provider to the check component's registration, and the handler requests that provider through Binder. Body/query properties are not service registrations. A shared service object must be concurrency-safe; request state belongs in context or a request-scoped instance.

For the demonstrated public/private policy, keep the check and lookup in separate package scopes, register both, and select only the check package with `runtime.WithExposedPackages`. The lookup remains available to internal component calls while public HTTP and business-MCP discovery/calls exclude it. A path beginning `/internal/` is not the policy. Package filtering does not imply same-package per-component filtering; ask the developer server for its actual selection capability if that is needed.

The same managed transaction may flow into downstream writes. A child does not commit its parent's transaction. Validate dependency cycles and authorization before exposing a public entry point.

## Responses and failures

A handler may intentionally control its output/status, but validation failure must not be accidentally reported as successful mutation. Return a typed failure when work must stop or roll back.

For full public error control, use the explicit response-body error contract and an internal cause. Preserve public message/error exactly; do not log secrets into Payload. Business MCP tool errors use isError plus structured content rather than an unauthorized transport response.

Use outcome-aware finalization for commit-dependent messaging. A successful call to DML.Insert or Data.Flush under a caller-owned transaction does not prove commit. Ordinary custom handlers retain their supported output finalizers; do not mix their signatures with generated writer completion.

## Rich-shape transformations

Reuse imported shapes and CAST/tag metadata. OnFetch builds logical output from internal physical columns. Input/entity Init maps supplied logical values back to backing columns and markers. Never hide physical columns by marking them non-SQL; never persist a logical pseudo field accidentally.

## Operational behavior

Respect request cancellation, cache/codec lifetimes, allowed selectors and configured connector names. Reader retries must not replay hook side effects. External API/message calls need their own explicit idempotency/completion policy; a framework read retry is not authorization to repeat arbitrary custom work.

The developer MCP server should preview the named factory, public schema, capabilities, dependency/exposure closure and generated/authored file boundaries. You do not need framework implementation details to use these contracts.

## Injected hook services and original async compatibility

For typed mutation hooks, declare `Bus handler.MessageBus` with
`bind:"kind=mbus,required"` and configure that capability for the component.
The same invocation-scoped hook object can collect typed business-message
payloads and publish from its root outcome-aware Finalize method, guarded by
`outcome.CommitConfirmed()`. No message is warranted merely by Queue, a failed
SQL operation, or a caller-pending transaction. Publication failure after commit
does not roll back the database; preserve the application's delivery policy.

Async execution must reuse original `DATLY_JOBS` schema/state conventions and
the canonical reader/mutator execution path. Original reader dry run skips SQL
execution; it is not proof of side-effect-free arbitrary mutation preview.
Treat unsupported runtime job behavior as a concrete connected-build gap.

JWT claims are available to authorization predicates only when explicitly part
of component input. Preserve original `JWTValidator` certificate/public-key
configuration and verified `JwtClaim` binding; do not inject ambient claims or
substitute token decoding for verification. Check connected-build support.
