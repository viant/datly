# Shared Bindly Binder

Xdatly's `handler.Binder` embeds the public `bindly.Binder` interface:

```go
Bind(context.Context, any, ...bindly.BindOption) error
```

`Lookup` and `BindScope` retain their existing contracts. Calls with no options
preserve the existing runtime path. This does not introduce a second injector,
change codec compilation or widen scoped binding admission.

| Runtime adapter | Explicit options |
|---|---|
| `runtime/handler.Binder` | Defaults to `WithSource(canonicalInput)`, then applies supplied options using Bindly's last-option-wins behavior. An override is local to the call. |
| Lifecycle binder | Forwards options to the delegate inside the existing lifetime/cancellation lease. |
| Component-result binder | Invokes the native child once, then forwards options to its result binder. |
| Canonical scoped binder | Rejects options; the canonical pointer, source and declared scope plan remain authoritative. `BindScope` is unchanged. |
| SQL input-view scope binder | Rejects options; the native `locator.Scope.BindTarget` API does not carry Bind options. Existing selector isolation is unchanged. |

Existing `Bind(ctx, target)` consumers need no edits. Implementations and mocks
must accept the variadic parameter to satisfy the interface. An assignment such as
`var fn func(context.Context, any) error = binder.Bind` needs a variadic function
type or a closure; Go method-set compatibility is stricter than call compatibility.

Bindly's shared interface and Xdatly v1's matching interface migration are published.
The current dynamic-contract additions are developed against the local workspace;
version pins are finalized after their dependency commits are pushed. Do not commit
filesystem replacements. See [dynamic contract status](dynamic-contract-v1.md).

### Dynamic contracts and requiredness

Dynamic field metadata uses explicit requiredness: an untagged field is optional,
regardless of its scalar type or JSON omission settings. Authored required binding
parameters and HTTP path identity remain required in discovery. `Field.Optional()`
removes the metadata requirement and the requirement in an existing native binding
tag; it leaves `validate` and `sqlx` constraints intact.

Binding and mutation validation have distinct owners. Bindly records supplied
values and rejects missing explicitly required bindings, but does not classify
inserts and updates. The writer selects per-record action and supplies field
coverage and previous-row provenance to SQL/Go validation. Sparse updates validate
supplied fields; inserts receive full final validation. `InsertValidationPresence`
only makes the initial insert pass presence-aware. An operation-specific discovery
policy must reflect this distinction without removing the canonical entity's
validation constraints. A required PATCH body can contain optional editable fields.

The create/update MCP tool split and a reusable operation policy remain design
work; they are not inferred automatically from a PATCH route.
