# Dynamic Contract: JSON codec boundary

Dynamic contracts retain the original linked type at every unchanged node. A changed
node is represented by a reflected type with adjusted field names/tags; the original
business handler type remains authoritative for execution.

## Supported behavior

- Configured JSON encoders and decoders remain selected by the component configuration.
- Ordinary struct fields use the effective representation for JSON decoding/encoding,
  with value projection between that representation and the canonical handler type.
- An unchanged custom JSON child retains its linked type and its methods, even when
  an enclosing wrapper changes. For example, hiding `Campaign.Spend` need not change
  the linked `campaignpatch.Campaign` stored in another field.
- Hiding an entire field containing a custom JSON value is supported when its parent
  can be rewritten without changing the custom value's representation.

## Custom method boundary

Adjusting fields **inside** a type that owns `MarshalJSON` or `UnmarshalJSON` is outside
this feature's supported scope. Such a change is rejected explicitly; the runtime
must not drop the method, silently ignore the policy, or bypass its normalization.
This is an accepted feature boundary, not an outstanding requirement to implement
arbitrary custom-codec rewriting in this delivery.

A custom method owns its complete JSON representation. Retagging a reflected type
cannot alter code executed by that method. Keeping the original receiver preserves
its behavior, but does not make that behavior honor the effective tags. Embedding
the original type in an anonymous wrapper does not solve this: a promoted JSON
method can still consume or emit the complete original representation.

Platform example: `adorder/patch.Adorder.UnmarshalJSON` normalizes raw JSON, decodes
through a local alias using `encoding/json`, then computes presence. An outer
Structology effective-type option cannot automatically reach that nested standard
library call or reinterpret the method's aliases and transformations.

## Structology extension decision

The original projection-only scope required no Structology extension. The user has
since expanded the requirement to support method-bearing embedded shapes. That
requires metadata separate from the canonical instance; `viant/x/shape.Representation`
and Structology support are implemented in v1. This expansion does not remove
the custom-JSON-method boundary. Supporting edits inside those methods would still
require a separate contract-aware codec/delegation API.

The same custom-method boundary applies to discovery and execution: do not publish
an effective field schema that the selected codec cannot enforce.

## Embedded shapes: approved metadata expansion

Go's `reflect.StructOf` cannot rebuild a multi-field struct containing an embedded
pointer type with methods. This can fail even when that embedded child is unchanged
and only a sibling is retagged. Ordinary embedded holders are preserved, and a
no-change result retains the original linked type including promoted methods.
`TestEmbeddedMethodReflectionRestrictionIsExplicit` records the native error in the
old reflected-type path. The approved implementation uses separate immutable field
metadata while retaining canonical types, field indexes, offsets and embedding.
The native traversal owns this representation; schema and codec consumers must use
it without flattening storage or stripping linked methods. Datly input/output, MCP and OpenAPI consumers now use the optional metadata path.
The old restriction test records only the limitation of reflected replacement types.
