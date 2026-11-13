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

## CSV and spreadsheet exports

Native CSV and XLS/XLSX encoders consume a typed presentation compiled once for
an effective output plan. Contract-hidden fields are physically absent from that
presentation, including nested fields. A request's selected fields compose with
Contract visibility using canonical indexes before projection. Original business
values and registered plans remain unchanged.

The existing CSV and spreadsheet codecs own their field names, column tags and
relation layout. The presentation compiles enum output metadata with the same
Tagly scalar codecs as JSON, including literal/ordinal conversion and output-only
aliases. Integer wire values remain numeric spreadsheet cells. Nulls keep each
format's existing representation. Hidden fields are removed before scalar
encoding, and a visible strict-unknown enum preserves its typed encoding error.
Removing enum metadata in the selected Contract restores storage representations
without modifying source values or another caller's plan. `enumAccept` remains
input-only. XML and tabular retain their existing scalar behavior.
A hidden declared row holder fails closed; another collection is never substituted.
Opaque/custom JSON owners and unsupported effective shapes remain fail-closed.
XML and tabular dynamic exports are not admitted.
