# Enum JSON representations

Go storage, accepted JSON input, and emitted JSON output are independent.

```go
State int `choice:"0:draft" choice:"10:active" enumAccept:"literal" enumStyle:"ordinal"`
```

Input `"active"` becomes Go `10`; output is JSON `10`. MCP and OpenAPI request
schemas advertise a string enum, and response schemas advertise an integer enum.
For a string Go field, numeric input can similarly become a stored literal.

## Choice declarations

- `choice:"draft" choice:"active"` supplies zero-based declaration ordinals 0 and 1.
- `choice:"0:draft" choice:"10:active"` supplies explicit ordinals 0 and 10.
- An explicit ordinal overrides its declaration position. Bare choices elsewhere
  retain their zero-based positions; duplicate ordinals or literals are errors.
- Existing all-numeric choices on numeric Go fields remain numeric domains:
  `choice:"3" choice:"7"` retains values 3 and 7. To give a numeric-looking literal
  another code, use an explicit pair such as `choice:"0:3"`.
- A numeric prefix before the first colon denotes an explicit ordinal. Other
  colons remain part of the literal. Explicit pairs are recommended for persisted
  codes and caller-filtered domains, so filtering cannot renumber stored values.

## Input and output tags

| Tag | Accepted values | Default |
|---|---|---|
| `enumAccept` | `ordinal` or `literal` | Go storage type |
| `enumStyle` | `ordinal` or `literal` | Go storage type |

Integer storage defaults to ordinal; string storage defaults to literal. Each tag
selects exactly one representation. `ordinal|literal` is rejected, not treated as
an input union. The input default is independent of the output style.

Ordinal JSON is numeric; literal JSON is a string. Wrong representation types fail
before business execution. Unknown values fail when conversion needs a missing
mapping. Ordinary choice metadata and identity-form values do not replace explicit
business membership validation. Existing null/presence and omission policies remain
with their native owners. Enum conversion does not change field storage or layout.

## Codecs and schema owners

Structology's native/formatted JSON APIs and its standard-compatible
`MarshalStandard` / `UnmarshalStandard` wrappers support these tags, including nested
fields and opt-in Contract metadata. Go's direct `encoding/json` functions ignore
these additional tags. Configured custom codecs must use an enum-aware implementation;
complete custom JSON/Text methods still own their representation.

Datly's native body path and scalar request bindings use the shared Tagly mapping.
MCP checks the accepted JSON representation before projecting scalar arguments into
HTTP-style sources, then uses the native binding conversion. MCP/OpenAPI input
schemas use `enumAccept`; output schemas use `enumStyle`. They never infer the wire
type solely from Go storage when an enum representation is selected.

A Contract callback can change the domain and either tag. Use a different stable
policy name for different schema variants. For example, `SetEnum("10:active")`
retains ordinal 10 while `SetTag("enumStyle", "literal")` changes only its output
representation. The same policy is applied to discovery and actual encoding/decoding.
