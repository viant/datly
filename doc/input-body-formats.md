# Nested authored JSON date formats

Canonical body binding honors an explicitly authored `format:"timeLayout=..."` or `format:"dateFormat=..."` on nested `time.Time` / `*time.Time` fields. DQL column tags remain the contract authority; keep the generated original Go body shape.

The input compiler adds a binding transformer only to a body collection/object containing an authored date layout and with no explicit overriding parameter codec. It requests raw JSON, strictly parses only the schema-authorized date leaves using the declared layout or RFC3339Nano, then delegates to the existing public typed Body source for ordinary JSON decoding and generated presence flags. This conversion lives inside the canonical input pipeline. It does not require application request preprocessing, a parallel input struct, or changes to the underlying frozen Bindly library.

An explicit `null` date remains nil and present; an omitted date remains absent. Ordinary fields, unknown-field behavior, hidden JSON keys, integer precision, malformed JSON rejection, and client set-marker rejection remain owned by the original typed decoder. Arrays and nested pointers retain their normal shape. Input errors are classified400. An unauthored body gains no alternate date parsing behavior.

The original typed JSON body remains the public MCP/documentation source contract through the transformer's wire source type. Actual internal binding source bytes do not become an opaque public byte-array schema.

For a short offset source protocol, author `timeLayout=2006-01-02T15:04:05Z07`. RFC3339 timestamps including fractional/nonhour offsets remain accepted and keep their exact instant. The converter uses strict standard time parsing instead of the permissive fallback that truncates timezone text in the available Structology date parser. It reuses existing schema-format metadata and typed/presence decoding; no application-specific names or privacy rules belong here.

The formatter retains original object token order, duplicate keys, and untouched value bytes. Typed JSON decoding still decides case-variant last-wins. Only standard JSON-authorized names are eligible: explicit json aliases do not grant the Go field name, anonymous fields follow normal flattening/dominance, and recursive type plans retain finite nested payload references. The compiler/engine regression matrices compare these behaviors with the ordinary decoder and include large integer precision.

Unformatted JSON siblings (including raw JSON scalars, arrays, and objects) remain byte-preserved. Reusable/domain types implementing `json.Unmarshaler` keep authority over their entire representation; nested internal date tags are not inspected or transformed. Tests cover custom scalar and custom object representations alongside a separately formatted sibling date.

# Authored MCP representations for custom JSON bodies

A custom JSON body decoder owns its accepted JSON shape. Native MCP discovery requires explicit representation authority instead of inferring that shape from the decoder's Go fields. Define a distinct method-free named representation in the application's package and attach its canonical identity to the bound input field:

```go
type RequestWire Request

type Input struct {
    Body *Request `parameter:",kind=body" wireType:"example.com/application/request.RequestWire"`
}
```

The application owner asserts that `RequestWire` describes the JSON accepted by `Request`. An alias (`type RequestWire = Request`) retains its custom methods and does not supply that authority. Discovery never executes JSON methods. Runtime HTTP and MCP binding continue using the original `*Request`; `wireType` affects schema discovery only. RawMessage without an explicit `wireType` remains unconstrained JSON, and pointer nullability and binding requiredness retain their existing rules.

Explicit `wireType` supports a direct named body argument, including an existing codec-backed custom source. It changes discovery only: the declared source, destination and codec still own decoding, errors and null behavior. A codec destination is never inferred as wire authority. Anonymous custom bodies fail clearly. The component-scoped catalog must resolve a linked non-pointer named representation; missing, ambiguous, conflicting, cyclic, ordinary-source overrides and simultaneous scalar WireSchema metadata fail. A representation with custom JSON methods also requires authority and is therefore unsuitable for this direct declaration.

DQL can retain the same Go annotation through the existing `WithTag('wireType:"example.com/application/request.RequestWire"')` mechanism. For a linked external Input, put the tag on the authored Go field. Additional DQL declarations must resolve to the same type. Generation retains the representation with normal reflect.TypeFor anchors so a fresh binary can rediscover it. No decoder replacement or global registration is required.
