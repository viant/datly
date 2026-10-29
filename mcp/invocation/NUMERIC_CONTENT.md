# Numeric structured tool content

Object-shaped JSON payloads returned by component tools preserve their numeric
JSON tokens when projected into MCP `structuredContent`. Integers above 2^53,
uint64 values, negative values, fractions, and exponent notation remain JSON
numbers with the same precision as the text content, including inside arrays and
nested objects. This applies to successful output, injected output encoders, and
explicit public error bodies. Raw transport responses keep their existing rules.

For Go callers that inspect `Execution.ToolResult().StructuredContent` directly,
numeric leaves parsed from component payloads are now `encoding/json.Number`
instead of `float64`, including small numbers. Use `json.Number.Int64`,
`json.Number.Float64`, or `strconv.ParseUint(number.String(), 10, 64)` according to
the declared output type; float conversion can lose precision. Nulls remain nil,
and object/array/string/bool values retain their existing shapes. Internally
constructed error envelopes keep their existing Go numeric types.

The wire format and text content are unchanged except that structured numeric
values no longer round through float64. Valid numeric exponents outside the
float64 range now also retain structured content instead of being omitted. Clients that decode JSON into float64
can still lose precision and should preserve raw fields or decode typed output.
The companion mcp-protocol tool-call decoder fix preserves these numbers through
normal Go request/result decoding as well. An outer `UseNumber` decoder cannot
override a custom `UnmarshalJSON` method that uses its own default decoder, so
end-to-end precision requires that protocol fix alongside this server change.
Malformed JSON, multiple JSON values, non-object JSON, and null continue
to omit `structuredContent`; trailing whitespace is accepted.

Regression tests marshal actual `schema.CallToolResult` values and inspect raw
wire fields and a standard-library `UseNumber` oracle independently of the MCP
protocol unmarshaller.
