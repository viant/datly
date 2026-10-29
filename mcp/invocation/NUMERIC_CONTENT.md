# Numeric structured tool content

Object-shaped component payloads use standard `encoding/json.Unmarshal` when
projected into MCP `structuredContent`. Generic numeric leaves are `float64`,
matching legacy Go callers. Large integers and precise decimals may round;
tiny exponents may underflow to zero. Float64 overflow, malformed or trailing
JSON, non-object values and null omit structured content. Original text content
is retained, including when structured content is omitted.

This applies to successful output, injected output encoders and explicit public
error bodies. Internally constructed error maps retain their existing numeric
Go types. Schema-specific exact enums and declared typed numeric fields keep
their own decoding behavior. Exact business identifiers should use strings.
