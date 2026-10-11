# Output encoding

Output contracts are compiled at bootstrap and published with the registered
component. A declared `FormatSelector()` input chooses the request source:
`header/Accept` negotiates media types, or a query source such as
`query/_format` accepts format names. Without a declaration the legacy `_format`
query source remains available. An absent value uses the route marshaller, then
the component format (JSON by default). A handler-provided response retains control of its
status, headers, and body. Execution and encoding failures use JSON errors.

Supported formats are `json`, `csv`, `xml`, `tabular`, and `xls`/`xlsx`. XLS
responses contain an XLSX workbook. Native SQLX, Structology, XMLify, and XLSy
encoders retain typed values; tabular JSON preserves the surrounding output
envelope and transforms its data slot.
Unsupported `Accept` values return 406; invalid query format names return 400.
Formats are checked before route execution, so a JSON-only custom output cannot
be used to reveal internal fields through a different encoder after a mutation.

CSV and tabular row authority includes a direct view declared with cardinality
`one`, or an explicitly named struct/pointer data slot (`DataField` or an
`output/view` / `output/body` parameter). A direct singleton's child collections
remain relations within that row. Invocation field selection retains this row
authority. Singleton and array carriers become typed slices only for these two
codecs; JSON, XML, and XLS retain the structural output shape.

A nil singleton represents zero rows: CSV emits its field header and tabular
emits `[]`, inside the existing envelope when one is present. A nil envelope
retains the existing empty CSV / `null` tabular behavior. CSV wire contracts
remain textual `text/csv` downloads; tabular still requires an explicit wire
schema projection for documentation.

Go component settings use the shared settings tags, for example:

```go
format:"csv" output:"{\"exclude\":[\"Rows.Secret\"],\"omitEmpty\":true,\"title\":\"Records\"}"
```

The same policy can be authored in DQL:

```sql
#setting($_ = $format('csv'))
#setting($_ = $output_exclude('Rows.Secret'))
#setting($_ = $output_omit_empty(true))
#setting($_ = $output_title('Records'))
```

Exclusions resolve to canonical Go-field paths at registration. Relative paths
first resolve against the main data rows. Unambiguous JSON aliases are accepted
at every nesting level; unknown or ambiguous paths fail registration. Export
formats receive typed copies with the excluded fields removed, leaving the
application's output unchanged. `omitEmpty` applies to JSON output, including a
tabular envelope. File formats use the title as an attachment filename.

`caseFormat` and `dateFormat` configure JSON and tabular JSON. Other formats
retain their native field-format tags. `jsonMarshal` resolves an authored Go
type through bootstrap's type authority. It must implement
`Marshal(any) ([]byte, error)`; as in original Datly, a custom JSON marshaller
owns the complete JSON representation instead of the default formatting policy.

With Tagly `8165180`, case conversion preserves numeric segments:
`SampleSeen_1Day` and `SampleSeen_7Day` format as `sampleSeen1Day` and
`sampleSeen7Day` under `caseFormat:"lc"`. Older versions collapsed both to
`sampleSeen_Day`. Rebuilding changes untagged output names and MCP schemas;
explicit JSON names remain authoritative. Regeneration also changes inferred
Go fields such as `SampleSeen_1Day` to `SampleSeen1Day`, while preserving SQL
column tags. Coordinate client expectations and handwritten field references
when upgrading, and regenerate previously inferred explicit JSON tags.
Collision checks still apply to other names that format identically.

CSV and XML codec metadata is reused safely by a registered plan. Each XLSX
workbook keeps its own mutable native encoder session. Field/type discovery and
serialization-shape construction use `viant/x/shape`.

HTTP response compression is opt-in component metadata:
`Settings.ResponseCompression = &spec.ResponseCompression{Encoding: "gzip", MinSizeBytes: 2048}`.
The immutable output plan carries it; ordinary `Encode` stays uncompressed so
internal/MCP consumers keep typed/raw output. HTTP applies gzip after encoding
only for lengths strictly above the threshold, independently of Accept-Encoding.
Explicit response objects own their streams/headers/encoding and bypass this policy.


Native JSON nil slices can be presented as arrays with an explicit component policy:

```sql
#setting($_ = $nil_slice_policy('empty_array'))
```

The programmatic equivalent is `spec.OutputSettings{NilSlicePolicy: "empty_array"}`,
carried in the existing output settings tag. Omission and explicit `null` preserve
existing encoder selection and defaults. An authored explicit `null` overrides an
inherited `empty_array` policy. Unknown values or repeated directives fail validation.

The policy uses the existing native JSON encoder, including when no other output
transformation is enabled. This also selects that encoder's byte-slice, embedding
and tag semantics: ordinary bytes are arrays rather than standard JSON base64.
Components already using native casing or other transformations retain that engine.
Custom JSON marshalers retain precedence and their opaque values are not rewritten.

Ordinary nil slices become arrays; pointer nullability and maps retain their existing
semantics. The compiled Wire and JSONSchema describe this policy without changing
Go values. Plain `encoding/json` and persistence serialization remain unchanged.
Existing CSV/XML/XLS formats remain available for their supported shapes; tabular
JSON applies the same policy to the surrounding envelope as ordinary JSON, while its
row representation remains tabular. This option does not make a component JSON-only.
