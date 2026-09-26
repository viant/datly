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

CSV and XML codec metadata is reused safely by a registered plan. Each XLSX
workbook keeps its own mutable native encoder session. Field/type discovery and
serialization-shape construction use `viant/x/shape`.
