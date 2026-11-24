# Named schema discovery connectors

`datly transcribe` and `datly validate` can inspect a graph that uses several
explicitly named database connectors. Supply `-schema -connector main
-schema-connectors /private/connectors.json`. The local JSON file contains one
array:

```json
[
  {"name": "main", "driver": "mysql", "dsn": "<private MySQL DSN>"},
  {"name": "history", "driver": "bigquery", "dsn": "bigquery://example-project/history"}
]
```

The default `-connector` must occur in the file. Each view's connector selects
its own connection; unknown or duplicate names fail. Driver packages must be
linked by the embedding CLI. Keep credentials in a private file. Opening is lazy,
connections are reused per name, and all opened connections close after discovery.
SQLite discovery retains its read-only file restriction.

Existing `-schema -connector name -driver driver -dsn connection` remains
supported. Choose that form or `-schema-connectors`; combining them fails rather
than overriding a connection. All connection options require explicit `-schema`.
Handler-only transcription does not accept database discovery options.

## Google Sheets

The stock `datly` CLI includes the `gsheets` driver and its SQLX metadata adapter.
Each native Google spreadsheet is a database, and each selected tab is a table.
For example, the following connector selects the `products` tab and uses
Application Default Credentials. Grant the Google identity Viewer access to the
spreadsheet for read-only discovery.

```json
[{"name":"book","driver":"gsheets","dsn":"gsheets://SPREADSHEET_ID?tab=products"}]
```

Use `-schema -connector book -schema-connectors connectors.json` with
`transcribe get`. The same connector name and DSN can be used by the runtime.
See the [gsheets driver documentation](https://github.com/viant/gsheets/blob/v0.1.0/README.md)
for authentication and tab mapping.
