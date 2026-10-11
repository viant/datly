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
