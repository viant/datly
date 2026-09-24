# Named Connections For Column Discovery

Available connections and view assignments are separate. A compiled application
can supply several named connections without a global default:

```go
connections, err := connector.Open(ctx, selectedConfigs, "")
// Check err before using connections; close the set when discovery finishes.
defer connections.Close()

discovery := transcribe.Discovery{
    BaseDir: root,
    Include: packages,
    ColumnRefiner: column.New(connections),
    // Connector is intentionally empty: no application-supplied fallback.
}
```

The application selects and validates configuration names before opening them.
`connector.Open` opens and pings every selected connection. Selection order never
establishes a default, and no `Discovery.Connectors` field is needed.

Use DQL `use_connector(view, 'name')` to assign a view explicitly. Otherwise,
related views inherit their parent's resolved connector. Root and independent
views start with the explicitly authored component default, if any. An explicit
`Discovery.Connector` supplies a fallback only when no component default exists;
do not populate it from list order or an unrelated runtime configuration default.

SQL-less holders require no connection and do not prevent traversal of SQL-backed
descendants. `in_memory(view)` suppresses runtime SQL, not discovery-only SQL:
placeholder SQL retained for type discovery still requires a resolved connector.
Missing or unavailable connectors fail with component/view context; discovery
does not retry another named connection or silently fall back to inferred types.

Database discovery is opt-in at the Datly library boundary. Applications requiring
it for readers must always supply `ColumnRefiner`, even without a default
connector. [Handler-only generation](HANDLER_ONLY.md) bypasses column discovery;
the calling application must choose that path before opening connections.
