# Named Connections For Column Discovery

## Prepared metadata without a database

Trusted compiler integrations can set `Source.PreparedColumns` or
`Discovery.PreparedColumns` instead of `ColumnRefiner`. This is a Go-only
authoring seam, not a request field or a new runtime schema gate. A resolver
receives the component/view identities, selected connector, and a detached
resource-expanded `spec.ViewSource`. It returns the complete ordered SQL result
columns derived from a checked-in schema or existing authoritative model:

```go
discovery := transcribe.Discovery{
    BaseDir: root,
    Include: packages,
    PreparedColumns: func(ctx context.Context, in column.PreparedColumnRequest) (*column.PreparedColumnResult, error) {
        // The application validates the exact selected source against its
        // offline schema/model; it must not infer physical facts from DQL CASTs.
        return model.ResultColumns(in.Component, in.Namespace, in.Source)
    },
}
```

The optional `Table` must match a direct physical source proven by the SQL.
Columns use vendor output names and canonical `spec.Column` type/key/constraint
metadata. Authored renames and CASTs retain their normal precedence. Metadata
cannot widen a closed SQL projection or remove annotation targets. The compiler
clones returned columns and preserves runtime SQL, predicates and embedded file
bytes. Partial/absent metadata fails; it never falls back to live discovery.
Database and prepared resolvers are mutually exclusive. Borrowed-source and
factory-composition proofs still require their existing supported path.

Use the normal `Generator{Operation: "get"}` or `"patch"` with the discovery
result. A resolver performs no implicit database access and makes no claim about
the deployed schema. The stock CLI still accepts database discovery only; an
application supplying offline metadata composes the public Go discovery and
operation-based generator APIs. Runtime column-presence compatibility must be
verified separately, including writer field selection and unsupported writes.

## Live discovery

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
