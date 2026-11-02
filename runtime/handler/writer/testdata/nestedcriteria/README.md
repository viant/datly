# Stock-generated nested mutation criteria fixture

The matching DQL is `dql/test/nestedcriteria/component.dql`. The schema is
`runtime/handler/writer/testdata/nestedcriteria/schema.sql`. Only `lifecycle.go`
in the generated package is authored; the remaining Go/SQL artifacts are stock
transcribe output and must not be edited by hand.

From a Datly module checkout with SQLite available:

```sh
sqlite3 /tmp/datly-nested-guards-schema.db < runtime/handler/writer/testdata/nestedcriteria/schema.sql
go run ./cmd/datly transcribe put -dir . -schema -connector main -driver sqlite3 -dsn /tmp/datly-nested-guards-schema.db github.com/viant/datly/dql/test/nestedcriteria
```

`TestStockGeneratedNestedMutationPredicateNativeRollback` uses the actual generated
holder, canonical component bootstrap, generated Current reads, and native owner
DML. A test-only trigger changes persisted child evidence after Previous but before
the child update. A native conflict must roll back ledger, document, child, and
trigger effects. Separate cases verify root state/hash conditions, successful
commit, and guarded explicit child deletion. The fixture is synthetic and has no
public route registration outside its isolated test runtime.
