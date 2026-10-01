package column

import (
	"context"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
	"testing"
	"testing/fstest"
)

func TestDirectSourceTableTuplePredicates(t *testing.T) {
	for _, tc := range []struct{ sql, want string }{
		{`SELECT r.* FROM records r ORDER BY CASE WHEN r.status NOT IN ('done','failed') THEN r.id END`, "records"},
		{`SELECT x.* FROM (SELECT r.* FROM records r) x ORDER BY CASE WHEN x.status NOT IN ('done','failed') THEN x.id END`, "records"},
		{`SELECT x.id,x.code FROM (SELECT r.id,d.code FROM records r JOIN details d ON d.record_id=r.id) x`, ""},
		{`SELECT r.* FROM records r JOIN details d ON d.record_id=r.id ORDER BY CASE WHEN r.status NOT IN ('done','failed') THEN r.id END`, "records"},
		{`SELECT r.id FROM records r UNION SELECT r.id FROM records r`, ""},
		{`SELECT x.* FROM (SELECT r.id FROM records r UNION SELECT r.id FROM records r) x`, ""},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			if got := directSourceTable(tc.sql); got != tc.want {
				t.Fatalf("physical source=%q, want %q", got, tc.want)
			}
		})
	}
}

func TestRefinerEmbeddedTupleOrderingPreservesSource(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(), `CREATE TABLE records(id INTEGER PRIMARY KEY,status TEXT)`); err != nil {
		t.Fatal(err)
	}
	const token = "${embed:sql/records.sql}"
	const authored = "SELECT x.* FROM (" + token + ") x ORDER BY CASE WHEN x.status NOT IN ('done','failed') THEN x.id END"
	view := &spec.View{Name: "Records", Source: &spec.ViewSource{SQL: authored, Embeds: []*spec.EmbeddedSQLRef{{Path: "sql/records.sql", Raw: token}}}}
	component := &spec.Component{Settings: &spec.Settings{DefaultConnector: "main"}, RootView: view}
	resources := fstest.MapFS{"sql/records.sql": &fstest.MapFile{Data: []byte("SELECT r.id,r.status FROM records r")}}
	if err := New(Connections{"main": h.DB}).Refine(context.Background(), component, resources, nil); err != nil {
		t.Fatal(err)
	}
	if view.Source.SQL != authored || view.Source.Table != "records" || len(view.Columns) != 2 || len(view.Source.Embeds) != 1 {
		t.Fatalf("source/metadata changed: %+v / %+v", view.Source, view.Columns)
	}
}
