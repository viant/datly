package column

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
)

func TestRefinerEmbeddedWildcardKeepsAuthoredProjection(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(), `CREATE TABLE records(id INTEGER PRIMARY KEY,status TEXT NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string]string{
		"closed SQL":         `SELECT r.id,r.status FROM records r`,
		"predicate template": `SELECT r.* FROM records r ${predicate.Builder().CombineAnd($predicate.FilterGroup(0, "AND")).Build("WHERE")}`,
	} {
		t.Run(name, func(t *testing.T) {
			const token = "${embed:sql/records.sql}"
			const authored = "SELECT records.* FROM (" + token + ") records"
			makeComponent := func(sql string, refs []*spec.EmbeddedSQLRef) *spec.Component {
				return &spec.Component{Settings: &spec.Settings{DefaultConnector: "main"}, RootView: &spec.View{Name: "Records", RowLock: "records r", RowLockOrder: "r.id", Source: &spec.ViewSource{SQL: sql, Embeds: refs}}}
			}
			inline := makeComponent(strings.ReplaceAll(authored, token, body), nil)
			embedded := makeComponent(authored, []*spec.EmbeddedSQLRef{{Path: "sql/records.sql", Raw: token}})
			resources := fstest.MapFS{"sql/records.sql": &fstest.MapFile{Data: []byte(body)}}
			refiner := New(Connections{"main": h.DB})
			if err := refiner.Refine(context.Background(), inline, nil, nil); err != nil {
				t.Fatal(err)
			}
			if err := refiner.Refine(context.Background(), embedded, resources, nil); err != nil {
				t.Fatal(err)
			}
			if embedded.RootView.Source.SQL != authored {
				t.Fatalf("resource expansion rewrote authored wildcard: %s", embedded.RootView.Source.SQL)
			}
			if len(embedded.RootView.Source.Embeds) != 1 {
				t.Fatal("source embed ownership was lost")
			}
			if !reflect.DeepEqual(inline.RootView.Columns, embedded.RootView.Columns) {
				t.Fatalf("inline and embedded columns differ: %#v / %#v", inline.RootView.Columns, embedded.RootView.Columns)
			}
			if embedded.RootView.RowLock != "records r" || embedded.RootView.RowLockOrder != "r.id" {
				t.Fatal("row lock capability changed")
			}
		})
	}
}
