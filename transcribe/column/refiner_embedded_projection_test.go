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

func TestRefinerNestedEmbeddedSourcesKeepOriginalSQLAndAliases(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	ctx := context.Background()
	if err := h.ExecStatements(ctx, `CREATE TABLE records(id INTEGER PRIMARY KEY,status TEXT NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	const authored = "SELECT c.* FROM (${embed:creative/creative.sql}) c"
	const creative = "SELECT cr.id,cr.status FROM records cr JOIN (${embed:ids.sql}) ids ON ids.id=cr.id"
	resources := fstest.MapFS{"creative/creative.sql": {Data: []byte(creative)}, "creative/ids.sql": {Data: []byte("SELECT id FROM records")}}
	embedded := &spec.Component{Settings: &spec.Settings{DefaultConnector: "main"}, RootView: &spec.View{Name: "Creative", Source: &spec.ViewSource{SQL: authored, Embeds: []*spec.EmbeddedSQLRef{{Path: "creative/creative.sql", Raw: "${embed:creative/creative.sql}"}}}}}
	inline := embedded.Clone()
	inline.RootView.Source = &spec.ViewSource{SQL: "SELECT c.* FROM (SELECT cr.id,cr.status FROM records cr JOIN (SELECT id FROM records) ids ON ids.id=cr.id) c"}
	refiner := New(Connections{"main": h.DB})
	if err := refiner.Refine(ctx, inline, nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := refiner.Refine(ctx, embedded, resources, nil); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(inline.RootView.Columns, embedded.RootView.Columns) {
		t.Fatalf("nested columns differ: %#v / %#v", inline.RootView.Columns, embedded.RootView.Columns)
	}
	if embedded.RootView.Source.SQL != authored || len(embedded.RootView.Source.Embeds) != 1 {
		t.Fatal("discovery rewrote authored SQL")
	}
	if string(resources["creative/creative.sql"].Data) != creative {
		t.Fatal("discovery rewrote source resource")
	}
}
