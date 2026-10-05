package column

import (
	"context"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
	"testing"
	"testing/fstest"
)

func TestRefinerEmbeddedAuxiliaryRetainsPrimarySourceAndIdentity(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	ctx := context.Background()
	if err := h.ExecStatements(ctx, `CREATE TABLE labels(id TEXT PRIMARY KEY, resource_id TEXT)`, `CREATE TABLE resources(id TEXT PRIMARY KEY, tenant_id TEXT)`); err != nil {
		t.Fatal(err)
	}
	const token = "${embed:sql/labels.sql}"
	const authored = "SELECT c.* FROM (" + token + ") c"
	view := &spec.View{Name: "Labels", Source: &spec.ViewSource{SQL: authored, Embeds: []*spec.EmbeddedSQLRef{{Path: "sql/labels.sql", Raw: token}}}}
	component := &spec.Component{Settings: &spec.Settings{DefaultConnector: "main"}, RootView: view}
	assets := fstest.MapFS{"sql/labels.sql": &fstest.MapFile{Data: []byte(`SELECT l.id,r.tenant_id FROM (labels) l JOIN resources r ON r.id=l.resource_id`)}}
	if err := New(Connections{"main": h.DB}).Refine(ctx, component, assets, nil); err != nil {
		t.Fatal(err)
	}
	if !view.Auxiliary || view.Source.Table != "labels" {
		t.Fatalf("auxiliary=%v source=%+v", view.Auxiliary, view.Source)
	}
	if view.Source.SQL != authored {
		t.Fatal("resource was flattened")
	}
	found := false
	for _, column := range view.Columns {
		if column.Name == "id" {
			found = true
			if !column.PrimaryKey {
				t.Fatalf("identity lost: %+v", column)
			}
		}
	}
	if !found {
		t.Fatal("id not discovered")
	}
}

func TestExplicitAuxiliarySourceDoesNotInferFromJoinsOrUnion(t *testing.T) {
	for _, tc := range []struct{ sql, want string }{
		{`SELECT l.id FROM (labels) l`, "labels"},
		{`SELECT x.* FROM (SELECT l.id FROM (labels) l) x`, "labels"},
		{`SELECT l.id FROM labels l JOIN (resources) r ON r.id=l.resource_id`, ""},
		{`SELECT id FROM (labels) UNION ALL SELECT id FROM (labels)`, ""},
		{`SELECT x.* FROM (SELECT id FROM (labels) UNION ALL SELECT id FROM (labels)) x`, ""},
		{`WITH labels AS (SELECT id FROM resources) SELECT id FROM labels`, ""},
	} {
		if got := explicitAuxiliarySource(tc.sql); got != tc.want {
			t.Fatalf("%s: got %q want %q", tc.sql, got, tc.want)
		}
	}
}
