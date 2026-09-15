package column

import (
	"context"
	"strings"
	"testing"

	"github.com/viant/datly/constant"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
)

func TestInstanceIdentityAugmentationKeepsUnexpandedSource(t *testing.T) {
	h := sqlite.New(t)
	if err := h.ExecStatements(context.Background(), "CREATE TABLE records (id INTEGER PRIMARY KEY,name TEXT)", "CREATE TABLE `e2e.filter` (id INTEGER)"); err != nil {
		t.Fatal(err)
	}
	values, _ := constant.New(map[string]string{"project": "e2e"})
	authored := "SELECT r.name FROM (SELECT id,name FROM records) r WHERE r.id IN (SELECT id FROM `$project.filter`)"
	component := &spec.Component{Settings: &spec.Settings{DefaultConnector: "main"}, RootView: &spec.View{Name: "Records", Namespace: "r", Source: &spec.ViewSource{SQL: authored, Table: "records"}}}
	if err := New(Connections{"main": h.DB}).Refine(context.Background(), component, nil, &TemplateInput{Const: values}); err != nil {
		t.Fatal(err)
	}
	if len(component.RootView.Columns) != 2 {
		t.Fatalf("identity was not retained: %+v", component.RootView.Columns)
	}
	source := component.RootView.Source.SQL
	if !strings.Contains(source, "$project.filter") || strings.Contains(source, "e2e.filter") {
		t.Fatalf("identity augmentation persisted expanded SQL: %q", source)
	}
	if component.RootView.Source.Table != "records" {
		t.Fatal("table metadata was expanded persistently")
	}
}
