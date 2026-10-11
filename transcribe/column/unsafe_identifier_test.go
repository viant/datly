package column

import (
	"context"
	"reflect"
	"testing"

	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
)

func TestRefinerUnsafeQuotedIdentifierPreservesAuthoredSQL(t *testing.T) {
	h := sqlite.New(t)
	ctx := context.Background()
	if err := h.ExecStatements(ctx, "CREATE TABLE `project.history` (campaignId INTEGER NOT NULL, frequencyTargetProportion REAL)"); err != nil {
		t.Fatal(err)
	}
	type input struct{ HistoryTable string }
	authored := "SELECT history.* FROM (SELECT campaignId, frequencyTargetProportion FROM `$Unsafe.HistoryTable`) history"
	component := &spec.Component{Settings: &spec.Settings{DefaultConnector: "main"}, RootView: &spec.View{Name: "History", Source: &spec.ViewSource{SQL: authored}}}
	if err := New(Connections{"main": h.DB}).Refine(ctx, component, nil, &TemplateInput{Value: reflect.ValueOf(input{HistoryTable: "project.history"})}); err != nil {
		t.Fatal(err)
	}
	columns := component.RootView.Columns
	if len(columns) != 2 || columns[0].Name != "campaignId" || columns[1].Name != "frequencyTargetProportion" {
		t.Fatalf("discovered columns: %+v", columns)
	}
	if component.RootView.Source.SQL != authored {
		t.Fatalf("authored SQL changed: %q", component.RootView.Source.SQL)
	}
}
