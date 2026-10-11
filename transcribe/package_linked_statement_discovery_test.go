package transcribe

import (
	"context"
	"reflect"
	"testing"

	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/testharness"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/tag"
	"github.com/viant/datly/transcribe/column"
)

type LinkedStatementDiscoveryOutput struct {
	Data []*PackageCompileView `parameter:"Data,kind=output,in=view" view:"Statement" sql:"$dml.Execute(\"UPDATE users SET id = 2\")"`
}

func TestLinkedStatementDiscoveryRetainsIndependentReads(t *testing.T) {
	ctx := context.Background()
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(ctx, "CREATE TABLE users(id INTEGER)", "INSERT INTO users VALUES (1)"); err != nil {
		t.Fatal(err)
	}
	input := reflect.TypeFor[PackageCompileIndependentInput]()
	output := reflect.TypeFor[LinkedStatementDiscoveryOutput]()
	route := &bootstrap.RouteSource{
		FieldName: "Statement", PackagePath: input.PkgPath(),
		InputType: input.Name(), OutputType: output.Name(),
		LinkedInputType: input, LinkedOutputType: output,
		LinkedHandler: func() (rhandler.TypedHandler, error) { return nil, nil },
		Tag:           tag.Component{Name: "Statement", Method: "POST", Path: "/statement", Connector: "main", Handler: "StatementHandler"},
	}
	result, err := (&PackageCompilation{
		Source:    &Source{Scope: input.PkgPath(), Name: "Statement", ColumnRefiner: column.New(column.Connections{"main": h.DB})},
		Component: route, InputType: input, OutputType: output,
	}).Compile(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result.Component.RootView.Source.SQL != "$dml.Execute(\"UPDATE users SET id = 2\")" {
		t.Fatal("authored statement changed")
	}
	if len(result.Component.Views) != 1 || len(result.Component.Views[0].Columns) != 1 || !result.Component.Views[0].Columns[0].Nullable {
		t.Fatalf("independent read metadata missing: %+v", result.Component.Views)
	}
	var id int
	if err := h.DB.QueryRowContext(ctx, "SELECT id FROM users").Scan(&id); err != nil || id != 1 {
		t.Fatalf("discovery executed buffered mutation: id=%d err=%v", id, err)
	}
}
