package transcribe

import (
	"reflect"
	"testing"

	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/tag"
	"github.com/viant/datly/transcribe/column"
)

type LinkedIdentifierDiscoveryInput struct {
	HistoryTable string `parameter:"HistoryTable,kind=const,in=historyTable,dataType=string,value=project.history"`
}

type LinkedIdentifierDiscoveryOutput struct {
	Data []*PackageCompileView "parameter:\"Data,kind=output,in=view\" view:\"History\" sql:\"SELECT id FROM `$Unsafe.HistoryTable`\""
}

func TestLinkedIdentifierDiscoveryUsesInputConstantDefault(t *testing.T) {
	ctx := t.Context()
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(ctx, "CREATE TABLE `project.history`(id INTEGER)"); err != nil {
		t.Fatal(err)
	}
	input, output := reflect.TypeFor[LinkedIdentifierDiscoveryInput](), reflect.TypeFor[LinkedIdentifierDiscoveryOutput]()
	route := &bootstrap.RouteSource{FieldName: "History", PackagePath: input.PkgPath(), InputType: input.Name(), OutputType: output.Name(), LinkedInputType: input, LinkedOutputType: output, Tag: tag.Component{Name: "History", Method: "GET", Path: "/history", Connector: "main"}}
	result, err := (&PackageCompilation{Source: &Source{Scope: input.PkgPath(), Name: "History", ColumnRefiner: column.New(column.Connections{"main": h.DB})}, Component: route, InputType: input, OutputType: output}).Compile(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result.Component.RootView.Source.SQL != "SELECT id FROM `$Unsafe.HistoryTable`" {
		t.Fatal("authored SQL changed")
	}
	if columns := result.Component.RootView.Columns; len(columns) != 1 || columns[0].Name != "ID" || columns[0].Source != "id" || !columns[0].Nullable {
		t.Fatalf("linked metadata: %+v", columns)
	}
}
