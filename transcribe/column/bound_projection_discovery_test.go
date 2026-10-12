package column

import (
	"context"
	"reflect"
	"testing"

	"github.com/viant/datly/constant"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
	sqltemplate "github.com/viant/datly/sql/template"
)

func TestRefinerDiscoversBoundConstantProjectionWithoutRows(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	ctx := context.Background()
	if err := h.ExecStatements(ctx, `CREATE TABLE records (id INTEGER)`); err != nil {
		t.Fatal(err)
	}
	input := struct {
		Label string
		Count int
	}{Label: "publisher's constant", Count: 7}
	values, err := constant.New(map[string]string{"Label": input.Label, "Count": "7"})
	if err != nil {
		t.Fatal(err)
	}
	label, count := input.Label, "7"
	authored := `SELECT id, $Label AS label, $Count AS count FROM records WHERE id > $Count`
	component := &spec.Component{
		Settings: &spec.Settings{DefaultConnector: "main"},
		Parameters: []*spec.Parameter{
			{Name: "Label", TypeExpr: "string", Value: &label, Source: spec.BindSource{Kind: "const", Name: "Label"}},
			{Name: "Count", TypeExpr: "int", Value: &count, Source: spec.BindSource{Kind: "const", Name: "Count"}},
		},
		RootView: &spec.View{Name: "Records", Source: &spec.ViewSource{SQL: authored}},
	}
	err = New(Connections{"main": h.DB}).Refine(ctx, component, nil, &TemplateInput{
		Const: values, Value: reflect.ValueOf(input),
		ParameterResolver: func(name string) (any, bool, error) {
			switch name {
			case "Label":
				return input.Label, true, nil
			case "Count":
				return input.Count, true, nil
			}
			return nil, false, nil
		},
		Variables: []sqltemplate.Variable{
			{Name: "Label", FieldIndex: []int{0}, Type: reflect.TypeFor[string]()},
			{Name: "Count", FieldIndex: []int{1}, Type: reflect.TypeFor[int]()},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if component.RootView.Source.SQL != authored {
		t.Fatalf("authored SQL changed: %s", component.RootView.Source.SQL)
	}
	columns := component.RootView.Columns
	if len(columns) != 3 || columns[1].Type.Name != "string" || columns[2].Type.Name != "int" {
		t.Fatalf("bound constant types lost: %+v", columns)
	}
}

func TestRefinerDiscoversQuotedUnsafeConstantProjectionWithoutRows(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	ctx := context.Background()
	if err := h.ExecStatements(ctx, `CREATE TABLE records (id INTEGER)`); err != nil {
		t.Fatal(err)
	}
	input := struct{ Constant string }{Constant: "publisher constant"}
	authored := `SELECT id, '$Unsafe.Constant' AS label FROM records`
	component := &spec.Component{Settings: &spec.Settings{DefaultConnector: "main"}, RootView: &spec.View{Name: "Records", Source: &spec.ViewSource{SQL: authored}}}
	if err := New(Connections{"main": h.DB}).Refine(ctx, component, nil, &TemplateInput{Value: reflect.ValueOf(input)}); err != nil {
		t.Fatal(err)
	}
	if component.RootView.Source.SQL != authored || len(component.RootView.Columns) != 2 || component.RootView.Columns[1].Type.Name != "string" {
		t.Fatalf("authored SQL/type changed: %+v", component.RootView)
	}
	program, err := (sqltemplate.Compiler{Source: authored, InputType: reflect.TypeOf(input)}).Compile()
	if err != nil || program == nil {
		t.Fatalf("compile: %v", err)
	}
	result, err := program.Evaluate(ctx, sqltemplate.Invocation{Input: reflect.ValueOf(input)})
	if err != nil || result.SQL != `SELECT id, 'publisher constant' AS label FROM records` || len(result.Args) != 0 {
		t.Fatalf("constant emission: %+v %v", result, err)
	}
}
