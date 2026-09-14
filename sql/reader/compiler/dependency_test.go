package compiler

import (
	"reflect"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/viant/datly/spec"
)

func TestCompileViewDependenciesBuildsTypedIndependentPlan(t *testing.T) {
	type row struct{ ID int }
	type input struct{ Rows []*row }
	component := &spec.Component{
		Name: "ReadRows",
		Parameters: []*spec.Parameter{
			{Name: "Rows", Source: spec.BindSource{Kind: "view", Name: "Rows"}},
			{Name: "Summary", Source: spec.BindSource{Kind: "output", Name: "summary"}},
		},
		Views: []*spec.View{{Name: "Rows", Source: &spec.ViewSource{SQL: "#if(true) SELECT id FROM rows #end"}}},
	}
	bindings, err := testBindings(component, reflect.TypeOf(input{}))
	if err != nil {
		t.Fatalf("testBindings() error = %v", err)
	}
	actual, err := CompileViewDependencies(Input{
		Component: component, InputType: reflect.TypeOf(input{}), Bindings: bindings,
	})
	if err != nil {
		t.Fatalf("CompileViewDependencies() error = %v", err)
	}
	if len(actual) != 1 || actual[0].Name != "Rows" || actual[0].TargetType != reflect.TypeOf([]*row{}) {
		t.Fatalf("unexpected view bindings: %#v", actual)
	}
	if actual[0].Plan == nil || actual[0].Plan.Root == nil || actual[0].Plan.Root.View == nil || !actual[0].Plan.DirectOutput || actual[0].Plan.OutputViewField != "" {
		t.Fatalf("independent reader plan was not compiled: %#v", actual[0].Plan)
	}
	if actual[0].Plan.Root.Template == nil {
		t.Fatal("independent SQL template was not compiled")
	}
	if len(actual[0].Component.Parameters) != 1 || actual[0].Component.Parameters[0].Name != "Rows" {
		t.Fatalf("output-only params leaked into independent component: %#v", actual[0].Component.Parameters)
	}
	if len(component.Parameters) != 2 {
		t.Fatal("canonical component was mutated")
	}
}

func TestCompileViewDependenciesResolvesEmbeddedSQL(t *testing.T) {
	type row struct{ ID int }
	type input struct{ Rows []*row }
	component := &spec.Component{
		Parameters: []*spec.Parameter{{Name: "Rows", Source: spec.BindSource{Kind: "view", Name: "Rows"}}},
		Views:      []*spec.View{{Name: "Rows", Source: &spec.ViewSource{URI: "rows.sql"}}},
	}
	bindings, err := testBindings(component, reflect.TypeOf(input{}))
	if err != nil {
		t.Fatal(err)
	}
	actual, err := CompileViewDependencies(Input{
		Component: component, InputType: reflect.TypeOf(input{}), Bindings: bindings,
		Resources: fstest.MapFS{"rows.sql": {Data: []byte("SELECT id FROM rows")}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(actual) != 1 || actual[0].Plan == nil || actual[0].Plan.Root == nil || actual[0].Plan.Root.View == nil ||
		actual[0].Plan.Root.View.Spec.Source.SQL != "SELECT id FROM rows" || len(actual[0].Plan.Root.View.Spec.Source.Embeds) != 0 {
		t.Fatalf("dependencies = %#v", actual)
	}
	if component.Views[0].Source.SQL != "" || component.Views[0].Source.URI != "rows.sql" || len(component.Views[0].Source.Embeds) != 0 {
		t.Fatal("dependency compilation mutated canonical metadata")
	}
}

func TestCompileViewDependenciesRejectsUnknownView(t *testing.T) {
	type input struct {
		Rows []struct{ ID int }
	}
	component := &spec.Component{
		Parameters: []*spec.Parameter{{Name: "Rows", Source: spec.BindSource{Kind: "view", Name: "Missing"}}},
		Views:      []*spec.View{{Name: "Rows", Source: &spec.ViewSource{SQL: "SELECT id FROM rows"}}},
	}
	bindings, err := testBindings(component, reflect.TypeOf(input{}))
	if err != nil {
		t.Fatalf("testBindings() error = %v", err)
	}
	_, err = CompileViewDependencies(Input{Component: component, InputType: reflect.TypeOf(input{}), Bindings: bindings})
	if err == nil || !strings.Contains(err.Error(), `unknown independent view "Missing"`) {
		t.Fatalf("expected unknown view error, got %v", err)
	}
}
