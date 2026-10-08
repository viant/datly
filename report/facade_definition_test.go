package report

import (
	handlercompiler "github.com/viant/datly/runtime/handler/compiler"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	"reflect"
	"testing"
)

// A persisted facade must bind by field identity, not runtime struct offsets.
func TestFacadeDefinitionRebindsLinkedShape(t *testing.T) {
	type dynamic struct {
		Dimensions struct{ Account bool }
		Measures   struct{ Spend bool }
		Filters    struct{ Active *bool }
		OrderBy    []string
		Limit      *int
		Offset     *int
	}
	type linked struct {
		Offset     *int
		Limit      *int
		OrderBy    []string
		Filters    struct{ Active *bool }
		Measures   struct{ Spend bool }
		Dimensions struct{ Account bool }
	}
	plan := &Plan{view: "rows", inputType: reflect.TypeFor[dynamic](), outputType: reflect.TypeFor[struct{}](), dimensions: []selection{{name: "account_id", index: []int{0, 0}}}, measures: []selection{{name: "spend", index: []int{1, 0}}}, orderIndex: []int{3}, limitIndex: []int{4}, offsetIndex: []int{5}, holderByName: map[string][]string{"account_id": {"Account"}}}
	definition, err := plan.Definition()
	if err != nil {
		t.Fatal(err)
	}
	rebound, err := definition.Compile(reflect.TypeFor[linked](), plan.outputType)
	if err != nil {
		t.Fatal(err)
	}
	input := linked{}
	input.Dimensions.Account = true
	input.Measures.Spend = true
	fields, err := rebound.selectedFields(reflect.ValueOf(input))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(fields, []string{"account_id", "Account", "spend"}) {
		t.Fatalf("%v", fields)
	}
	definition.Relations[0].Dimensions[0] = "changed"
	fields, err = rebound.selectedFields(reflect.ValueOf(input))
	if err != nil || !reflect.DeepEqual(fields, []string{"account_id", "Account", "spend"}) {
		t.Fatalf("definition mutation leaked: %v %v", fields, err)
	}
}

func TestDynamicCubeTypesStayInOwnerScope(t *testing.T) {
	var sources []Source
	for _, scope := range []string{"example.com/studio/report/one/v1", "example.com/studio/report/two/v1", "example.com/studio/report/one/v2"} {
		source := reportSource(t, &spec.ReportSettings{Enabled: true})
		source.Component.Key.Scope = scope
		source.Component.TypeContext.DefaultPackage = "example.com/shared/imports"
		source.Component.Routes[0].Path = "/" + scope
		input, err := handlercompiler.New(handlercompiler.Input{Component: source.Component, InputType: reflect.TypeFor[reportSourceInput]()}).Compile()
		if err != nil {
			t.Fatal(err)
		}
		source.Input = input.Input
		sources = append(sources, source)
	}
	project, err := NewProjectCompiler(ProjectConfig{Types: typecatalog.NewCatalog()}).Compile(sources)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, cube := range project.Derived() {
		if cube.Type.PkgPath != cube.Plan.Target().Component.Scope+"/_datly_cube" || seen[cube.Type.Key()] {
			t.Fatalf("cube package collision: %s", cube.Type.Key())
		}
		seen[cube.Type.Key()] = true
	}
	if len(seen) != 3 {
		t.Fatal("lost a versioned cube")
	}
}

func TestDynamicCubeDoesNotShadowLinkedGoInput(t *testing.T) {
	source := reportSource(t, &spec.ReportSettings{Enabled: true})
	types := typecatalog.NewCatalog()
	linked := &x.Type{PkgPath: source.Component.Key.Scope, Name: "SpendCubeInput", Type: reflect.TypeFor[struct{ Existing string }]()}
	if err := types.Register(typecatalog.TypeOriginPackage, linked); err != nil {
		t.Fatal(err)
	}
	project, err := NewProjectCompiler(ProjectConfig{Types: types}).Compile([]Source{source})
	if err != nil {
		t.Fatal(err)
	}
	cube := project.Derived()[0]
	if cube.Type.Key() == linked.Key() {
		t.Fatal("dynamic cube reuses linked Go type identity")
	}
	got, ok, err := types.Resolve(typecatalog.PackageAuthority, linked.Key())
	if err != nil || !ok || got.Type != linked.Type {
		t.Fatal("linked type overwritten")
	}
}
