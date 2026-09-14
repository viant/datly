package golang

import (
	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
	"testing"
)

func TestTargetTypeReferenceUsesCanonicalImports(t *testing.T) {
	for _, tc := range []struct {
		name string
		ref  spec.TypeRef
		want string
	}{
		{"map", spec.TypeRef{Name: "map[string][]int"}, "map[string][]int"},
		{"pointer metadata", spec.TypeRef{Name: "int64", Pointer: true}, "*int64"},
		{"pointer normalized", spec.TypeRef{Name: "*int64", Pointer: true}, "*int64"},
		{"pointer map", spec.TypeRef{Name: "map[string]int", Pointer: true}, "*map[string]int"},
		{"collection metadata", spec.TypeRef{Name: "Row", Package: "example.com/model", Pointer: true, Cardinality: spec.CardinalityMany}, "[]*model.Row"},
		{"foreign qualified nested", spec.TypeRef{Name: "map[string][]example.com/model.Row"}, "map[string][]model.Row"},
		{"local", spec.TypeRef{Name: "Row", Package: "example.com/local"}, "Row"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			semantic := rootSemanticPlan(plan.OperationPost, false)
			l := &lowerer{plan: semantic, config: Config{Package: "events", PackagePath: "example.com/local", Factory: "NewEventsHandler", InputType: "Input", OutputType: "Output", Records: rootRecordTypes(semantic, "[]*Record", ""), Imports: []spec.ImportSpec{{Alias: "model", Package: "example.com/model"}}}}
			if err := l.prepare(); err != nil {
				t.Fatal(err)
			}
			expression, err := l.typeReference(tc.ref)
			if err != nil {
				t.Fatal(err)
			}
			got, err := renderExpr(expression)
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Fatalf("got%s want%s", got, tc.want)
			}
		})
	}
}
