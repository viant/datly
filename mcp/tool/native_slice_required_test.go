package tool

import (
	"reflect"
	"testing"

	"github.com/viant/bindly"
	bindstate "github.com/viant/bindly/state"
	"github.com/viant/datly/spec"
)

func TestNativeSliceSchemaRequiredTag(t *testing.T) {
	for _, tc := range []struct {
		name, kind, in, tag string
		want                bool
	}{
		{"explicit whole body", "body", "", `mcp:"name=Ids" required:"true"`, true},
		{"generated optional whole body with schema requirement", "body", "", `mcp:"name=Ids" parameter:"Body,kind=body,in=,required=false" required:"true"`, true},
		{"untagged whole body", "body", "", `mcp:"name=Ids"`, false},
		{"false whole body", "body", "", `mcp:"name=Ids" required:"false"`, false},
		{"explicit query schema", "query", "ids", `mcp:"name=Ids" required:"true"`, true},
		{"explicit named body schema", "body", "ids", `mcp:"name=Ids" required:"true"`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			typ := reflect.StructOf([]reflect.StructField{{Name: "Body", Type: reflect.TypeFor[*[]int](), Tag: reflect.StructTag(tc.tag)}})
			optional := false
			binding := bindly.BindingSpec{Path: "Body", Name: "Body", Location: bindstate.Location{Kind: tc.kind, In: tc.in}, Required: &optional, Extension: &spec.Parameter{Name: "Body", Tag: tc.tag}}
			contract := testRouteContract(t, typ, []bindly.BindingSpec{binding})
			plan, err := NewCompiler().Compile(Input{Component: spec.Key{Kind: spec.KindComponent, Scope: "test", Name: "Array"}, Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "array"}, Contract: contract})
			if err != nil {
				t.Fatal(err)
			}
			required := false
			for _, name := range plan.Metadata().InputSchema.Required {
				if name == "Ids" {
					required = true
				}
			}
			if required != tc.want {
				t.Fatalf("required=%v want %v", required, tc.want)
			}
			if plan.args[0].required {
				t.Fatal("schema tag changed runtime requiredness")
			}
			if tc.kind == "body" && tc.in == "" {
				for _, args := range []map[string]any{{}, {"Ids": nil}, {"Ids": []any{}}, {"Ids": []any{float64(1), float64(2)}}} {
					scope, err := plan.Scope(args)
					if err != nil {
						t.Fatal(err)
					}
					_, _, err = scope.Body().Locate(nil).Value(t.Context(), reflect.TypeFor[[]int](), "")
					scope.Close()
					if err != nil {
						t.Fatal(err)
					}
				}
			}
		})
	}
}
