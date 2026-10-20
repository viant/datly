package tool

import (
	"reflect"
	"testing"

	"github.com/viant/bindly"
	bindstate "github.com/viant/bindly/state"
	"github.com/viant/datly/spec"
)

// Discovery must retain explicit requiredness even where legacy inference treats
// pointers, query parameters and slices as optional. Aliases share the canonical
// required argument; discovery advertises it only once.
func TestCompilerAdvertisesExplicitRequiredNamedArguments(t *testing.T) {
	type input struct {
		Value *string `json:"value,omitempty" mcp:"name=Value,aliases=value"`
	}
	type sliceInput struct {
		Value []string `json:"value,omitempty" mcp:"name=Value,aliases=value"`
	}
	for _, shape := range []struct {
		name  string
		typ   reflect.Type
		value interface{}
	}{
		{"pointer", reflect.TypeFor[input](), "provided"},
		{"slice", reflect.TypeFor[sliceInput](), []string{"provided"}},
	} {
		for _, kind := range []string{"query", "path", "header", "cookie", "form", "body"} {
			t.Run(shape.name+"/"+kind, func(t *testing.T) {
				required := true
				contract := testRouteContract(t, shape.typ, []bindly.BindingSpec{
					{Path: "Value", Name: "Value", Location: bindstate.Location{Kind: kind, In: "value"}, Required: &required},
				})
				plan, err := NewCompiler().Compile(Input{Contract: contract, Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "required"}})
				if err != nil {
					t.Fatal(err)
				}
				metadata := plan.Metadata().InputSchema
				if !reflect.DeepEqual(metadata.Required, []string{"Value"}) || len(metadata.Properties) != 1 || metadata.Properties["Value"] == nil {
					t.Fatalf("schema = %+v", metadata)
				}
				if _, err := plan.Scope(nil); err == nil {
					t.Fatal("missing required argument accepted")
				}
				for _, name := range []string{"Value", "value"} {
					scope, err := plan.Scope(map[string]interface{}{name: shape.value})
					if err != nil {
						t.Fatalf("%s: %v", name, err)
					}
					scope.Close()
				}
			})
		}
	}
}
