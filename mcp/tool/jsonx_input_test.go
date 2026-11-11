package tool

import (
	"encoding/json"
	"github.com/viant/bindly"
	bindstate "github.com/viant/bindly/state"
	"github.com/viant/datly/spec"
	"github.com/xeipuuv/gojsonschema"
	"reflect"
	"strings"
	"testing"
)

type jsonxInputRow struct {
	Flag   *int                         `json:"flag" jsonx:"boolToInt,null=omit"`
	Secret *int                         `json:"secret" internal:"true"`
	Has    *struct{ Flag, Secret bool } `json:"-" setMarker:"true"`
}
type jsonxToolInput struct {
	Body *jsonxInputRow `parameter:"Body,kind=body"`
}

func TestJSONXToolNativeBinding(t *testing.T) {
	binding := bindly.BindingSpec{Path: "Body", Name: "Body", Location: bindstate.Location{Kind: "body"}}
	contract := testRouteContract(t, reflect.TypeFor[jsonxToolInput](), []bindly.BindingSpec{binding})
	plan, err := NewCompiler().Compile(Input{Component: spec.Key{Kind: spec.KindComponent, Scope: "test", Name: "JSONX"}, Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "jsonx"}, Contract: contract})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		value    any
		expected int
		present  bool
	}{{true, 1, true}, {false, 0, true}, {2, 2, true}, {-1, -1, true}, {nil, 0, false}} {
		scope, err := plan.Scope(map[string]any{"body": map[string]any{"flag": tc.value, "secret": 8}})
		if err != nil {
			t.Fatal(err)
		}
		value, _, err := scope.Body().Locate(nil).Value(t.Context(), reflect.TypeFor[*jsonxInputRow](), "")
		scope.Close()
		if err != nil {
			t.Fatal(err)
		}
		row := value.(*jsonxInputRow)
		if (row.Flag != nil) != tc.present || row.Flag != nil && *row.Flag != tc.expected || row.Has == nil || row.Has.Flag != tc.present || row.Secret != nil {
			t.Fatalf("wrong row %+v", row)
		}
	}
}
func TestJSONXToolInputSchemaIncludesBoolean(t *testing.T) {
	binding := bindly.BindingSpec{Path: "Body", Name: "Body", Location: bindstate.Location{Kind: "body"}}
	contract := testRouteContract(t, reflect.TypeFor[jsonxToolInput](), []bindly.BindingSpec{binding})
	plan, err := NewCompiler().Compile(Input{Component: spec.Key{Kind: spec.KindComponent, Scope: "test", Name: "JSONX"}, Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "jsonx"}, Contract: contract})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(plan.Metadata().InputSchema)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"boolean"`) {
		t.Fatalf("input schema omits accepted boolean representation: %s", raw)
	}
}

func TestJSONXSchemaProjectionBoundaries(t *testing.T) {
	type row struct {
		Flag    *int `json:"flag" jsonx:"boolToInt,null=omit"`
		Plain   *int `json:"plain"`
		Hidden  *int `json:"hidden" internal:"true" jsonx:"boolToInt"`
		Private *int `json:"private" mcp:"-" jsonx:"boolToInt"`
	}
	type body struct {
		Rows []row `json:"rows"`
	}
	for _, native := range []bool{false, true} {
		projected, err := (&schemaProjector{nativeBody: native}).argument(reflect.TypeFor[body](), "Body", "Body", nil)
		if err != nil {
			t.Fatal(err)
		}
		_, flag, _ := schemaMember(projected, []string{"rows", "flag"})
		raw, _ := json.Marshal(flag["type"])
		if native {
			validator, err := gojsonschema.NewSchema(gojsonschema.NewGoLoader(flag))
			if err != nil {
				t.Fatal(err)
			}
			for _, value := range []any{true, false, 17, nil} {
				result, err := validator.Validate(gojsonschema.NewGoLoader(value))
				if err != nil || !result.Valid() {
					t.Fatalf("native value %v rejected: %v", value, err)
				}
			}
		} else if string(raw) != `["integer","null"]` {
			t.Fatalf("generic types changed: %s", raw)
		}
		_, plain, _ := schemaMember(projected, []string{"rows", "plain"})
		raw, _ = json.Marshal(plain["type"])
		if string(raw) != `["integer","null"]` {
			t.Fatalf("untagged field changed: %s", raw)
		}
		for _, name := range []string{"hidden", "private"} {
			_, hidden, _ := schemaMember(projected, []string{"rows", name})
			if hidden != nil {
				t.Fatalf("exposed %s", name)
			}
		}
	}
}

func TestJSONXOutputSchemaUnchanged(t *testing.T) {
	projected, err := outputContractSchema(reflect.TypeFor[jsonxInputRow](), nil)
	if err != nil {
		t.Fatal(err)
	}
	if projected == nil {
		t.Fatal("missing typed output schema")
	}
	raw, err := json.Marshal(projected)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), `"boolean"`) {
		t.Fatalf("input alternatives leaked into output: %s", raw)
	}
	if !strings.Contains(string(raw), `"integer"`) {
		t.Fatalf("integer output lost: %s", raw)
	}
}
