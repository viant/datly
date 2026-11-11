package tool

import (
	"github.com/viant/bindly"
	bindstate "github.com/viant/bindly/state"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/xeipuuv/gojsonschema"
	"reflect"
	"testing"
)

type scalarAliasRow struct {
	Mode *string `json:"mode" choice:"0:ASAP" choice:"1:SPEND_EVENLY" enumAccept:"scalar" enumStyle:"literal" enumAlias:"0:0" enumAlias:"1:1" enumAlias:"1:Even" enumCase:"fold"`
}
type scalarAliasBody struct {
	Rows []*scalarAliasRow `json:"rows"`
}
type scalarAliasInput struct {
	Body *scalarAliasBody `parameter:"Body,kind=body"`
}

func TestScalarAliasNestedMCPContract(t *testing.T) {
	binding := bindly.BindingSpec{Path: "Body", Name: "Body", Location: bindstate.Location{Kind: "body"}}
	contract := testRouteContract(t, reflect.TypeFor[scalarAliasInput](), []bindly.BindingSpec{binding})
	plan, err := NewCompiler().Compile(Input{Component: spec.Key{Kind: spec.KindComponent, Scope: "test", Name: "Aliases"}, Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "aliases"}, Contract: contract})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name  string
		value any
		valid bool
		want  string
	}{
		{"integer", 1, true, "SPEND_EVENLY"}, {"numeric string", "0", true, "ASAP"}, {"alias", "eVeN", true, "SPEND_EVENLY"}, {"unicode", "ſPEND_EVENLY", true, "SPEND_EVENLY"}, {"null", nil, true, ""}, {"unknown", 2, false, ""}, {"unknown string", "bad", false, ""}, {"newline", "Even\n", false, ""}, {"boolean", true, false, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			args := map[string]any{"body": map[string]any{"rows": []any{map[string]any{"mode": tc.value}}}}
			result, err := gojsonschema.Validate(gojsonschema.NewGoLoader(plan.Metadata().InputSchema), gojsonschema.NewGoLoader(args))
			if err != nil {
				t.Fatal(err)
			}
			if result.Valid() != tc.valid {
				t.Errorf("schema valid=%v want %v: %v", result.Valid(), tc.valid, result.Errors())
			}
			scope, err := plan.Scope(args)
			if err != nil {
				if tc.valid {
					t.Fatal(err)
				}
				return
			}
			defer scope.Close()
			value, _, err := scope.Body().Locate(nil).Value(t.Context(), reflect.TypeFor[*scalarAliasBody](), "")
			if !tc.valid {
				if err == nil {
					t.Error("invalid alias bound")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			mode := value.(*scalarAliasBody).Rows[0].Mode
			if tc.value == nil {
				if mode != nil {
					t.Fatal("null changed")
				}
			} else if mode == nil || *mode != tc.want {
				t.Fatalf("mode=%v want %s", mode, tc.want)
			}
		})
	}
}

type scalarAliasTopInput struct {
	Mode *string `parameter:"Mode,kind=body" choice:"0:ASAP" choice:"1:SPEND_EVENLY" enumAccept:"scalar" enumStyle:"literal" enumAlias:"0:0" enumAlias:"1:1" enumAlias:"1:Even" enumCase:"fold"`
}

func TestScalarAliasTopLevelMCPSchema(t *testing.T) {
	binding := bindly.BindingSpec{Path: "Mode", Name: "Mode", Location: bindstate.Location{Kind: "body"}}
	contract := testRouteContract(t, reflect.TypeFor[scalarAliasTopInput](), []bindly.BindingSpec{binding})
	plan, err := NewCompiler().Compile(Input{Component: spec.Key{Kind: spec.KindComponent, Scope: "test", Name: "TopAliases"}, Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "top_aliases"}, Contract: contract})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		value any
		valid bool
	}{{1, true}, {"Even", true}, {nil, true}, {2, false}, {true, false}, {"Even\n", false}} {
		args := map[string]any{"mode": tc.value}
		result, err := gojsonschema.Validate(gojsonschema.NewGoLoader(plan.Metadata().InputSchema), gojsonschema.NewGoLoader(args))
		if err != nil {
			t.Fatal(err)
		}
		if result.Valid() != tc.valid {
			t.Errorf("value=%v valid=%v want %v", tc.value, result.Valid(), tc.valid)
		}
	}
}

func TestScalarAliasTopLevelMCPExecution(t *testing.T) {
	field := reflect.TypeFor[scalarAliasTopInput]().Field(0)
	binding, err := registry.EnumBinding(bindly.BindingSpec{Path: "Mode", Name: "Mode", Location: bindstate.Location{Kind: "body"}}, field)
	if err != nil {
		t.Fatal(err)
	}
	contract := testRouteContract(t, reflect.TypeFor[scalarAliasTopInput](), []bindly.BindingSpec{binding})
	plan, err := NewCompiler().Compile(Input{Component: spec.Key{Kind: spec.KindComponent, Scope: "test", Name: "TopExecution"}, Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "top_execution"}, Contract: contract})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		value any
		want  string
	}{{1, "SPEND_EVENLY"}, {"eVeN", "SPEND_EVENLY"}, {"0", "ASAP"}} {
		scope, err := plan.Scope(map[string]any{"mode": tc.value})
		if err != nil {
			t.Fatal(err)
		}
		injector, err := bindly.NewInjector()
		if err != nil {
			t.Fatal(err)
		}
		bound, err := injector.ForScope(scope.Providers()...)
		if err != nil {
			t.Fatal(err)
		}
		var actual scalarAliasTopInput
		err = bound.Bind(t.Context(), &actual, bindly.WithPlan(contract.Plan()))
		scope.Close()
		if err != nil {
			t.Fatal(err)
		}
		if actual.Mode == nil || *actual.Mode != tc.want {
			t.Fatalf("input %v bound %v want %s", tc.value, actual.Mode, tc.want)
		}
	}
}

func TestScalarAliasNestedRequiredMetadata(t *testing.T) {
	projected, err := (&schemaProjector{nativeBody: true, schemas: map[string]*spec.WireSchema{"Body.Rows.Mode": {Required: true}}}).argument(reflect.TypeFor[scalarAliasBody](), "Body", "Body", nil)
	if err != nil {
		t.Fatal(err)
	}
	_, member, _ := schemaMember(projected, []string{"rows", "mode"})
	result, err := gojsonschema.Validate(gojsonschema.NewGoLoader(member), gojsonschema.NewStringLoader("null"))
	if err != nil {
		t.Fatal(err)
	}
	if result.Valid() {
		t.Fatal("native alias projection restored null after required metadata removed it")
	}
}
