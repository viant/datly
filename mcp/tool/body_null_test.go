package tool

import (
	"context"
	"github.com/viant/bindly"
	bindstate "github.com/viant/bindly/state"
	"github.com/viant/datly/spec"
	"reflect"
	"testing"
)

type nullMCPRow struct {
	Name string               `json:"name"`
	Has  *struct{ Name bool } `setMarker:"true" json:"-"`
}
type nullMCPInput struct {
	View *nullMCPRow          `json:"view" mcp:"name=view,aliases=legacy_view"`
	Has  *struct{ View bool } `setMarker:"true" json:"-"`
}

func TestRequiredNullableBodyPolicyMCPSchemaAndRuntime(t *testing.T) {
	required := true
	for _, policy := range []string{"", "empty-record"} {
		contract := testRouteContract(t, reflect.TypeFor[nullMCPInput](), []bindly.BindingSpec{{Path: "View", Name: "View", Location: bindstate.Location{Kind: "body"}, Required: &required, BodyNullPolicy: policy}})
		plan, err := NewCompiler().Compile(Input{Contract: contract, Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "null.body"}})
		if err != nil {
			t.Fatal(err)
		}
		schema := plan.Metadata().InputSchema
		if len(schema.Required) != 1 || schema.Required[0] != "view" {
			t.Fatalf("schema=%+v", schema)
		}
		if policy == "" {
			if schema.Properties["view"]["type"] != "object" {
				t.Fatal(schema)
			}
		} else {
			types, ok := schema.Properties["view"]["type"].([]string)
			if !ok || !reflect.DeepEqual(types, []string{"object", "null"}) {
				t.Fatalf("types=%+v", schema.Properties["view"])
			}
		}
		for _, args := range []map[string]interface{}{{}, {"view": nil}, {"legacy_view": nil}, {"view": map[string]interface{}{}}, {"view": map[string]interface{}{"name": "hello"}}} {
			scope, err := plan.Scope(args)
			if len(args) == 0 {
				if err == nil {
					t.Fatal("missing accepted")
				}
				continue
			}
			if err != nil {
				t.Fatal(err)
			}
			injector, _ := bindly.NewInjector()
			bound, err := injector.ForScope(scope.Providers()...)
			if err != nil {
				t.Fatal(err)
			}
			var input nullMCPInput
			err = bound.Bind(context.Background(), &input, bindly.WithPlan(contract.Plan()))
			isNull := args["view"] == nil && args["legacy_view"] == nil
			if policy == "" && isNull {
				if err == nil {
					t.Fatal("strict null accepted")
				}
				continue
			}
			if err != nil {
				t.Fatal(err)
			}
			if input.View == nil || input.View.Has == nil || input.Has == nil || !input.Has.View {
				t.Fatalf("input=%+v", input)
			}
			if isNull && input.View.Has.Name {
				t.Fatal("null fabricated presence")
			}
		}
	}
}
func TestBodyNullPolicyRejectsAnonymousMCPProjection(t *testing.T) {
	type input struct {
		View *nullMCPRow `anonymous:"true"`
	}
	contract := testRouteContract(t, reflect.TypeFor[input](), []bindly.BindingSpec{{Path: "View", Name: "View", Location: bindstate.Location{Kind: "body"}, BodyNullPolicy: "empty-record"}})
	if _, err := NewCompiler().Compile(Input{Contract: contract, Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "null.body"}}); err == nil {
		t.Fatal("anonymous policy accepted")
	}
}
