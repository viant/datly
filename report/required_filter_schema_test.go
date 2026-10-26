package report

import (
	"reflect"
	"slices"
	"testing"

	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/mcp/tool"
	handlercompiler "github.com/viant/datly/runtime/handler/compiler"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
)

func TestCubeSchemaPreservesRequiredSourceFilters(t *testing.T) {
	for _, tc := range []struct {
		name                string
		required, defaulted bool
	}{
		{"required", true, false}, {"optional", false, false}, {"defaulted", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := reportSource(t, &spec.ReportSettings{Enabled: true})
			source.Component.Parameters[0].Required = &tc.required
			if tc.defaulted {
				value := "1"
				source.Component.Parameters[0].Value = &value
			}
			compiled, err := handlercompiler.New(handlercompiler.Input{Component: source.Component, InputType: reflect.TypeFor[reportSourceInput]()}).Compile()
			if err != nil {
				t.Fatal(err)
			}
			source.Input = compiled.Input
			catalog := typecatalog.NewCatalog()
			project, err := NewProjectCompiler(ProjectConfig{Types: catalog}).Compile([]Source{source})
			if err != nil {
				t.Fatal(err)
			}
			for _, derived := range project.Derived() {
				artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: derived.Component, InputType: derived.InputType, OutputType: derived.OutputType, Types: catalog})
				if err != nil {
					t.Fatal(err)
				}
				route := derived.Component.Routes[0]
				contract, ok := artifact.Input.ForRoute(spec.RouteRef{Method: route.Method, Path: route.Path})
				if !ok {
					t.Fatal("derived route contract missing")
				}
				plan, err := tool.NewCompiler().Compile(tool.Input{Component: derived.Component.Key, Contract: contract, Exposure: route.MCP[0]})
				if err != nil {
					t.Fatal(err)
				}
				metadata := plan.Metadata().InputSchema
				filters := metadata.Properties["filters"]
				if filters == nil {
					t.Fatal("ordinary cube filter schema missing")
				}
				wantRequired := tc.required && !tc.defaulted
				if slices.Contains(metadata.Required, "filters") != wantRequired {
					t.Fatalf("root required = %v", metadata.Required)
				}
				required, _ := filters["required"].([]string)
				if slices.Contains(required, "accountIds") != wantRequired {
					t.Fatalf("filter required = %v", required)
				}
				property := filters["properties"].(map[string]interface{})["accountIds"].(map[string]interface{})
				if wantRequired && property["type"] != "array" {
					t.Fatalf("required array must exclude null: %+v", property)
				}
				if property["items"].(map[string]interface{})["type"] != "integer" {
					t.Fatal("requiredness discarded array element type")
				}
				if wantRequired && property["minItems"] != 1 {
					t.Fatal("required source collections reject empty arrays")
				}
				if !wantRequired && property["minItems"] != nil {
					t.Fatal("optional/defaulted collection acquired a new constraint")
				}
			}
		})
	}
}
