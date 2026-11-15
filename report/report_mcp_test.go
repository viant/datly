package report

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
)

func TestExplicitReportMCPConvergesAndPreservesQueryDefinition(t *testing.T) {
	strict := false
	exposure := &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "aggregate_spend", Description: "Quoted, \"text\"\nsecond line", DescriptionPath: "docs/cube.md", CaseFormat: "lc", StrictArguments: &strict}
	source := reportSource(t, &spec.ReportSettings{Enabled: true, MCP: exposure})
	source.Component.Routes[0].MCP = []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "get_spend", CaseFormat: "uc"}}
	projected := ProjectedComponents(source.Component)
	project, err := NewProjectCompiler(ProjectConfig{Types: typecatalog.NewCatalog()}).Compile([]Source{source})
	if err != nil {
		t.Fatal(err)
	}
	actual := project.Derived()[0]
	selected := actual.Component.Routes[0].MCP[0]
	if !reflect.DeepEqual(selected, exposure) || !reflect.DeepEqual(projected[0].MCP, selected) || projected[0].Description != exposure.Description || projected[0].Name != exposure.Name {
		t.Fatalf("metadata differs: %+v %+v", projected, selected)
	}
	if !reflect.DeepEqual(actual.Definition.MCP, exposure) || actual.Definition.MCP.StrictArguments == exposure.StrictArguments {
		t.Fatal("portable metadata aliases source")
	}
	encoded, err := json.Marshal(actual.Definition)
	if err != nil {
		t.Fatal(err)
	}
	var definition Definition
	if err = json.Unmarshal(encoded, &definition); err != nil {
		t.Fatal(err)
	}
	rebound, err := definition.Compile(actual.InputType, actual.OutputType)
	if err != nil {
		t.Fatal(err)
	}
	roundtrip, err := rebound.Definition()
	if err != nil || !reflect.DeepEqual(roundtrip.MCP, exposure) {
		t.Fatalf("definition roundtrip %v %+v", err, roundtrip.MCP)
	}
	legacy, _ := generatedProject(t, &spec.ReportSettings{Enabled: true})
	old := legacy.Derived()[0]
	withoutMetadata := actual.Definition
	withoutMetadata.MCP = nil
	if !reflect.DeepEqual(withoutMetadata, old.Definition) || actual.Component.Key != old.Component.Key || !reflect.DeepEqual(actual.Component.Routes[0].Method, old.Component.Routes[0].Method) || actual.Component.Routes[0].Path != old.Component.Routes[0].Path {
		t.Fatal("publication metadata changed query contract or component identity")
	}
	if source.Component.Routes[0].MCP[0].Name != "get_spend" {
		t.Fatal("source exposure changed")
	}
}

func TestReportMCPInvalidOverrideAndDisable(t *testing.T) {
	for _, exposure := range []*spec.MCPExposure{{}, {Kind: spec.MCPExposureResource, Name: "bad"}, {Kind: spec.MCPExposureTool, Name: " "}} {
		source := reportSource(t, &spec.ReportSettings{Enabled: true, MCP: exposure})
		if _, err := NewProjectCompiler(ProjectConfig{Types: typecatalog.NewCatalog()}).Compile([]Source{source}); err == nil {
			t.Fatalf("accepted %+v", exposure)
		}
		disabled := false
		source.Component.Settings.Report.MCPTool = &disabled
		project, err := NewProjectCompiler(ProjectConfig{Types: typecatalog.NewCatalog()}).Compile([]Source{source})
		if err != nil {
			t.Fatal(err)
		}
		if len(project.Derived()[0].Component.Routes[0].MCP) != 0 || ProjectedComponents(source.Component)[0].MCPEnabled {
			t.Fatal("disabled override exposed")
		}
	}
}
