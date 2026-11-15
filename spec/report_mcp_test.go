package spec

import (
	"reflect"
	"testing"
)

func TestReportMCPExposureSelection(t *testing.T) {
	strict := true
	source := &Route{MCP: []*MCPExposure{{Kind: MCPExposureTool, Name: "source", Description: "source description", CaseFormat: "uc", StrictArguments: &strict}}}
	legacy := ReportMCPExposure(nil, source, "Fallback", "component description")
	if !reflect.DeepEqual(legacy, &MCPExposure{Kind: MCPExposureTool, Name: "sourceCube", Description: "component description", CaseFormat: "uc"}) {
		t.Fatalf("legacy changed: %+v", legacy)
	}
	settings := &ReportSettings{MCP: &MCPExposure{Kind: MCPExposureTool, Name: "aggregate_records", Description: "Quoted, \"description\"\nsecond line", DescriptionPath: "docs/cube.md", CaseFormat: "lc", StrictArguments: &strict}}
	selected := ReportMCPExposure(settings, source, "Fallback", "component description")
	if !reflect.DeepEqual(selected, settings.MCP) || selected == settings.MCP || selected.StrictArguments == settings.MCP.StrictArguments {
		t.Fatal("selection is not detached")
	}
	clone := settings.Clone()
	clone.MCP.Name = "changed"
	*clone.MCP.StrictArguments = false
	if settings.MCP.Name != "aggregate_records" || !*settings.MCP.StrictArguments {
		t.Fatal("clone aliases source")
	}
	disabled := false
	settings.MCPTool = &disabled
	if ReportMCPExposure(settings, source, "Fallback", "") != nil {
		t.Fatal("disabled exposure enabled")
	}
	if source.MCP[0].Name != "source" {
		t.Fatal("source changed")
	}
}

func TestReportMCPExposureAdmission(t *testing.T) {
	for _, exposure := range []*MCPExposure{{}, {Kind: MCPExposureTool, Name: " "}, {Kind: MCPExposureResource, Name: "resource"}, {Kind: MCPExposureTool, Name: "valid", CaseFormat: "bogus"}} {
		if err := (&ReportSettings{MCP: exposure}).ValidateMCPExposure(); err == nil {
			t.Fatalf("accepted %+v", exposure)
		}
	}
}
