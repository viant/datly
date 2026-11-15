package spec

import "testing"

func TestCubeComposeSettingsDefaultsAndClone(t *testing.T) {
	disabled := false
	s := &ReportSettings{Compose: &CubeComposeSettings{Enabled: true, MCPTool: &disabled}}
	copy := s.Clone()
	*copy.Compose.MCPTool = true
	if *s.Compose.MCPTool {
		t.Fatal("clone aliases source visibility")
	}
	n := s.Compose.Normalize()
	if n.MaxCubes != 8 || n.MaxLimit != 100 || n.TimeoutMs != 30000 {
		t.Fatalf("defaults: %+v", n)
	}
	if s.Compose.MaxCubes != 0 {
		t.Fatal("normalization changed source")
	}
}

func TestComposeMCPExposureOwnership(t *testing.T) {
	strict := true
	settings := &CubeComposeSettings{MCP: &MCPExposure{Kind: MCPExposureTool, Name: "compose_records", Description: "Quoted, \"text\"\nline", DescriptionPath: "docs/compose.md", StrictArguments: &strict}}
	clone := settings.Clone()
	if clone.MCP == settings.MCP || clone.MCP.StrictArguments == settings.MCP.StrictArguments {
		t.Fatal("metadata aliases")
	}
	route := &Route{MCP: []*MCPExposure{{Kind: MCPExposureTool, Name: "first"}}}
	if got := ComposeMCPExposure(nil, route, "Fallback", "description"); got.Name != "firstCubeCompose" {
		t.Fatal(got)
	}
	route.MCP[0].Name = "second"
	if got := ComposeMCPExposure(settings, route, "Fallback", "description"); got.Name != "compose_records" {
		t.Fatal(got)
	}
	disabled := false
	settings.MCPTool = &disabled
	if ComposeMCPExposure(settings, route, "Fallback", "") != nil {
		t.Fatal("disable lost")
	}
	for _, m := range []*MCPExposure{{}, {Kind: MCPExposureResource, Name: "x"}, {Kind: MCPExposureTool, Name: " "}, {Kind: MCPExposureTool, Name: "x", CaseFormat: "bogus"}} {
		if (&CubeComposeSettings{MCP: m}).ValidateMCPExposure() == nil {
			t.Fatalf("accepted %+v", m)
		}
	}
}
