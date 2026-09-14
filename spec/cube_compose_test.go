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
