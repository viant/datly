package dql

import "testing"

func TestCubeComposeDirective(t *testing.T) {
	for _, directives := range []string{
		"#set($_ = $cube())\n#set($_ = $cubeCompose(true))",
		"#setting($_ = $cubeCompose(true))\n#setting($_ = $cube())",
	} {
		prepared := PrepareSource(directives + "\nSELECT id FROM events")
		if err := prepared.Err(); err != nil {
			t.Fatal(err)
		}
		s := prepared.Directives.Settings
		if s == nil || s.Report == nil || !s.Report.Enabled || s.Report.Compose == nil || !s.Report.Compose.Enabled {
			t.Fatalf("settings=%+v", s)
		}
	}
	if err := PrepareSource("#set($_ = $cubeCompose('yes'))\nSELECT id FROM events").Err(); err == nil {
		t.Fatal("accepted invalid compose flag")
	}
}

func TestCubeComposeDirectiveCarriesMCPAndBudgets(t *testing.T) {
	prepared := PrepareSource("#setting($_ = $cubeCompose(true,false,4,50,12000))\nSELECT id FROM events")
	if err := prepared.Err(); err != nil {
		t.Fatal(err)
	}
	compose := prepared.Directives.Settings.Report.Compose
	if compose == nil || !compose.Enabled || compose.MCPTool == nil || *compose.MCPTool || compose.MaxCubes != 4 || compose.MaxLimit != 50 || compose.TimeoutMs != 12000 {
		t.Fatalf("compose=%+v", compose)
	}
	for _, source := range []string{
		"#setting($_ = $cubeCompose(true,false,0))\nSELECT id FROM events",
		"#setting($_ = $cubeCompose(true,false,4,50,12000,1))\nSELECT id FROM events",
	} {
		if err := PrepareSource(source).Err(); err == nil {
			t.Fatalf("accepted %s", source)
		}
	}
}
