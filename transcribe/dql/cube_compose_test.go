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

func TestCubeComposeAuthoredValuesShape(t *testing.T) {
	shape := "struct { Threshold int64 `json:\"threshold\"`; Metric int `json:\"metric\" choice:\"1\" choice:\"2\" validate:\"choice(1,2)\"` }"
	prepared := PrepareSource("#setting($_ = $cubeCompose(true,true,4,50,12000,'" + shape + "'))\nSELECT id FROM events")
	if err := prepared.Err(); err != nil {
		t.Fatal(err)
	}
	if prepared.Directives.Settings.Report.Compose.ValuesType != shape {
		t.Fatal("authored scalar shape not preserved")
	}
}
