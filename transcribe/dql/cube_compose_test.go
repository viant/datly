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
