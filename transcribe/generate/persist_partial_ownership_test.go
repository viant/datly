package generate

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPartialOwnershipAllowsKnownFieldRemoval(t *testing.T) {
	dir := t.TempDir()
	plan := &Plan{ComponentName: "Records", ViewDest: "records.go", RouterDest: "router.go", Input: generatedContract("Input", "input.go"), Output: generatedContract("Output", "output.go"), Views: []ViewPlan{{Name: "Row", Type: "Row", Destination: "records.go", Ownership: ViewGenerated, Fields: []Field{{Name: "ID", Type: "int"}}}}}
	if _, e := EmitScaffold(dir, plan); e != nil {
		t.Fatal(e)
	}
	m, e := readScaffoldManifest(dir)
	if e != nil {
		t.Fatal(e)
	}
	m.Version = 3
	m.ProjectionFields = nil
	data, e := json.Marshal(m)
	if e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(filepath.Join(dir, scaffoldManifestName), data, 0644); e != nil {
		t.Fatal(e)
	}
	path := filepath.Join(dir, "records.go")
	data, e = os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	data = append(data, []byte("\n// custom source note\n")...)
	if e = os.WriteFile(path, data, 0644); e != nil {
		t.Fatal(e)
	}
	plan.Views[0].Fields = append(plan.Views[0].Fields, Field{Name: "Extra", Type: "int"})
	if _, e = EmitScaffold(dir, plan); e != nil {
		t.Fatal("append", e)
	}
	m, e = readScaffoldManifest(dir)
	if e != nil {
		t.Fatal(e)
	}
	t.Logf("ownership after append: %+v", m.ProjectionFields["records.go"])
	if len(m.ProjectionFields["records.go"].Fields) != 1 || m.ProjectionFields["records.go"].Fields[0].Name != "Extra" {
		t.Fatal("new field ownership missing")
	}
	if m.ProjectionFields["records.go"].Complete {
		t.Fatal("unexpected complete ownership")
	}
	plan.Views[0].Fields = plan.Views[0].Fields[:1]
	if e = plan.ValidateDestination(dir); e != nil {
		t.Error("known-owned Extra removal preflight rejected:", e)
	}
	if _, e = EmitScaffold(dir, plan); e != nil {
		t.Fatal("known-owned Extra removal rejected:", e)
	}
	data, e = os.ReadFile(path)
	if e != nil || strings.Contains(string(data), "Extra int") {
		t.Fatal("stale Extra", e)
	}
	if !strings.Contains(string(data), "custom source note") {
		t.Fatal("authored comment lost")
	}
	retained := string(data)
	plan.Views[0].Fields = nil
	if err := plan.ValidateDestination(dir); err == nil {
		t.Fatal("unproven original field removal accepted by preflight")
	}
	if _, err := EmitScaffold(dir, plan); err == nil {
		t.Fatal("unproven original field removal accepted")
	}
	data, e = os.ReadFile(path)
	if e != nil || string(data) != retained {
		t.Fatalf("rejected removal changed the shape: %v", e)
	}
}
