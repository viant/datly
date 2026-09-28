package generate

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestProjectionRelationJSONMetadataRegenerates(t *testing.T) {
	dir := t.TempDir()
	plan := &Plan{ComponentName: "Records", ViewDest: "records.go", RouterDest: "router.go", Input: generatedContract("Input", "input.go"), Output: generatedContract("Output", "output.go"), Views: []ViewPlan{
		{Name: "Row", Type: "Row", Destination: "records.go", Ownership: ViewGenerated, Fields: []Field{{Name: "Child", Type: "*Child", Tag: `view:"child,type=Child,table=CHILD" on:"Id:root.ID=ParentId:child.PARENT_ID" sql:"uri=child.sql"`, RelationHolder: true}}},
		{Name: "Child", Type: "Child", Destination: "records.go", Ownership: ViewGenerated, Fields: []Field{{Name: "ParentId", Type: "int"}}},
	}}
	if _, err := EmitScaffold(dir, plan); err != nil {
		t.Fatal(err)
	}
	plan.Views[0].Fields[0].Tag = `view:"child,type=Child,table=CHILD" on:"Id:root.ID=ParentId:child.PARENT_ID" json:"child" sql:"uri=child.sql"`
	if _, err := EmitScaffold(dir, plan); err != nil {
		t.Fatalf("regenerate relation JSON metadata: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "records.go"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `json:"child"`) {
		t.Fatalf("relation JSON metadata missing: %s", data)
	}
	if _, err := EmitScaffold(dir, plan); err != nil {
		t.Fatalf("regenerate unchanged relation: %v", err)
	}
}
