package generate

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestProjectionRelationAliasAuthority(t *testing.T) {
	previous := projectionField{Tag: `view:"items,table=ITEMS,dest=items.go" on:"Id:orders.ID=OrderId:items.ORDER_ID" sql:"uri=items.sql"`}
	for _, tc := range []struct {
		tag     string
		allowed bool
	}{
		{`view:"items,table=ITEMS,dest=items.go" on:"RootKey:orders.RootKey=ParentKey:items.ParentKey" sql:"uri=items.sql"`, true},
		{`view:"items,table=ITEMS,dest=other.go" on:"RootKey:orders.RootKey=ParentKey:items.ParentKey" sql:"uri=items.sql"`, false},
		{`view:"items,table=ITEMS,dest=items.go" on:"RootKey:orders.RootKey=ParentKey:items.ParentKey" sql:"uri=other.sql"`, false},
	} {
		allowed, err := previous.tagChange(projectionField{Tag: tc.tag}, "on")
		if err != nil || allowed != tc.allowed {
			t.Fatalf("on authority %q: %t %v", tc.tag, allowed, err)
		}
	}
}

func TestProjectionRelationEditConflictsWithUnchangedProposal(t *testing.T) {
	dir := t.TempDir()
	plan := &Plan{ComponentName: "Records", ViewDest: "records.go", RouterDest: "router.go", Input: generatedContract("Input", "input.go"), Output: generatedContract("Output", "output.go"), Views: []ViewPlan{
		{Name: "Row", Type: "Row", Destination: "records.go", Ownership: ViewGenerated, Fields: []Field{{Name: "Child", Type: "[]*Child", Tag: `on:"Id:root.ID=ParentId:child.PARENT_ID"`, RelationHolder: true}}},
		{Name: "Child", Type: "Child", Destination: "records.go", Ownership: ViewGenerated, Fields: []Field{{Name: "ParentId", Type: "int"}}},
	}}
	if _, err := EmitScaffold(dir, plan); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "records.go")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	edited := strings.Replace(string(data), "child.PARENT_ID", "child.EDITED", 1)
	if edited == string(data) {
		t.Fatal("missing edit target")
	}
	if err = os.WriteFile(path, []byte(edited), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err = EmitScaffold(dir, plan); err == nil || !strings.Contains(err.Error(), "customized type or tag") {
		t.Fatalf("edited relation accepted: %v", err)
	}
}
