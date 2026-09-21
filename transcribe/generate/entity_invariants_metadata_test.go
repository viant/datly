package generate

import (
	"go/ast"
	"os"
	"path/filepath"
	"testing"
)

func TestInvariantIndexIsRemovedWithLastInvariant(t *testing.T) {
	dir := t.TempDir()
	file := &ast.File{Name: ast.NewIdent("records")}
	plan := &Plan{ComponentName: "Records", RouterDest: "router.go", Input: generatedContract("Input", "input.go"), Output: generatedContract("Output", "output.go"), EntitySupport: &EntitySupportPlan{File: file, Destination: "setters.go", Invariants: []EntityInvariant{{Identity: "root", Group: "Window"}}}, Views: []ViewPlan{{Identity: "root", Name: "Row", Type: "Row", Destination: "views.go", Ownership: ViewGenerated, Fields: []Field{{Name: "Start", Type: "int", Tag: `invariant:"Window"`}}}}}
	if _, err := EmitScaffold(dir, plan); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "invariants.go")
	if _, err := os.Stat(path); err != nil {
		t.Fatal(err)
	}
	plan.Views[0].Fields[0].Tag = ""
	if _, err := EmitScaffold(dir, plan); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("invariant index retained: %v", err)
	}
}

func TestEntityInvariantMetadataClone(t *testing.T) {
	asset := &EntitySupportAsset{File: &ast.File{Name: ast.NewIdent("records")}, CaptureFunction: "capture", Invariants: []EntityInvariant{{Identity: "root", Path: []string{"Input", "Records"}, Group: "Window", BackfillFunction: "backfillWindow"}}}
	cloned, err := asset.Clone()
	if err != nil {
		t.Fatal(err)
	}
	cloned.Invariants[0].Path[0] = "Changed"
	cloned.Invariants[0].BackfillFunction = "changed"
	if asset.Invariants[0].Path[0] != "Input" || asset.Invariants[0].BackfillFunction != "backfillWindow" {
		t.Fatal("clone shares invariant metadata")
	}
	resolver := &planResolver{input: Input{EntitySupport: asset}, plan: &Plan{ComponentName: "Records"}}
	if err := resolver.resolveEntitySupport(); err != nil {
		t.Fatal(err)
	}
	resolver.plan.EntitySupport.Invariants[0].Path[0] = "Changed"
	if asset.Invariants[0].Path[0] != "Input" {
		t.Fatal("plan shares invariant paths")
	}
}
