package generate

import (
	"go/ast"
	"testing"
)

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
