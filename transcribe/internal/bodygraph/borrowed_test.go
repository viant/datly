package bodygraph

import (
	"github.com/viant/datly/spec"
	"testing"
)

func TestResolveOccurrencePreservesNonleafOwnership(t *testing.T) {
	leaf := &spec.View{Key: spec.Key{Kind: spec.KindView, Name: "item"}, Name: "item"}
	owner := &spec.View{Key: spec.Key{Kind: spec.KindView, Name: "owner"}, Name: "owner", Relations: []*spec.Relation{{Name: "Items", Holder: "Items", View: leaf}}}
	c := &spec.Component{Name: "Rows", RootView: owner}
	got, graph, err := ResolveOccurrence(c, "Rows")
	if err != nil || got != owner || len(graph) != 1 || len(owner.Relations) != 1 {
		t.Fatalf("complete occurrence: %v %v", graph, err)
	}
	if _, _, err := ResolveBorrowed(c, "Rows"); err == nil {
		t.Fatal("generated owner admitted nonleaf")
	}
	got, graph, err = ResolveBorrowed(c, "Rows/Items")
	if err != nil || got != leaf || len(graph) != 2 {
		t.Fatalf("existing leaf: %v %v", graph, err)
	}
	for _, path := range []string{"Other", "Rows/Missing", "Rows/Items/Items"} {
		if _, _, err := ResolveOccurrence(c, path); err == nil {
			t.Fatalf("accepted invalid path %s", path)
		}
	}
	owner.Relations = append(owner.Relations, &spec.Relation{Name: "Duplicate", Holder: "Items", View: leaf})
	if _, _, err := ResolveOccurrence(c, "Rows/Items"); err == nil {
		t.Fatal("ambiguous occurrence accepted")
	}
	owner.Relations = []*spec.Relation{{Name: "Cycle", Holder: "Cycle", View: owner}}
	if _, _, err := ResolveOccurrence(c, "Rows/Cycle"); err == nil {
		t.Fatal("cyclic occurrence accepted")
	}
}
