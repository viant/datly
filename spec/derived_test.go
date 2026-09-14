package spec

import (
	"encoding/json"
	"testing"
)

func TestDerivedViewMetadataBoundary(t *testing.T) {
	for _, spelling := range []string{"derived", "summary", " SUMMARY "} {
		param := &Parameter{Source: BindSource{Kind: "output", Name: spelling}}
		if !param.IsDerivedOutput() {
			t.Fatalf("derived output %q not recognized", spelling)
		}
	}
	if (&Parameter{Source: BindSource{Kind: "query", Name: "derived"}}).IsDerivedOutput() {
		t.Fatal("query input classified as derived view")
	}
	for _, spelling := range []string{"derived", "summary"} {
		var relation Relation
		if err := json.Unmarshal([]byte(`{"kind":"`+spelling+`"}`), &relation); err != nil {
			t.Fatal(err)
		}
		if relation.Kind != RelationKindDerived {
			t.Fatalf("kind=%s", relation.Kind)
		}
		encoded, err := json.Marshal(relation)
		if err != nil {
			t.Fatal(err)
		}
		var actual map[string]any
		if err := json.Unmarshal(encoded, &actual); err != nil {
			t.Fatal(err)
		}
		if actual["kind"] != "derived" {
			t.Fatalf("emitted kind=%s", encoded)
		}
	}
}
