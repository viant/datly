package compile

import (
	"github.com/viant/datly/spec"
	"strings"
	"testing"
)

const compositionSQL = `SELECT type(rb,'RouteBody'), type(n0,'Campaign','Data'), type(n1,'CampaignRow','Campaign'), n0.CampaignCreativeIDs, CAST(n0.CampaignCreativeIDs AS '[]int'), n1.ID FROM RouteBody rb CROSS JOIN rb.Data n0 CROSS JOIN CI_CAMPAIGN n1`

func compositionProof() *FactoryComposition {
	return &FactoryComposition{RootSource: "RouteBody", RootAlias: "rb", RootType: "RouteBody", Joins: []FactoryCompositionJoin{
		{Source: "rb.Data", Alias: "n0", ParentAlias: "rb", Holder: "Data", TypeName: "Campaign", Cardinality: spec.CardinalityMany, Logical: true},
		{Source: "CI_CAMPAIGN", Alias: "n1", ParentAlias: "n0", Holder: "Campaign", TypeName: "CampaignRow", Cardinality: spec.CardinalityOne},
	}}
}
func TestFactoryCompositionSourceAndDiscoveryBoundary(t *testing.T) {
	proof := compositionProof()
	root, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "Graph", Source: &spec.ViewSource{SQL: compositionSQL}}, SQL: compositionSQL, FactoryComposition: proof})
	if err != nil {
		t.Fatal(err)
	}
	logical := root.Relations[0].View
	physical := logical.Relations[0].View
	for _, view := range []*spec.View{root, logical} {
		if view.Source == nil || view.Source.Table != "" || view.Source.SQL != "" {
			t.Fatalf("logical source became executable: %+v", view.Source)
		}
	}
	if physical.Source.Table != "CI_CAMPAIGN" {
		t.Fatalf("physical source lost: %+v", physical.Source)
	}
	if len(root.Relations[0].On) != 0 || root.Relations[0].Kind == spec.RelationKindDerived {
		t.Fatal("containment was lowered as a fake SQL relation")
	}
	component := &spec.Component{RootView: root}
	discovery, transfer := proof.Discovery(component)
	if discovery.RootView.Source != nil || discovery.RootView.Relations[0].View.Source != nil {
		t.Fatal("logical discovery source survived")
	}
	discovered := discovery.RootView.Relations[0].View.Relations[0].View
	if discovered.Source != physical.Source {
		t.Fatal("physical authority pointer changed")
	}
	discovered.Columns = []*spec.Column{{Name: "ID"}}
	transfer()
	if len(physical.Columns) != 1 || physical.Columns[0].Name != "ID" {
		t.Fatal("fresh physical discovery facts not transferred")
	}
	if root.Source == nil || logical.Source == nil {
		t.Fatal("discovery mutated InputShape")
	}
}
func TestFactoryCompositionRejectsUnprovedChanges(t *testing.T) {
	for _, sql := range []string{
		strings.Replace(compositionSQL, "RouteBody rb", "CI_CAMPAIGN rb", 1),
		strings.Replace(compositionSQL, "rb.Data n0", "rb.Other n0", 1),
		strings.Replace(compositionSQL, "CI_CAMPAIGN n1", "WRONG_TABLE n1", 1),
		strings.Replace(compositionSQL, "'Campaign','Data'", "'Campaign','Wrong'", 1),
		strings.Replace(compositionSQL, "n1.ID FROM", "rb.ID, n1.ID FROM", 1),
		strings.Replace(compositionSQL, "n1.ID FROM", "rb.*, n1.ID FROM", 1),
		strings.Replace(compositionSQL, "n1.ID FROM", "1 AS ID, n1.ID FROM", 1),
		compositionSQL + " WHERE rb.ID=1", compositionSQL + " ORDER BY rb.ID", compositionSQL + " LIMIT 1",
		strings.Replace(compositionSQL, "rb.Data n0", "rb.Data n0 ON n0.ID=rb.ID", 1),
	} {
		if _, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "Graph"}, SQL: sql, FactoryComposition: compositionProof()}); err == nil {
			t.Fatalf("accepted unproved change: %s", sql)
		}
	}
	if _, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "Reader"}, SQL: compositionSQL}); err == nil {
		t.Fatal("ordinary reader accepted factory composition")
	}
}
