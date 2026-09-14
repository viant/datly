package compiler

import (
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/spec"
)

func TestBuildDataView_AppendsTypedOutputRelations(t *testing.T) {
	type totals struct {
		Count int `sqlx:"count"`
	}
	type activity struct {
		LatestID int `sqlx:"latest_id"`
	}
	type output struct {
		Data     []*struct{ ID int }
		Totals   totals
		Activity *activity
	}
	component := summaryComponent(
		canonicalOutputRelation("Totals", "SELECT COUNT(*) AS count FROM users"),
		canonicalOutputRelation("Activity", "SELECT MAX(id) AS latest_id FROM users"),
	)

	views, err := buildDataViews(component, reflect.TypeOf(output{}), "")
	if err != nil {
		t.Fatalf("BuildDataView failed: %v", err)
	}
	view := views.root
	if len(view.Relations) != 2 {
		t.Fatalf("expected two output relations, got %d", len(view.Relations))
	}
	for index, holder := range []string{"Totals", "Activity"} {
		relation := view.Relations[index]
		if relation.Kind != spec.RelationKindDerived || relation.Holder != holder || relation.Cardinality != spec.CardinalityOne {
			t.Fatalf("unexpected relation %d: %+v", index, relation)
		}
		if !relation.IsOutput() || relation.Of == nil || relation.Of.View == nil {
			t.Fatalf("expected typed output relation %d: %+v", index, relation)
		}
		if views.rowTypes[relation.Of.View] == nil {
			t.Fatalf("expected relation %d to carry a typed schema", index)
		}
	}
}

func TestBuildDataViewsReconcilesCanonicalOutputRelationWithoutDuplication(t *testing.T) {
	type total struct {
		Count int `sqlx:"count"`
	}
	type output struct {
		Data   []struct{ ID int }
		Totals total
	}
	component := summaryComponent(canonicalOutputRelation("Totals", "SELECT COUNT(*) AS count FROM users"))
	views, err := buildDataViews(component, reflect.TypeOf(output{}), "Data")
	if err != nil {
		t.Fatalf("buildDataViews() error = %v", err)
	}
	if len(views.root.Relations) != 1 {
		t.Fatalf("relations = %#v", views.root.Relations)
	}
	relation := views.root.Relations[0]
	if !relation.IsOutput() || relation.Holder != "Totals" || views.rowTypes[relation.Of.View] != reflect.TypeOf(total{}) {
		t.Fatalf("relation = %#v, row type = %v", relation, views.rowTypes[relation.Of.View])
	}
}

func TestBuildDataViews_RejectsMissingOutputTypeForDirectView(t *testing.T) {
	_, err := buildDataViews(summaryComponent(), nil, "Data")
	if err == nil || !strings.Contains(err.Error(), "output type is required") {
		t.Fatalf("expected direct-view output type error, got %v", err)
	}
}

func TestBuildDataViews_RejectsRecursiveRelationShape(t *testing.T) {
	type node struct {
		ID       int
		ParentID int
		Children []*node `view:"children" sql:"SELECT id, parent_id FROM nodes" on:"ID:id=ParentID:parent_id"`
	}
	type output struct {
		Data []node
	}

	_, err := buildDataViews(summaryComponent(), reflect.TypeOf(output{}), "Data")
	if err == nil || !strings.Contains(err.Error(), "recursive relation type") || !strings.Contains(err.Error(), "self tag") {
		t.Fatalf("expected recursive relation guidance, got %v", err)
	}
}

func TestBuildDataView_RejectsUntypedOutputRelation(t *testing.T) {
	type output struct {
		Summary map[string]any
	}
	component := summaryComponent(canonicalOutputRelation("Summary", "SELECT COUNT(*) AS count FROM users"))
	if _, err := buildDataViews(component, reflect.TypeOf(output{}), ""); err == nil {
		t.Fatalf("expected untyped output relation to be rejected")
	}
}

func summaryComponent(relations ...*spec.Relation) *spec.Component {
	component := &spec.Component{
		Key:  spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo", Name: "Users"},
		Name: "Users",
		RootView: &spec.View{
			Key:       spec.Key{Kind: spec.KindView, Scope: "example.com/demo", Name: "UsersView"},
			Name:      "UsersView",
			Source:    &spec.ViewSource{SQL: "SELECT id FROM users"},
			Relations: relations,
		},
	}
	for _, relation := range relations {
		if relation == nil {
			continue
		}
		component.Parameters = append(component.Parameters, &spec.Parameter{Name: relation.Holder, Source: spec.BindSource{Kind: "output", Name: "summary"}})
	}
	return component
}

func canonicalOutputRelation(name, sqlText string) *spec.Relation {
	return &spec.Relation{
		Name: name, Kind: spec.RelationKindDerived, Holder: name, Cardinality: spec.CardinalityOne,
		View: &spec.View{Name: name, Source: &spec.ViewSource{SQL: sqlText}},
	}
}
