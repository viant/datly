package compiler

import (
	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
	"strings"
	"testing"
)

func TestGeneratedCurrentExcludesInputOnlyPseudoField(t *testing.T) {
	view := &spec.View{Name: "Run", Source: &spec.ViewSource{Table: "run", SQL: "SELECT r.*, NULL AS condition FROM run r WHERE r.owner_id=7"}, Columns: []*spec.Column{{Name: "id", Source: "id", PrimaryKey: true, Type: spec.TypeRef{Name: "string"}}, {Name: "condition", Source: "condition", Type: spec.TypeRef{Name: "RunPatchCondition", Package: "fixture/predicate", Pointer: true}, Tag: `sqlx:"-" json:"condition,omitempty"`}}}
	got, e := (&Compiler{}).BuildInput(Request{Component: &spec.Component{Name: "Run", RootView: view}, Operation: plan.OperationPatch}, "RunInput")
	if e != nil {
		t.Fatal(e)
	}
	current := got.Component.Views[0]
	if len(current.Columns) != 1 || strings.Contains(strings.SplitN(current.Source.SQL, " FROM ", 2)[0], "condition") || !strings.Contains(current.Source.SQL, "WHERE r.owner_id=7") {
		t.Fatal("logical field leaked into Previous or scope changed", current.Source.SQL)
	}
	if len(got.Component.RootView.Columns) != 2 || got.Component.RootView.Columns[1].Tag != view.Columns[1].Tag || !got.Component.RootView.Columns[1].Type.Pointer {
		t.Fatal("nullable input field contract changed")
	}
}
func TestGeneratedCurrentRetainsRequiredTransientKeys(t *testing.T) {
	for _, kind := range []string{"identity", "parentLink", "childLink", "self"} {
		t.Run(kind, func(t *testing.T) {
			column := &spec.Column{Name: "derived_key", Source: "derived_key", Type: spec.TypeRef{Name: "string"}, Tag: `sqlx:"-"`}
			view := &spec.View{Name: "Root", Columns: []*spec.Column{column}}
			component := &spec.Component{Name: "Fixture", RootView: view}
			switch kind {
			case "identity":
				column.PrimaryKey = true
			case "parentLink":
				view.Relations = []*spec.Relation{{View: &spec.View{Name: "Child"}, On: []*spec.RelationLink{{ParentColumn: "derived_key", ChildColumn: "owner"}}}}
			case "childLink":
				parent := &spec.View{Name: "Parent", Relations: []*spec.Relation{{View: view, On: []*spec.RelationLink{{ParentColumn: "owner", ChildColumn: "derived_key"}}}}}
				component.RootView = parent
			case "self":
				view.SelfReference = &spec.SelfReference{Parent: "derived_key", Child: "id"}
			}
			b := inputGeneration{request: Request{Component: component}}
			if b.omitTransientCurrentColumn(view, column) {
				t.Fatal("required transient key omitted")
			}
		})
	}
}
