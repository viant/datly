package compiler

import (
	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
	"testing"
)

func TestBuildInputDoesNotAdoptAuxiliarySameTableEvidenceAsCurrent(t *testing.T) {
	for _, count := range []int{1, 2} {
		root := &spec.View{Name: "Records", Source: &spec.ViewSource{Table: "records", SQL: "SELECT id FROM records WHERE active=1"}, Columns: []*spec.Column{{Name: "id", Source: "id", PrimaryKey: true, Type: spec.TypeRef{Name: "int"}}}}
		component := &spec.Component{Name: "Patch", RootView: root}
		bindings := map[string]string{}
		for _, name := range []string{"Latest", "Oldest"}[:count] {
			view := root.Clone()
			view.Name = name
			view.Auxiliary = true
			view.Source.SQL = "SELECT id FROM records ORDER BY id LIMIT 1"
			parameter := &spec.Parameter{Name: name, Source: spec.BindSource{Kind: "view", Name: name}, TypeExpr: "*Evidence"}
			identity, err := view.Identity()
			if err != nil {
				t.Fatal(err)
			}
			bindings[parameter.Identity()] = identity
			component.Views = append(component.Views, view)
			component.Parameters = append(component.Parameters, parameter)
		}
		got, err := (&Compiler{}).BuildInput(Request{Component: component, Operation: plan.OperationPatch, ViewBindings: bindings}, "Record")
		if err != nil {
			t.Fatal(err)
		}
		if len(got.Currents) != 1 || got.Currents[0].Param != "CurrentRecords" {
			t.Fatalf("currents=%+v", got.Currents)
		}
		if len(got.Component.Views) != count+1 {
			t.Fatal("evidence or canonical Current was lost")
		}
	}
}
