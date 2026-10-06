package compiler

import (
	"encoding/json"
	"fmt"
	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
	"strings"
	"testing"
)

// Direct compiler metadata boundary tests complement genuine stock Generator/CLI proofs.
// They do not supply custom Currents to a stock success path.
func nestedAuxiliaryRequest(t *testing.T) (Request, *spec.View, *spec.View) {
	t.Helper()
	root := &spec.View{Name: "Root", Source: &spec.ViewSource{Table: "roots", SQL: "SELECT id FROM roots WHERE tenant=7"}, Columns: []*spec.Column{{Name: "id", Source: "id", PrimaryKey: true, Type: spec.TypeRef{Name: "int"}}}}
	aux := &spec.View{Name: "Carrier", Auxiliary: true, Source: &spec.ViewSource{Table: "carriers", SQL: "SELECT id,root_id FROM carriers WHERE tenant=7"}, Columns: []*spec.Column{{Name: "id", Source: "id", PrimaryKey: true, Type: spec.TypeRef{Name: "int"}}, {Name: "root_id", Source: "root_id", Type: spec.TypeRef{Name: "int"}}}}
	child := &spec.View{Name: "Children", Source: &spec.ViewSource{Table: "children", SQL: "SELECT id,carrier_id FROM children WHERE enabled=1"}, Columns: []*spec.Column{{Name: "id", Source: "id", PrimaryKey: true, Type: spec.TypeRef{Name: "int"}}, {Name: "carrier_id", Source: "carrier_id", Type: spec.TypeRef{Name: "int"}}}}
	aux.Relations = []*spec.Relation{{Name: "Children", Holder: "Selections", View: child, On: []*spec.RelationLink{{ParentColumn: "id", ChildColumn: "carrier_id"}}}}
	root.Relations = []*spec.Relation{{Name: "Carrier", Holder: "Groups", View: aux, On: []*spec.RelationLink{{ParentColumn: "id", ChildColumn: "root_id"}}}}
	read := aux.Clone()
	read.Name = "CurrentCarrier"
	read.Relations = nil
	param := &spec.Parameter{Name: "CurrentCarrier", Source: spec.BindSource{Kind: "view", Name: "CurrentCarrier"}, TypeExpr: "[]*Evidence", Cardinality: "Many"}
	id, err := read.Identity()
	if err != nil {
		t.Fatal(err)
	}
	return Request{Component: &spec.Component{Name: "Root", RootView: root, Views: []*spec.View{read}, Parameters: []*spec.Parameter{param}}, Operation: plan.OperationPatch, ViewBindings: map[string]string{param.Identity(): id}}, aux, child
}
func TestBuildInputNestedExplicitAuxiliaryImmediateScopeAndDetachedSource(t *testing.T) {
	request, _, _ := nestedAuxiliaryRequest(t)
	before, _ := json.Marshal(request)
	got, err := (&Compiler{}).BuildInput(request, "RootBody")
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, v := range got.Component.Views {
		if v.CanonicalName() == "CurrentChildren" {
			if !strings.Contains(v.Source.SQL, "WHERE enabled=1") || !strings.Contains(v.Source.SQL, "ProjectCurrentChildrenParentKeys($CurrentCarrier)") {
				t.Fatal("unbounded/improper immediate scope", v.Source.SQL)
			}
			found = true
		}
	}
	if !found {
		t.Fatal("missing derived physical Current")
	}
	if len(got.Currents) != 3 {
		t.Fatal("duplicate/missing bindings", got.Currents)
	}
	after, _ := json.Marshal(request)
	if string(after) != string(before) {
		t.Fatal("canonical source mutated")
	}
}
func TestBuildInputNestedExplicitAuxiliaryStructuralRejections(t *testing.T) {
	for _, tc := range []struct {
		name, want string
		change     func(*Request, *spec.View, *spec.View)
	}{
		{"nil relation", "generation contains a nil relation", func(r *Request, a, c *spec.View) { a.Relations = append(a.Relations, nil) }},
		{"nil relation view", "generation relation requires a view", func(r *Request, a, c *spec.View) { a.Relations[0].View = nil }},
		{"repeated physical role", "occurs in multiple roles", func(r *Request, a, c *spec.View) { a.Relations = append(a.Relations, a.Relations[0]) }},
		{"repeated auxiliary role", "multiple parent roles", func(r *Request, a, c *spec.View) {
			r.Component.RootView.Relations = append(r.Component.RootView.Relations, r.Component.RootView.Relations[0])
		}},
		{"missing physical key", "requires discovered primary keys", func(r *Request, a, c *spec.View) { c.Columns[0].PrimaryKey = false }},
		{"empty exact ViewBindings", "conflicts with authored binding", func(r *Request, a, c *spec.View) { r.ViewBindings = nil }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			request, a, c := nestedAuxiliaryRequest(t)
			tc.change(&request, a, c)
			_, err := (&Compiler{}).BuildInput(request, "RootBody")
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("wanted %q got %v", tc.want, err)
			}
		})
	}
}
func TestBuildInputNestedExplicitAuxiliaryDerivedViewSkipped(t *testing.T) {
	request, a, _ := nestedAuxiliaryRequest(t)
	a.Relations = append(a.Relations, &spec.Relation{Name: "Derived", Kind: spec.RelationKindDerived})
	got, err := (&Compiler{}).BuildInput(request, "RootBody")
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Currents) != 3 {
		t.Fatal("derived view changed Current ownership", got.Currents)
	}
}

func TestBuildInputNestedExplicitAuxiliaryConcurrentDetached(t *testing.T) {
	request, _, _ := nestedAuxiliaryRequest(t)
	before, _ := json.Marshal(request)
	done := make(chan error, 8)
	for i := 0; i < 8; i++ {
		go func() {
			got, err := (&Compiler{}).BuildInput(request, "RootBody")
			if err == nil && len(got.Currents) != 3 {
				err = fmt.Errorf("bindings: %v", got.Currents)
			}
			done <- err
		}()
	}
	for i := 0; i < 8; i++ {
		if err := <-done; err != nil {
			t.Error(err)
		}
	}
	after, _ := json.Marshal(request)
	if string(after) != string(before) {
		t.Fatal("shared authored request mutated")
	}
}
