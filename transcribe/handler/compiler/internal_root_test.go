package compiler

import (
	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
	"testing"
)

func TestBuildInputInternalDeleteRoot(t *testing.T) {
	for _, operation := range []plan.Operation{plan.OperationPut, plan.OperationPatch} {
		t.Run(string(operation), func(t *testing.T) {
			root := &spec.View{Name: "Views", Source: &spec.ViewSource{Table: "CI_GRID_USER_VIEW", SQL: "SELECT ID FROM CI_GRID_USER_VIEW"}, Columns: []*spec.Column{{Name: "ID", Source: "ID", PrimaryKey: true, Type: spec.TypeRef{Name: "int"}}}}
			internal := &spec.Parameter{Name: "Delete", Source: spec.BindSource{Kind: "internal"}, TypeExpr: "[]*DeleteView", Cardinality: "Many"}
			component := &spec.Component{Name: "Viewsdelete", Routes: []*spec.Route{{Method: "DELETE", Path: "/views/{id}"}}, RootView: root, Parameters: []*spec.Parameter{internal}}
			got, err := (&Compiler{}).BuildInput(Request{Component: component, Operation: operation}, "DeleteView")
			if err != nil {
				t.Fatal(err)
			}
			if got.Input != "Delete" {
				t.Fatalf("mutation root = %q; expected authored internal root", got.Input)
			}
			for _, p := range got.Component.Parameters {
				if p.Source.Kind == "body" {
					t.Fatal("internal DELETE synthesized a transport body")
				}
			}
			selected, err := (&compiler{}).selectInput(got.Component, "Delete")
			if err != nil || selected == nil || selected.Source.Kind != "internal" {
				t.Fatalf("internal input selection: %v, %v", selected, err)
			}
		})
	}
}

func TestInternalDeleteRootRejectsInvalidScopes(t *testing.T) {
	for _, tc := range []struct {
		name, method, operation, source string
		body, required                  bool
	}{
		{name: "POST operation", method: "DELETE", operation: "post"},
		{name: "PATCH route", method: "PATCH", operation: "patch"},
		{name: "missing route", operation: "put"},
		{name: "body conflict", method: "DELETE", operation: "put", body: true},
		{name: "transport name", method: "DELETE", operation: "put", source: "data"},
		{name: "required", method: "DELETE", operation: "put", required: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			required := tc.required
			c := &spec.Component{RootView: &spec.View{Name: "Views"}, Parameters: []*spec.Parameter{{Name: "Delete", TypeExpr: "[]*DeleteView", Source: spec.BindSource{Kind: "internal", Name: tc.source}, Required: &required}}}
			if tc.method != "" {
				c.Routes = []*spec.Route{{Method: tc.method}}
			}
			if tc.body {
				c.Parameters = append(c.Parameters, &spec.Parameter{Name: "Body", Source: spec.BindSource{Kind: "body"}})
			}
			if _, err := (&Compiler{}).BuildInput(Request{Component: c, Operation: plan.Operation(tc.operation)}, "DeleteView"); err == nil {
				t.Fatal("invalid internal mutation root accepted")
			}
		})
	}
}
