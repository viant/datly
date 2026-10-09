package bootstrap

import (
	"github.com/viant/datly/spec"
	dtag "github.com/viant/datly/tag"
	"reflect"
	"strings"
	"testing"
)

type handlerReuseInput struct {
	Rows    []*packageViewRow `parameter:"Rows,kind=body,in=data" view:"rows,finiteReconciliation=eyJtb2RlIjoic2FtZS1wYXJlbnQtcm9vdC1maXJzdCIsInJvbGVzIjpbeyJob2xkZXIiOiJMaW5lcyIsImZpZWxkcyI6WyJQYXJlbnRJZCJdfV19"`
	Current []*packageViewRow `parameter:"Current,kind=view,in=Current" view:"Current,table=records" sql:"SELECT id, name FROM records WHERE id = :ID"`
	ID      int               `parameter:"ID,kind=query,in=id"`
}

func TestContractResolverHandlerBodyRetainsReadDependencies(t *testing.T) {
	c, err := (ContractResolver{Component: &spec.Component{Routes: []*spec.Route{{Method: "POST", Path: "/prepare", Handler: "example.Prepare"}}}, InputType: linkedContractType(reflect.TypeFor[handlerReuseInput]())}).Resolve()
	if err != nil {
		t.Fatal(err)
	}
	if c.RootView != nil || len(c.Views) != 1 || c.Views[0].Name != "Current" || len(c.Parameters) != 3 {
		t.Fatalf("body became writer or lost bindings: %+v", c)
	}
	if c.Views[0].Source.SQL != "SELECT id, name FROM records WHERE id = :ID" {
		t.Fatal("read SQL changed")
	}
}
func TestHandlerBodyReconciliationOwnershipGuards(t *testing.T) {
	policy := &spec.Reconciliation{Mode: "same-parent-root-first", Roles: []spec.ReconciliationRole{{Holder: "Lines", Fields: []string{"ParentId"}}}}
	for _, tc := range []struct {
		name           string
		c              *spec.Component
		wantError      bool
		wantRootPolicy bool
	}{
		{"explicit handler", &spec.Component{Routes: []*spec.Route{{Handler: "Prepare"}}}, false, false},
		{"no route", &spec.Component{}, true, false},
		{"unowned route", &spec.Component{Routes: []*spec.Route{{}}}, true, false},
		{"mixed routes", &spec.Component{Routes: []*spec.Route{{Handler: "Prepare"}, {}}}, true, false},
		{"nil route", &spec.Component{Routes: []*spec.Route{nil}}, true, false},
		{"handler with mutation", &spec.Component{Routes: []*spec.Route{{Handler: "Prepare"}}, Settings: &spec.Settings{Mutation: "patch"}}, true, false},
		{"actual mutation root", &spec.Component{RootView: &spec.View{Name: "records"}, Settings: &spec.Settings{Mutation: "patch"}}, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := &packageComponentResolver{component: tc.c}
			err := r.applyInput(&resolvedContractField{param: &spec.Parameter{Name: "Rows", Source: spec.BindSource{Kind: "body", Name: "data"}}, metadata: &dtag.Field{View: &dtag.View{Reconciliation: policy}}})
			if (err != nil) != tc.wantError {
				t.Fatalf("error=%v expected error=%v", err, tc.wantError)
			}
			if err != nil && !strings.Contains(err.Error(), "requires a mutation root") {
				t.Fatal(err)
			}
			if tc.wantRootPolicy && (tc.c.RootView.Reconciliation == nil || tc.c.RootView.Reconciliation == policy) {
				t.Fatal("actual writer policy must be retained and cloned")
			}
		})
	}
}
