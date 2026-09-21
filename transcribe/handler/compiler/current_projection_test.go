package compiler

import (
	"strings"
	"testing"

	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
)

func TestCurrentProjectionCompilesCanonicalAssignments(t *testing.T) {
	for _, tc := range []struct {
		name       string
		change     func(*spec.Component)
		fields     int
		current    string
		conversion plan.LinkConversion
		errorText  string
	}{
		{name: "all fields", fields: 2, current: "Name", conversion: plan.LinkDirect},
		{name: "identity only", fields: 1, change: func(c *spec.Component) { c.Views[0].Columns = c.Views[0].Columns[:1] }},
		{name: "renamed source", fields: 2, current: "PreviousName", conversion: plan.LinkDirect, change: func(c *spec.Component) { c.Views[0].Columns[1].Name = "previous_name" }},
		{name: "nullable entity", fields: 2, current: "Name", conversion: plan.LinkAddress, change: func(c *spec.Component) { c.RootView.Columns[1].Nullable = true }},
		{name: "nullable current", fields: 2, current: "Name", conversion: plan.LinkDereference, change: func(c *spec.Component) { c.Views[0].Columns[1].Nullable = true }},
		{name: "both nullable", fields: 2, current: "Name", conversion: plan.LinkDirect, change: func(c *spec.Component) {
			c.RootView.Columns[1].Nullable = true
			c.Views[0].Columns[1].Type.Pointer = true
		}},
		{name: "unrelated current field", fields: 2, current: "Name", conversion: plan.LinkDirect, change: func(c *spec.Component) {
			c.Views[0].Columns = append(c.Views[0].Columns, &spec.Column{Name: "unrelated", Type: spec.TypeRef{Name: "int"}})
		}},
		{name: "source conflict", errorText: "conflicts", change: func(c *spec.Component) { c.Views[0].Columns[1].Source = "other_name" }},
		{name: "ambiguous source", errorText: "ambiguous", change: func(c *spec.Component) {
			c.Views[0].Columns = append(c.Views[0].Columns, &spec.Column{Name: "alias", Source: "NAME", Type: spec.TypeRef{Name: "string"}})
		}},
		{name: "different type", errorText: "cannot project", change: func(c *spec.Component) { c.Views[0].Columns[1].Type.Name = "int" }},
		{name: "different package", errorText: "cannot project", change: func(c *spec.Component) { c.Views[0].Columns[1].Type.Package = "example.com/other" }},
		{name: "different cardinality", errorText: "cannot project", change: func(c *spec.Component) { c.Views[0].Columns[1].Type.Cardinality = spec.CardinalityMany }},
		{name: "duplicate destination", errorText: "duplicate entity", change: func(c *spec.Component) {
			c.RootView.Columns = append(c.RootView.Columns, c.RootView.Columns[1].Clone())
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			component := testComponent()
			if tc.change != nil {
				tc.change(component)
			}
			compiled, err := (&Compiler{}).Compile(Request{Component: component, Operation: plan.OperationPatch, Current: "CurrentEvents", ViewBindings: testViewBindings(t, component, viewBindingIndex{param: 2, view: 0})})
			if tc.errorText != "" {
				if err == nil || !strings.Contains(err.Error(), tc.errorText) {
					t.Fatalf("error = %v, want %s", err, tc.errorText)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			fields := compiled.Root.Current.Fields
			if len(fields) != tc.fields || fields[0].Entity.Field != "Id" || fields[0].Current.Field != "Id" {
				t.Fatalf("fields = %+v", fields)
			}
			if tc.fields > 1 && (fields[1].Entity.Field != "Name" || fields[1].Current.Field != tc.current || fields[1].Conversion != tc.conversion) {
				t.Fatalf("field = %+v", fields[1])
			}
			clone := compiled.Clone()
			clone.Root.Current.Fields[0].Current.Field = "changed"
			if compiled.Root.Current.Fields[0].Current.Field != "Id" {
				t.Fatal("clone shares projection metadata")
			}
		})
	}
}

func TestCurrentSourceOutputsPreservesOuterAliases(t *testing.T) {
	actual := currentSourceOutputs(`SELECT orders.ID AS RootKey, orders.NAME FROM (SELECT o.* FROM ORDERS o) orders`)
	if actual["id"] != "RootKey" || actual["rootkey"] != "RootKey" || actual["name"] != "NAME" {
		t.Fatalf("outputs = %+v", actual)
	}
}

func TestCurrentSourceOutputsPreservesAliasesAfterTemplatePrelude(t *testing.T) {
	actual := currentSourceOutputs("#set($X = 1)\nSELECT orders.ID AS RootKey FROM (SELECT o.* FROM ORDERS o) orders")
	if actual["id"] != "RootKey" {
		t.Fatalf("outputs = %+v", actual)
	}
}

func TestCurrentProjectionKeepsCompositeChildAndRootAuthority(t *testing.T) {
	component, item, detail := recursiveComponent()
	for index, view := range []*spec.View{component.RootView, item, detail} {
		key := &spec.Column{Name: "TENANT_ID", Source: "TENANT_ID", PrimaryKey: true, Type: spec.TypeRef{Name: "int64"}}
		view.Columns = append([]*spec.Column{key}, view.Columns...)
		component.Views[index].Columns = append([]*spec.Column{key.Clone()}, component.Views[index].Columns...)
		component.Views[index].Columns[0].Name = "current_tenant"
	}
	itemID, _ := item.Identity()
	detailID, _ := detail.Identity()
	compiled, err := (&Compiler{}).Compile(Request{
		Component: component, Operation: plan.OperationPatch, Current: "CurrentOrders",
		Currents:     []CurrentBinding{{ViewIdentity: itemID, Param: "CurrentItems"}, {ViewIdentity: detailID, Param: "CurrentDetails"}},
		ViewBindings: testViewBindings(t, component, viewBindingIndex{param: 2, view: 0}, viewBindingIndex{param: 3, view: 1}, viewBindingIndex{param: 4, view: 2}),
	})
	if err != nil {
		t.Fatal(err)
	}
	for index, record := range []*plan.RecordPlan{compiled.Root, compiled.Root.Relations[0].Child, compiled.Root.Relations[0].Child.Relations[0].Child} {
		current := record.Current
		if current.ViewIdentity == "" || current.Keys[0].Field != "CurrentTenant" || current.Keys[1].Field != "Id" {
			t.Fatalf("role %d keys: %+v", index, current)
		}
		if current.Fields[0].Entity.Field != "TenantId" || current.Fields[0].Current.Field != "CurrentTenant" || current.Fields[1].Entity.Field != "Id" {
			t.Fatalf("role %d fields: %+v", index, current.Fields)
		}
		if index > 0 && len(current.Fields) != 3 {
			t.Fatalf("child %d lost loaded parent-link mapping: %+v", index, current.Fields)
		}
	}
}
