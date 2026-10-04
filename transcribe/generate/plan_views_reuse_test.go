package generate

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
)

func sharedPayloadComponent() (*spec.Component, *spec.View) {
	payload := func(name string) *spec.View {
		return &spec.View{Name: name, Namespace: name, TypeName: "PayloadView", Source: &spec.ViewSource{Table: "payload", SQL: "SELECT id, body FROM payload WHERE kind='" + name + "'"}, Columns: []*spec.Column{
			{Name: "id", Source: "id", Type: spec.TypeRef{Name: "string"}},
			{Name: "body", Source: "body", Type: spec.TypeRef{Name: "string", Pointer: true}},
		}}
	}
	first, second := payload("request"), payload("response")
	root := &spec.View{Name: "messages", Namespace: "m", Source: &spec.ViewSource{Table: "message", SQL: "SELECT id FROM message"}, Columns: []*spec.Column{{Name: "id", Type: spec.TypeRef{Name: "string"}}}}
	for _, child := range []*spec.View{first, second} {
		root.Relations = append(root.Relations, &spec.Relation{Name: child.Name, Holder: child.Name, View: child, Cardinality: spec.CardinalityOne, On: []*spec.RelationLink{{ParentNamespace: "m", ParentColumn: "id", ChildNamespace: child.Name, ChildColumn: "id"}}})
	}
	return &spec.Component{Name: "Messages", RootView: root}, second
}

func TestResolvePlan_ReusesExplicitIdenticalLeafTypes(t *testing.T) {
	component, _ := sharedPayloadComponent()
	plan, err := New(Input{Component: component, TargetPackage: "example.com/app/read", SQLResources: true}).Plan()
	require.NoError(t, err)
	require.Len(t, plan.Views, 2)
	require.Equal(t, "PayloadView", plan.Views[1].Name)
	require.Equal(t, "*PayloadView", plan.Views[0].Fields[1].Type)
	require.Equal(t, "*PayloadView", plan.Views[0].Fields[2].Type)
	require.Contains(t, plan.Views[0].Fields[1].Tag, "request.id")
	require.Contains(t, plan.Views[0].Fields[2].Tag, "response.id")
	require.Equal(t, 1, strings.Count(viewFile("read", plan), "type PayloadView struct"))
	require.NotNil(t, plan.Resources)
	resources := map[string]string{}
	for _, file := range plan.Resources.Files {
		resources[file.Path] = file.Content
	}
	require.Contains(t, resources["sql/request.sql"], "kind='request'")
	require.Contains(t, resources["sql/response.sql"], "kind='response'")
}

func TestResolvePlan_RejectsDifferentExplicitLeafShapes(t *testing.T) {
	for name, change := range map[string]func(*spec.View){
		"field type":     func(v *spec.View) { v.Columns[1].Type.Name = "int" },
		"nullability":    func(v *spec.View) { v.Columns[1].Type.Pointer = false },
		"field name":     func(v *spec.View) { v.Columns[1].Name = "content" },
		"column mapping": func(v *spec.View) { v.Columns[1].Source = "content" },
		"JSON tag":       func(v *spec.View) { v.Columns[1].Tag = `json:"content"` },
		"destination":    func(v *spec.View) { v.Dest = "other.go" },
	} {
		t.Run(name, func(t *testing.T) {
			component, second := sharedPayloadComponent()
			change(second)
			_, err := New(Input{Component: component}).Plan()
			require.ErrorContains(t, err, "map to type")
		})
	}
	component, _ := sharedPayloadComponent()
	identity, err := component.RootView.Relations[0].View.Identity()
	require.NoError(t, err)
	_, err = New(Input{Component: component, SetMarkerViews: map[string]bool{identity: true}}).Plan()
	require.ErrorContains(t, err, "map to type")
}

func TestResolvePlan_ReusesReadOnlyLeavesBesideWritableRoot(t *testing.T) {
	component, _ := sharedPayloadComponent()
	identity, err := component.RootView.Identity()
	require.NoError(t, err)
	plan, err := New(Input{Component: component, SetMarkerViews: map[string]bool{identity: true}}).Plan()
	require.NoError(t, err)
	require.Len(t, plan.Views, 2)
	require.Equal(t, "PayloadView", plan.Views[1].Name)
	require.Equal(t, 1, strings.Count(viewFile("read", plan), "type PayloadView struct"))
	for _, field := range plan.Views[1].Fields {
		require.NotEqual(t, "Has", field.Name, "read-only shared type must not acquire mutation presence")
	}
}
