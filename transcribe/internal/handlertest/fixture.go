// Package handlertest provides canonical writer graphs for transcription tests.
package handlertest

import (
	"testing"

	"github.com/viant/datly/spec"
	gen "github.com/viant/datly/transcribe/generate"
)

func Component() *spec.Component {
	return &spec.Component{
		Name: "Events",
		Parameters: []*spec.Parameter{
			{Name: "Events", Source: spec.BindSource{Kind: "body", Name: "Data"}, TypeExpr: "[]*EventsView", Cardinality: string(spec.CardinalityMany)},
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "body"}, TypeExpr: "[]*EventsView"},
			{Name: "Status", Source: spec.BindSource{Kind: "output", Name: "status"}, TypeExpr: "int", EmitOutput: true},
			{Name: "CurrentEvents", Source: spec.BindSource{Kind: "View", Name: "CurrentEvents"}, TypeExpr: "[]*CurrentEventsView", Cardinality: string(spec.CardinalityMany)},
		},
		RootView: &spec.View{Name: "Events", Source: &spec.ViewSource{Table: "EVENTS"}, Columns: []*spec.Column{
			{Name: "ID", Source: "ID", Type: spec.TypeRef{Name: "int64"}, PrimaryKey: true, AutoIncrement: true},
			{Name: "NAME", Source: "NAME", Type: spec.TypeRef{Name: "string"}},
		}},
		Views: []*spec.View{{
			Key: spec.Key{Kind: spec.KindView, Scope: "example.com/events", Name: "CurrentEvents"}, Name: "CurrentEvents", TypeName: "CurrentEventsView",
			Columns: []*spec.Column{
				{Name: "ID", Source: "ID", Type: spec.TypeRef{Name: "int64"}, PrimaryKey: true},
				{Name: "NAME", Source: "NAME", Type: spec.TypeRef{Name: "string"}},
			},
		}},
	}
}

func RecursiveComponent() (*spec.Component, *spec.View, *spec.View) {
	idType := spec.TypeRef{Name: "int64", Pointer: true}
	detail := &spec.View{
		Name: "Details", TypeName: "DetailsView", Source: &spec.ViewSource{Table: "DETAILS"},
		Columns: []*spec.Column{
			{Name: "ID", Source: "ID", Type: idType, PrimaryKey: true},
			{Name: "ITEM_ID", Source: "ITEM_ID", Type: spec.TypeRef{Name: "int64"}},
			{Name: "NOTE", Source: "NOTE", Type: spec.TypeRef{Name: "string"}},
		},
	}
	child := &spec.View{
		Name: "Items", TypeName: "ItemsView", Source: &spec.ViewSource{Table: "ITEMS"},
		Columns: []*spec.Column{
			{Name: "ID", Source: "ID", Type: idType, PrimaryKey: true},
			{Name: "ORDER_ID", Source: "ORDER_ID", Type: spec.TypeRef{Name: "int64"}},
			{Name: "NAME", Source: "NAME", Type: spec.TypeRef{Name: "string"}},
		},
		Relations: []*spec.Relation{{
			Name: "Details", Holder: "Details", Cardinality: spec.CardinalityMany, View: detail,
			On: []*spec.RelationLink{{ParentColumn: "ID", ChildColumn: "ITEM_ID"}},
		}},
	}
	root := &spec.View{
		Name: "Orders", TypeName: "OrdersView", Source: &spec.ViewSource{Table: "ORDERS"},
		Columns: []*spec.Column{
			{Name: "ID", Source: "ID", Type: idType, PrimaryKey: true},
			{Name: "NAME", Source: "NAME", Type: spec.TypeRef{Name: "string"}},
		},
		Relations: []*spec.Relation{{
			Name: "Items", Holder: "Items", Cardinality: spec.CardinalityMany, View: child,
			On: []*spec.RelationLink{{ParentColumn: "ID", ChildColumn: "ORDER_ID"}},
		}},
	}
	currentOrders := &spec.View{Name: "CurrentOrders", TypeName: "CurrentOrdersView", Columns: []*spec.Column{root.Columns[0].Clone(), root.Columns[1].Clone()}}
	currentItems := &spec.View{Name: "CurrentItems", TypeName: "CurrentItemsView", Columns: []*spec.Column{child.Columns[0].Clone(), child.Columns[1].Clone(), child.Columns[2].Clone()}}
	currentDetails := &spec.View{Name: "CurrentDetails", TypeName: "CurrentDetailsView", Columns: []*spec.Column{detail.Columns[0].Clone(), detail.Columns[1].Clone(), detail.Columns[2].Clone()}}
	return &spec.Component{
		Name: "Orders", RootView: root,
		Parameters: []*spec.Parameter{
			{Name: "Orders", Source: spec.BindSource{Kind: "body", Name: "Data"}, TypeExpr: "[]*OrdersView", Cardinality: string(spec.CardinalityMany)},
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "body"}, TypeExpr: "[]*OrdersView"},
			{Name: "CurrentOrders", Source: spec.BindSource{Kind: "View", Name: "CurrentOrders"}, TypeExpr: "[]*CurrentOrdersView", Cardinality: string(spec.CardinalityMany)},
			{Name: "CurrentItems", Source: spec.BindSource{Kind: "View", Name: "CurrentItems"}, TypeExpr: "[]*CurrentItemsView", Cardinality: string(spec.CardinalityMany)},
			{Name: "CurrentDetails", Source: spec.BindSource{Kind: "View", Name: "CurrentDetails"}, TypeExpr: "[]*CurrentDetailsView", Cardinality: string(spec.CardinalityMany)},
		},
		Views: []*spec.View{currentOrders, currentItems, currentDetails},
	}, child, detail
}

type Binding struct {
	Param int
	View  int
}

func Bindings(t *testing.T, component *spec.Component, bindings ...Binding) gen.ViewBindings {
	t.Helper()
	result := gen.ViewBindings{}
	for _, binding := range bindings {
		identity, err := component.Views[binding.View].Identity()
		if err != nil {
			t.Fatal(err)
		}
		result[component.Parameters[binding.Param].Identity()] = identity
	}
	return result
}
