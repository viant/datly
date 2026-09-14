package compiler

import (
	"strings"
	"testing"

	"github.com/viant/datly/spec"
	handlerast "github.com/viant/datly/transcribe/handler/ast"
)

func TestCompilerBuildsRecursivePatchPlan(t *testing.T) {
	component, item, detail := recursiveComponent()
	itemIdentity, _ := item.Identity()
	detailIdentity, _ := detail.Identity()
	plan, err := (&Compiler{}).Compile(Request{
		Component: component,
		ViewBindings: testViewBindings(t, component,
			viewBindingIndex{param: 2, view: 0},
			viewBindingIndex{param: 3, view: 1},
			viewBindingIndex{param: 4, view: 2}),
		Operation: handlerast.OperationPatch, Current: "CurrentOrders",
		Currents: []CurrentBinding{
			{ViewIdentity: itemIdentity, Param: "CurrentItems"},
			{ViewIdentity: detailIdentity, Param: "CurrentDetails"},
		},
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	if len(plan.Root.Relations) != 1 || plan.Root.Relations[0].Child == nil {
		t.Fatalf("root relations = %+v", plan.Root.Relations)
	}
	items := plan.Root.Relations[0]
	if strings.Join(items.Child.Sequence.Selector, "/") != "Items/Id" || strings.Join(items.Child.Sequence.Destination, ".") != "Input.Orders" {
		t.Fatalf("item sequence = %+v", items.Child.Sequence)
	}
	if len(items.Links) != 1 || items.Links[0].Parent.Field != "Id" || items.Links[0].Child.Field != "OrderId" || items.Links[0].Conversion != handlerast.LinkDereference {
		t.Fatalf("item links = %+v", items.Links)
	}
	if items.Child.Current == nil || strings.Join(items.Child.Current.InputPath, ".") != "Input.CurrentItems" {
		t.Fatalf("item current = %+v", items.Child.Current)
	}
	if len(items.Child.Relations) != 1 {
		t.Fatalf("item relations = %+v", items.Child.Relations)
	}
	details := items.Child.Relations[0]
	if strings.Join(details.Child.Sequence.Selector, "/") != "Items/Details/Id" || details.Child.Write.Order != 2 {
		t.Fatalf("detail plan = %+v", details.Child)
	}
	if plan.Root.Write.Order != 0 || items.Child.Write.Order != 1 {
		t.Fatalf("write order root=%d item=%d", plan.Root.Write.Order, items.Child.Write.Order)
	}
}

func TestCompilerRequiresExplicitNestedCurrentBinding(t *testing.T) {
	component, _, _ := recursiveComponent()
	_, err := (&Compiler{}).Compile(Request{
		Component: component, ViewBindings: testViewBindings(t, component, viewBindingIndex{param: 2, view: 0}),
		Operation: handlerast.OperationPatch, Current: "CurrentOrders",
	})
	if err == nil || !strings.Contains(err.Error(), "requires an explicit current binding") {
		t.Fatalf("Compile() error = %v", err)
	}
}

func TestCompilerRejectsUnusedCurrentBinding(t *testing.T) {
	component := testComponent()
	component.Parameters = append(component.Parameters, &spec.Parameter{
		Name: "CurrentUnused", Source: spec.BindSource{Kind: "view", Name: "CurrentUnused"},
		TypeExpr: "[]*Event", Cardinality: string(spec.CardinalityMany),
	})
	component.Views = append(component.Views, cloneCurrentView("CurrentUnused", component.RootView))
	_, err := (&Compiler{}).Compile(Request{
		Component: component,
		ViewBindings: testViewBindings(t, component,
			viewBindingIndex{param: 2, view: 0}, viewBindingIndex{param: 3, view: 1}),
		Operation: handlerast.OperationPatch, Current: "CurrentEvents",
		Currents: []CurrentBinding{{ViewIdentity: "view::missing|namespace:", Param: "CurrentUnused"}},
	})
	if err == nil || !strings.Contains(err.Error(), "unused") {
		t.Fatalf("Compile() error = %v", err)
	}
}

func TestCompilerRejectsCurrentParameterReusedAcrossWritableViews(t *testing.T) {
	component, item, detail := recursiveComponent()
	itemIdentity, _ := item.Identity()
	detailIdentity, _ := detail.Identity()
	_, err := (&Compiler{}).Compile(Request{
		Component: component,
		ViewBindings: testViewBindings(t, component,
			viewBindingIndex{param: 2, view: 0}, viewBindingIndex{param: 3, view: 1}),
		Operation: handlerast.OperationPatch, Current: "CurrentOrders",
		Currents: []CurrentBinding{
			{ViewIdentity: itemIdentity, Param: "CurrentItems"},
			{ViewIdentity: detailIdentity, Param: "CurrentItems"},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "bound to more than one writable view") {
		t.Fatalf("Compile() error = %v", err)
	}
}

func TestCompilerRejectsRootCurrentReusedByNestedWritableView(t *testing.T) {
	component, item, _ := recursiveComponent()
	itemIdentity, _ := item.Identity()
	_, err := (&Compiler{}).Compile(Request{
		Component: component,
		ViewBindings: testViewBindings(t, component,
			viewBindingIndex{param: 2, view: 0}, viewBindingIndex{param: 4, view: 2}),
		Operation: handlerast.OperationPatch, Current: "CurrentOrders",
		Currents: []CurrentBinding{
			{ViewIdentity: itemIdentity, Param: "CurrentOrders"},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "bound to more than one writable view") {
		t.Fatalf("Compile() error = %v", err)
	}
}

func TestCompilerRejectsNestedCurrentWithoutExactViewBinding(t *testing.T) {
	component, item, detail := recursiveComponent()
	itemIdentity, _ := item.Identity()
	detailIdentity, _ := detail.Identity()
	_, err := (&Compiler{}).Compile(Request{
		Component: component,
		ViewBindings: testViewBindings(t, component,
			viewBindingIndex{param: 2, view: 0}, viewBindingIndex{param: 4, view: 2}),
		Operation: handlerast.OperationPatch, Current: "CurrentOrders",
		Currents: []CurrentBinding{
			{ViewIdentity: itemIdentity, Param: "CurrentItems"},
			{ViewIdentity: detailIdentity, Param: "CurrentDetails"},
		},
	})
	if err == nil || !strings.Contains(err.Error(), `current-state input "CurrentItems" requires an exact canonical view binding`) {
		t.Fatalf("Compile() error = %v", err)
	}
}

func TestCompilerRejectsSameNameCurrentWithoutExactViewBinding(t *testing.T) {
	component := testComponent()
	_, err := (&Compiler{}).Compile(Request{
		Component: component, Operation: handlerast.OperationPatch, Current: "CurrentEvents",
	})
	if err == nil || !strings.Contains(err.Error(), "requires an exact canonical view binding") {
		t.Fatalf("Compile() error = %v", err)
	}
}

func TestCompilerRejectsWriteRelationCycle(t *testing.T) {
	component, item, _ := recursiveComponent()
	component.RootView.Relations[0].View = item
	item.Relations = []*spec.Relation{{
		Name: "Orders", Holder: "Orders", Cardinality: spec.CardinalityMany, View: component.RootView,
		On: []*spec.RelationLink{{ParentColumn: "ORDER_ID", ChildColumn: "ID"}},
	}}
	_, err := (&Compiler{}).Compile(Request{Component: component, Operation: handlerast.OperationPost})
	if err == nil || !strings.Contains(err.Error(), "cycle") {
		t.Fatalf("Compile() error = %v", err)
	}
}

func TestCompilerSkipsSummaryRelationsForWrites(t *testing.T) {
	component := testComponent()
	component.RootView.Relations = []*spec.Relation{{
		Name: "Totals", Holder: "Totals", Kind: spec.RelationKindDerived,
		View: &spec.View{Name: "Totals", Source: &spec.ViewSource{SQL: "SELECT COUNT(*)"}},
	}}
	plan, err := (&Compiler{}).Compile(Request{Component: component, Operation: handlerast.OperationPost})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	if len(plan.Root.Relations) != 0 {
		t.Fatalf("write plan included summary relations: %+v", plan.Root.Relations)
	}
}

func TestCompilerPreservesCompoundRelationLinkOrder(t *testing.T) {
	component, item, _ := recursiveComponent()
	component.RootView.Columns = append(component.RootView.Columns, &spec.Column{Name: "TENANT_ID", Source: "TENANT_ID", Type: spec.TypeRef{Name: "int64"}})
	item.Columns = append(item.Columns, &spec.Column{Name: "TENANT_ID", Source: "TENANT_ID", Type: spec.TypeRef{Name: "int64"}})
	component.RootView.Relations[0].On = append(component.RootView.Relations[0].On,
		&spec.RelationLink{ParentColumn: "TENANT_ID", ChildColumn: "TENANT_ID"})
	plan, err := (&Compiler{}).Compile(Request{Component: component, Operation: handlerast.OperationPost})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	links := plan.Root.Relations[0].Links
	if len(links) != 2 || links[0].Parent.Field != "Id" || links[0].Child.Field != "OrderId" ||
		links[1].Parent.Field != "TenantId" || links[1].Child.Field != "TenantId" {
		t.Fatalf("compound links = %+v", links)
	}
}

func recursiveComponent() (*spec.Component, *spec.View, *spec.View) {
	intType := spec.TypeRef{Name: "int64"}
	ptrIntType := spec.TypeRef{Name: "int64", Pointer: true}
	detail := &spec.View{
		Name: "Details", Source: &spec.ViewSource{Table: "DETAILS"},
		Columns: []*spec.Column{
			{Name: "ID", Source: "ID", Type: ptrIntType, PrimaryKey: true},
			{Name: "ITEM_ID", Source: "ITEM_ID", Type: intType},
		},
	}
	item := &spec.View{
		Name: "Items", Source: &spec.ViewSource{Table: "ITEMS"},
		Columns: []*spec.Column{
			{Name: "ID", Source: "ID", Type: ptrIntType, PrimaryKey: true},
			{Name: "ORDER_ID", Source: "ORDER_ID", Type: intType},
		},
		Relations: []*spec.Relation{{
			Name: "Details", Holder: "Details", Cardinality: spec.CardinalityMany, View: detail,
			On: []*spec.RelationLink{{ParentColumn: "ID", ChildColumn: "ITEM_ID"}},
		}},
	}
	root := &spec.View{
		Name: "Orders", Source: &spec.ViewSource{Table: "ORDERS"},
		Columns: []*spec.Column{{Name: "ID", Source: "ID", Type: ptrIntType, PrimaryKey: true}},
		Relations: []*spec.Relation{{
			Name: "Items", Holder: "Items", Cardinality: spec.CardinalityMany, View: item,
			On: []*spec.RelationLink{{ParentColumn: "ID", ChildColumn: "ORDER_ID"}},
		}},
	}
	currentOrders := cloneCurrentView("CurrentOrders", root)
	currentItems := cloneCurrentView("CurrentItems", item)
	currentDetails := cloneCurrentView("CurrentDetails", detail)
	component := &spec.Component{
		Name: "Orders", RootView: root,
		Parameters: []*spec.Parameter{
			{Name: "Orders", Source: spec.BindSource{Kind: "body", Name: "Data"}, TypeExpr: "[]*Order", Cardinality: string(spec.CardinalityMany)},
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "body"}, TypeExpr: "[]*Order"},
			{Name: "CurrentOrders", Source: spec.BindSource{Kind: "view", Name: "CurrentOrders"}, TypeExpr: "[]*Order", Cardinality: string(spec.CardinalityMany)},
			{Name: "CurrentItems", Source: spec.BindSource{Kind: "view", Name: "CurrentItems"}, TypeExpr: "[]*Item", Cardinality: string(spec.CardinalityMany)},
			{Name: "CurrentDetails", Source: spec.BindSource{Kind: "view", Name: "CurrentDetails"}, TypeExpr: "[]*Detail", Cardinality: string(spec.CardinalityMany)},
		},
		Views: []*spec.View{currentOrders, currentItems, currentDetails},
	}
	return component, item, detail
}

func cloneCurrentView(name string, source *spec.View) *spec.View {
	result := &spec.View{Name: name}
	for _, column := range source.Columns {
		result.Columns = append(result.Columns, column.Clone())
	}
	return result
}
