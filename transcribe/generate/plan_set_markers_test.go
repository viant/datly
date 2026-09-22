package generate

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/spec"
)

func TestGeneratorEmitsSetMarkersForGeneratedWritableViews(t *testing.T) {
	component, root, child := setMarkerComponent()
	rootIdentity, err := root.Identity()
	if err != nil {
		t.Fatal(err)
	}
	childIdentity, err := child.Identity()
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	result, err := New(Input{
		Component:      component,
		SetMarkerViews: map[string]bool{rootIdentity: true, childIdentity: true},
	}).Generate(dir)
	if err != nil {
		t.Fatalf("Generate() error = %v", err)
	}
	rootPlan := generatedViewByIdentity(result.Plan, rootIdentity)
	childPlan := generatedViewByIdentity(result.Plan, childIdentity)
	if rootPlan == nil || !reflect.DeepEqual(rootPlan.SetMarkerFields, []string{"Id", "Name", "Items"}) ||
		childPlan == nil || !reflect.DeepEqual(childPlan.SetMarkerFields, []string{"Id", "OrderId"}) {
		t.Fatalf("root = %+v, child = %+v", rootPlan, childPlan)
	}
	if marker := viewPlanFieldType(rootPlan, "Has"); marker != "*OrdersViewHas" {
		t.Fatalf("root marker = %q", marker)
	}
	content, err := os.ReadFile(filepath.Join(dir, result.Plan.ViewDest))
	if err != nil {
		t.Fatal(err)
	}
	text := string(content)
	for _, expected := range []string{
		`Has *OrdersViewHas ` + "`" + `setMarker:"true" format:"-" sqlx:"-" diff:"-" json:"-" typeName:"OrdersViewHas"` + "`",
		"type OrdersViewHas struct {\n\tId bool\n\tName bool\n\tItems bool\n}",
		"type ItemsViewHas struct {\n\tId bool\n\tOrderId bool\n}",
	} {
		if !containsNormalized(text, expected) {
			t.Fatalf("generated view source does not contain %q:\n%s", expected, text)
		}
	}
}

func TestApplySetMarkerViewsLeavesLinkedAuthorityUntouched(t *testing.T) {
	component, root, child := setMarkerComponent()
	identity, err := root.Identity()
	if err != nil {
		t.Fatal(err)
	}
	plan := &Plan{Views: []ViewPlan{{Identity: identity, Name: "Orders", Type: "models.Orders", Ownership: ViewLinked}}}
	childIdentity, err := child.Identity()
	if err != nil {
		t.Fatal(err)
	}
	resolver := &planResolver{plan: plan, input: Input{Component: component, SetMarkerViews: map[string]bool{identity: true, childIdentity: true}}}
	if err = resolver.applySetMarkerViews(); err != nil {
		t.Fatalf("planResolver.applySetMarkerViews() error = %v", err)
	}
	if len(plan.Views[0].Fields) != 0 || len(plan.Views[0].SetMarkerFields) != 0 {
		t.Fatalf("linked plan = %+v", plan.Views[0])
	}
}

func TestApplySetMarkerViewsRejectsUnknownIdentityAndFieldCollision(t *testing.T) {
	component, root, _ := setMarkerComponent()
	resolver := &planResolver{plan: &Plan{}, input: Input{Component: component, SetMarkerViews: map[string]bool{"view:missing": true}}}
	if err := resolver.applySetMarkerViews(); err == nil || !strings.Contains(err.Error(), "not canonical") {
		t.Fatalf("unknown identity error = %v", err)
	}
	root.Columns = append(root.Columns, &spec.Column{Name: "Has", Type: spec.TypeRef{Name: "bool"}})
	identity, err := root.Identity()
	if err != nil {
		t.Fatal(err)
	}
	plan, err := New(Input{Component: component}).Plan()
	if err != nil {
		t.Fatal(err)
	}
	resolver = &planResolver{plan: plan, input: Input{Component: component, SetMarkerViews: map[string]bool{identity: true}}}
	if err = resolver.applySetMarkerViews(); err == nil || !strings.Contains(err.Error(), "collides with field Has") {
		t.Fatalf("collision error = %v", err)
	}
}

func TestApplySetMarkerViewsIncludesTransientParentRelationKey(t *testing.T) {
	component, root, child := setMarkerComponent()
	root.Relations[0].On = []*spec.RelationLink{{ParentColumn: "INTERNAL", ChildColumn: "ORDER_ID"}}
	rootIdentity, err := root.Identity()
	if err != nil {
		t.Fatal(err)
	}
	childIdentity, err := child.Identity()
	if err != nil {
		t.Fatal(err)
	}
	plan, err := New(Input{Component: component}).Plan()
	if err != nil {
		t.Fatal(err)
	}
	resolver := &planResolver{plan: plan, input: Input{Component: component, SetMarkerViews: map[string]bool{rootIdentity: true, childIdentity: true}}}
	if err = resolver.applySetMarkerViews(); err != nil {
		t.Fatal(err)
	}
	rootPlan := generatedViewByIdentity(plan, rootIdentity)
	if rootPlan == nil || !reflect.DeepEqual(rootPlan.SetMarkerFields, []string{"Id", "Name", "Internal", "Items"}) {
		t.Fatalf("root marker fields = %+v", rootPlan)
	}
}

func setMarkerComponent() (*spec.Component, *spec.View, *spec.View) {
	child := &spec.View{
		Name: "Items", TypeName: "ItemsView",
		Columns: []*spec.Column{
			{Name: "ID", Type: spec.TypeRef{Name: "int64"}, PrimaryKey: true},
			{Name: "ORDER_ID", Type: spec.TypeRef{Name: "int64"}},
		},
	}
	root := &spec.View{
		Name: "Orders", TypeName: "OrdersView",
		Columns: []*spec.Column{
			{Name: "ID", Type: spec.TypeRef{Name: "int64"}, PrimaryKey: true},
			{Name: "NAME", Type: spec.TypeRef{Name: "string"}},
			{Name: "INTERNAL", Type: spec.TypeRef{Name: "string"}, Tag: `sqlx:"-"`},
		},
		Relations: []*spec.Relation{{Name: "Items", Holder: "Items", Cardinality: spec.CardinalityMany, View: child}},
	}
	return &spec.Component{
		Name: "Orders", RootView: root,
		Parameters: []*spec.Parameter{{Name: "Orders", Source: spec.BindSource{Kind: "body", Name: "Data"}, TypeExpr: "[]*OrdersView"}},
	}, root, child
}

func generatedViewByIdentity(plan *Plan, identity string) *ViewPlan {
	if plan == nil {
		return nil
	}
	for index := range plan.Views {
		if plan.Views[index].Identity == identity {
			return &plan.Views[index]
		}
	}
	return nil
}

func viewPlanFieldType(plan *ViewPlan, name string) string {
	if plan == nil {
		return ""
	}
	for _, field := range plan.Fields {
		if field.Name == name {
			return field.Type
		}
	}
	return ""
}
