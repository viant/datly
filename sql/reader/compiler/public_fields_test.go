package compiler

import (
	"github.com/viant/datly/data"
	"github.com/viant/datly/spec"
	"reflect"
	"testing"
)

func TestCompiledPublicNamesForNestedViews(t *testing.T) {
	type child struct {
		ParentID int    `sqlx:"parent_id"`
		VendorId int    `sqlx:"vendor_id"`
		Day7     int    `sqlx:"day_7" json:"day-7"`
		Hidden   string `sqlx:"hidden" json:"-"`
	}
	type parent struct {
		ID       int      `sqlx:"id"`
		Children []*child `json:"child-items"`
	}
	type output struct{ Data []*parent }
	for _, format := range []string{"", "lc"} {
		t.Run("case="+format, func(t *testing.T) {
			root := &spec.View{Name: "parents", Source: &spec.ViewSource{SQL: "SELECT id FROM parents"}, Relations: []*spec.Relation{{Name: "children", Holder: "Children", Cardinality: spec.CardinalityMany, View: &spec.View{Name: "children", Source: &spec.ViewSource{SQL: "SELECT parent_id,vendor_id,day_7,hidden FROM children"}}, On: []*spec.RelationLink{{ParentColumn: "id", ChildColumn: "parent_id"}}}}}
			plan, err := Compile(Input{Component: &spec.Component{Name: "Nested", RootView: root, Settings: &spec.Settings{CaseFormat: format}}, InputType: reflect.TypeFor[struct{}](), OutputType: reflect.TypeFor[output](), DirectViewField: "Data"})
			if err != nil {
				t.Fatal(err)
			}
			find := func(fields []data.SelectorField, name string) *data.SelectorField {
				for i := range fields {
					if fields[i].PublicName == name {
						return &fields[i]
					}
				}
				return nil
			}
			holder := find(plan.Root.View.SelectorFields, "child-items")
			if holder == nil || !holder.Holder || holder.GoName != "Children" {
				t.Fatalf("holder naming lost: %#v", plan.Root.View.SelectorFields)
			}
			childView := plan.Root.Relations[0].Target.View
			vendorName := "VendorId"
			if format == "lc" {
				vendorName = "vendorId"
			}
			vendor := find(childView.SelectorFields, vendorName)
			numeric := find(childView.SelectorFields, "day-7")
			if vendor == nil || vendor.Column != "vendor_id" || numeric == nil || numeric.Column != "day_7" || find(childView.SelectorFields, "Hidden") != nil {
				t.Fatalf("nested aliases/visibility incorrect: %#v", childView.SelectorFields)
			}
		})
	}
}
