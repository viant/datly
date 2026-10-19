package compiler

import (
	"github.com/viant/datly/data"
	graphModel "github.com/viant/datly/internal/readergraph/model"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"reflect"
	"strings"
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

// Canonical projection names and physical sources are independent of public Go names.
func TestCompiledPublicNamesForCanonicalColumns(t *testing.T) {
	type row struct {
		Category   *int `sqlx:"category" source:"TYPE" json:"category"`
		CategoryID *int `sqlx:"category_id" source:"CATEGORY" selectorAlias:"categoryId" json:"categoryId"`
	}
	type result struct {
		Data []*row `json:"data"`
	}
	for _, tc := range []struct {
		name          string
		columns       []*data.Column
		authoritative bool
		sources       []string
	}{
		{"canonical result before physical collision", []*data.Column{{Name: "category", Column: "TYPE"}, {Name: "category_id", Column: "CATEGORY"}}, true, []string{"category", "category_id"}},
		{"canonical physical fallback", []*data.Column{{Name: "category_result", Column: "category"}, {Name: "id_result", Column: "category_id"}}, true, []string{"category_result", "id_result"}},
		{"native Go names", []*data.Column{{Name: "Category", Column: "category"}, {Name: "CategoryID", Column: "category_id"}}, false, []string{"category", "category_id"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			component := &spec.Component{Name: "Categories"}
			if tc.authoritative {
				component.ReaderGraph = &graphModel.Authority{}
			}
			view := &data.View{Columns: tc.columns}
			compiler := &planCompiler{input: Input{Component: component, OutputType: reflect.TypeFor[result]()}}
			err := compiler.compilePublicFields(&viewSet{root: view, rowTypes: map[*data.View]reflect.Type{view: reflect.TypeFor[row]()}}, "Data")
			if err != nil {
				t.Fatal(err)
			}
			if len(view.SelectorFields) != 2 {
				t.Fatalf("public fields = %#v", view.SelectorFields)
			}
			for i, field := range view.SelectorFields {
				expectedGo := []string{"Category", "CategoryID"}[i]
				expectedPublic := []string{"category", "categoryId"}[i]
				projection := dsql.SelectorProjection{SQL: "SELECT " + strings.Join(tc.sources, ",") + " FROM records", View: view}
				selected, err := projection.Columns([]string{expectedPublic})
				if err != nil || len(selected) != 1 || selected[0].OutputName() != tc.sources[i] {
					t.Fatalf("projection %s = %#v, %v", expectedPublic, selected, err)
				}
				if field.GoName != expectedGo || field.PublicName != expectedPublic || field.Column != tc.sources[i] {
					t.Fatalf("field %d = %#v", i, field)
				}
			}
		})
	}
}
