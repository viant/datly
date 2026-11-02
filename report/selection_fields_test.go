package report

import (
	"reflect"
	"testing"
)

func TestPlanSelectFieldsReusesCompleteRelationKeys(t *testing.T) {
	type dimensions struct {
		Tenant bool
		Item   *bool
	}
	type input struct {
		Dimensions *dimensions
		Measures   struct{ Units bool }
		OrderBy    []string
		Limit      *int
		Offset     *int
	}
	definition := Definition{View: "facts", Dimensions: []SelectionDefinition{{Field: "Dimensions.Tenant", Name: "tenant"}, {Field: "Dimensions.Item", Name: "item_id"}}, Measures: []SelectionDefinition{{Field: "Measures.Units", Name: "units"}}, Relations: []RelationDefinition{{Holder: "Dictionary", Dimensions: []string{"tenant", "item_id"}}}, OrderBy: "OrderBy", Limit: "Limit", Offset: "Offset"}
	plan, err := definition.Compile(reflect.TypeFor[input](), reflect.TypeFor[struct{}]())
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct{ names, want []string }{
		{[]string{"tenant", "item_id", "units"}, []string{"tenant", "Dictionary", "item_id", "units"}},
		{[]string{"item_id", "units"}, []string{"item_id", "units"}},
		{[]string{"units"}, []string{"units"}},
		{[]string{"item_id", "tenant", "item_id"}, []string{"tenant", "Dictionary", "item_id"}},
	} {
		actual, err := plan.SelectFields(test.names)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(actual, test.want) {
			t.Fatalf("SelectFields(%v) = %v, want %v", test.names, actual, test.want)
		}
		actual[0] = "changed"
	}
	for _, name := range []string{"Dictionary", "unknown", "item_id.label"} {
		if _, err := plan.SelectFields([]string{name}); err == nil {
			t.Fatalf("accepted unknown or holder selection %q", name)
		}
	}
	empty, err := plan.SelectFields(nil)
	if err != nil || len(empty) != 0 {
		t.Fatalf("empty selection = %v, %v", empty, err)
	}
	empty, err = plan.SelectFields([]string{})
	if err != nil || len(empty) != 0 {
		t.Fatalf("empty list selection = %v, %v", empty, err)
	}
	var nilPlan *Plan
	if _, err := nilPlan.SelectFields(nil); err == nil {
		t.Fatal("nil plan accepted")
	}
}
