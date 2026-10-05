package generate

import (
	"reflect"
	"testing"

	"github.com/viant/datly/spec"
)

func TestInternalLogicalPresenceAllDeclaredScalars(t *testing.T) {
	// A structured CAST value is a scalar column, not a mutable relation.
	columns := []*spec.Column{
		{Name: "id", Type: spec.TypeRef{Name: "int64"}},
		{Name: "public", Type: spec.TypeRef{Name: "string"}, Tag: `sqlx:"-"`},
		{Name: "AdOrder", Type: spec.TypeRef{Name: "*AdOrder"}, Tag: `sqlx:"-" internal:"true"`},
		{Name: "CallerAdOrder", Type: spec.TypeRef{Name: "*AdOrder"}, Tag: `sqlx:"-" json:"-" internal:"true"`},
		{Name: "Campaign", Type: spec.TypeRef{Name: "*Campaign"}, Tag: `sqlx:"-" internal:"true"`},
		{Name: "Advertiser", Type: spec.TypeRef{Name: "*Advertiser"}, Tag: `sqlx:"-" internal:"true"`},
		{Name: "TimeZone", Type: spec.TypeRef{Name: "*advertiser.TimeZoneView"}, Tag: `sqlx:"-" json:",omitempty" internal:"true"`},
		{Name: "ToBeSaved", Type: spec.TypeRef{Name: "bool"}, Tag: `sqlx:"-" json:"-" internal:"true"`},
		{Name: "hiddenformat", Type: spec.TypeRef{Name: "int"}, Tag: `sqlx:"-" format:"-"`},
	}
	view := &spec.View{Name: "Flight", Columns: columns, Relations: []*spec.Relation{
		{Name: "Derived", Holder: "Derived", Kind: spec.RelationKindDerived},
		{Name: "Auxiliary", Holder: "Auxiliary", View: &spec.View{Auxiliary: true}},
		{Name: "Items", Holder: "Items"},
	}}
	plan := &ViewPlan{Name: "FlightView"}
	for _, column := range columns {
		name := column.Name
		if name == "id" {
			name = "Id"
		}
		if name == "public" {
			name = "Public"
		}
		if name == "hiddenformat" {
			name = "Hiddenformat"
		}
		plan.Fields = append(plan.Fields, Field{Name: name, Type: column.Type.Name, Tag: column.Tag})
	}
	for _, name := range []string{"Derived", "Auxiliary", "Items"} {
		plan.Fields = append(plan.Fields, Field{Name: name, Type: "[]*Child"})
	}
	if err := addViewSetMarker(plan, view); err != nil {
		t.Fatal(err)
	}
	want := []string{"Id", "Public", "AdOrder", "CallerAdOrder", "Campaign", "Advertiser", "TimeZone", "ToBeSaved", "Hiddenformat", "Items"}
	if !reflect.DeepEqual(plan.SetMarkerFields, want) {
		t.Fatalf("markers = %v, want %v", plan.SetMarkerFields, want)
	}
	if len(plan.Fields) != len(columns)+4 {
		t.Fatalf("marker changed original fields: %+v", plan.Fields)
	}
	for i, column := range columns {
		if plan.Fields[i].Tag != column.Tag || plan.Fields[i].Type != column.Type.Name {
			t.Fatalf("scalar authority changed: %+v", plan.Fields[i])
		}
	}
}

func TestInternalLogicalPresenceRequiresCanonicalField(t *testing.T) {
	plan := &ViewPlan{Name: "MissingView"}
	view := &spec.View{Columns: []*spec.Column{{Name: "internal", Tag: `sqlx:"-" internal:"true"`}}}
	if err := addViewSetMarker(plan, view); err == nil {
		t.Fatal("missing hidden scalar field accepted")
	}
}
