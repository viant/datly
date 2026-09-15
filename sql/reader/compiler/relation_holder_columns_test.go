package compiler

import (
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/data"
	"github.com/viant/datly/spec"
)

func TestCanonicalRelationHolderColumnAuthority(t *testing.T) {
	type child struct{ ID, ParentID int }
	type plain struct {
		ID      int
		Items   []*child
		Label   string
		Payload []*child `sqlx:"Items,enc=JSON"`
	}
	type scalarConflict struct {
		ID    int
		Items []*child `sqlx:"items_json,enc=JSON"`
	}
	type codecConflict struct {
		ID    int
		Items []*child `codec:"JSON"`
	}
	for _, tc := range []struct {
		name    string
		row     reflect.Type
		invalid bool
	}{
		{"plain holder and scalar mapping", reflect.TypeFor[plain](), false},
		{"scalar holder conflict", reflect.TypeFor[scalarConflict](), true},
		{"codec holder conflict", reflect.TypeFor[codecConflict](), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			childView := &data.View{Spec: spec.View{Name: "Items"}}
			view := &data.View{Spec: spec.View{Name: "Parents"}, Relations: []*data.Relation{{Name: "items", Holder: "Items", Cardinality: spec.CardinalityMany, On: data.Links{{Column: "ID"}}, Of: &data.RelationRef{View: childView, On: data.Links{{Column: "ParentID"}}}}}}
			err := newViewDeriver(map[*data.View]reflect.Type{}).enrich(view, tc.row)
			if tc.invalid {
				if err == nil || !strings.Contains(err.Error(), "also declares a scalar SQL or codec mapping") {
					t.Fatalf("error=%v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			var names []string
			for _, column := range view.Columns {
				names = append(names, column.Name)
				if column.Name == "Payload" && column.Column != "Items" {
					t.Fatalf("scalar mapping changed: %+v", column)
				}
			}
			if !reflect.DeepEqual(names, []string{"ID", "Label", "Payload"}) {
				t.Fatalf("physical columns=%v", names)
			}
			if len(view.Relations) != 1 || view.Relations[0].Holder != "Items" {
				t.Fatal("relation was lost")
			}
		})
	}
}
