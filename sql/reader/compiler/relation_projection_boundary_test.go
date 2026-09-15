package compiler

import (
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/spec"
)

func TestRequiredRelationProjectionBoundary(t *testing.T) {
	type item struct {
		OrderID int    `sqlx:"ORDER_ID"`
		Name    string `sqlx:"NAME"`
	}
	type order struct {
		ID    int    `sqlx:"ID"`
		Name  string `sqlx:"NAME"`
		Items []*item
	}
	type output struct{ Data []*order }
	for _, tc := range []struct {
		name, parent, child string
		fail                bool
	}{
		{"physical child projection deferred", `SELECT orders.ID FROM ORDERS orders`, `SELECT items.NAME FROM ITEMS items`, false},
		{"physical parent projection deferred", `SELECT orders.NAME FROM ORDERS orders`, `SELECT items.ORDER_ID,items.NAME FROM ITEMS items`, false},
		{"invalid projected child selector", `SELECT orders.ID FROM ORDERS orders`, `SELECT items.ORDER_ID,items.NAME FROM (SELECT i.NAME FROM ITEMS i) items`, true},
		{"invalid projected parent selector", `SELECT orders.ID FROM (SELECT o.NAME FROM ORDERS o) orders`, `SELECT items.ORDER_ID,items.NAME FROM ITEMS items`, true},
		{"closed wildcard omits key", `SELECT orders.ID FROM ORDERS orders`, `SELECT items.* FROM (SELECT i.NAME FROM ITEMS i) items`, true},
		{"computed parent value", `SELECT 1 AS ID`, `SELECT items.ORDER_ID,items.NAME FROM ITEMS items`, false},
		{"opaque child source deferred", `SELECT orders.ID FROM ORDERS orders`, `SELECT items.ORDER_ID,items.NAME FROM (WITH RECURSIVE keys(ORDER_ID,NAME) AS (VALUES(1,'one')) SELECT ORDER_ID,NAME FROM keys) items`, false},
		{"malformed child deferred", `SELECT orders.ID FROM ORDERS orders`, `SELECT FROM ITEMS`, false},
		{"malformed parent deferred", `SELECT FROM ORDERS`, `SELECT items.ORDER_ID,items.NAME FROM ITEMS items`, false},
		{"known outputs", `SELECT orders.ID FROM ORDERS orders`, `SELECT items.ORDER_ID,items.NAME FROM ITEMS items`, false},
		{"physical wildcards", `SELECT orders.* FROM ORDERS orders`, `SELECT items.* FROM ITEMS items`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := &spec.View{Name: "Orders", Namespace: "orders", Source: &spec.ViewSource{SQL: tc.parent}}
			root.Relations = []*spec.Relation{{Name: "Items", Holder: "Items", Cardinality: spec.CardinalityMany, View: &spec.View{Name: "Items", Namespace: "items", Source: &spec.ViewSource{SQL: tc.child}}, On: []*spec.RelationLink{{ParentNamespace: "orders", ParentColumn: "ID", ChildNamespace: "items", ChildColumn: "ORDER_ID"}}}}
			before := root.Clone()
			_, err := Compile(Input{Component: &spec.Component{Name: "Orders", RootView: root}, InputType: reflect.TypeFor[struct{}](), OutputType: reflect.TypeFor[output](), DirectViewField: "Data"})
			if !reflect.DeepEqual(root, before) {
				t.Fatal("compiled input changed")
			}
			if tc.fail {
				if err == nil || !strings.Contains(err.Error(), "required relation output") {
					t.Fatalf("missing output accepted: %v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
		})
	}
}
