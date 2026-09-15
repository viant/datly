package compile

import (
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/spec"
)

func TestRequiredNamedRelationOutputs(t *testing.T) {
	for _, tc := range []struct {
		name, root, child, list, on string
		missing                     bool
	}{
		{"missing child", "SELECT o.ID FROM ORDERS o", "SELECT i.NAME FROM ITEMS i", "orders.ID,items.NAME", "items.ORDER_ID=orders.ID", true},
		{"missing root", "SELECT o.NAME FROM ORDERS o", "SELECT i.ORDER_ID FROM ITEMS i", "orders.NAME,items.ORDER_ID", "items.ORDER_ID=orders.ID", true},
		{"outer wildcards do not invent keys", "SELECT o.ID FROM ORDERS o", "SELECT i.NAME FROM ITEMS i", "orders.*,items.*", "items.ORDER_ID=orders.ID", true},
		{"explicit invalid outer key", "SELECT o.ID FROM ORDERS o", "SELECT i.NAME FROM ITEMS i", "orders.ID,items.ORDER_ID", "items.ORDER_ID=orders.ID", true},
		{"nested closed wildcard", "SELECT o.ID FROM ORDERS o", "SELECT n.* FROM (SELECT i.NAME FROM ITEMS i) n", "orders.ID,items.NAME", "items.ORDER_ID=orders.ID", true},
		{"renamed source must use alias", "SELECT o.ID FROM ORDERS o", "SELECT i.ORDER_ID AS ParentKey,i.NAME FROM ITEMS i", "orders.ID,items.NAME", "items.ORDER_ID=orders.ID", true},
		{"composite omission", "SELECT o.ID,o.TENANT FROM ORDERS o", "SELECT i.ORDER_ID FROM ITEMS i", "orders.ID,items.ORDER_ID", "items.ORDER_ID=orders.ID AND items.TENANT=orders.TENANT", true},
		{"physical wildcard deferred", "SELECT o.* FROM ORDERS o", "SELECT i.* FROM ITEMS i", "orders.ID,items.NAME", "items.ORDER_ID=orders.ID", false},
		{"valid source and outer aliases", "SELECT o.ID AS SourceKey FROM ORDERS o", "SELECT i.ORDER_ID AS ParentKey,i.NAME FROM ITEMS i", "orders.SourceKey AS RootKey,items.NAME", "items.ParentKey=orders.SourceKey", false},
		{"inner distinct outputs", "SELECT DISTINCT o.ID FROM ORDERS o", "SELECT DISTINCT i.ORDER_ID,i.NAME FROM ITEMS i", "orders.ID,items.NAME", "items.ORDER_ID=orders.ID", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			SQL := "SELECT " + tc.list + " FROM (" + tc.root + ") orders JOIN (" + tc.child + ") items ON " + tc.on
			input := &spec.View{Name: "Orders", Source: &spec.ViewSource{SQL: SQL}}
			before := input.Clone()
			actual, err := NewReader().Compile(ReadInput{View: input, SQL: SQL})
			if !reflect.DeepEqual(before, input) {
				t.Fatal("authored input mutated")
			}
			if tc.missing {
				if err == nil || !strings.Contains(err.Error(), "required relation output") {
					t.Fatalf("missing source key accepted: %v %+v", err, actual)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(actual.Source.SQL, tc.root) || !strings.Contains(actual.Relations[0].View.Source.SQL, tc.child) {
				t.Fatal("inner source rewritten")
			}
		})
	}
}
