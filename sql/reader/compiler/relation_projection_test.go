package compiler

import (
	"strings"
	"testing"

	"github.com/viant/datly/data"
	"github.com/viant/datly/spec"
)

func TestRelationProjectionSourceScope(t *testing.T) {
	for _, tc := range []struct {
		name, SQL, namespace, want string
		fail                       bool
	}{
		{"outer alias", `SELECT items.ORDER_ID AS ParentKey FROM (SELECT i.* FROM ITEMS i WHERE i.VISIBLE=1) items`, "items", "ORDER_ID", false},
		{"inner alias", `SELECT items.StoredParent AS ParentKey FROM (SELECT i.ORDER_ID AS StoredParent FROM ITEMS i WHERE i.VISIBLE=1) items`, "items", "StoredParent", false},
		{"wrong namespace", `SELECT items.ORDER_ID AS ParentKey FROM ITEMS items`, "other", "", true},
		{"wrong namespace unresolved SQL", `SELECT FROM ITEMS`, "other", "", true},
		{"duplicate outputs", `SELECT items.ORDER_ID AS ParentKey,items.ORDER_ID AS ParentKey FROM ITEMS items`, "items", "", true},
		{"duplicate inner key", `SELECT items.ORDER_ID AS ParentKey FROM (SELECT a AS ORDER_ID,b AS ORDER_ID FROM ITEMS) items`, "items", "", true},
		{"computed match", `SELECT items.ORDER_ID+1 AS ParentKey FROM ITEMS items`, "items", "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			child := &data.View{Spec: spec.View{Name: "Items", Namespace: "items", Source: &spec.ViewSource{Table: "ITEMS", SQL: tc.SQL}}, Columns: []*data.Column{{Name: "ParentKey", Column: "ORDER_ID"}}}
			link := data.NewLink(tc.namespace, "ParentKey", "ParentKey")
			root := &data.View{Relations: []*data.Relation{{Name: "Items", Of: &data.RelationRef{View: child, On: data.Links{link}}}}}
			err := resolveRelationProjections(root)
			if tc.fail {
				if err == nil {
					t.Fatal("invalid source association accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if link.Field != "ParentKey" || link.Namespace != "items" || link.Column != tc.want || link.OutputColumn() != "ParentKey" {
				t.Fatalf("link=%+v", link)
			}
			if child.Spec.Source.SQL != tc.SQL || !strings.Contains(child.Spec.Source.SQL, "i.VISIBLE=1") {
				t.Fatal("source restriction rewritten")
			}
			if err = resolveRelationProjections(root); err != nil || link.Column != tc.want {
				t.Fatal("not idempotent", err)
			}
		})
	}
}
