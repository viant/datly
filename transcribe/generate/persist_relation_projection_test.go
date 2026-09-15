package generate

import "testing"

func TestProjectionRelationAliasAuthority(t *testing.T) {
	previous := projectionField{Tag: `view:"items,table=ITEMS,dest=items.go" on:"Id:orders.ID=OrderId:items.ORDER_ID" sql:"uri=items.sql"`}
	for _, tc := range []struct {
		tag     string
		allowed bool
	}{
		{`view:"items,table=ITEMS,dest=items.go" on:"RootKey:orders.RootKey=ParentKey:items.ParentKey" sql:"uri=items.sql"`, true},
		{`view:"items,table=ITEMS,dest=other.go" on:"RootKey:orders.RootKey=ParentKey:items.ParentKey" sql:"uri=items.sql"`, false},
		{`view:"items,table=ITEMS,dest=items.go" on:"RootKey:orders.RootKey=ParentKey:items.ParentKey" sql:"uri=other.sql"`, false},
	} {
		allowed, err := previous.tagChange(projectionField{Tag: tc.tag}, "on")
		if err != nil || allowed != tc.allowed {
			t.Fatalf("on authority %q: %t %v", tc.tag, allowed, err)
		}
	}
}
