package compile

import (
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/spec"
)

func TestReaderNamedOuterProjection(t *testing.T) {
	for _, tc := range []struct{ name, projection string }{
		{"narrow root and child", "orders.ID,items.ID"},
		{"matching fields internal", "orders.NAME,items.NAME"},
		{"aliased matching fields", "orders.ID AS RootKey,items.ID AS ItemKey,items.ORDER_ID AS ParentKey"},
		{"wildcard plus alias", "orders.*,orders.NAME AS DisplayName,items.ID"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			SQL := "SELECT " + tc.projection + ` FROM (SELECT o.* FROM ORDERS o WHERE o.TENANT=7) orders JOIN (SELECT i.* FROM ITEMS i WHERE i.VISIBLE=1) items ON items.ORDER_ID=orders.ID WHERE orders.ACTIVE=1 ORDER BY orders.ID LIMIT 4`
			input := &spec.View{Name: "Orders", Source: &spec.ViewSource{SQL: SQL}}
			before := input.Clone()
			actual, err := NewReader().Compile(ReadInput{View: input, SQL: SQL})
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(before, input) {
				t.Fatal("mutated authored source")
			}
			child := actual.Relations[0].View
			if actual.Source.Table != "ORDERS" || child.Source.Table != "ITEMS" || actual.Namespace != "orders" || child.Namespace != "items" {
				t.Fatal("source identity lost")
			}
			for _, fragment := range []string{"o.TENANT=7", "orders.ACTIVE = 1", "ORDER BY orders.ID", "LIMIT 4"} {
				if !strings.Contains(actual.Source.SQL, fragment) {
					t.Fatalf("lost %s: %s", fragment, actual.Source.SQL)
				}
			}
			if !strings.Contains(child.Source.SQL, "i.VISIBLE=1") {
				t.Fatal(child.Source.SQL)
			}
			if tc.name == "narrow root and child" && (strings.HasPrefix(actual.Source.SQL, "SELECT *") || strings.HasPrefix(child.Source.SQL, "SELECT *")) {
				t.Fatal("projection discarded")
			}
			if tc.name == "matching fields internal" {
				for _, v := range []*spec.View{actual, child} {
					if len(v.Columns) != 1 || reflect.StructTag(v.Columns[0].Tag).Get("internal") != "true" {
						t.Fatalf("backing fields %+v", v.Columns)
					}
				}
			}
			if tc.name == "aliased matching fields" {
				link := actual.Relations[0].On[0]
				if link.ParentColumn != "RootKey" || link.ChildColumn != "ParentKey" || len(actual.Columns) != 0 || len(child.Columns) != 0 {
					t.Fatalf("alias association: %+v", link)
				}
			}
			if tc.name == "wildcard plus alias" && !strings.Contains(actual.Source.SQL, "orders.NAME AS DisplayName") {
				t.Fatal(actual.Source.SQL)
			}
		})
	}
}

func TestReaderNamedProjectionRejectsStaleAnnotations(t *testing.T) {
	for _, annotation := range []string{"CAST(orders.NAME AS string)", "invariant(orders.NAME,'Window')", "tag(orders.NAME,'json:\"name\"')", "CAST(items.NAME AS string)", "invariant(items.NAME,'Window')", "invariant(orders.ID,'Window')"} {
		t.Run(annotation, func(t *testing.T) {
			SQL := `SELECT orders.NAME AS Label,items.ID,` + annotation + ` FROM (SELECT o.* FROM ORDERS o) orders JOIN (SELECT i.* FROM ITEMS i) items ON items.ORDER_ID=orders.ID`
			_, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "Orders"}, SQL: SQL})
			if err == nil || !strings.Contains(err.Error(), "absent from the SQL projection") {
				t.Fatalf("stale annotation: %v", err)
			}
		})
	}
	for _, SQL := range []string{
		`SELECT orders.NAME AS Label,invariant(orders.Label,'Window') FROM (SELECT o.NAME FROM ORDERS o) orders`,
		`SELECT orders.NAME AS Label,CAST(orders.Label AS string) FROM (SELECT o.NAME FROM ORDERS o) orders`,
	} {
		if _, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "Orders"}, SQL: SQL}); err != nil {
			t.Fatalf("legitimate alias: %v", err)
		}
	}
}

func TestReaderNamedProjectionUnknownNamespace(t *testing.T) {
	_, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "Orders"}, SQL: `SELECT other.NAME FROM (SELECT o.* FROM ORDERS o) orders JOIN (SELECT i.* FROM ITEMS i) items ON items.ORDER_ID=orders.ID`})
	if err == nil || !strings.Contains(err.Error(), "no canonical view") {
		t.Fatalf("unknown projection was broadened: %v", err)
	}
}

func TestReaderSingleNamedSourceAuthority(t *testing.T) {
	SQL := `SELECT orders.NAME FROM (SELECT o.* FROM ORDERS o WHERE o.TENANT=7) orders`
	actual, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "Orders"}, SQL: SQL})
	if err != nil {
		t.Fatal(err)
	}
	if actual.Source.Table != "ORDERS" || actual.Namespace != "orders" || !strings.Contains(actual.Source.SQL, "orders.NAME") || !strings.Contains(actual.Source.SQL, "o.TENANT=7") {
		t.Fatalf("named SQL replaced by table: %+v", actual.Source)
	}
}

func TestReaderUnlistedRelationProjectionBoundary(t *testing.T) {
	for _, tc := range []struct {
		name, source, projection string
		internal                 bool
	}{
		{"table root", "ORDERS orders", "orders.*", false},
		{"table marker root", "(ORDERS) orders", "orders.*", false},
		{"named root", "(SELECT * FROM ORDERS) orders", "orders.*", true},
		{"explicit child", "ORDERS orders", "orders.*,items.NAME", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			SQL := "SELECT " + tc.projection + " FROM " + tc.source + " JOIN (SELECT ORDER_ID,NAME FROM ITEMS) items ON items.ORDER_ID=orders.ID"
			actual, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "Orders"}, SQL: SQL})
			if err != nil {
				t.Fatal(err)
			}
			child := actual.Relations[0].View
			if tc.internal {
				if !strings.HasPrefix(child.Source.SQL, "SELECT items.ORDER_ID FROM") {
					t.Fatalf("unlisted named output broadened: %s", child.Source.SQL)
				}
			} else if tc.name == "explicit child" {
				if !strings.HasPrefix(child.Source.SQL, "SELECT items.NAME, items.ORDER_ID FROM") {
					t.Fatalf("explicit child projection lost: %s", child.Source.SQL)
				}
			} else if !strings.HasPrefix(child.Source.SQL, "SELECT * FROM") {
				t.Fatalf("child row contract narrowed: %s", child.Source.SQL)
			}
		})
	}
}
