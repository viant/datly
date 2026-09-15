package compile

import (
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/data"
	"github.com/viant/datly/spec"
)

func TestParenthesizedTableAuxiliaryIntent(t *testing.T) {
	for _, test := range []struct {
		source    string
		auxiliary bool
		table     string
	}{{"SELECT * FROM records", false, "records"}, {"SELECT * FROM (records)", true, "records"}, {"SELECT * FROM (schema.records)", true, "schema.records"}, {"SELECT * FROM (SELECT * FROM records) r", false, "records"}, {"SELECT * FROM (SELECT * FROM (records) r WHERE r.id > 0) records", true, "records"}} {
		t.Run(test.source, func(t *testing.T) {
			input := &spec.View{Name: "Records", Source: &spec.ViewSource{SQL: test.source}}
			actual, err := NewReader().Compile(ReadInput{View: input, SQL: test.source})
			if err != nil {
				t.Fatal(err)
			}
			if actual.Auxiliary != test.auxiliary || actual.Source.Table != test.table {
				t.Fatalf("view=%+v source=%+v", actual, actual.Source)
			}
			if input.Auxiliary || input.Source.SQL != test.source {
				t.Fatal("canonical source input mutated")
			}
			if actual.Clone().Auxiliary != actual.Auxiliary {
				t.Fatal("clone lost intent")
			}
		})
	}
	if _, found := reflect.TypeFor[data.View]().FieldByName("Auxiliary"); found {
		t.Fatal("mutation intent leaked into runtime view shape")
	}
}

func TestNamedViewAuxiliarySourceAuthority(t *testing.T) {
	for _, tc := range []struct {
		name, source, table string
		auxiliary           bool
	}{
		{"derived auxiliary", `(SELECT k.* FROM (ORDER_KINDS) k WHERE k.ID>0)`, "ORDER_KINDS", true},
		{"nested auxiliary", `(SELECT filtered.* FROM (SELECT k.* FROM (ORDER_KINDS) k WHERE k.ID>0) filtered)`, "ORDER_KINDS", true},
		{"derived writable", `(SELECT k.* FROM ORDER_KINDS k WHERE k.ID>0)`, "ORDER_KINDS", false},
		{"auxiliary join does not change root", `(SELECT k.* FROM ORDER_KINDS k JOIN (OTHER) lookup ON lookup.ID=k.ID WHERE k.ID>0)`, "ORDER_KINDS", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			SQL := `SELECT orders.*, kind.* FROM (SELECT o.* FROM ORDERS o WHERE o.ID>0) orders JOIN ` + tc.source + ` kind ON kind.ID=orders.KIND_ID AND 1=1`
			input := &spec.View{Name: "Orders", Source: &spec.ViewSource{SQL: SQL}}
			actual, err := NewReader().Compile(ReadInput{View: input, SQL: SQL})
			if err != nil {
				t.Fatal(err)
			}
			child := actual.Relations[0].View
			if actual.Auxiliary || actual.Source.Table != "ORDERS" || child.Auxiliary != tc.auxiliary || child.Source.Table != tc.table {
				t.Fatalf("root=%+v child=%+v", actual, child)
			}
			if !strings.Contains(child.Source.SQL, tc.source) || !strings.Contains(actual.Source.SQL, "WHERE o.ID>0") {
				t.Fatalf("query restriction/alias lost: %s / %s", actual.Source.SQL, child.Source.SQL)
			}
			if input.Source.SQL != SQL || input.Auxiliary {
				t.Fatal("mutated authored view")
			}
		})
	}
}
