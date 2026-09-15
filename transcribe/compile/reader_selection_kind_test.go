package compile

import (
	"errors"
	"strings"
	"testing"

	"github.com/viant/datly/spec"
	"github.com/viant/sqlparser"
)

func TestReaderOuterSelectionKind(t *testing.T) {
	for _, tc := range []struct {
		name, SQL string
		reject    bool
	}{
		{"DISTINCT tables", `SELECT DISTINCT orders.*,items.* FROM ORDERS orders JOIN ITEMS items ON items.ORDER_ID=orders.ID`, true},
		{"distinct named", `SELECT distinct orders.ID,items.NAME FROM (SELECT o.* FROM ORDERS o) orders JOIN (SELECT i.* FROM ITEMS i) items ON items.ORDER_ID=orders.ID`, true},
		{"ALL", `SELECT ALL orders.ID,items.NAME FROM (SELECT o.* FROM ORDERS o) orders JOIN (SELECT i.* FROM ITEMS i) items ON items.ORDER_ID=orders.ID`, false},
		{"inner DISTINCT", `SELECT orders.ID,items.NAME FROM (SELECT DISTINCT o.ID FROM ORDERS o) orders JOIN (SELECT DISTINCT i.ORDER_ID,i.NAME FROM ITEMS i) items ON items.ORDER_ID=orders.ID`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parsed, err := sqlparser.ParseQuery(tc.SQL)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("native Select.Kind=%q", parsed.Kind)
			actual, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "Orders", Source: &spec.ViewSource{SQL: tc.SQL}}, SQL: tc.SQL})
			if tc.reject {
				var problem *Error
				if !errors.As(err, &problem) || problem.Code != CodeRelationUnsupported || !strings.Contains(err.Error(), "DISTINCT") {
					t.Fatalf("DISTINCT row semantics accepted: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if tc.name == "inner DISTINCT" && (!strings.Contains(actual.Source.SQL, "SELECT DISTINCT o.ID") || !strings.Contains(actual.Relations[0].View.Source.SQL, "SELECT DISTINCT i.ORDER_ID")) {
				t.Fatal("inner DISTINCT changed")
			}
		})
	}
}
