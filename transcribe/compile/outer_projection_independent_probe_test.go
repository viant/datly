package compile

import (
	"errors"
	"strings"
	"testing"

	"github.com/viant/datly/spec"
)

func TestIndependentProbeRejectsOuterDistinctWithRelations(t *testing.T) {
	_, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "Orders", Source: &spec.ViewSource{}}, SQL: `
SELECT DISTINCT orders.*, items.* FROM orders orders JOIN items items ON items.order_id = orders.id`})
	var compileError *Error
	if !errors.As(err, &compileError) || compileError.Code != CodeRelationUnsupported ||
		!strings.Contains(err.Error(), "DISTINCT") {
		t.Fatalf("Compile() error = %#v", err)
	}
}

func TestIndependentProbeRejectsUnavailableDerivedRelationColumn(t *testing.T) {
	actual, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "Orders", Source: &spec.ViewSource{}}, SQL: `
SELECT orders.ID, items.NAME
FROM (SELECT o.ID FROM ORDERS o) orders
JOIN (SELECT i.NAME FROM ITEMS i) items ON items.ORDER_ID = orders.ID`})
	if err == nil {
		t.Fatalf("derived child relation key omitted by inner SQL was accepted: root=%s child=%s", actual.Source.SQL, actual.Relations[0].View.Source.SQL)
	}
}
