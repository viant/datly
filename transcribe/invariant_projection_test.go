package transcribe

import (
	"context"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness/sqlite"
	column "github.com/viant/datly/transcribe/column"
)

func TestInvariantProjectedColumnDiscovery(t *testing.T) {
	h := sqlite.New(t)
	ctx := context.Background()
	if err := h.ExecStatements(ctx, `CREATE TABLE ORDERS(ID INTEGER, WINDOW_START INTEGER, WINDOW_END INTEGER)`); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, projection, annotation string
		invalid                      bool
	}{
		{"wildcard missing", `o.*`, `invariant(orders.MISSING,'Window')`, true},
		{"explicit missing", `o.ID`, `invariant(orders.WINDOW_START,'Window')`, true},
		{"cast cannot invent invariant member", `o.*`, `CAST(orders.MISSING AS int),invariant(orders.MISSING,'Window')`, true},
		{"alias output", `o.ID,o.WINDOW_START AS START_AT`, `invariant(orders.START_AT,'Window')`, false},
		{"alias hides source", `o.ID,o.WINDOW_START AS START_AT`, `invariant(orders.WINDOW_START,'Window')`, true},
		{"wildcard present", `o.*`, `invariant(orders.WINDOW_START,'Window'),invariant(orders.WINDOW_END,'Window')`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			SQL := `SELECT orders.*,` + tc.annotation + ` FROM (SELECT ` + tc.projection + ` FROM ORDERS o) orders`
			source := &Source{Name: "Orders", Connector: "main", ColumnRefiner: column.New(column.Connections{"main": h.DB}), Text: "#setting($_ = $route('/orders','GET'))\n" + SQL}
			got, err := NewCompiler().Compile(ctx, source)
			if tc.invalid {
				if err == nil || !strings.Contains(err.Error(), "absent from the SQL projection") {
					t.Fatalf("error=%v result=%+v", err, got)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			for _, field := range got.Component.RootView.Columns {
				if field.Name == "MISSING" {
					t.Fatal("invented column")
				}
			}
			if strings.Contains(strings.ToLower(got.Component.RootView.Source.SQL), "invariant(") {
				t.Fatal("annotation reached DB source")
			}
		})
	}
}

func TestInvariantWithoutDiscoveryRequiresKnownProjection(t *testing.T) {
	for _, tc := range []struct {
		name, SQL string
		invalid   bool
	}{
		{"explicit output", `SELECT orders.*,invariant(orders.ID,'Window') FROM (SELECT o.ID FROM ORDERS o) orders`, false},
		{"explicit alias", `SELECT orders.*,invariant(orders.KEY,'Window') FROM (SELECT o.ID AS KEY FROM ORDERS o) orders`, false},
		{"unknown wildcard", `SELECT orders.*,invariant(orders.MISSING,'Window') FROM (SELECT o.* FROM ORDERS o) orders`, true},
		{"typed annotation is not schema", `SELECT orders.*,CAST(orders.MISSING AS int),invariant(orders.MISSING,'Window') FROM (SELECT o.* FROM ORDERS o) orders`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewCompiler().Compile(context.Background(), &Source{Name: "Orders", Text: "#setting($_ = $route('/orders','GET'))\n" + tc.SQL})
			if tc.invalid {
				if err == nil || !strings.Contains(err.Error(), "requires column discovery") {
					t.Fatalf("error=%v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}
