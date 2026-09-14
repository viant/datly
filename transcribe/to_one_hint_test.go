package transcribe

import (
	"context"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/transcribe/column"
)

func TestJoinToOneHintChangesGeneratedHolder(t *testing.T) {
	ctx := context.Background()
	db := sqlite.New(t)
	if err := db.ExecStatements(ctx, "CREATE TABLE orders(id INTEGER)", "CREATE TABLE items(order_id INTEGER, name TEXT)"); err != nil {
		t.Fatal(err)
	}
	for _, one := range []bool{false, true} {
		name := "many"
		hint := ""
		want := "[]"
		if one {
			name = "one"
			hint = " AND 1=1"
			want = "*"
		}
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			testharness.WriteGeneratedGoMod(t, root)
			generated, err := transcribeSource(ctx, root, &Source{Scope: "example.com/hints", Name: "Orders", Connector: "main", ColumnRefiner: column.New(column.Connections{"main": db.DB}), Text: "#setting($_ = $route('/orders','GET'))\nSELECT o.*, i.* FROM orders o JOIN items i ON i.order_id=o.id" + hint})
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, view := range generated.Result.Plan.Views {
				for _, field := range view.Fields {
					if field.Name == "I" {
						found = true
						if !strings.HasPrefix(field.Type, want) {
							t.Fatalf("generated holder %s, want prefix %s", field.Type, want)
						}
					}
				}
			}
			if !found {
				t.Fatal("generated relation holder missing")
			}
		})
	}
}
