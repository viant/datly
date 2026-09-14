package transcribe

import (
	"context"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/transcribe/column"
	"github.com/viant/datly/typecatalog"
	xshape "github.com/viant/x/shape"
)

func TestJoinToOneHintRegeneratesExistingHolder(t *testing.T) {
	ctx := context.Background()
	db := sqlite.New(t)
	if err := db.ExecStatements(ctx, "CREATE TABLE orders(id INTEGER)", "CREATE TABLE items(order_id INTEGER, name TEXT)"); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	for _, one := range []bool{false, true, false} {
		hint, want := "", "[]"
		if one {
			hint = " AND 1=1"
			want = "*"
		}
		generated, err := transcribeSource(ctx, root, &Source{Scope: "example.com/hints", Name: "Orders", Connector: "main", ColumnRefiner: column.New(column.Connections{"main": db.DB}), Text: "#setting($_ = $route('/orders','GET'))\nSELECT o.*, i.* FROM orders o JOIN items i ON i.order_id=o.id" + hint})
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, view := range generated.Result.Plan.Views {
			for _, field := range view.Fields {
				if field.Name == "I" {
					found = true
					descriptor, ok, err := generated.Types.Resolve(typecatalog.TranscribeAuthority, generated.Package.PkgPath+"."+view.Name)
					if err != nil || !ok {
						t.Fatalf("emitted type lookup: %v %v", ok, err)
					}
					fields, err := xshape.New(descriptor, nil).Fields()
					if err != nil {
						t.Fatal(err)
					}
					emitted := false
					for _, actual := range fields {
						if actual.Name == "I" {
							emitted = true
							if !strings.HasPrefix(actual.TypeExpr, want) {
								t.Fatalf("one=%v emitted holder=%s", one, actual.TypeExpr)
							}
						}
					}
					if !emitted {
						t.Fatal("holder absent from parsed emitted Go")
					}
					if !strings.HasPrefix(field.Type, want) {
						t.Fatalf("one=%v generated holder %s, want %s", one, field.Type, want)
					}
				}
			}
		}
		if !found {
			t.Fatal("missing generated holder")
		}
	}
}
