package transcribe

import (
	"context"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness/sqlite"
	tcolumn "github.com/viant/datly/transcribe/column"
)

func TestFluentDeclaredTypeAuthorizesOnlyNamedComputedColumn(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t)
	if err := h.ExecStatements(ctx, `CREATE TABLE records(id INTEGER, unknown)`); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ name, declaration, projection, failure string }{
		{"declared", ".WithColumnType('label','string')", "COALESCE((SELECT 'x'), '') AS label", ""},
		{"alias", ".ColumnType('label','string')", "COALESCE((SELECT 'x'), '') AS label", ""},
		{"undeclared", "", "COALESCE((SELECT 'x'), '') AS label", "unable discover column label type"},
		{"other unknown", ".ColumnType('label','string')", "COALESCE((SELECT 'x'), '') AS label, unknown", "unable discover column unknown type"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := `#setting($_ = $route('/records','GET'))
#define($_ = $Details<[]*Detail>(view/Details)` + tc.declaration + ` /* SELECT ` + tc.projection + ` FROM records */)
SELECT r.* FROM records r`
			// Root deliberately excludes the untyped physical column.
			source = strings.Replace(source, "SELECT r.* FROM records r", "SELECT r.id FROM records r", 1)
			compiled, err := NewCompiler().Compile(ctx, &Source{Name: "Records", Connector: "main", ColumnRefiner: tcolumn.New(tcolumn.Connections{"main": h.DB}), Text: source})
			if tc.failure != "" {
				if err == nil || !strings.Contains(err.Error(), tc.failure) {
					t.Fatalf("want %s, got %v", tc.failure, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, view := range compiled.Component.Views {
				for _, col := range view.Columns {
					if col.Name == "label" {
						found = true
						if !col.ExplicitType || col.Type.Name != "string" {
							t.Fatalf("column=%+v", col)
						}
					}
				}
			}
			if !found {
				t.Fatal("missing declared projection")
			}
		})
	}
}
