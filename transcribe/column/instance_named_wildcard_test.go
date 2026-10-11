package column

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
	sqltemplate "github.com/viant/datly/sql/template"
)

func TestNamedWildcardRawTableConstantPreservesSourceAndIdentity(t *testing.T) {
	h := sqlite.New(t)
	ctx := context.Background()
	if err := h.ExecStatements(ctx, "CREATE TABLE actual_records (id INTEGER PRIMARY KEY AUTOINCREMENT,name TEXT)"); err != nil {
		t.Fatal(err)
	}
	input := &TemplateInput{Value: reflect.ValueOf(struct{ Table string }{"actual_records"}), Variables: []sqltemplate.Variable{{Name: "Table", FieldIndex: []int{0}}}}
	for _, tc := range []struct {
		name, inner string
		key         bool
	}{
		{"all columns", "SELECT * FROM $Unsafe.Table WHERE 1=0", true},
		{"omitted key", "SELECT name FROM $Unsafe.Table WHERE 1=0", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			authored := "SELECT r.* FROM (" + tc.inner + ") r"
			view := &spec.View{Name: "Records", Namespace: "r", Source: &spec.ViewSource{Table: "actual_records", SQL: authored}}
			component := &spec.Component{Name: "Records", RootView: view, Settings: &spec.Settings{DefaultConnector: "main"}}
			if err := New(Connections{"main": h.DB}).RefineRoot(ctx, component, nil, input); err != nil {
				t.Fatal(err)
			}
			found := false
			for _, column := range view.Columns {
				found = found || column.PrimaryKey
			}
			if found != tc.key {
				t.Fatalf("primary key authority: got %v want %v; columns=%+v", found, tc.key, view.Columns)
			}
			if view.Source.SQL != authored || !strings.Contains(view.Source.SQL, "$Unsafe.Table") {
				t.Fatalf("discovery changed authored source: %s", view.Source.SQL)
			}
		})
	}
}
