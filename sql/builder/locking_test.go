package builder

import (
	"context"
	"reflect"
	"strings"
	"testing"

	sqltemplate "github.com/viant/datly/sql/template"
	"github.com/viant/sqlx/metadata/database"
	"github.com/viant/sqlx/metadata/info"
)

func TestBuilderForUpdateRequiresTransactionAndKnownDialect(t *testing.T) {
	program, err := (sqltemplate.Compiler{Source: `SELECT id FROM records ORDER BY id ${View.ForUpdate()}`, InputType: reflect.TypeFor[struct{}]()}).Compile()
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		dialect string
		active  bool
		want    string
		fails   bool
	}{
		{"mysql", true, "SELECT id FROM records ORDER BY id FOR UPDATE", false},
		{"postgresql", true, "SELECT id FROM records ORDER BY id FOR UPDATE", false},
		{"sqlite", true, "SELECT id FROM records ORDER BY id", false},
		{"mysql", false, "", true},
		{"sqlite", false, "", true},
		{"other", true, "", true},
	} {
		t.Run(tc.dialect+"/"+reflect.ValueOf(tc.active).String(), func(t *testing.T) {
			query, err := NewBuilder().Build(context.Background(), WithBuilderTemplate(program), WithBuilderInput(reflect.ValueOf(struct{}{})),
				WithBuilderDialect(&info.Dialect{Product: database.Product{Name: tc.dialect}, Placeholder: "?"}), WithBuilderTransactionActive(tc.active))
			if tc.fails {
				if err == nil {
					t.Fatal("unsafe locking query succeeded")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if strings.TrimSpace(query.SQL) != tc.want || len(query.Args) != 0 {
				t.Fatalf("query=%+v want=%s", query, tc.want)
			}
		})
	}
}
