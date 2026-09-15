package compile

import (
	"strings"
	"testing"

	"github.com/viant/datly/spec"
)

func TestReaderRetainsPredicateExpressions(t *testing.T) {
	where := `${predicate.Builder().CombineAnd($predicate.FilterGroup(0, "AND")).Build("WHERE")}`
	and := `${predicate.Builder().CombineAnd($predicate.FilterGroup(0, "AND")).Build("AND")}`
	for _, tt := range []struct {
		name, sql, expression string
		rewrite               bool
	}{
		{"where suffix", "SELECT id FROM records r " + where + " ORDER BY id", where, false},
		{"and suffix", "SELECT id FROM records r WHERE 1=1 " + and + " ORDER BY id", and, false},
		{"rewritten where", "SELECT id, use_connector(r, 'main') FROM records r " + where + " ORDER BY id", where, true},
		{"rewritten and", "SELECT id, use_connector(r, 'main') FROM records r WHERE 1=1 " + and + " ORDER BY id", and, true},
		{"nested SQL with outer control", "SELECT r.*, use_connector(r, 'main') FROM (SELECT id FROM records r " + where + " ORDER BY id) r", where, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			actual, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "Records", Source: &spec.ViewSource{SQL: tt.sql}}, SQL: tt.sql})
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(actual.Source.SQL, tt.expression) || !strings.Contains(strings.ToUpper(actual.Source.SQL), "ORDER BY") {
				t.Fatalf("lost executable source: %s", actual.Source.SQL)
			}
			if tt.rewrite && strings.Contains(actual.Source.SQL, "use_connector(") {
				t.Fatal("directive was not lowered")
			}
		})
	}
}
