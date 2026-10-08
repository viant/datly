package sql

import (
	"testing"

	"github.com/viant/sqlparser"
)

func TestProjectionListPreservesQueryParserExpressions(t *testing.T) {
	for _, projection := range []string{"p.id AS external_id, p.name", "p . id AS external_id", "p\u00a0.\u00a0id AS external_id", "SUM(amount) AS total, region", "COALESCE(amount, 0) AS total", "true AS active, NULL AS missing", "'id' AS label", "\"id\" AS label", "id AS `external id`", "DISTINCT id", "*"} {
		parsed, err := sqlparser.ParseQuery("SELECT " + projection + " FROM criteria_source")
		actual, actualErr := ParseProjectionList(projection)
		if (err == nil) != (actualErr == nil) {
			t.Fatalf("projection %q error changed: %v -> %v", projection, err, actualErr)
		}
		if err != nil {
			continue
		}
		if len(actual) != len(parsed.List) {
			t.Fatalf("projection %q length changed", projection)
		}
		for i, expected := range parsed.List {
			if sqlparser.Stringify(actual[i].Expr) != sqlparser.Stringify(expected.Expr) || actual[i].Alias != expected.Alias {
				t.Errorf("projection %q item %d changed: %+v -> %+v", projection, i, expected, actual[i])
			}
		}
	}
}
