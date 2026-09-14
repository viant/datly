package column

import (
	"context"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
	"github.com/viant/sqlparser"
)

func TestTableFreeDiscoverySQLExecutes(t *testing.T) {
	query, err := discoveryQuery(&spec.ViewSource{SQL: "SELECT 1 AS id"})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(query, "FROM") {
		t.Fatalf("invented FROM: %s", query)
	}
	h := sqlite.New(t)
	rows, err := h.DB.QueryContext(context.Background(), query)
	if err != nil {
		t.Fatalf("discovery SQL %s: %v", query, err)
	}
	defer rows.Close()
	if rows.Next() {
		t.Fatal("discovery must not return rows")
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
}

func TestDiscoveryQueryUsesSQLParserForTableAndQuerySources(t *testing.T) {
	tests := []*spec.ViewSource{
		{Table: "events"},
		{SQL: "SELECT id, name FROM events WHERE tenant_id = 7 LIMIT 20"},
		{SQL: "WITH active AS (SELECT id FROM events) SELECT id FROM active UNION SELECT id FROM events"},
	}
	for _, source := range tests {
		actual, err := discoveryQuery(source)
		if err != nil {
			t.Fatalf("discoveryQuery(%+v) error = %v", source, err)
		}
		if _, err = sqlparser.ParseQuery(actual); err != nil {
			t.Fatalf("discovery SQL is not parseable: %v\n%s", err, actual)
		}
		if !strings.Contains(actual, "1 = 0") {
			t.Fatalf("discovery SQL was not falsified: %s", actual)
		}
	}
}

func TestDiscoveryQueryFalsifiesFromAndJoinSubqueries(t *testing.T) {
	actual, err := discoveryQuery(&spec.ViewSource{SQL: `SELECT o.id, i.name
FROM (SELECT id FROM orders) o
JOIN (SELECT order_id, name FROM items) i ON i.order_id = o.id`})
	if err != nil {
		t.Fatalf("discoveryQuery() error = %v", err)
	}
	if count := strings.Count(actual, "1 = 0"); count != 3 {
		t.Fatalf("expected outer and two nested false predicates, got %d:\n%s", count, actual)
	}
	if _, err = sqlparser.ParseQuery(actual); err != nil {
		t.Fatalf("rewritten nested SQL is invalid: %v\n%s", err, actual)
	}
}
