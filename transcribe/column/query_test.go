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

func TestDiscoveryQueryPreservesFromAndJoinSubqueries(t *testing.T) {
	actual, err := discoveryQuery(&spec.ViewSource{SQL: `SELECT o.id, i.name
FROM (SELECT id FROM orders) o
JOIN (SELECT order_id, name FROM items) i ON i.order_id = o.id`})
	if err != nil {
		t.Fatalf("discoveryQuery() error = %v", err)
	}
	if count := strings.Count(actual, "1 = 0"); count != 1 {
		t.Fatalf("expected only the outer false predicate, got %d:\n%s", count, actual)
	}
	if !strings.Contains(actual, "(SELECT id FROM orders)") || !strings.Contains(actual, "(SELECT order_id, name FROM items)") {
		t.Fatalf("inner SQL changed: %s", actual)
	}
	if _, err = sqlparser.ParseQuery(actual); err != nil {
		t.Fatalf("rewritten nested SQL is invalid: %v\n%s", err, actual)
	}
}

func TestDiscoveryPreservesParenthesizedTablesAndRestrictions(t *testing.T) {
	h := sqlite.New(t)
	ctx := context.Background()
	if err := h.ExecStatements(ctx, `CREATE TABLE VENDOR(ID INTEGER)`, `INSERT INTO VENDOR VALUES(2)`); err != nil {
		t.Fatal(err)
	}
	for _, SQL := range []string{
		`SELECT * FROM (VENDOR) v WHERE v.ID=2`,
		`SELECT vendor.* FROM (SELECT v.* FROM (VENDOR) v WHERE v.ID=2) vendor`,
		`WITH selected_vendor AS (SELECT v.* FROM (VENDOR) v WHERE v.ID=2) SELECT * FROM selected_vendor`,
	} {
		t.Run(SQL, func(t *testing.T) {
			actual, err := discoveryQuery(&spec.ViewSource{SQL: SQL})
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(actual, "(VENDOR)") || !strings.Contains(actual, "v.ID") {
				t.Fatalf("source restrictions lost: %s", actual)
			}
			rows, err := h.DB.QueryContext(ctx, actual)
			if err != nil {
				t.Fatalf("SQL %s: %v", actual, err)
			}
			defer rows.Close()
			if rows.Next() {
				t.Fatal("discovery executed a data-producing query")
			}
			if err = rows.Err(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
