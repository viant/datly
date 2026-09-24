package column

import (
	"context"
	"strings"
	"testing"

	"github.com/mattn/go-sqlite3"
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

// Execute against populated tables: checking for the text "1 = 0" alone
// cannot detect an OR branch that escapes the discovery predicate.
func TestDiscoveryQuerySuppressesMatchingRows(t *testing.T) {
	h := sqlite.New(t)
	ctx := context.Background()
	if err := h.ExecStatements(ctx,
		`CREATE TABLE discovery_events(id INTEGER, name TEXT)`,
		`INSERT INTO discovery_events VALUES (1, 'first'), (2, 'second'), (3, 'third')`,
	); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name string
		SQL  string
		args []any
	}{
		{name: "no predicate", SQL: `SELECT id, name FROM discovery_events`},
		{name: "and", SQL: `SELECT id, name FROM discovery_events WHERE id > 0 AND id < 3`},
		{name: "or", SQL: `SELECT id, name FROM discovery_events WHERE id = 1 OR id = 2`},
		{name: "multiple or", SQL: `SELECT id, name FROM discovery_events WHERE id = 1 OR id = 2 OR id = 3`},
		{name: "mixed precedence", SQL: `SELECT id, name FROM discovery_events WHERE id = 1 AND name = 'first' OR id = 2 AND name = 'second'`},
		{name: "parenthesized or", SQL: `SELECT id, name FROM discovery_events WHERE (id = 1 OR id = 2)`},
		{name: "nested conditions", SQL: `SELECT id, name FROM discovery_events WHERE (id = 1 AND (name = 'first' OR name = 'second')) OR (id = 2 AND name = 'second')`},
		{name: "not", SQL: `SELECT id, name FROM discovery_events WHERE NOT (id = 3) OR id = 3`},
		{name: "parameters", SQL: `SELECT id, name FROM discovery_events WHERE id = ? OR name = ?`, args: []any{1, "second"}},
		{name: "limit offset", SQL: `SELECT id, name FROM discovery_events WHERE id = 1 OR id = 2 ORDER BY id LIMIT 1 OFFSET 1`},
		{name: "union", SQL: `SELECT id, name FROM discovery_events WHERE id = 1 OR id = 2 UNION SELECT id, name FROM discovery_events WHERE id = 2 OR id = 3`},
		{name: "union all chain", SQL: `SELECT id, name FROM discovery_events WHERE id = 1 OR id = 2 UNION ALL SELECT id, name FROM discovery_events UNION ALL SELECT id, name FROM discovery_events WHERE id = 2 OR id = 3`},
		{name: "cte", SQL: `WITH active AS (SELECT id, name FROM discovery_events WHERE id = 1 OR id = 2) SELECT id, name FROM active WHERE id = 1 OR id = 2`},
		{name: "derived table", SQL: `SELECT id, name FROM (SELECT id, name FROM discovery_events WHERE id = 1 OR id = 2) e WHERE id = 1 OR id = 2`},
		{name: "join", SQL: `SELECT e.id, e.name FROM discovery_events e JOIN discovery_events other ON e.id = other.id WHERE e.id = 1 OR other.id = 2`},
		{name: "grouped aggregate", SQL: `SELECT id, MAX(name) AS name FROM discovery_events WHERE id = 1 OR id = 2 GROUP BY id HAVING COUNT(*) > 0`},
		{name: "nested union all", SQL: `SELECT id, name FROM (
SELECT id, name FROM discovery_events WHERE id = 1 OR id = 2
UNION ALL SELECT id, name FROM discovery_events WHERE id = 2 OR id = 3
UNION ALL SELECT id, name FROM (SELECT id, name FROM discovery_events UNION ALL SELECT id, name FROM discovery_events) nested
) combined WHERE id = 1 OR id = 2`},
		{name: "union all of derived unions", SQL: `SELECT id, name FROM (
SELECT id, name FROM discovery_events UNION ALL SELECT id, name FROM discovery_events
) first WHERE id = 1 OR id = 2
UNION ALL SELECT id, name FROM (
SELECT id, name FROM discovery_events UNION ALL SELECT id, name FROM discovery_events
) second WHERE id = 2 OR id = 3
UNION ALL SELECT id, name FROM discovery_events WHERE id = 1 OR id = 3`},
		{name: "cte unions join grouping and pagination", SQL: `WITH combined AS (
SELECT id, name FROM discovery_events WHERE id = 1 OR id = 2
UNION ALL SELECT id, name FROM discovery_events WHERE id = 2 OR id = 3
UNION ALL SELECT id, name FROM discovery_events
), grouped AS (SELECT id, MAX(name) AS name FROM combined GROUP BY id)
SELECT g.id, g.name FROM grouped g JOIN discovery_events e ON e.id = g.id WHERE g.id = 1 OR e.id = 2
UNION ALL SELECT id, name FROM combined WHERE id = 2 OR id = 3
UNION ALL SELECT id, name FROM grouped WHERE id = 1 OR id = 3
ORDER BY id LIMIT 2 OFFSET 1`},
	}
	branches := make([]string, 64)
	for i := range branches {
		branches[i] = `SELECT id, name FROM discovery_events WHERE id = 1 OR id = 2`
	}
	tests = append(tests, struct {
		name string
		SQL  string
		args []any
	}{name: "64 union all branches", SQL: strings.Join(branches, " UNION ALL ")})
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Establish that the fixture actually exercises a matching branch.
			original, err := h.DB.QueryContext(ctx, tt.SQL, tt.args...)
			if err != nil {
				t.Fatal(err)
			}
			matched := original.Next()
			originalErr := original.Err()
			original.Close()
			if originalErr != nil {
				t.Fatal(originalErr)
			}
			if !matched {
				t.Fatal("authored query must match fixture rows")
			}

			source := &spec.ViewSource{SQL: tt.SQL}
			actual, err := discoveryQuery(source)
			if err != nil {
				t.Fatal(err)
			}
			if source.SQL != tt.SQL {
				t.Fatal("discovery mutated authored SQL")
			}
			rows, err := h.DB.QueryContext(ctx, actual, tt.args...)
			if err != nil {
				t.Fatalf("discovery SQL %s: %v", actual, err)
			}
			defer rows.Close()
			columns, err := rows.Columns()
			if err != nil {
				t.Fatal(err)
			}
			if len(columns) != 2 || columns[0] != "id" || columns[1] != "name" {
				t.Fatalf("projection metadata lost: %v; SQL: %s", columns, actual)
			}
			if rows.Next() {
				t.Fatalf("discovery returned data: %s", actual)
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			if tt.name == "limit offset" && (strings.Contains(actual, "LIMIT") || strings.Contains(actual, "OFFSET")) {
				t.Fatalf("discovery retained pagination: %s", actual)
			}
		})
	}
}

func TestDiscoveryQueryAggregateHasNoMatchingInput(t *testing.T) {
	h := sqlite.New(t)
	ctx := context.Background()
	if err := h.ExecStatements(ctx, `CREATE TABLE discovery_events(id INTEGER)`, `INSERT INTO discovery_events VALUES (1), (2), (3)`); err != nil {
		t.Fatal(err)
	}
	actual, err := discoveryQuery(&spec.ViewSource{SQL: `SELECT COUNT(*) AS total FROM discovery_events WHERE id = 1 OR id = 2`})
	if err != nil {
		t.Fatal(err)
	}
	// An ungrouped aggregate still returns a row over empty input. Its count
	// must be zero, proving that the OR branch did not admit underlying data.
	var count int
	if err := h.DB.QueryRowContext(ctx, actual).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("discovery aggregate consumed %d matching rows: %s", count, actual)
	}
}

func TestDiscoveryComplexUnionSkipsExpensiveExpressions(t *testing.T) {
	h := sqlite.New(t)
	ctx := context.Background()
	if err := h.ExecStatements(ctx, `CREATE TABLE discovery_events(id INTEGER)`, `INSERT INTO discovery_events VALUES (1), (2), (3)`); err != nil {
		t.Fatal(err)
	}
	conn, err := h.DB.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	calls := 0
	if err := conn.Raw(func(driverConn any) error {
		return driverConn.(*sqlite3.SQLiteConn).RegisterFunc("discovery_expensive", func(id int64) int64 {
			calls++
			return id
		}, false)
	}); err != nil {
		t.Fatal(err)
	}
	SQL := `WITH combined AS (
 SELECT discovery_expensive(id) AS id FROM discovery_events
 UNION ALL SELECT discovery_expensive(id) AS id FROM discovery_events
 UNION ALL SELECT discovery_expensive(id) AS id FROM discovery_events
 ), grouped AS (SELECT id, COUNT(*) AS total FROM combined GROUP BY id)
 SELECT id FROM grouped WHERE id = 1 OR id = 2
 UNION ALL SELECT id FROM (
 SELECT discovery_expensive(id) AS id FROM discovery_events
 UNION ALL SELECT discovery_expensive(id) AS id FROM discovery_events
 ) nested WHERE id = 2 OR id = 3
 UNION ALL SELECT discovery_expensive(id) AS id FROM discovery_events WHERE id = 1 OR id = 3`
	run := func(query string) int {
		t.Helper()
		rows, err := conn.QueryContext(ctx, query)
		if err != nil {
			t.Fatalf("SQL %s: %v", query, err)
		}
		defer rows.Close()
		count := 0
		for rows.Next() {
			count++
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		return count
	}
	if count := run(SQL); count == 0 || calls == 0 {
		t.Fatalf("fixture did not exercise expensive expressions: rows=%d calls=%d", count, calls)
	}
	calls = 0
	actual, err := discoveryQuery(&spec.ViewSource{SQL: SQL})
	if err != nil {
		t.Fatal(err)
	}
	if count := run(actual); count != 0 {
		t.Fatalf("discovery returned %d rows: %s", count, actual)
	}
	if calls != 0 {
		t.Fatalf("discovery evaluated expensive expression %d times: %s", calls, actual)
	}
}
