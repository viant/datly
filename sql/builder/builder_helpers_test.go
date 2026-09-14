package builder

import (
	"database/sql"
	"reflect"
	"strings"
	"testing"
)

func normalizeSQLForAssert(value string) string {
	return strings.Join(strings.Fields(value), " ")
}

func countRows(t *testing.T, db *sql.DB, query string, args ...interface{}) int {
	t.Helper()
	rows, err := db.Query(query, args...)
	if err != nil {
		t.Fatalf("query failed: %v; SQL=%s", err, query)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("row iteration failed: %v", err)
	}
	return count
}

func TestBuilderHelperBranches(t *testing.T) {
	t.Run("expand positional in clause no expansion", func(t *testing.T) {
		if actual := expandPositionalInClause("SELECT * FROM users WHERE id = ?", 1); actual != "SELECT * FROM users WHERE id = ?" {
			t.Fatalf("unexpected single-arg positional expansion: %q", actual)
		}
		if actual := expandPositionalInClause("SELECT * FROM users WHERE id = ? AND tenant_id = ?", 3); actual != "SELECT * FROM users WHERE id = ? AND tenant_id = ?" {
			t.Fatalf("unexpected multi-placeholder positional expansion: %q", actual)
		}
	})

	t.Run("expand positional in clause single unnamed placeholder", func(t *testing.T) {
		actual := expandPositionalInClause("SELECT * FROM users WHERE id IN (?) AND tenant_id = :TenantID", 3)
		expect := "SELECT * FROM users WHERE id IN (?,?,?) AND tenant_id = :TenantID"
		if actual != expect {
			t.Fatalf("unexpected positional expansion:\nactual: %q\nexpect: %q", actual, expect)
		}
	})

	t.Run("expand positional in clause skips protected question marks", func(t *testing.T) {
		actual := expandPositionalInClause("SELECT '?' AS marker FROM users WHERE id IN (?) -- ?", 2)
		expect := "SELECT '?' AS marker FROM users WHERE id IN (?,?) -- ?"
		if actual != expect {
			t.Fatalf("unexpected protected positional expansion:\nactual: %q\nexpect: %q", actual, expect)
		}
	})

	t.Run("flatten composite args", func(t *testing.T) {
		if actual := flattenCompositeArgs(nil); actual != nil {
			t.Fatalf("expected nil flattened args, got %#v", actual)
		}
		actual := flattenCompositeArgs([][]interface{}{{1, "a"}, {2, "b"}})
		expect := []any{1, "a", 2, "b"}
		if !reflect.DeepEqual(actual, expect) {
			t.Fatalf("unexpected flattened composite args: got %#v, want %#v", actual, expect)
		}
	})

	t.Run("append composite where", func(t *testing.T) {
		sqlText, args := appendCompositeWhere("SELECT * FROM users", nil, [][]interface{}{{1, 7}, {2, 8}}, []string{"tenant_id", "user_id"}, nil)
		expectSQL := "SELECT * FROM users WHERE (tenant_id, user_id) IN ((?, ?), (?, ?))"
		expectArgs := []any{1, 7, 2, 8}
		if sqlText != expectSQL {
			t.Fatalf("unexpected composite where SQL: %q", sqlText)
		}
		if !reflect.DeepEqual(args, expectArgs) {
			t.Fatalf("unexpected composite where args: got %#v, want %#v", args, expectArgs)
		}
	})

	t.Run("append composite where with no columns is noop", func(t *testing.T) {
		sqlText, args := appendCompositeWhere("SELECT * FROM users", []any{7}, [][]interface{}{{1, 7}}, nil, nil)
		if sqlText != "SELECT * FROM users" {
			t.Fatalf("unexpected no-column composite where SQL: %q", sqlText)
		}
		if !reflect.DeepEqual(args, []any{7}) {
			t.Fatalf("unexpected no-column composite where args: %#v", args)
		}
	})
}
