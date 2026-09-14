package builder

import (
	"testing"

	"github.com/viant/datly/data"
)

func TestRelationColumnInHelpers(t *testing.T) {
	t.Run("nil relation strips explicit tokens", func(t *testing.T) {
		sqlText, replaced := (relationFilter{}).applyColumnIn("SELECT * FROM t WHERE $COLUMN_IN", false)
		if replaced {
			t.Fatalf("expected no replacement for nil relation")
		}
		if sqlText != "SELECT * FROM t" {
			t.Fatalf("unexpected stripped SQL: %q", sqlText)
		}
	})

	t.Run("scalar expr with namespace", func(t *testing.T) {
		relation := &data.Relation{
			Of: &data.RelationRef{
				On: data.Links{
					{Namespace: "acct", Column: "user_id"},
				},
			},
		}
		actual := (relationFilter{relation: relation, positionalArgs: []any{1, 2}}).columnInExpression()
		if actual != "acct.user_id IN (?,?)" {
			t.Fatalf("unexpected relation column-in expr: %q", actual)
		}
	})

	t.Run("composite empty rows become no match", func(t *testing.T) {
		relation := &data.Relation{
			Of: &data.RelationRef{
				On: data.Links{{Column: "user_id"}},
			},
		}
		actual := (relationFilter{relation: relation, compositeColumns: []string{"tenant_id", "user_id"}}).columnInExpression()
		if actual != "1 = 0" {
			t.Fatalf("unexpected empty composite expr: %q", actual)
		}
	})

	t.Run("composite fallback with no rows keeps sql untouched", func(t *testing.T) {
		relation := &data.Relation{
			Of: &data.RelationRef{
				On: data.Links{{Column: "user_id"}},
			},
		}
		filter := relationFilter{relation: relation, compositeColumns: []string{"tenant_id", "user_id"}}
		actual, replaced := filter.applyColumnIn("SELECT * FROM accounts", false)
		if replaced || actual != "SELECT * FROM accounts" {
			t.Fatalf("expected empty composite fallback noop, got %q %v", actual, replaced)
		}
	})

	t.Run("fallback inserts before order by", func(t *testing.T) {
		relation := &data.Relation{
			Of: &data.RelationRef{
				On: data.Links{{Column: "user_id"}},
			},
		}
		actual, replaced := (relationFilter{relation: relation, positionalArgs: []any{7, 8}}).applyColumnIn("SELECT * FROM accounts ORDER BY id", false)
		if !replaced {
			t.Fatalf("expected fallback replacement")
		}
		if actual != "SELECT * FROM accounts WHERE user_id IN (?,?) ORDER BY id" {
			t.Fatalf("unexpected fallback SQL: %q", actual)
		}
	})

	t.Run("literal question mark does not suppress fallback", func(t *testing.T) {
		relation := &data.Relation{
			Of: &data.RelationRef{On: data.Links{{Column: "user_id"}}},
		}
		actual, replaced := (relationFilter{relation: relation, positionalArgs: []any{7}}).applyColumnIn("SELECT '?' AS marker, user_id FROM accounts ORDER BY user_id", false)
		if !replaced {
			t.Fatal("expected scalar fallback replacement")
		}
		expect := "SELECT '?' AS marker, user_id FROM accounts WHERE user_id IN (?) ORDER BY user_id"
		if actual != expect {
			t.Fatalf("unexpected literal-placeholder fallback SQL: %q", actual)
		}
	})

	t.Run("and token with no values becomes no match", func(t *testing.T) {
		relation := &data.Relation{
			Of: &data.RelationRef{
				On: data.Links{{Column: "user_id"}},
			},
		}
		actual, replaced := (relationFilter{relation: relation}).applyColumnIn("SELECT * FROM accounts WHERE 1=1 $AND_COLUMN_IN", false)
		if !replaced {
			t.Fatalf("expected explicit token replacement")
		}
		if actual != "SELECT * FROM accounts WHERE 1=1  AND ( 1 = 0 )" {
			t.Fatalf("unexpected no-match AND SQL: %q", actual)
		}
	})

	t.Run("explicit token skips protected SQL text", func(t *testing.T) {
		relation := &data.Relation{
			Of: &data.RelationRef{On: data.Links{{Column: "user_id"}}},
		}
		source := "SELECT '$COLUMN_IN' AS marker FROM accounts WHERE $COLUMN_IN -- $COLUMN_IN"
		actual, replaced := (relationFilter{relation: relation, positionalArgs: []any{7}}).applyColumnIn(source, false)
		if !replaced {
			t.Fatal("expected executable relation token replacement")
		}
		expect := "SELECT '$COLUMN_IN' AS marker FROM accounts WHERE user_id IN (?) -- $COLUMN_IN"
		if actual != expect {
			t.Fatalf("unexpected protected relation SQL:\nactual: %q\nexpect: %q", actual, expect)
		}
	})

	t.Run("disable fallback keeps SQL untouched", func(t *testing.T) {
		relation := &data.Relation{
			Of: &data.RelationRef{
				On: data.Links{{Column: "user_id"}},
			},
		}
		actual, replaced := (relationFilter{relation: relation, positionalArgs: []any{7}}).applyColumnIn("SELECT * FROM accounts", true)
		if replaced || actual != "SELECT * FROM accounts" {
			t.Fatalf("expected disable-fallback noop, got %q %v", actual, replaced)
		}
	})

	t.Run("scalar fallback with no values becomes no match", func(t *testing.T) {
		relation := &data.Relation{
			Of: &data.RelationRef{
				On: data.Links{{Column: "user_id"}},
			},
		}
		actual, replaced := (relationFilter{relation: relation}).applyColumnIn("SELECT * FROM accounts", false)
		if !replaced || actual != "SELECT * FROM accounts WHERE 1 = 0" {
			t.Fatalf("expected empty scalar fallback no-match, got %q %v", actual, replaced)
		}
	})

	t.Run("scalar fallback with invalid relation strips tokens", func(t *testing.T) {
		relation := &data.Relation{
			Of: &data.RelationRef{
				On: data.Links{{Column: "   "}},
			},
		}
		actual, replaced := (relationFilter{relation: relation, positionalArgs: []any{7}}).applyColumnIn("SELECT * FROM accounts $COLUMN_IN", false)
		if !replaced || actual != "SELECT * FROM accounts 1 = 0" {
			t.Fatalf("expected invalid scalar relation to resolve to no-match replacement, got %q %v", actual, replaced)
		}
	})

	t.Run("scalar fallback with invalid relation and no token is noop-strip", func(t *testing.T) {
		relation := &data.Relation{
			Of: &data.RelationRef{
				On: data.Links{{Column: "   "}},
			},
		}
		actual, replaced := (relationFilter{relation: relation, positionalArgs: []any{7}}).applyColumnIn("SELECT * FROM accounts", false)
		if replaced || actual != "SELECT * FROM accounts" {
			t.Fatalf("expected invalid scalar relation fallback noop, got %q %v", actual, replaced)
		}
	})

	t.Run("composite fallback with existing where adds and", func(t *testing.T) {
		relation := &data.Relation{
			Of: &data.RelationRef{
				On: data.Links{{Column: "user_id"}},
			},
		}
		filter := relationFilter{relation: relation, compositeColumns: []string{"tenant_id", "user_id"}, compositeRows: [][]interface{}{{1, 7}}}
		actual, replaced := filter.applyColumnIn("SELECT * FROM accounts WHERE active = 1 ORDER BY id", false)
		if !replaced {
			t.Fatalf("expected composite fallback replacement")
		}
		if actual != "SELECT * FROM accounts WHERE active = 1 AND (tenant_id, user_id) IN ((?, ?)) ORDER BY id" {
			t.Fatalf("unexpected composite fallback SQL: %q", actual)
		}
	})

	t.Run("composite fallback skips placeholder sql", func(t *testing.T) {
		relation := &data.Relation{
			Of: &data.RelationRef{
				On: data.Links{{Column: "user_id"}},
			},
		}
		filter := relationFilter{relation: relation, compositeColumns: []string{"tenant_id", "user_id"}, compositeRows: [][]interface{}{{1, 7}}}
		actual, replaced := filter.applyColumnIn("SELECT * FROM accounts WHERE tenant_id = ?", false)
		if replaced || actual != "SELECT * FROM accounts WHERE tenant_id = ?" {
			t.Fatalf("expected placeholder sql noop, got %q %v", actual, replaced)
		}
	})

	t.Run("relation scalar expr blank column fails", func(t *testing.T) {
		relation := &data.Relation{
			Of: &data.RelationRef{
				On: data.Links{{Column: "   "}},
			},
		}
		if _, ok := relationScalarColumnExpr(relation); ok {
			t.Fatalf("expected blank column to fail")
		}
	})

	t.Run("relation column in expr without args becomes no match", func(t *testing.T) {
		relation := &data.Relation{
			Of: &data.RelationRef{
				On: data.Links{{Column: "user_id"}},
			},
		}
		if actual := (relationFilter{relation: relation}).columnInExpression(); actual != "1 = 0" {
			t.Fatalf("unexpected scalar no-arg expr: %q", actual)
		}
	})

	t.Run("insert relation clause appends when no ordering clauses", func(t *testing.T) {
		if actual := insertRelationClause("SELECT * FROM accounts", " WHERE user_id IN (?)"); actual != "SELECT * FROM accounts WHERE user_id IN (?)" {
			t.Fatalf("unexpected appended relation clause: %q", actual)
		}
	})
}
