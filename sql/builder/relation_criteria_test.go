package builder

import (
	"testing"

	"github.com/viant/datly/data"
)

func TestRelationCriteriaExpr(t *testing.T) {
	t.Run("nil relation", func(t *testing.T) {
		if actual, ok := (relationFilter{}).expression(); ok || actual != "" {
			t.Fatalf("expected nil relation to return empty criteria, got %q %v", actual, ok)
		}
	})

	t.Run("composite no rows", func(t *testing.T) {
		relation := &data.Relation{
			Of: &data.RelationRef{
				On: data.Links{{Column: "user_id"}},
			},
		}
		if actual, ok := (relationFilter{relation: relation, compositeColumns: []string{"tenant_id", "user_id"}}).expression(); !ok || actual != "1 = 0" {
			t.Fatalf("expected empty composite no-match criteria, got %q %v", actual, ok)
		}
	})

	t.Run("scalar no args", func(t *testing.T) {
		relation := &data.Relation{
			Of: &data.RelationRef{
				On: data.Links{{Column: "user_id"}},
			},
		}
		if actual, ok := (relationFilter{relation: relation}).expression(); !ok || actual != "1 = 0" {
			t.Fatalf("expected empty scalar no-match criteria, got %q %v", actual, ok)
		}
	})

	t.Run("scalar invalid relation", func(t *testing.T) {
		relation := &data.Relation{
			Of: &data.RelationRef{
				On: data.Links{{Column: "   "}},
			},
		}
		if actual, ok := (relationFilter{relation: relation, positionalArgs: []any{7}}).expression(); ok || actual != "" {
			t.Fatalf("expected invalid scalar relation to return empty criteria, got %q %v", actual, ok)
		}
	})

	t.Run("scalar args", func(t *testing.T) {
		relation := &data.Relation{
			Of: &data.RelationRef{
				On: data.Links{{Namespace: "acct", Column: "user_id"}},
			},
		}
		actual, ok := (relationFilter{relation: relation, positionalArgs: []any{7, 8}}).expression()
		if !ok || actual != "acct.user_id IN (?,?)" {
			t.Fatalf("unexpected scalar criteria: %q %v", actual, ok)
		}
	})

	t.Run("composite rows", func(t *testing.T) {
		relation := &data.Relation{
			Of: &data.RelationRef{
				On: data.Links{{Column: "user_id"}},
			},
		}
		actual, ok := (relationFilter{relation: relation, compositeColumns: []string{"tenant_id", "user_id"}, compositeRows: [][]interface{}{{1, 7}}}).expression()
		if !ok || actual != "(tenant_id, user_id) IN ((?, ?))" {
			t.Fatalf("unexpected composite criteria: %q %v", actual, ok)
		}
	})
}

func TestApplyRelationCriteriaTokens(t *testing.T) {
	relation := &data.Relation{
		Of: &data.RelationRef{
			On: data.Links{{Column: "user_id"}},
		},
	}

	filter := relationFilter{relation: relation, positionalArgs: []any{7, 8}}
	actual := filter.applyCriteriaTokens("SELECT * FROM accounts $WHERE_CRITERIA ORDER BY id")
	if actual != "SELECT * FROM accounts WHERE user_id IN (?,?) ORDER BY id" {
		t.Fatalf("unexpected WHERE criteria SQL: %q", actual)
	}

	filter.positionalArgs = []any{7}
	actual = filter.applyCriteriaTokens("SELECT * FROM accounts WHERE 1=1 $AND_CRITERIA ORDER BY id")
	if actual != "SELECT * FROM accounts WHERE 1=1 AND (user_id IN (?)) ORDER BY id" {
		t.Fatalf("unexpected AND criteria SQL: %q", actual)
	}

	actual = filter.applyCriteriaTokens("SELECT * FROM accounts WHERE 1=0 $OR_CRITERIA ORDER BY id")
	if actual != "SELECT * FROM accounts WHERE 1=0 OR (user_id IN (?)) ORDER BY id" {
		t.Fatalf("unexpected OR criteria SQL: %q", actual)
	}

	actual = filter.applyCriteriaTokens("SELECT '$WHERE_CRITERIA' AS marker FROM accounts $WHERE_CRITERIA -- $WHERE_CRITERIA")
	expect := "SELECT '$WHERE_CRITERIA' AS marker FROM accounts WHERE user_id IN (?) -- $WHERE_CRITERIA"
	if actual != expect {
		t.Fatalf("unexpected protected relation criteria SQL:\nactual: %q\nexpect: %q", actual, expect)
	}
}
