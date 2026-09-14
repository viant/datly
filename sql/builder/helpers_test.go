package builder

import (
	"testing"

	"github.com/viant/datly/data"
	"github.com/viant/sqlx/metadata/info"
)

func TestCompositeInUsesNativeDefaults(t *testing.T) {
	if actual := renderCompositeIn(nil, nil, 0); actual != "1 = 0" {
		t.Fatalf("unexpected empty composite in: %q", actual)
	}
	actual := renderCompositeIn(nil, []string{"tenant_id", "user_id"}, 2)
	expect := "(tenant_id, user_id) IN ((?, ?), (?, ?))"
	if actual != expect {
		t.Fatalf("unexpected composite in: %q", actual)
	}
	if actual := renderCompositeIn(nil, []string{"id"}, 2); actual != "id IN (?, ?)" {
		t.Fatalf("scalar native IN=%q", actual)
	}
}

func TestRelationScalarColumnExpr(t *testing.T) {
	if _, ok := relationScalarColumnExpr(nil); ok {
		t.Fatalf("expected nil relation to fail")
	}
	relation := &data.Relation{
		Of: &data.RelationRef{
			On: data.Links{
				{Column: "user_id", Namespace: "acct"},
			},
		},
	}
	actual, ok := relationScalarColumnExpr(relation)
	if !ok || actual != "acct.user_id" {
		t.Fatalf("unexpected relation scalar expr: %q %v", actual, ok)
	}
}

func TestSelectorCriteriaReplacement(t *testing.T) {
	if actual := selectorCriteriaReplacement(whereSelectorCriteriaToken, ""); actual != "" {
		t.Fatalf("expected empty criteria replacement to noop, got %q", actual)
	}
	if actual := selectorCriteriaReplacement(whereSelectorCriteriaToken, "id = ?"); actual != " WHERE id = ?" {
		t.Fatalf("unexpected where replacement: %q", actual)
	}
	if actual := selectorCriteriaReplacement(andSelectorCriteriaToken, "id = ?"); actual != " AND id = ?" {
		t.Fatalf("unexpected and replacement: %q", actual)
	}
	if actual := selectorCriteriaReplacement(selectorCriteriaToken, "id = ?"); actual != "id = ?" {
		t.Fatalf("unexpected plain replacement: %q", actual)
	}
}

func TestRenderCompositeIn_UsesDialectRenderer(t *testing.T) {
	dialect := &info.Dialect{
		CompositeInRenderer: func(columns []string, rowCount int) string {
			return "custom(" + columns[0] + ")"
		},
	}
	actual := renderCompositeIn(dialect, []string{"tenant_id", "user_id"}, 2)
	if actual != "custom(tenant_id)" {
		t.Fatalf("unexpected custom composite renderer result: %q", actual)
	}
}

func TestRenderCompositeIn_DefaultsWithoutDialectRenderer(t *testing.T) {
	actual := renderCompositeIn(nil, []string{"tenant_id", "user_id"}, 2)
	expect := "(tenant_id, user_id) IN ((?, ?), (?, ?))"
	if actual != expect {
		t.Fatalf("unexpected default composite renderer result: %q", actual)
	}
}

func TestRenderCompositeIn_UsesDialectCompositeInWhenNoCustomRenderer(t *testing.T) {
	dialect := &info.Dialect{}
	actual := renderCompositeIn(dialect, []string{"tenant_id", "user_id"}, 2)
	expect := "(tenant_id, user_id) IN ((?, ?), (?, ?))"
	if actual != expect {
		t.Fatalf("unexpected dialect composite renderer result: %q", actual)
	}
}
