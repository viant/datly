package sql

import (
	"testing"

	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/query"
)

func TestNormalizeProjectionItem(t *testing.T) {
	t.Run("nil item", func(t *testing.T) {
		actual, keep, changed := normalizeProjectionItem(nil, "")
		if actual != "" || keep || changed {
			t.Fatalf("unexpected nil item result: %q %v %v", actual, keep, changed)
		}
	})

	t.Run("non call item", func(t *testing.T) {
		stmt, err := sqlparser.ParseQuery("SELECT name FROM users")
		if err != nil {
			t.Fatalf("parse query: %v", err)
		}
		actual, keep, changed := normalizeProjectionItem(stmt.List[0], "name")
		if actual != "name" || !keep || changed {
			t.Fatalf("unexpected plain item result: %q %v %v", actual, keep, changed)
		}
	})

	t.Run("removable select call", func(t *testing.T) {
		item := &query.Item{
			Expr: &expr.Call{X: &expr.Ident{Name: "allow_nulls"}},
		}
		actual, keep, changed := normalizeProjectionItem(item, "allow_nulls(name)")
		if actual != "" || keep || !changed {
			t.Fatalf("unexpected removable call result: %q %v %v", actual, keep, changed)
		}
	})

	t.Run("required preserves alias", func(t *testing.T) {
		stmt, err := sqlparser.ParseQuery("SELECT required(name) AS display_name FROM users")
		if err != nil {
			t.Fatalf("parse query: %v", err)
		}
		item := stmt.List[0]
		actual, keep, changed := normalizeProjectionItem(item, "required(name) AS display_name")
		if actual != "name AS display_name" || !keep || !changed {
			t.Fatalf("unexpected required result: %q %v %v", actual, keep, changed)
		}
	})

	t.Run("tag preserves alias", func(t *testing.T) {
		stmt, err := sqlparser.ParseQuery("SELECT tag(name) AS display_name FROM users")
		if err != nil {
			t.Fatalf("parse query: %v", err)
		}
		actual, keep, changed := normalizeProjectionItem(stmt.List[0], "tag(name) AS display_name")
		if actual != "name AS display_name" || !keep || !changed {
			t.Fatalf("unexpected tag result: %q %v %v", actual, keep, changed)
		}
	})

	t.Run("cast normalizes and preserves alias", func(t *testing.T) {
		stmt, err := sqlparser.ParseQuery("SELECT cast(name as \"TEXT\") AS display_name FROM users")
		if err != nil {
			t.Fatalf("parse query: %v", err)
		}
		actual, keep, changed := normalizeProjectionItem(stmt.List[0], "cast(name as \"TEXT\") AS display_name")
		if actual != "CAST(name AS TEXT) AS display_name" || !keep || !changed {
			t.Fatalf("unexpected cast result: %q %v %v", actual, keep, changed)
		}
	})

	t.Run("cast invalid arg keeps original", func(t *testing.T) {
		stmt, err := sqlparser.ParseQuery("SELECT cast(name) AS display_name FROM users")
		if err != nil {
			t.Fatalf("parse query: %v", err)
		}
		actual, keep, changed := normalizeProjectionItem(stmt.List[0], "cast(name) AS display_name")
		if actual != "cast(name) AS display_name" || !keep || changed {
			t.Fatalf("unexpected invalid cast result: %q %v %v", actual, keep, changed)
		}
	})
}

func TestNormalizeCastArg(t *testing.T) {
	useCases := []struct {
		input  string
		expect string
	}{
		{input: "", expect: ""},
		{input: "name", expect: ""},
		{input: "name as \"TEXT\"", expect: "CAST(name AS TEXT)"},
		{input: "price AS 'DECIMAL(10,2)'", expect: "CAST(price AS DECIMAL(10,2))"},
		{input: "name AS ''", expect: ""},
	}

	for _, useCase := range useCases {
		if actual := normalizeCastArg(useCase.input); actual != useCase.expect {
			t.Fatalf("normalizeCastArg(%q) = %q, want %q", useCase.input, actual, useCase.expect)
		}
	}
}

func TestApplyAlias(t *testing.T) {
	if actual := applyAlias("name", "display_name"); actual != "name AS display_name" {
		t.Fatalf("unexpected alias application: %q", actual)
	}
	if actual := applyAlias("name", ""); actual != "name" {
		t.Fatalf("unexpected empty alias application: %q", actual)
	}
	if actual := applyAlias("", "display_name"); actual != "" {
		t.Fatalf("unexpected empty expr application: %q", actual)
	}
}

func TestProjectionHelpers(t *testing.T) {
	if actual := canonicalProjectionName("`user_id`.Total_Value"); actual != `identifier:["user_id","total_value"]` {
		t.Fatalf("unexpected canonical projection name: %q", actual)
	}
	if actual := canonicalProjectionName("   "); actual != "" {
		t.Fatalf("expected empty canonical projection name, got %q", actual)
	}

	stmt, err := sqlparser.ParseQuery("SELECT id, name FROM users")
	if err != nil {
		t.Fatalf("parse query: %v", err)
	}
	matched, names := projectionItemSelected(stmt.List[0], "id", []string{"name"})
	if matched || len(names) != 0 {
		t.Fatalf("expected projection item to not match, got %v %+v", matched, names)
	}

	if actual := projectionCallName(nil, "custom(name)"); actual != "custom" {
		t.Fatalf("unexpected projection call name fallback: %q", actual)
	}
	if actual := projectionCallName(nil, "name"); actual != "name" {
		t.Fatalf("unexpected projection call plain fallback: %q", actual)
	}
}
