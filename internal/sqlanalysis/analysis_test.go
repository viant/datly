package sqlanalysis

import (
	"strings"
	"testing"

	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/query"
	veltyexpr "github.com/viant/velty/ast/expr"
)

func TestParseRetainsCheckedProjection(t *testing.T) {
	source := `SELECT records.* FROM (SELECT r.id, r.name FROM records r ${predicate.Builder().CombineOr($predicate.FilterGroup(0, "AND")).Build("WHERE")}) records`
	analysis, parsed, err := Parse(source, nil)
	if err != nil {
		t.Fatal(err)
	}
	if parsed == nil || !parsed.List.IsStarExpr() {
		t.Fatal("missing checked outer projection")
	}
	if strings.Contains(analysis, "${predicate") || !strings.Contains(analysis, "__datly_analysis_predicate_") {
		t.Fatal("predicate was not statically masked")
	}
	if !strings.Contains(sqlparser.Stringify(parsed), "records") {
		t.Fatal("source identity lost")
	}
	if !strings.Contains(source, "${predicate") {
		t.Fatal("executable source changed")
	}
}

func TestParseRejectsDynamicAuthority(t *testing.T) {
	for _, source := range []string{
		`SELECT ${predicate.Expand("x")} FROM records`,
		`SELECT r.id FROM $Unsafe.Table r`,
		`SELECT r.id FROM ${Input.Table} r`,
	} {
		t.Run(source, func(t *testing.T) {
			if _, _, err := Parse(source, nil); err == nil {
				t.Fatal("unowned SQL structure admitted")
			}
		})
	}
}

func TestParseUsesDeclaredInputAuthority(t *testing.T) {
	source := `SELECT r.id FROM records r WHERE r.id = ${Input.Id}`
	if _, _, err := Parse(source, nil); err == nil {
		t.Fatal("input accepted without declaration authority")
	}
	calls := 0
	text, parsed, err := Parse(source, func(selector *veltyexpr.Select) (string, error) {
		calls++
		if selector.ID != "Input" {
			t.Fatalf("unexpected input %s", selector.ID)
		}
		return "Input.Id", nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 || parsed == nil || strings.Contains(text, "${Input.Id}") {
		t.Fatal("declared input was not checked exactly once")
	}
}

func TestParsePlainQueryAndExistingSQLBehavior(t *testing.T) {
	text, parsed, err := Parse(`((SELECT r.id FROM records r))`, nil)
	if err != nil || parsed == nil || text != `((SELECT r.id FROM records r))` {
		t.Fatalf("plain query: %q %v", text, err)
	}
	// SQL-only callers historically defer parsing plain statements to their own
	// consumer; retaining that boundary avoids widening this refactor's behavior.
	if text, err := SQL("not a query", nil); err != nil || text != "not a query" {
		t.Fatalf("SQL-only contract changed: %q %v", text, err)
	}
	// Structural validation is owned by SQLParser; preserve its current result
	// rather than introducing a new SQL admission policy in this extraction.
	_, directErr := sqlparser.ParseQuery("not a query", sqlparser.WithStructuralValidation())
	_, _, sharedErr := Parse("not a query", nil)
	if (directErr == nil) != (sharedErr == nil) {
		t.Fatal("native parser behavior changed")
	}
}

func TestCheckedRetainsNestedASTWithoutChangingSource(t *testing.T) {
	for _, source := range []string{
		`SELECT records.* FROM (SELECT r.id,r.name FROM records r) records`,
		`SELECT records.* FROM (SELECT r.id,r.name FROM records r ${predicate.Builder().CombineOr($predicate.FilterGroup(0,"AND")).Build("WHERE")}) records`,
	} {
		t.Run(source, func(t *testing.T) {
			_, parsed, err := Checked(source, nil)
			if err != nil {
				t.Fatal(err)
			}
			raw, ok := parsed.From.X.(*expr.Raw)
			if !ok {
				t.Fatal("derived source missing")
			}
			child, ok := raw.X.(*query.Select)
			if !ok || len(child.List) != 2 {
				t.Fatal("checked complete child AST not retained")
			}
			if strings.Contains(source, "__datly_analysis_") {
				t.Fatal("analysis replaced executable source")
			}
			if !strings.Contains(raw.Raw, "SELECT r.id,r.name FROM records r") {
				t.Fatal("analysis source bytes lost")
			}
		})
	}
}
