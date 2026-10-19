package sql

import (
	"testing"

	"github.com/viant/datly/data"
)

func TestProjectionSQLRolesSurviveDerivedStars(t *testing.T) {
	inner := "SELECT region AS area, LOWER(country) AS country_key, SUM(amount) AS total FROM sales GROUP BY region, LOWER(country)"
	for _, text := range []string{inner, "SELECT c.* FROM (" + inner + ") c", "SELECT d.* FROM (SELECT c.* FROM (" + inner + ") c) d"} {
		columns, err := (SelectorProjection{SQL: text}).Columns(nil)
		if err != nil {
			t.Fatal(err)
		}
		names := []string{"area", "country_key", "total"}
		if len(columns) != len(names) {
			t.Fatalf("columns=%d", len(columns))
		}
		for i, column := range columns {
			role, known := column.Groupable()
			if column.OutputName() != names[i] || !known || role != (i < 2) {
				t.Fatalf("%s column %d: %s role=%v known=%v", text, i, column.OutputName(), role, known)
			}
		}
	}
}

func TestPhysicalWildcardHasNoSQLRole(t *testing.T) {
	view := &data.View{Columns: []*data.Column{{Name: "region", Column: "region", Groupable: true}}}
	columns, err := (SelectorProjection{SQL: "SELECT * FROM sales", View: view}).Columns(nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, column := range columns {
		if _, known := column.Groupable(); known {
			t.Fatal("physical metadata fabricated SQL role")
		}
	}
}

func TestProjectionSQLRolesWithParenthesesAndPredicateTemplates(t *testing.T) {
	text := `( SELECT s.region,
CASE WHEN 1=0 ${predicate.Builder().CombineOr($predicate.FilterGroup(2, "AND")).Build("OR")} THEN s.country ELSE LOWER(s.country) END AS country_key,
SUM(s.amount) AS total FROM sales s
WHERE s.region IS NOT NULL
${predicate.Builder().CombineOr($predicate.FilterGroup(0, "AND")).Build("AND")}
GROUP BY s.region,country_key
${predicate.Builder().CombineOr($predicate.FilterGroup(1, "AND")).Build("HAVING")} )`
	columns, err := (SelectorProjection{SQL: text}).Columns(nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(columns) != 3 {
		t.Fatalf("columns=%d", len(columns))
	}
	for i, column := range columns {
		role, known := column.Groupable()
		if !known || role != (i < 2) {
			t.Fatalf("column %s role=%v known=%v", column.OutputName(), role, known)
		}
	}
}
