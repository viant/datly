package sql

import (
	"strings"
	"testing"

	"github.com/viant/datly/data"
	"github.com/viant/datly/spec"
	"github.com/viant/sqlparser"
)

func TestGroupedProjectionWindowClassification(t *testing.T) {
	cases := []struct {
		name, expression string
		aggregate        bool
	}{
		{"rank order aggregate", "RANK() OVER(ORDER BY AVG(amount) DESC)", true},
		{"partition aggregate", "RANK() OVER(PARTITION BY SUM(amount) ORDER BY category)", true},
		{"window aggregate argument", "AVG(AVG(amount)) OVER()", true},
		{"window null treatment aggregate argument", "FIRST_VALUE(AVG(amount) IGNORE NULLS) OVER(ORDER BY category)", true},
		{"window null treatment scalar argument", "FIRST_VALUE(amount IGNORE NULLS) OVER(ORDER BY category)", false},
		{"plain rank", "RANK() OVER(ORDER BY amount DESC)", false},
		{"plain window sum", "SUM(amount) OVER(PARTITION BY category)", false},
		{"wrapped window", "COALESCE(SUM(amount) OVER(), 0)", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := sqlparser.ParseQuery("SELECT " + tc.expression + " AS metric FROM sales")
			if err != nil {
				t.Fatal(err)
			}
			if got := ContainsAggregate(stmt.List[0].Expr); got != tc.aggregate {
				t.Fatalf("ContainsAggregate(%s)=%v, want %v", tc.expression, got, tc.aggregate)
			}
			if got := groupedProjectionGroupBy(stmt.List); len(got) != 0 {
				t.Fatalf("window projection included in GROUP BY: %v", got)
			}
		})
	}
}

func TestGroupedProjectionWindowRewrite(t *testing.T) {
	on := true
	view := &data.View{Spec: spec.View{Groupable: &on}}
	source := `SELECT category, day, SUM(amount) AS total, RANK() OVER(PARTITION BY category ORDER BY AVG(amount) DESC) AS position, AVG(amount)/AVG(AVG(amount)) OVER()*100 AS average_index FROM sales GROUP BY category, day HAVING SUM(amount)>0 QUALIFY position<=3 ORDER BY position`
	got, err := ApplySelectorProjection(source, []string{"category", "total", "position", "average_index"}, view)
	if err != nil {
		t.Fatal(err)
	}
	stmt, err := sqlparser.ParseQuery(got)
	if err != nil {
		t.Fatal(err)
	}
	if len(stmt.GroupBy) != 1 || sqlparser.Stringify(stmt.GroupBy[0].Expr) != "1" {
		t.Fatalf("expected only selected dimension in GROUP BY: %s", got)
	}
	for _, fragment := range []string{"PARTITION BY category", "ORDER BY AVG(amount) DESC", "HAVING SUM(amount) > 0", "QUALIFY position <= 3", "ORDER BY position"} {
		if !strings.Contains(got, fragment) {
			t.Fatalf("missing window dependency or clause %q: %s", fragment, got)
		}
	}
}

func TestWindowOnlyProjectionDoesNotIntroduceGrouping(t *testing.T) {
	on := true
	view := &data.View{Spec: spec.View{Groupable: &on}}
	for _, expression := range []string{"RANK() OVER(ORDER BY amount DESC)", "SUM(amount) OVER(PARTITION BY category)", "COALESCE(SUM(amount) OVER(), 0)"} {
		source := "SELECT category, day, " + expression + " AS metric FROM sales"
		got, err := ApplySelectorProjection(source, []string{"category", "metric"}, view)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(got, "GROUP BY") {
			t.Fatalf("window-only query acquired grouping: %s", got)
		}
	}
}

func TestWindowAggregateDependenciesIntroduceGrouping(t *testing.T) {
	on := true
	view := &data.View{Spec: spec.View{Groupable: &on}}
	for _, expression := range []string{
		"RANK() OVER(ORDER BY AVG(amount) DESC)",
		"RANK() OVER(PARTITION BY SUM(amount) ORDER BY category)",
		"AVG(AVG(amount)) OVER()",
		"FIRST_VALUE(AVG(amount) IGNORE NULLS) OVER(ORDER BY category)",
	} {
		source := "SELECT category, day, " + expression + " AS metric FROM sales"
		got, err := ApplySelectorProjection(source, []string{"category", "metric"}, view)
		if err != nil {
			t.Fatal(err)
		}
		stmt, err := sqlparser.ParseQuery(got)
		if err != nil {
			t.Fatal(err)
		}
		if len(stmt.GroupBy) != 1 || sqlparser.Stringify(stmt.GroupBy[0].Expr) != "1" {
			t.Fatalf("grouped window dependency did not induce the selected grain: %s", got)
		}
	}
}
