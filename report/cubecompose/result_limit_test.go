package cubecompose

import (
	"fmt"
	"strings"
	"testing"
)

func TestSemanticPrefixPagingKeepsSourceOrderAndBound(t *testing.T) {
	end := 5
	for _, tc := range []struct{ limit, offset, read int }{{10, 0, 5}, {2, 0, 3}, {2, 2, 3}, {2, 4, 1}, {2, 5, 0}} {
		sql := fmt.Sprintf("SELECT t1.category AS category,t1.total_spend AS amount FROM $CubeSQL1 AS t1 ORDER BY amount DESC LIMIT %d OFFSET %d", tc.limit, tc.offset)
		p, err := CompileWithResultLimit(sql, unionCatalog(t), 1, 10, &end)
		if err != nil {
			t.Fatal(err)
		}
		actual, args, err := p.RenderPage([]Frame{{SQL: "SELECT category,SUM(amount) AS total_spend FROM facts WHERE tenant=? GROUP BY category HAVING SUM(amount)>=?", Args: []any{7, 0}}})
		if err != nil {
			t.Fatal(err)
		}
		tail := fmt.Sprintf("ORDER BY amount DESC\nLIMIT %d", tc.read)
		if tc.offset > 0 {
			tail += fmt.Sprintf(" OFFSET %d", tc.offset)
		}
		if !strings.HasSuffix(actual, tail) || len(args) != 2 || args[0] != 7 || args[1] != 0 {
			t.Fatalf("prefix/page changed native scope or order: %s %+v", actual, args)
		}
		if p.Limit != tc.limit || p.Offset() != tc.offset {
			t.Fatalf("mutated public page contract: %+v", p)
		}
		exact, _, err := p.Render([]Frame{{SQL: "SELECT category,SUM(amount) AS total_spend FROM facts GROUP BY category"}})
		if err != nil {
			t.Fatal(err)
		}
		exactCount := tc.read
		if exactCount > tc.limit {
			exactCount = tc.limit
		}
		exactTail := fmt.Sprintf("LIMIT %d", exactCount)
		if tc.offset > 0 {
			exactTail += fmt.Sprintf(" OFFSET %d", tc.offset)
		}
		if !strings.HasSuffix(exact, exactTail) {
			t.Fatalf("exact rendering ignored semantic prefix: %s", exact)
		}
	}
}

func TestSemanticPrefixRejectsUnprovedShapeAndPreservesLegacy(t *testing.T) {
	end := 5
	base := "SELECT t1.category AS category,t1.total_spend AS amount FROM $CubeSQL1 AS t1 ORDER BY amount DESC LIMIT 2"
	for _, sql := range []string{strings.Replace(base, " ORDER BY amount DESC", "", 1), strings.Replace(base, " ORDER BY", " GROUP BY t1.category ORDER BY", 1), base + " OFFSET -1", base + " OFFSET 11"} {
		if _, err := CompileWithResultLimit(sql, unionCatalog(t), 1, 10, &end); err == nil {
			t.Fatalf("unsupported prefix admitted: %s", sql)
		}
	}
	for _, value := range []int{-1, 0, 11} {
		if _, err := CompileWithResultLimit(base, unionCatalog(t), 1, 10, &value); err == nil {
			t.Fatalf("unbounded prefix %d admitted", value)
		}
	}
	if _, err := CompileWithResultLimit(hierarchySQL, unionCatalog(t), 2, 10, &end); err == nil {
		t.Fatal("union prefix admitted")
	}
	if _, err := CompileWithResultLimit(base+" OFFSET 2", unionCatalog(t), 1, 10, nil); err == nil {
		t.Fatal("legacy ordinary OFFSET contract changed")
	}
	p, err := CompileWithResultLimit(base, unionCatalog(t), 1, 10, nil)
	if err != nil {
		t.Fatal(err)
	}
	sql, _, err := p.RenderPage([]Frame{{SQL: "SELECT category,SUM(amount) AS total_spend FROM facts GROUP BY category"}})
	if err != nil || !strings.HasSuffix(sql, "LIMIT 3") {
		t.Fatalf("legacy lookahead changed: %s %v", sql, err)
	}
}
