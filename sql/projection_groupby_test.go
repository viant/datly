package sql

import (
	"strings"
	"testing"

	"github.com/viant/datly/data"
	"github.com/viant/datly/spec"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/node"
	"github.com/viant/sqlparser/query"
)

func TestGroupedProjectionHelpers(t *testing.T) {
	t.Run("needs rewrite empty returns false", func(t *testing.T) {
		if groupedProjectionNeedsRewrite(nil) {
			t.Fatalf("did not expect empty grouped rewrite need")
		}
	})

	t.Run("rewrite grouped projection and group by helpers", func(t *testing.T) {
		stmt, err := sqlparser.ParseQuery("SELECT tenant_id, region, SUM(amount) AS total FROM sales GROUP BY tenant_id, region ORDER BY region, total")
		if err != nil {
			t.Fatalf("parse query: %v", err)
		}
		selectedItems := query.List{stmt.List[0], stmt.List[2]}
		groupBy := groupedProjectionGroupBy(selectedItems)
		if len(groupBy) != 1 || sqlparser.Stringify(groupBy[0].Expr) != "1" {
			t.Fatalf("unexpected grouped projection group by: %+v", groupBy)
		}
		actual := rewriteGroupedProjectionSQL(stmt, selectedItems)
		expect := "SELECT tenant_id, SUM(amount) AS total FROM sales GROUP BY 1 ORDER BY total"
		if actual != expect {
			t.Fatalf("unexpected grouped projection rewrite:\nactual: %q\nexpect: %q", actual, expect)
		}
	})

	t.Run("needs rewrite for mixed aggregate and dimension", func(t *testing.T) {
		stmt, err := sqlparser.ParseQuery("SELECT tenant_id, SUM(amount) AS total FROM users")
		if err != nil {
			t.Fatalf("parse query: %v", err)
		}
		if !groupedProjectionNeedsRewrite(stmt.List) {
			t.Fatalf("expected grouped rewrite need")
		}
	})

	t.Run("does not need rewrite for aggregate only", func(t *testing.T) {
		stmt, err := sqlparser.ParseQuery("SELECT SUM(amount) AS total FROM users")
		if err != nil {
			t.Fatalf("parse query: %v", err)
		}
		if groupedProjectionNeedsRewrite(stmt.List) {
			t.Fatalf("did not expect grouped rewrite need")
		}
	})

	t.Run("does not need rewrite for plain dimensions", func(t *testing.T) {
		stmt, err := sqlparser.ParseQuery("SELECT tenant_id, user_id FROM users")
		if err != nil {
			t.Fatalf("parse query: %v", err)
		}
		if groupedProjectionNeedsRewrite(stmt.List) {
			t.Fatalf("did not expect grouped rewrite need")
		}
	})

	t.Run("does not guess aggregate from raw expression text", func(t *testing.T) {
		item := &query.Item{Expr: &expr.Raw{Raw: "ROUND(SUM(amount), 2)"}}
		if isAggregateSelectItem(item) {
			t.Fatalf("raw text must not bypass parser-owned aggregate detection")
		}
	})

	t.Run("filter grouped order by keeps alias", func(t *testing.T) {
		stmt, err := sqlparser.ParseQuery("SELECT tenant_id, SUM(amount) AS total FROM users GROUP BY tenant_id ORDER BY total DESC, tenant_id")
		if err != nil {
			t.Fatalf("parse query: %v", err)
		}
		filtered := filterGroupedOrderBy(stmt.OrderBy, query.List{stmt.List[1]})
		if len(filtered) != 1 || sqlparser.Stringify(filtered[0].Expr) != "total" {
			t.Fatalf("unexpected filtered order by: %v", len(filtered))
		}
	})

	t.Run("filter grouped order by handles empty inputs", func(t *testing.T) {
		if actual := filterGroupedOrderBy(nil, query.List{{Expr: &expr.Ident{Name: "id"}}}); actual != nil {
			t.Fatalf("expected nil order by passthrough")
		}
		stmt, err := sqlparser.ParseQuery("SELECT id FROM users ORDER BY id")
		if err != nil {
			t.Fatalf("parse query: %v", err)
		}
		if actual := filterGroupedOrderBy(stmt.OrderBy, nil); len(actual) != len(stmt.OrderBy) {
			t.Fatalf("expected unchanged order by when no items")
		}
	})

	t.Run("filter grouped order by skips nil and nil expr items", func(t *testing.T) {
		orderBy := query.List{
			nil,
			{},
			{Expr: &expr.Ident{Name: "total"}},
		}
		items := query.List{{Expr: &expr.Ident{Name: "total"}}}
		filtered := filterGroupedOrderBy(orderBy, items)
		if len(filtered) != 1 || sqlparser.Stringify(filtered[0].Expr) != "total" {
			t.Fatalf("unexpected filtered order by with nil items: %+v", filtered)
		}
	})

	t.Run("filter grouped order by skips nil selected items", func(t *testing.T) {
		stmt, err := sqlparser.ParseQuery("SELECT tenant_id, SUM(amount) AS total FROM users ORDER BY total")
		if err != nil {
			t.Fatalf("parse query: %v", err)
		}
		filtered := filterGroupedOrderBy(stmt.OrderBy, query.List{nil, {Alias: "total"}})
		if len(filtered) != 1 || sqlparser.Stringify(filtered[0].Expr) != "total" {
			t.Fatalf("unexpected filtered order by with nil selected items: %+v", filtered)
		}
	})

	t.Run("rewrite grouped projection empty", func(t *testing.T) {
		if actual := rewriteGroupedProjectionSQL(nil, nil); actual != "" {
			t.Fatalf("expected empty rewrite, got %q", actual)
		}
	})

	t.Run("contains aggregate via parenthesis unary and binary", func(t *testing.T) {
		node := &expr.Binary{
			X: &expr.Parenthesis{
				X: &expr.Unary{
					Op: "-",
					X:  &expr.Call{X: &expr.Ident{Name: "SUM"}},
				},
			},
			Y:  &expr.Ident{Name: "tenant_id"},
			Op: "+",
		}
		if !containsAggregateNode(node) {
			t.Fatalf("expected aggregate detection through nested nodes")
		}
	})

	t.Run("contains aggregate via qualify", func(t *testing.T) {
		node := &expr.Qualify{
			X: &expr.Call{X: &expr.Ident{Name: "COUNT"}},
		}
		if !containsAggregateNode(node) {
			t.Fatalf("expected aggregate detection through qualify")
		}
	})

	t.Run("direct aggregate ident does not match without call context", func(t *testing.T) {
		if containsAggregateNode(&expr.Ident{Name: "SUM"}) {
			t.Fatalf("did not expect bare aggregate ident to match")
		}
	})

	t.Run("direct aggregate selector does not match without call context", func(t *testing.T) {
		if containsAggregateNode(&expr.Selector{Name: "MAX"}) {
			t.Fatalf("did not expect bare aggregate selector to match")
		}
	})

	t.Run("qualify without aggregate returns false", func(t *testing.T) {
		node := &expr.Qualify{
			X: &expr.Ident{Name: "tenant_id"},
		}
		if containsAggregateNode(node) {
			t.Fatalf("did not expect non-aggregate qualify to match")
		}
	})

	t.Run("contains aggregate via call inside switch case", func(t *testing.T) {
		switchNode := &expr.Switch{
			Ident: expr.Ident{Name: "tenant_id"},
			Cases: []*expr.Case{{
				Y: &expr.Call{X: &expr.Ident{Name: "MAX"}},
			}},
		}
		if !containsAggregateNode(switchNode) {
			t.Fatalf("expected aggregate detection through switch case")
		}
	})

	t.Run("contains aggregate via nil call target but aggregate arg", func(t *testing.T) {
		node := &expr.Call{
			Args: []node.Node{
				&expr.Call{X: &expr.Ident{Name: "SUM"}},
			},
		}
		if !containsAggregateNode(node) {
			t.Fatalf("expected aggregate detection through nil-X call args")
		}
	})

	t.Run("call without aggregate returns false", func(t *testing.T) {
		node := &expr.Call{X: &expr.Ident{Name: "COALESCE"}}
		if containsAggregateNode(node) {
			t.Fatalf("did not expect plain call to match")
		}
	})

	t.Run("call with nil target and no args returns false", func(t *testing.T) {
		node := &expr.Call{}
		if containsAggregateNode(node) {
			t.Fatalf("did not expect empty call to match")
		}
	})

	t.Run("call with selector target aggregate", func(t *testing.T) {
		node := &expr.Call{X: &expr.Selector{Name: "COUNT"}}
		if !containsAggregateNode(node) {
			t.Fatalf("expected aggregate selector call to match")
		}
	})

	t.Run("known aggregate functions", func(t *testing.T) {
		for _, name := range []string{"BIT_AND", "BIT_OR", "BIT_XOR", "GROUP_CONCAT"} {
			if !containsAggregateNode(&expr.Call{X: &expr.Ident{Name: name}}) {
				t.Fatalf("expected %s to be classified as aggregate", name)
			}
		}
	})

	t.Run("grouped projection does not group by bit or group concat aggregates", func(t *testing.T) {
		stmt, err := sqlparser.ParseQuery("SELECT tenant_id, BIT_OR(flags) AS flags, GROUP_CONCAT(name) AS names FROM users GROUP BY tenant_id")
		if err != nil {
			t.Fatalf("parse query: %v", err)
		}
		groupBy := groupedProjectionGroupBy(query.List{stmt.List[0], stmt.List[1], stmt.List[2]})
		if len(groupBy) != 1 || sqlparser.Stringify(groupBy[0].Expr) != "1" {
			t.Fatalf("unexpected grouped projection group by: %+v", groupBy)
		}
	})

	t.Run("call with qualify target aggregate child", func(t *testing.T) {
		node := &expr.Call{
			X: &expr.Qualify{
				X: &expr.Call{X: &expr.Ident{Name: "SUM"}},
			},
		}
		if !containsAggregateNode(node) {
			t.Fatalf("expected aggregate detection through qualify call target")
		}
	})

	t.Run("raw node without aggregate returns false", func(t *testing.T) {
		node := &expr.Raw{Raw: "tenant_id"}
		if containsAggregateNode(node) {
			t.Fatalf("did not expect raw node without aggregate to match")
		}
	})

	t.Run("raw node without child is not guessed", func(t *testing.T) {
		node := &expr.Raw{Unparsed: "SUM(amount)"}
		if containsAggregateNode(node) {
			t.Fatalf("raw unparsed text must not bypass parser-owned aggregate detection")
		}
	})

	t.Run("switch with nil cases returns false", func(t *testing.T) {
		node := &expr.Switch{Ident: expr.Ident{Name: "tenant_id"}, Cases: []*expr.Case{nil}}
		if containsAggregateNode(node) {
			t.Fatalf("did not expect nil switch cases to match")
		}
	})

	t.Run("switch case qualify value does not match pointer-only branch", func(t *testing.T) {
		node := &expr.Switch{
			Ident: expr.Ident{Name: "tenant_id"},
			Cases: []*expr.Case{{
				X: expr.Qualify{X: &expr.Call{X: &expr.Ident{Name: "COUNT"}}},
			}},
		}
		if containsAggregateNode(node) {
			t.Fatalf("did not expect qualify value to match pointer-only branch")
		}
	})

	t.Run("non aggregate item returns false", func(t *testing.T) {
		item := &query.Item{Expr: &expr.Ident{Name: "tenant_id"}}
		if isAggregateSelectItem(item) {
			t.Fatalf("did not expect aggregate select item")
		}
	})

	t.Run("raw custom aggregate text is not guessed", func(t *testing.T) {
		node := &expr.Raw{Unparsed: "APPROX_COUNT_DISTINCT(uid)"}
		if containsAggregateNode(node) {
			t.Fatalf("raw custom aggregate text must not bypass parser-owned aggregate detection")
		}
	})

	t.Run("nil aggregate node returns false", func(t *testing.T) {
		if containsAggregateNode(nil) {
			t.Fatalf("did not expect nil aggregate node to match")
		}
	})

	t.Run("nil select item returns false", func(t *testing.T) {
		if isAggregateSelectItem(nil) {
			t.Fatalf("did not expect nil select item to match")
		}
	})

	t.Run("select item with nil expr returns false", func(t *testing.T) {
		if isAggregateSelectItem(&query.Item{}) {
			t.Fatalf("did not expect nil expr select item to match")
		}
	})

	t.Run("call with aggregate in args", func(t *testing.T) {
		node := &expr.Call{
			X: &expr.Ident{Name: "COALESCE"},
			Args: []node.Node{
				&expr.Call{X: &expr.Ident{Name: "SUM"}},
			},
		}
		if !containsAggregateNode(node) {
			t.Fatalf("expected aggregate detection via call args")
		}
	})

	t.Run("raw node with nested aggregate child", func(t *testing.T) {
		node := &expr.Raw{
			X: &expr.Call{X: &expr.Ident{Name: "AVG"}},
		}
		if !containsAggregateNode(node) {
			t.Fatalf("expected aggregate detection via raw child")
		}
	})

	t.Run("switch ident alone is not aggregate", func(t *testing.T) {
		node := &expr.Switch{
			Ident: expr.Ident{Name: "MIN"},
		}
		if containsAggregateNode(node) {
			t.Fatalf("did not expect bare switch ident to count as aggregate")
		}
	})

	t.Run("parenthesis without aggregate returns false", func(t *testing.T) {
		node := &expr.Parenthesis{X: &expr.Ident{Name: "tenant_id"}}
		if containsAggregateNode(node) {
			t.Fatalf("did not expect plain parenthesis node to match")
		}
	})

	t.Run("unary without aggregate returns false", func(t *testing.T) {
		node := &expr.Unary{Op: "-", X: &expr.Ident{Name: "tenant_id"}}
		if containsAggregateNode(node) {
			t.Fatalf("did not expect plain unary node to match")
		}
	})

	t.Run("binary without aggregate returns false", func(t *testing.T) {
		node := &expr.Binary{X: &expr.Ident{Name: "tenant_id"}, Y: &expr.Ident{Name: "other"}, Op: "+"}
		if containsAggregateNode(node) {
			t.Fatalf("did not expect plain binary node to match")
		}
	})

	t.Run("unsupported node returns false", func(t *testing.T) {
		if containsAggregateNode(query.NewItem(&expr.Ident{Name: "tenant_id"})) {
			t.Fatalf("did not expect unsupported query item node to match")
		}
	})
}

func TestGroupedProjectionRewritesTransparentWrapper(t *testing.T) {
	on := true
	view := &data.View{Spec: spec.View{Groupable: &on}}
	source := `SELECT selector_features.category, selector_features.feature_count
	FROM (
	  SELECT id, name, category, COUNT(*) AS feature_count
	  FROM selector_features
	  GROUP BY id, name, category
	  HAVING COUNT(*) > 0
	) selector_features`
	result, err := ApplySelectorProjection(source, []string{"selector_features.category", "selector_features.feature_count"}, view)
	if err != nil {
		t.Fatalf("ApplySelectorProjection() error = %v", err)
	}
	for _, fragment := range []string{"COUNT(*) AS feature_count", "GROUP BY 1", "HAVING COUNT(*) > 0"} {
		if !strings.Contains(result, fragment) {
			t.Fatalf("missing %q in %s", fragment, result)
		}
	}
	for _, fragment := range []string{"FROM (\n  SELECT", "GROUP BY id, name, category"} {
		if strings.Contains(result, fragment) {
			t.Fatalf("unexpected %q in %s", fragment, result)
		}
	}
	measureOnly, err := ApplySelectorProjection(source, []string{"selector_features.feature_count"}, view)
	if err != nil {
		t.Fatalf("ApplySelectorProjection() measure-only error = %v", err)
	}
	if strings.Contains(measureOnly, "GROUP BY") {
		t.Fatalf("measure-only projection retained grouping: %s", measureOnly)
	}
	if !strings.Contains(measureOnly, "COUNT(*) AS feature_count") {
		t.Fatalf("measure-only projection lost aggregate: %s", measureOnly)
	}
}
