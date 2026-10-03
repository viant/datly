package sql

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/data"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/node"
	"github.com/viant/sqlparser/query"
)

func TestWildcardRedundantSubqueryParentheses(t *testing.T) {
	for _, inner := range []string{
		"SELECT a.id AS id, a.name AS `display_name` FROM items a",
		"SELECT a.id AS id, a.name AS `display_name` FROM items a UNION ALL SELECT b.key AS other_id, b.label AS other_name FROM archived b",
	} {
		for depth := 1; depth <= 3; depth++ {
			wrapped := strings.Repeat("(", depth) + inner + strings.Repeat(")", depth)
			for _, star := range []string{"*", "p.*"} {
				for _, prepared := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/%d/%s/prepared=%v", inner, depth, star, prepared), func(t *testing.T) {
						view := &data.View{}
						if prepared {
							// Neither stale metadata nor its order can define an explicit inner SELECT.
							view.Columns = []*data.Column{{Name: "Stale", Column: "stale"}, {Name: "DisplayName", Column: "display_name"}, {Name: "Id", Column: "id"}}
						}
						source := "SELECT " + star + " FROM " + wrapped + " p"
						projection := SelectorProjection{SQL: source, View: view}
						all, err := projection.Columns(nil)
						require.NoError(t, err)
						require.Len(t, all, 2)
						require.Equal(t, "id", all[0].OutputName())
						require.Equal(t, "`display_name`", all[1].OutputName())
						selected, err := projection.Columns([]string{"display_name"})
						require.NoError(t, err)
						require.Len(t, selected, 1)
						require.Equal(t, "`display_name`", selected[0].OutputName())
						result, err := ApplySelectorProjection(source, []string{"display_name"}, view)
						require.NoError(t, err)
						require.Contains(t, result, wrapped, "projection must not flatten or rewrite the inner source")
						for _, invalid := range []string{"stale", "other_id", "other_name", "unknown"} {
							_, err = projection.Columns([]string{invalid})
							require.ErrorContains(t, err, "not found column", invalid)
						}
					})
				}
			}
		}
	}
}

func TestWildcardSourceWrapperNodes(t *testing.T) {
	sqlText := "SELECT id AS id FROM items"
	stmt, err := sqlparser.ParseQuery(sqlText)
	require.NoError(t, err)
	for _, src := range []node.Node{
		&expr.Raw{Raw: "((" + sqlText + "))", X: &query.Select{}},
		&expr.Parenthesis{Raw: "(((" + sqlText + ")))", X: &query.Select{}},
		&expr.Raw{X: &expr.Parenthesis{X: &expr.Raw{Raw: "(" + sqlText + ")", X: stmt}}},
	} {
		nested, raw := (wildcardSource{node: src}).query(nil)
		require.NotNil(t, nested)
		require.Len(t, nested.List, 1)
		require.Equal(t, sqlText, raw)
	}
	// Native table markers may have a misleading parsed child; raw table identity wins.
	for _, src := range []node.Node{
		&expr.Raw{Raw: "((items))", X: stmt},
		&expr.Parenthesis{Raw: "((items))", X: stmt},
	} {
		nested, _ := (wildcardSource{node: src}).query(nil)
		require.Nil(t, nested)
	}
}

func TestWildcardWrappedSourceBoundaries(t *testing.T) {
	view := &data.View{Columns: []*data.Column{{Name: "Id", Column: "id"}, {Name: "Secret", Column: "secret"}}}
	for _, source := range []string{
		"SELECT p.* FROM ((SELECT wrong.* FROM items a)) p",
		"SELECT p.* FROM ((SELECT a.* FROM items a JOIN other b ON a.id=b.id)) p",
		"SELECT p.* FROM ((SELECT a.* FROM items a UNION ALL SELECT b.* FROM other b)) p",
		"SELECT p.* FROM ((SELECT id FROM items)) p JOIN ((SELECT id FROM other)) q ON p.id=q.id WHERE q.secret=1", // qualified output remains closed
	} {
		_, err := (SelectorProjection{SQL: source, View: view}).Columns([]string{"secret"})
		require.Error(t, err, source)
	}
	for _, source := range []string{
		"SELECT * FROM ((SELECT id FROM items)) p JOIN ((SELECT id FROM other)) q ON p.id=q.id",
		"SELECT p.* FROM ((SELECT id, id FROM items)) p",
	} {
		_, err := (SelectorProjection{SQL: source, View: view}).Columns([]string{"id"})
		require.ErrorContains(t, err, "duplicate output column")
	}
}

func TestWildcardSourceWrapperLimitsAndEnclosures(t *testing.T) {
	for _, raw := range []string{
		"(SELECT id FROM items) UNION ALL (SELECT id FROM archived)",
		"(SELECT id FROM items) trailing",
		"(SELECT id FROM items",
	} {
		_, recovered := (wildcardSource{node: &expr.Raw{Raw: raw}}).query(nil)
		require.Equal(t, raw, recovered, "only complete enclosing pairs may be removed")
	}
	for _, raw := range []string{
		"((SELECT ')' AS label, id FROM items))",
		"((SELECT COALESCE(name, '(none)') AS label, id FROM items))",
	} {
		nested, recovered := (wildcardSource{node: &expr.Raw{Raw: raw}}).query(nil)
		require.NotNil(t, nested)
		require.Len(t, nested.List, 2)
		require.Equal(t, raw[2:len(raw)-2], recovered)
	}
	stmt, err := sqlparser.ParseQuery("SELECT id FROM items")
	require.NoError(t, err)
	var deep node.Node = stmt
	for i := 0; i < 34; i++ {
		deep = &expr.Parenthesis{X: deep}
	}
	cyclic := &expr.Raw{}
	cyclic.X = cyclic
	for _, src := range []node.Node{
		deep, cyclic,
		&expr.Raw{Raw: strings.Repeat("(", 33) + "SELECT id FROM items" + strings.Repeat(")", 33)},
	} {
		nested, _ := (wildcardSource{node: src}).query(nil)
		require.Nil(t, nested)
	}
}
