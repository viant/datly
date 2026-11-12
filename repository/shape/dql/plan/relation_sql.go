package plan

import (
	"strings"

	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/node"
	"github.com/viant/sqlparser/query"
)

func analyzeSQL(source string) (map[string]bool, projectionMeta, bool) {
	aliases := map[string]bool{}
	proj := projectionMeta{Columns: map[string]bool{}}
	source = strings.TrimSpace(source)
	if source == "" {
		return aliases, proj, false
	}
	query, err := sqlparser.ParseQuery(source)
	if err != nil || query == nil {
		return aliases, proj, false
	}
	collectSQLAliases(query, aliases)
	collectSQLProjection(query, &proj)
	return aliases, proj, true
}

func collectSQLAliases(query *query.Select, aliases map[string]bool) {
	registerAlias(aliases, query.From.Alias)
	registerFromNodeAlias(aliases, query.From.X)
	for _, join := range query.Joins {
		if join == nil {
			continue
		}
		registerAlias(aliases, join.Alias)
		registerFromNodeAlias(aliases, join.With)
	}
}

func collectSQLProjection(query *query.Select, projection *projectionMeta) {
	columns := sqlparser.NewColumns(query.List)
	projection.HasStar = columns.IsStarExpr()
	for _, col := range columns {
		if col == nil {
			continue
		}
		registerProjection(projection.Columns, col.Name)
		registerProjection(projection.Columns, col.Alias)
		registerProjection(projection.Columns, col.Expression)
	}
}

func registerProjection(index map[string]bool, value string) {
	value = strings.TrimSpace(value)
	if value == "" || strings.Contains(value, "*") {
		return
	}
	index[normalizedProjectionKey(value)] = true
	if i := strings.LastIndex(value, "."); i != -1 && i+1 < len(value) {
		suffix := strings.TrimSpace(value[i+1:])
		if suffix != "" {
			index[normalizedProjectionKey(suffix)] = true
		}
	}
}

func registerAlias(index map[string]bool, alias string) {
	alias = strings.TrimSpace(alias)
	if alias == "" {
		return
	}
	index[strings.ToLower(alias)] = true
}

func registerFromNodeAlias(index map[string]bool, n node.Node) {
	switch actual := n.(type) {
	case *expr.Ident:
		registerAlias(index, actual.Name)
	case *expr.Selector:
		registerAlias(index, actual.Name)
	}
}
