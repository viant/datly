package options

import (
	"github.com/viant/datly/utils/formatter"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/node"
	"github.com/viant/sqlparser/query"
	"github.com/viant/toolbox/format"
)

type mapper map[string]string

func (m mapper) Map(name string) string {
	ret, ok := m[name]
	if ok {
		return ret
	}
	return name
}

func (r *Rule) NormalizeSQL(SQL string) string {
	if !r.Generated {
		return SQL
	}
	sqlQuery, err := sqlparser.ParseQuery(SQL)
	if err != nil {
		return SQL
	}
	ns := mapper{}
	if sqlQuery.From.Alias != "" {
		ns[sqlQuery.From.Alias] = normalizeName(sqlQuery.From.Alias)
	}
	for _, join := range sqlQuery.Joins {
		ns[join.Alias] = normalizeName(join.Alias)
	}

	sqlparser.Traverse(sqlQuery, func(n node.Node) bool {
		switch actual := n.(type) {
		case *expr.Selector:
			actual.Name = ns.Map(actual.Name)
		case *query.Join:
			actual.Alias = ns.Map(actual.Alias)
		case *query.Item:
			actual.Alias = ns.Map(actual.Alias)
		case *query.From:
			actual.Alias = ns.Map(actual.Alias)
		}
		return true
	})
	result := sqlparser.Stringify(sqlQuery)
	return result
}

func normalizeName(k string) string {
	detectedCase := formatter.DetectCase(k)
	caser, _ := format.NewCase(detectedCase)
	name := caser.Format(k, format.CaseUpperCamel)
	return name
}