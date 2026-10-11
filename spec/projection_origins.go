package spec

import (
	"github.com/viant/sqlparser"
	sqlxio "github.com/viant/sqlx/io"
	"reflect"
	"strings"
)

// projectionOrigins is derived per occurrence and never serialized or authored.
// Its map is published once and only queried through value-returning methods.
type projectionOrigins struct {
	table   string
	columns map[string]projectionOrigin
}
type projectionOrigin struct{ table, column string }

// CompileProjectionOrigins derives attribution from already resolved source SQL.
// Rebuilding replaces all previous attribution; opaque SQL keeps canonical identity.
func (v *View) CompileProjectionOrigins(columns ...*Column) {
	v.projectionOrigins = nil
	if v.Source == nil || strings.TrimSpace(v.Source.SQL) == "" {
		return
	}
	query, err := sqlparser.ParseQuery(v.Source.SQL, sqlparser.WithStructuralValidation())
	if err != nil || query == nil || len(query.List) == 0 {
		return
	}
	lineage := (sqlparser.Lineage{Query: query}).Compile()
	result := &projectionOrigins{table: lineage.RootTable(), columns: map[string]projectionOrigin{}}
	if columns == nil {
		columns = v.Columns
	}
	for _, c := range columns {
		if c == nil {
			continue
		}
		name := c.Output
		if name == "" {
			mapping := sqlxio.ParseTag(reflect.StructTag(c.Tag))
			if _, projected, ok := strings.Cut(mapping.Column, "|"); ok {
				projected, _, _ = strings.Cut(projected, "|")
				name = strings.TrimSpace(projected)
			}
		}
		if name == "" {
			name = c.Source
		}
		if name == "" {
			name = c.Name
		}
		origin := projectionOrigin{}
		if value, ok := lineage.Lookup(name); ok {
			origin.table, origin.column = value.Table, value.Column
		}
		result.columns[strings.ToLower(c.Name)] = origin
	}
	v.projectionOrigins = result
}

// ProjectionOrigin returns compiled attribution by value. A known empty result
// suppresses misleading dictionary attribution for computed or ambiguous outputs.
func (v *View) ProjectionOrigin(name string) (table, column string, known bool) {
	if v == nil || v.projectionOrigins == nil {
		return "", "", false
	}
	result, ok := v.projectionOrigins.columns[strings.ToLower(name)]
	return result.table, result.column, ok
}
func (v *View) ProjectionTable() string {
	if v == nil {
		return ""
	}
	if v.Source != nil && v.Source.Table != "" {
		return v.Source.Table
	}
	if v.projectionOrigins != nil {
		return v.projectionOrigins.table
	}
	return ""
}
