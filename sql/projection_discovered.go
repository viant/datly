package sql

import (
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlx/metadata/info"
)

// ResolveDiscoveredColumns materializes an opaque named wildcard from actual
// database result labels. Only the outer list changes; the inner dialect SQL
// and all template/binding text remain intact. Prepared Go fields are not input.
func (p SelectorProjection) ResolveDiscoveredColumns(names []string, dialect *info.Dialect) (string, bool, error) {
	_, complete, probeErr := p.HasOutput("")
	if probeErr == nil && complete {
		return p.SQL, false, nil
	}
	parts, ok := newSelectProjectionSource(p.SQL)
	if !ok {
		return p.SQL, false, nil
	}
	parsed, err := sqlparser.ParseQuery(p.SQL)
	if err != nil || parsed == nil || len(parsed.List) != 1 || len(parsed.Joins) > 0 {
		return p.SQL, false, nil
	}
	star := projectionStar(parsed.List[0])
	if star == nil || len(star.Except) > 0 {
		return p.SQL, false, nil
	}
	if table, _, err := sqlparser.SourceTable(parsed.From.X); err != nil || table != "" {
		return p.SQL, false, nil
	}
	src := wildcardSource{alias: parsed.From.Alias, node: parsed.From.X}
	if probeErr == nil && src.preservesPhysicalWildcard(parsed.WithSelects, 0, true) {
		return p.SQL, false, nil
	}
	qualifier := ""
	switch x := star.X.(type) {
	case *expr.Selector:
		qualifier = x.Name
	case *expr.Ident:
		qualifier = x.Name
	}
	if qualifier != "" && qualifier != "*" {
		nameParts, e := sqlparser.TableIdentifierParts(qualifier)
		if e != nil || !src.matchesQualifier(nameParts) {
			return p.SQL, false, nil
		}
	}
	if len(names) == 0 {
		return p.SQL, false, nil
	}
	projected := make([]string, len(names))
	for i, name := range names {
		quoted, err := dialect.QuoteIdentifier(name)
		if err != nil {
			return "", false, err
		}
		if src.alias != "" {
			quoted = src.alias + "." + quoted
		}
		projected[i] = quoted
	}
	result := parts.render(p.SQL, projected)
	return result, true, nil
}
