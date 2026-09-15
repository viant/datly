package sql

import (
	"fmt"
	"strings"

	"github.com/viant/datly/data"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/node"
	"github.com/viant/sqlparser/query"
	sqltext "github.com/viant/sqlparser/source"
)

type wildcardSource struct {
	alias  string
	node   node.Node
	joined bool
}

func (p SelectorProjection) wildcardColumns(stmt *query.Select, item *query.Item, depth int) ([]ProjectionColumn, error) {
	if depth > 32 {
		return nil, fmt.Errorf("wildcard source projection is unresolved: recursive source")
	}
	qualifier := ""
	if selector, ok := item.Expr.(*expr.Selector); ok {
		qualifier = selector.Name
	}
	if selector, ok := projectionStar(item).X.(*expr.Selector); ok {
		qualifier = selector.Name
	}
	sources := []wildcardSource{{stmt.From.Alias, stmt.From.X, len(stmt.Joins) > 0}}
	for _, join := range stmt.Joins {
		sources = append(sources, wildcardSource{join.Alias, join.With, true})
	}
	var result []ProjectionColumn
	for _, src := range sources {
		if src.alias == "" {
			switch src.node.(type) {
			case *expr.Ident, *expr.Selector:
				src.alias = sqlparser.NewColumn(query.NewItem(src.node)).Identity()
			}
		}
		if qualifier != "" {
			parts, err := sqlparser.TableIdentifierParts(qualifier)
			if err != nil || !src.matchesQualifier(parts) {
				continue
			}
		}
		columns, err := p.wildcardSourceColumns(stmt, src, depth)
		if err != nil {
			return nil, err
		}
		result = append(result, columns...)
	}
	for _, name := range projectionStar(item).Except {
		found := false
		for _, column := range result {
			if column.MatchesOutput(name) {
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("EXCEPT column %q is not a declared source output", name)
		}
	}
	kept := result[:0]
	for _, column := range result {
		if !column.excludedBy(projectionStar(item)) {
			kept = append(kept, column)
		}
	}
	if len(kept) == 0 {
		return nil, fmt.Errorf("wildcard source projection is unresolved or empty")
	}
	return kept, nil
}

func (s wildcardSource) matchesQualifier(parts []string) bool {
	own, err := sqlparser.TableIdentifierParts(s.alias)
	if err != nil || len(own) != len(parts) {
		return false
	}
	for i := range own {
		if !strings.EqualFold(own[i], parts[i]) {
			return false
		}
	}
	return true
}

func (s wildcardSource) query(ctes query.WithSelects) (*query.Select, string) {
	// Native table markers may carry a partially populated query in Raw.X.
	// Their source identity, not that partial query, owns schema discovery.
	switch s.node.(type) {
	case *expr.Raw, *expr.Parenthesis:
		if table, _, err := sqlparser.SourceTable(s.node); err == nil && table != "" {
			return nil, ""
		}
	}
	var nested *query.Select
	raw := ""
	switch value := s.node.(type) {
	case *expr.Raw:
		nested, _ = value.X.(*query.Select)
		raw = value.Raw
	case *expr.Parenthesis:
		nested, _ = value.X.(*query.Select)
		raw = value.Raw
	case *query.Select:
		nested = value
	case *expr.Ident:
		for _, cte := range ctes {
			if strings.EqualFold(sqltext.TrimQuote(cte.Alias), sqltext.TrimQuote(value.Name)) {
				nested = cte.X
				raw = cte.Raw
				break
			}
		}
	}
	raw = strings.TrimSuffix(strings.TrimPrefix(strings.TrimSpace(raw), "("), ")")
	if nested == nil && raw != "" {
		nested, _ = sqlparser.ParseQuery(raw)
	}
	return nested, raw
}

func (p SelectorProjection) wildcardSourceColumns(stmt *query.Select, src wildcardSource, depth int) ([]ProjectionColumn, error) {
	nested, raw := src.query(stmt.WithSelects)
	if nested == nil {
		if table, _, err := sqlparser.SourceTable(src.node); err != nil {
			return nil, err
		} else if table != "" {
			return p.wildcardMetadataColumns(stmt, src)
		}
		return nil, &UnresolvedProjectionError{}
	}
	// An incomplete dialect AST supplies no closed output contract.
	if len(nested.List) == 0 {
		return nil, &databaseProjectionSchema{}
	}
	// A sole wildcard over one derived view has exactly that view's prepared
	// output contract only when every inner projection is the same wildcard.
	// Explicit aliases/exclusions remain authoritative over prepared metadata.
	// Resolve at this boundary instead of treating output
	// metadata as the schema of an inner physical table. Joined or mixed outer
	// projections still need source-specific resolution below.
	prepared := !src.joined && len(stmt.List) == 1 && projectionStar(stmt.List[0]) != nil && p.View != nil && len(p.View.Columns) > 0
	if prepared && src.preservesPhysicalWildcard(stmt.WithSelects, 0, false) {
		return p.wildcardMetadataColumns(stmt, src)
	}
	// Literal pseudo fields add outputs without renaming physical columns.
	// Resolve their aliases from SQL, using the prepared scan columns only for
	// the unchanged physical wildcard. Arbitrary mixed projections cannot do so.
	prepared = prepared && src.preservesPhysicalWildcard(stmt.WithSelects, 0, true)
	parts, hasParts := newSelectProjectionSource(raw)
	var columns []ProjectionColumn
	for i, inner := range nested.List {
		if projectionStar(inner) != nil {
			// Only a proven unchanged physical wildcard may use prepared columns.
			child := SelectorProjection{}
			if prepared {
				child = SelectorProjection{SQL: raw, View: p.View}
			}
			copyStmt := *nested
			copyStmt.WithSelects = append(append(query.WithSelects(nil), nested.WithSelects...), stmt.WithSelects...)
			resolved, err := child.wildcardColumns(&copyStmt, inner, depth+1)
			if err != nil {
				return nil, err
			}
			columns = append(columns, resolved...)
			continue
		}
		output := ""
		if hasParts && i < len(parts.parts) {
			_, output = sqltext.SplitTopLevelAlias(parts.parts[i])
		}
		if output == "" {
			output = sqlparser.NewColumn(inner).Identity()
		}
		if output == "" {
			return nil, fmt.Errorf("wildcard source projection has unnamed expression")
		}
		columns = append(columns, ProjectionColumn{output: output})
	}
	for i := range columns {
		name := columns[i].output
		ref := name
		if src.alias != "" {
			ref = src.alias + "." + name
		}
		columns[i] = ProjectionColumn{names: []string{name}, order: ref, source: ref, output: name, wildcard: true}
	}
	return columns, nil
}

func (p SelectorProjection) wildcardMetadataColumns(stmt *query.Select, src wildcardSource) ([]ProjectionColumn, error) {
	if p.View == nil || len(p.View.Columns) == 0 {
		return nil, &databaseProjectionSchema{}
	}
	parts, hasParts := newSelectProjectionSource(p.SQL)
	var columns []ProjectionColumn
	for _, col := range p.View.Columns {
		if col == nil {
			continue
		}
		name := col.Column
		if name == "" {
			name = col.Name
		}
		// A mixed list's explicit output aliases have their own SQL authority.
		explicit := false
		for i, projected := range stmt.List {
			if projectionStar(projected) != nil {
				continue
			}
			alias := projected.Alias
			if hasParts && i < len(parts.parts) {
				_, alias = sqltext.SplitTopLevelAlias(parts.parts[i])
			}
			if alias != "" && canonicalProjectionName(alias) == canonicalProjectionName(name) {
				explicit = true
				break
			}
		}
		if explicit {
			continue
		}
		parts, err := sqlparser.TableIdentifierParts(name)
		if err != nil {
			return nil, fmt.Errorf("wildcard column %q is unresolved: %w", name, err)
		}
		if len(parts) > 1 && !src.matchesQualifier(parts[:len(parts)-1]) {
			continue
		}
		if src.joined && len(parts) == 1 {
			return nil, fmt.Errorf("joined wildcard projection requires qualified prepared columns")
		}

		output, err := starOutputColumn(col)
		if err != nil {
			return nil, err
		}
		ref := src.alias + "." + output
		names := projectionItemNames(nil, ref)

		columns = append(columns, ProjectionColumn{names: names, order: ref, source: ref, output: output, metadata: col, wildcard: true})
	}
	if len(columns) == 0 {
		return nil, fmt.Errorf("wildcard source projection is unresolved or empty")
	}
	return columns, nil
}

func (c ProjectionColumn) excludedBy(star *expr.Star) bool {
	if star != nil {
		for _, name := range star.Except {
			if c.Matches(name) {
				return true
			}
		}
	}
	return false
}

func projectionMetadataColumns(item *query.Item, view *data.View) ([]*data.Column, error) {
	if view == nil || len(view.Columns) == 0 {
		return nil, fmt.Errorf("wildcard projection requires prepared columns")
	}
	var result []*data.Column
	for _, column := range view.Columns {
		if column == nil {
			continue
		}
		output, err := starOutputColumn(column)
		if err != nil {
			return nil, err
		}
		candidate := ProjectionColumn{names: []string{output}}
		if !candidate.excludedBy(projectionStar(item)) {
			result = append(result, column)
		}
	}
	return result, nil
}

// preservesPhysicalWildcard recognizes an unchanged row contract through named
// sources, optionally with aliased literals added to it. It does not lend a row
// schema to joined, computed or renamed physical outputs.
func (s wildcardSource) preservesPhysicalWildcard(ctes query.WithSelects, depth int, allowLiterals bool) bool {
	if depth > 32 {
		return false
	}
	isCTE := false
	if value, ok := s.node.(*expr.Ident); ok {
		for _, cte := range ctes {
			if cte != nil && strings.EqualFold(sqltext.TrimQuote(cte.Alias), sqltext.TrimQuote(value.Name)) {
				isCTE = true
				break
			}
		}
	}
	if !isCTE {
		if table, _, err := sqlparser.SourceTable(s.node); err == nil && table != "" {
			return true
		}
	}
	nested, _ := s.query(ctes)
	if nested == nil || nested.Union != nil || len(nested.Joins) != 0 {
		return false
	}
	var star *expr.Star
	for _, item := range nested.List {
		if candidate := projectionStar(item); candidate != nil {
			if star != nil {
				return false
			}
			star = candidate
		} else {
			if item == nil || !allowLiterals || item.Alias == "" {
				return false
			}
			if _, literal := item.Expr.(*expr.Literal); !literal {
				return false
			}
		}
	}
	if star == nil || len(star.Except) > 0 {
		return false
	}
	child := wildcardSource{alias: nested.From.Alias, node: nested.From.X}
	qualifier := ""
	switch value := star.X.(type) {
	case *expr.Selector:
		qualifier = value.Name
	case *expr.Ident:
		qualifier = value.Name
	}
	if qualifier != "" && qualifier != "*" {
		if child.alias == "" {
			switch child.node.(type) {
			case *expr.Ident, *expr.Selector:
				child.alias = sqlparser.NewColumn(query.NewItem(child.node)).Identity()
			}
		}
		parts, err := sqlparser.TableIdentifierParts(qualifier)
		if err != nil || !child.matchesQualifier(parts) {
			return false
		}
	}
	scoped := append(append(query.WithSelects(nil), nested.WithSelects...), ctes...)
	return child.preservesPhysicalWildcard(scoped, depth+1, allowLiterals)
}
