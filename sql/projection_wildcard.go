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

func (p SelectorProjection) wildcardColumns(stmt *query.Select, item *query.Item, depth int, preparedSource ...bool) ([]ProjectionColumn, error) {
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
		columns, err := p.wildcardSourceColumns(stmt, src, depth, preparedSource...)
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
	return s.queryAtDepth(ctes, 0)
}

func (s wildcardSource) queryAtDepth(ctes query.WithSelects, depth int) (*query.Select, string) {
	if depth > 32 {
		return nil, ""
	}
	// Native table markers may carry a partially populated query in Raw.X.
	// Their source identity, not that partial query, owns schema discovery.
	switch s.node.(type) {
	case *expr.Raw, *expr.Parenthesis:
		if table, _, err := sqlparser.SourceTable(s.node); err == nil && table != "" {
			return nil, ""
		}
	}
	var nested *query.Select
	var child node.Node
	raw := ""
	switch value := s.node.(type) {
	case *expr.Raw:
		child = value.X
		raw = value.Raw
	case *expr.Parenthesis:
		child = value.X
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
	if child != nil {
		var childSQL string
		nested, childSQL = (wildcardSource{node: child}).queryAtDepth(ctes, depth+1)
		if raw == "" {
			raw = childSQL
		}
	}
	// Peel only complete enclosures. UNION operands, expressions, and trailing
	// clauses must not be mistaken for redundant query parentheses.
	for wrappers := 0; ; wrappers++ {
		raw = strings.TrimSpace(raw)
		group, end, ok := sqltext.ReadGroupString(raw, 0, '(', ')')
		if !ok || end != len(raw) {
			break
		}
		if wrappers >= 32 {
			return nil, ""
		}
		raw = group[1 : len(group)-1]
	}
	// The parser can retain an empty Select for a parenthesized SELECT. That
	// partial AST is not authoritative when the raw source has a closed list.
	if (nested == nil || len(nested.List) == 0) && raw != "" {
		nested, _ = sqlparser.ParseQuery(raw)
	}
	return nested, raw
}

func (p SelectorProjection) wildcardSourceColumns(stmt *query.Select, src wildcardSource, depth int, preparedSource ...bool) ([]ProjectionColumn, error) {
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
	trustedSubset := len(preparedSource) > 0 && preparedSource[0]
	prepared := !src.joined && (trustedSubset || len(stmt.List) == 1 && projectionStar(stmt.List[0]) != nil) && p.View != nil && len(p.View.Columns) > 0
	if prepared && src.preservesPhysicalWildcard(stmt.WithSelects, 0, false) {
		return p.wildcardMetadataColumns(stmt, src)
	}
	// Added expressions retain their SQL aliases. Nonliteral outputs must be
	// present in the prepared scan contract; an unprepared expression cannot
	// lend the final row schema to an inner physical table.
	prepared = prepared && src.preservesPreparedWildcard(stmt.WithSelects, 0, true, p.preparedWildcardOutputs(), p.preparedWildcardTable())
	parts, hasParts := newSelectProjectionSource(raw)
	var columns []ProjectionColumn
	for i, inner := range nested.List {
		if projectionStar(inner) != nil {
			// Only a proven unchanged physical wildcard may use prepared columns.
			child := SelectorProjection{}
			copyStmt := *nested
			copyStmt.WithSelects = append(append(query.WithSelects(nil), nested.WithSelects...), stmt.WithSelects...)
			if prepared {
				// The proof below permits joined sources only for a qualified
				// wildcard of the primary FROM source. Other outputs are explicit
				// prepared aliases, so joined tables contribute no wildcard columns.
				// This copy is used only for output membership; SQL stays intact.
				copyStmt.Joins = nil
				// Explicit joined/computed outputs belong to this SELECT, not
				// to the deeper physical wildcard. Pass only its proven subset.
				scoped := SelectorProjection{SQL: raw, View: p.View, Dialect: p.Dialect}
				primary := wildcardSource{alias: nested.From.Alias, node: nested.From.X}
				metadata, err := scoped.wildcardMetadataColumns(&copyStmt, primary)
				if err != nil {
					return nil, err
				}
				view := *p.View
				view.Columns = nil
				for _, column := range metadata {
					view.Columns = append(view.Columns, column.metadata)
				}
				child = SelectorProjection{SQL: raw, View: &view, Dialect: p.Dialect}
			}
			resolved, err := child.wildcardColumns(&copyStmt, inner, depth+1, prepared)
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
		rendered := ref
		var err error
		if p.Dialect != nil {
			rendered, err = p.Dialect.ColumnIdentifier(ref)
			if err != nil {
				return nil, err
			}
		}
		columns[i] = ProjectionColumn{names: []string{name}, order: ref, source: rendered, output: name, wildcard: true}
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
			if alias == "" {
				if column := sqlparser.NewColumn(projected); column.Namespace != "" && !strings.EqualFold(column.Namespace, src.alias) {
					alias = column.Identity()
				}
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

		rendered := ref
		if p.Dialect != nil {
			rendered, err = p.Dialect.ColumnIdentifier(ref)
			if err != nil {
				return nil, err
			}
		}
		columns = append(columns, ProjectionColumn{names: names, order: ref, source: rendered, output: output, metadata: col, wildcard: true})
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
// sources, optionally with aliased outputs added to it. Computed additions need
// prepared scan-column authority. A joined query must select a qualified
// wildcard from its primary source; exclusions and replaced physical
// projections cannot borrow that authority.
func (s wildcardSource) preservesPhysicalWildcard(ctes query.WithSelects, depth int, allowAdditions bool) bool {
	return s.preservesPreparedWildcard(ctes, depth, allowAdditions, nil, "")
}

func (s wildcardSource) preservesPreparedWildcard(ctes query.WithSelects, depth int, allowAdditions bool, preparedOutputs map[string]bool, preparedTable string) bool {
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
	if nested == nil || nested.Union != nil {
		return false
	}
	joined := len(nested.Joins) != 0
	if joined {
		primary := wildcardSource{alias: nested.From.Alias, node: nested.From.X}
		table := primary.primaryPhysicalTable(append(append(query.WithSelects(nil), nested.WithSelects...), ctes...), depth+1)
		if table == "" || preparedTable == "" || canonicalProjectionName(table) != canonicalProjectionName(preparedTable) {
			return false
		}
	}
	var star *expr.Star
	for _, item := range nested.List {
		if candidate := projectionStar(item); candidate != nil {
			if star != nil {
				return false
			}
			star = candidate
		} else {
			if item == nil || !allowAdditions {
				return false
			}
			output := item.Alias
			if output == "" && joined {
				output = explicitJoinedOutput(nested, item)
			}
			if output == "" {
				return false
			}
			_, literal := item.Expr.(*expr.Literal)
			if !literal && !preparedOutputs[canonicalProjectionName(output)] {
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
	if joined && (qualifier == "" || qualifier == "*") {
		return false
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
	return child.preservesPreparedWildcard(scoped, depth+1, allowAdditions, preparedOutputs, preparedTable)
}

// primaryPhysicalTable follows only FROM sources; projected fields and sibling
// joins cannot supply authority for the wildcard's underlying table.
func (s wildcardSource) primaryPhysicalTable(ctes query.WithSelects, depth int) string {
	if depth > 32 {
		return ""
	}
	if nested, _ := s.query(ctes); nested != nil {
		if nested.Union != nil {
			return ""
		}
		child := wildcardSource{alias: nested.From.Alias, node: nested.From.X}
		return child.primaryPhysicalTable(append(append(query.WithSelects(nil), nested.WithSelects...), ctes...), depth+1)
	}
	table, _, err := sqlparser.SourceTable(s.node)
	if err != nil {
		return ""
	}
	return table
}

func explicitJoinedOutput(stmt *query.Select, item *query.Item) string {
	if _, ok := item.Expr.(*expr.Selector); !ok {
		return ""
	}
	column := sqlparser.NewColumn(item)
	if column.Namespace == "" || strings.EqualFold(column.Namespace, stmt.From.Alias) {
		return ""
	}
	matches := 0
	for _, join := range stmt.Joins {
		alias := join.Alias
		if alias == "" {
			alias, _, _ = sqlparser.SourceTable(join.With)
		}
		if strings.EqualFold(column.Namespace, alias) {
			matches++
		}
	}
	if matches != 1 {
		return ""
	}
	return column.Identity()
}

func (p SelectorProjection) preparedWildcardOutputs() map[string]bool {
	if p.View == nil {
		return nil
	}
	result := make(map[string]bool, len(p.View.Columns))
	for _, column := range p.View.Columns {
		if column == nil {
			continue
		}
		name, err := starOutputColumn(column)
		if err != nil {
			return nil
		}
		name = canonicalProjectionName(name)
		if result[name] {
			return nil
		}
		result[name] = true
	}
	return result
}

func (p SelectorProjection) preparedWildcardTable() string {
	if p.View == nil || p.View.Spec.Source == nil {
		return ""
	}
	return p.View.Spec.Source.Table
}
