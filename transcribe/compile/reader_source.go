package compile

import (
	"fmt"
	"strings"

	"github.com/viant/datly/spec"
	"github.com/viant/datly/transcribe/dql"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/node"
	"github.com/viant/sqlparser/query"
)

// decomposeReadSources removes the outer DQL relation declaration from the
// root executable source. Each canonical relation already owns its parser-
// derived child table or subquery and is read independently by the reader.
func decomposeReadSources(parsed *query.Select, root *spec.View, frame TemplateFrame) (bool, error) {
	if parsed == nil || root == nil || len(root.Relations) == 0 {
		return false, nil
	}
	if parsed.Union != nil || strings.EqualFold(strings.TrimSpace(parsed.Kind), "DISTINCT") {
		return false, &Error{Code: CodeRelationUnsupported, Cause: fmt.Errorf("multi-view read declarations cannot use an outer UNION or DISTINCT")}
	}
	source, err := canonicalRootSource(parsed, root, frame)
	if err != nil {
		return false, err
	}
	if source == nil || (strings.TrimSpace(source.Table) == "" && strings.TrimSpace(source.SQL) == "") {
		return false, &Error{Code: CodeRelationUnsupported, Cause: fmt.Errorf("multi-view root source is not independently executable")}
	}
	if source.Table != "" {
		root.Source.Table = source.Table
	}
	root.Source.SQL = source.SQL
	root.Source.Embeds = source.Embeds
	views := canonicalViews(root)
	for _, join := range parsed.Joins {
		if join == nil {
			continue
		}
		alias := strings.TrimSpace(join.Alias)
		if alias == "" {
			alias = terminalName(join.With)
		}
		child := views[strings.ToLower(alias)]
		projection, err := readViewProjection(parsed, root, child)
		if err != nil {
			return false, err
		}
		if len(projection) == 0 {
			continue
		}
		statement := &query.Select{List: projection, From: query.From{X: join.With, Alias: alias}}
		if referencesCTE(join.With, parsed.WithSelects) {
			statement.WithSelects, statement.WithRecursive = parsed.WithSelects, parsed.WithRecursive
		}
		child.Source.SQL = wrapReadProgram(frame, strings.TrimSpace((sqlparser.Stringifier{PreserveWindow: true}).String(statement)))
		child.Source.Embeds = dql.EmbeddedSQLRefs(child.Source.SQL)
	}
	return true, nil
}

func canonicalRootSource(parsed *query.Select, root *spec.View, frame TemplateFrame) (*spec.ViewSource, error) {
	if parsed == nil {
		return nil, nil
	}
	if len(parsed.GroupBy) > 0 || parsed.Having != nil {
		return nil, &Error{Code: CodeRelationUnsupported, Cause: fmt.Errorf("multi-view root GROUP BY and HAVING must be declared inside the root source")}
	}
	if err := validateRootClauseOwnership(parsed, root); err != nil {
		return nil, err
	}
	projection, err := readViewProjection(parsed, root, root)
	if err != nil {
		return nil, err
	}
	cteBacked := referencesCTE(parsed.From.X, parsed.WithSelects)
	if len(projection) == 0 && !cteBacked && parsed.Qualify == nil && len(parsed.OrderBy) == 0 && parsed.Limit == nil && parsed.Offset == nil {
		return canonicalReadSource(parsed.From.X, queryNamespace(parsed), nil, false, frame)
	}
	if len(projection) == 0 {
		projection = query.List{query.NewItem(expr.NewSelector("*"))}
	}
	standalone := &query.Select{
		List: projection, From: parsed.From,
		Qualify: parsed.Qualify, OrderBy: parsed.OrderBy, Limit: parsed.Limit, Offset: parsed.Offset,
	}
	if cteBacked {
		standalone.WithSelects = parsed.WithSelects
		standalone.WithRecursive = parsed.WithRecursive
	}
	SQL := wrapReadProgram(frame, strings.TrimSpace((sqlparser.Stringifier{PreserveWindow: true}).String(standalone)))
	return &spec.ViewSource{SQL: SQL, Embeds: dql.EmbeddedSQLRefs(SQL)}, nil
}

func validateRootClauseOwnership(parsed *query.Select, root *spec.View) error {
	known := canonicalViews(root)
	rootNamespace := strings.ToLower(strings.TrimSpace(queryNamespace(parsed)))
	clauses := []node.Node{}
	if parsed.Qualify != nil {
		clauses = append(clauses, parsed.Qualify)
	}
	for _, item := range parsed.OrderBy {
		clauses = append(clauses, item)
	}
	for _, clause := range clauses {
		if err := validateRootClauseNode(clause, known, rootNamespace); err != nil {
			return &Error{Code: CodeRelationUnsupported, Cause: err}
		}
	}
	return nil
}

func validateRootClauseNode(candidate node.Node, known map[string]*spec.View, rootNamespace string) error {
	switch actual := candidate.(type) {
	case nil, *expr.Literal, *expr.Placeholder, *expr.Values:
		return nil
	case *expr.Ident:
		name := strings.TrimSpace(actual.Name)
		if strings.HasPrefix(name, "$") {
			return nil
		}
		return fmt.Errorf("outer multi-view clause has ambiguous unqualified column %q", name)
	case *expr.Selector:
		namespace := strings.ToLower(strings.TrimSpace(actual.Name))
		if strings.HasPrefix(namespace, "$") {
			return nil
		}
		if known[namespace] == nil {
			return fmt.Errorf("outer multi-view clause references unknown namespace %q", actual.Name)
		}
		if namespace != rootNamespace {
			return fmt.Errorf("outer root clause references child view %q; move it into that view source", actual.Name)
		}
		return nil
	case *expr.Binary:
		if err := validateRootClauseNode(actual.X, known, rootNamespace); err != nil {
			return err
		}
		return validateRootClauseNode(actual.Y, known, rootNamespace)
	case *expr.Unary:
		return validateRootClauseNode(actual.X, known, rootNamespace)
	case *expr.Parenthesis:
		return validateRootClauseNode(actual.X, known, rootNamespace)
	case *expr.Qualify:
		return validateRootClauseNode(actual.X, known, rootNamespace)
	case *expr.Collate:
		return validateRootClauseNode(actual.X, known, rootNamespace)
	case *expr.Range:
		if err := validateRootClauseNode(actual.Min, known, rootNamespace); err != nil {
			return err
		}
		return validateRootClauseNode(actual.Max, known, rootNamespace)
	case *expr.Call:
		for _, argument := range actual.Args {
			if err := validateRootClauseNode(argument, known, rootNamespace); err != nil {
				return err
			}
		}
		return nil
	case *query.Item:
		return validateRootClauseNode(actual.Expr, known, rootNamespace)
	case query.List:
		for _, item := range actual {
			if err := validateRootClauseNode(item, known, rootNamespace); err != nil {
				return err
			}
		}
		return nil
	case *expr.Raw:
		if actual.X == nil {
			return fmt.Errorf("outer multi-view clause contains unsupported raw expression %q", strings.TrimSpace(actual.Raw))
		}
		return validateRootClauseNode(actual.X, known, rootNamespace)
	default:
		return fmt.Errorf("outer multi-view clause contains unsupported expression %T", candidate)
	}
}

func canonicalReadSource(source node.Node, alias string, withs query.WithSelects, recursive bool, frame TemplateFrame) (*spec.ViewSource, error) {
	if !referencesCTE(source, withs) {
		result, err := relationSource(source)
		if err != nil {
			return result, err
		}
		if result == nil {
			return nil, nil
		}
		// Keep the declared derived-table alias in executable SQL. Relation
		// filters refer to its output columns, not to the subquery's inner
		// tables or computed expressions.
		if strings.TrimSpace(result.SQL) != "" && strings.TrimSpace(alias) != "" {
			standalone := &query.Select{
				List: query.List{query.NewItem(expr.NewSelector("*"))},
				From: query.From{X: source, Alias: strings.TrimSpace(alias)},
			}
			result.SQL = strings.TrimSpace((sqlparser.Stringifier{PreserveWindow: true}).String(standalone))
			result.Embeds = dql.EmbeddedSQLRefs(result.SQL)
		}
		if strings.TrimSpace(frame.Prefix) == "" && strings.TrimSpace(frame.Suffix) == "" {
			return result, nil
		}
		SQL := strings.TrimSpace(result.SQL)
		if SQL == "" && strings.TrimSpace(result.Table) != "" {
			SQL = "SELECT * FROM " + strings.TrimSpace(result.Table)
			if strings.TrimSpace(alias) != "" {
				SQL += " " + strings.TrimSpace(alias)
			}
			result.Table = ""
		}
		result.SQL = wrapReadProgram(frame, SQL)
		result.Embeds = dql.EmbeddedSQLRefs(result.SQL)
		return result, nil
	}
	standalone := &query.Select{
		WithSelects: withs, WithRecursive: recursive,
		List: query.List{query.NewItem(expr.NewSelector("*"))},
		From: query.From{X: source, Alias: strings.TrimSpace(alias)},
	}
	SQL := wrapReadProgram(frame, strings.TrimSpace((sqlparser.Stringifier{PreserveWindow: true}).String(standalone)))
	return &spec.ViewSource{SQL: SQL, Embeds: dql.EmbeddedSQLRefs(SQL)}, nil
}

func wrapReadProgram(frame TemplateFrame, SQL string) string {
	prefix := strings.TrimSpace(frame.Prefix)
	SQL = strings.TrimSpace(SQL)
	suffix := strings.TrimSpace(frame.Suffix)
	parts := make([]string, 0, 3)
	for _, part := range []string{prefix, SQL, suffix} {
		if part != "" {
			parts = append(parts, part)
		}
	}
	return strings.Join(parts, "\n")
}

func referencesCTE(source node.Node, withs query.WithSelects) bool {
	identifier, ok := source.(*expr.Ident)
	if !ok {
		return false
	}
	name := strings.TrimSpace(identifier.Name)
	for _, with := range withs {
		if with != nil && strings.EqualFold(strings.TrimSpace(with.Alias), name) {
			return true
		}
	}
	return false
}

// readSourceTable follows the primary FROM source for table/auxiliary metadata.
// It never substitutes that table for the authored executable SQL.
func readSourceTable(source node.Node, withs query.WithSelects, depth int) (string, bool, error) {
	if depth > 32 {
		return "", false, nil
	}
	if identifier, ok := source.(*expr.Ident); ok {
		for _, with := range withs {
			if with == nil || !strings.EqualFold(with.Alias, identifier.Name) {
				continue
			}
			if with.X != nil {
				return readSourceTable(with.X, withs, depth+1)
			}
			return readSourceTable(&expr.Raw{Raw: with.Raw}, withs, depth+1)
		}
	}
	if table, auxiliary, err := sqlparser.SourceTable(source); err != nil || table != "" {
		return table, auxiliary, err
	}
	var parsed *query.Select
	var raw string
	switch actual := source.(type) {
	case *query.Select:
		parsed = actual
	case *expr.Raw:
		raw = actual.Raw
	case *expr.Parenthesis:
		raw = actual.Raw
	default:
		return "", false, nil
	}
	if parsed == nil && strings.TrimSpace(raw) != "" {
		prepared, err := prepareSubquery(raw)
		if err != nil {
			return "", false, err
		}
		if prepared != nil {
			if len(prepared.embeds) > 0 {
				return "", false, nil
			}
			parsed = prepared.query
		}
	}
	if parsed == nil {
		return "", false, nil
	}
	// Set operations do not establish one writable table.
	if parsed.Union != nil {
		return "", false, nil
	}
	scoped := append(append(query.WithSelects(nil), parsed.WithSelects...), withs...)
	return readSourceTable(parsed.From.X, scoped, depth+1)
}
