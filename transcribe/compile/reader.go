package compile

import (
	"errors"
	"fmt"
	"strings"

	"github.com/viant/datly/spec"
	"github.com/viant/datly/transcribe/dql"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/node"
	"github.com/viant/sqlparser/query"
)

const (
	CodeSQLParse            = "DQL-SQL-PARSE"
	CodeRelationMissingOn   = "DQL-REL-MISSING-ON"
	CodeRelationUnsupported = "DQL-REL-UNSUPPORTED"
	CodeRelationAmbiguous   = "DQL-REL-AMBIGUOUS"
	CodeViewDirective       = "DQL-VIEW-DIRECTIVE"
)

type Reader struct{}

// TemplateFrame is the parser-owned template envelope around SQL.
type TemplateFrame struct {
	Prefix string
	Suffix string
}

// ReadInput is the normalized parser product consumed by read compilation.
type ReadInput struct {
	View        *spec.View
	SQL         string
	Template    TemplateFrame
	Types       *typecatalog.Resolver
	TypeContext *spec.TypeContext
}

func NewReader() *Reader {
	return &Reader{}
}

func (r *Reader) Compile(input ReadInput) (*spec.View, error) {
	view := input.View
	parseSQL := input.SQL
	if view == nil {
		return nil, fmt.Errorf("read view is required")
	}
	parseSQL = strings.TrimSpace(parseSQL)
	if parseSQL == "" {
		return nil, fmt.Errorf("read SQL is required")
	}
	parsed, err := parseReadSQL(parseSQL)
	if err != nil {
		return nil, err
	}
	root := view.Clone()
	root.Namespace = queryNamespace(parsed)
	if root.Source == nil {
		root.Source = &spec.ViewSource{}
	}
	table, auxiliary, err := sqlparser.SourceTable(parsed.From.X)
	if err != nil {
		return nil, err
	}
	if table == "" {
		// Bare DQL table constants remain unresolved canonical table metadata
		// until the constant owner substitutes/validates them. Wrapped template
		// sources are not identifiers and must never enter this path.
		switch parsed.From.X.(type) {
		case *expr.Ident, *expr.Selector:
			table = strings.TrimSpace(sqlparser.Stringify(parsed.From.X))
		}
	}
	root.Source.Table = table
	root.Auxiliary = root.Auxiliary || auxiliary
	if auxiliary {
		// Mutation intent is retained on canonical metadata; executable SQL
		// uses the underlying table, avoiding a synthetic empty subquery.
		parsed.From.X = expr.NewSelector(table)
		root.Namespace = queryNamespace(parsed)
	}
	nestedDirectives, err := extractNestedViewDirectives(parsed)
	if err != nil {
		return nil, err
	}
	directives, err := extractViewDirectives(parsed)
	if err != nil {
		return nil, err
	}
	directives = append(nestedDirectives, directives...)
	relations, err := r.compileRelations(parsed, root, input.Template)
	if err != nil {
		return nil, err
	}
	root.Relations = relations
	if err := validateViewNamespaces(root); err != nil {
		return nil, err
	}
	projectionRewritten, err := lowerProjectionExclusions(parsed, root)
	if err != nil {
		return nil, err
	}
	columnsRewritten, err := lowerColumnDeclarations(parsed, root, input.Types, input.TypeContext)
	if err != nil {
		return nil, &Error{Code: CodeViewDirective, Cause: err}
	}
	sourceDecomposed, err := decomposeReadSources(parsed, root, input.Template)
	if err != nil {
		return nil, err
	}
	if err := applyViewDirectives(root, directives); err != nil {
		return nil, err
	}
	if !sourceDecomposed && (projectionRewritten || columnsRewritten || len(directives) > 0 || auxiliary) {
		root.Source.SQL = wrapReadProgram(input.Template, strings.TrimSpace((sqlparser.Stringifier{PreserveWindow: true}).String(parsed)))
	}
	if err := validateView(root, map[*spec.View]bool{}); err != nil {
		return nil, err
	}
	return root, nil
}

func (r *Reader) compileRelations(parsed *query.Select, root *spec.View, frame TemplateFrame) ([]*spec.Relation, error) {
	if parsed == nil || len(parsed.Joins) == 0 {
		return nil, nil
	}
	views := map[string]*spec.View{}
	rootNamespace := strings.ToLower(strings.TrimSpace(queryNamespace(parsed)))
	if rootNamespace == "" || root == nil {
		return nil, &Error{Code: CodeRelationAmbiguous, Cause: fmt.Errorf("root query namespace is required for relations")}
	}
	root.Relations = nil
	views[rootNamespace] = root
	for index, join := range parsed.Joins {
		relation, err := r.compileRelation(join, index, parsed.WithSelects, parsed.WithRecursive, frame)
		if err != nil {
			return nil, err
		}
		parentNamespace := strings.ToLower(strings.TrimSpace(relation.ParentNamespace))
		parent := views[parentNamespace]
		if parent == nil {
			return nil, relationError(CodeRelationAmbiguous, fmt.Errorf("join %s parent namespace %q is not an earlier compiled view", relation.Name, relation.ParentNamespace), join.OnSpan)
		}
		childNamespace := strings.ToLower(strings.TrimSpace(relation.View.Namespace))
		if childNamespace == "" || views[childNamespace] != nil {
			return nil, relationError(CodeRelationAmbiguous, fmt.Errorf("join namespace %q is duplicated", relation.View.Namespace), join.Span)
		}
		parent.Relations = append(parent.Relations, relation)
		views[childNamespace] = relation.View
	}
	return root.Relations, nil
}

func (r *Reader) compileRelation(join *query.Join, index int, withs query.WithSelects, recursive bool, frame TemplateFrame) (*spec.Relation, error) {
	if join == nil {
		return nil, fmt.Errorf("join %d is required", index+1)
	}
	childNamespace := strings.TrimSpace(join.Alias)
	if childNamespace == "" {
		childNamespace = terminalName(join.With)
	}
	if childNamespace == "" {
		return nil, relationError(CodeRelationAmbiguous, fmt.Errorf("join %d requires an alias", index+1), join.Span)
	}
	if join.On == nil || join.On.X == nil {
		return nil, relationError(CodeRelationMissingOn, fmt.Errorf("join %s requires an ON predicate", childNamespace), join.Span)
	}
	pairs, toOne, unsupported := relationPairs(join.On.X)
	if unsupported || len(pairs) == 0 {
		return nil, relationError(CodeRelationUnsupported, fmt.Errorf("join %s ON predicate must contain AST equality links", childNamespace), join.OnSpan)
	}
	links := make([]*spec.RelationLink, 0, len(pairs))
	parentNamespace := ""
	for _, pair := range pairs {
		link, err := orientPair(pair, childNamespace)
		if err != nil {
			return nil, relationError(CodeRelationAmbiguous, err, join.OnSpan)
		}
		if parentNamespace == "" {
			parentNamespace = link.ParentNamespace
		} else if !strings.EqualFold(parentNamespace, link.ParentNamespace) {
			return nil, relationError(CodeRelationAmbiguous, fmt.Errorf("join %s links resolve to multiple parents", childNamespace), join.OnSpan)
		}
		links = append(links, link)
	}
	childSource, err := canonicalReadSource(join.With, childNamespace, withs, recursive, frame)
	if err != nil {
		var compileError *Error
		if errors.As(err, &compileError) {
			return nil, relationError(compileError.Code, compileError.Cause, join.Span)
		}
		return nil, relationError(CodeRelationUnsupported, err, join.Span)
	}
	child := &spec.View{
		Name: childNamespace, Namespace: childNamespace,
		Source: childSource,
	}
	_, child.Auxiliary, err = sqlparser.SourceTable(join.With)
	if err != nil {
		return nil, relationError(CodeRelationUnsupported, err, join.Span)
	}
	cardinality := spec.CardinalityMany
	if toOne {
		cardinality = spec.CardinalityOne
		child.Cardinality = spec.CardinalityOne
	}
	return &spec.Relation{
		Name: childNamespace, Kind: spec.RelationKindSubview,
		Holder: typecatalog.FieldName(childNamespace), Cardinality: cardinality,
		Join: strings.TrimSpace(join.Kind), ParentNamespace: parentNamespace,
		View: child, On: links,
	}, nil
}

func relationError(code string, cause error, span node.Span) *Error {
	start, end := int(span.Begin), int(span.End)
	if end < start {
		end = start
	}
	return &Error{Code: code, Offset: start, End: end, Cause: cause}
}

type relationPair struct {
	left  string
	right string
}

func relationPairs(value node.Node) ([]relationPair, bool, bool) {
	switch actual := value.(type) {
	case *expr.Parenthesis:
		return relationPairs(actual.X)
	case *expr.Binary:
		switch strings.ToUpper(strings.TrimSpace(actual.Op)) {
		case "AND":
			left, leftOne, leftUnsupported := relationPairs(actual.X)
			right, rightOne, rightUnsupported := relationPairs(actual.Y)
			return append(left, right...), leftOne || rightOne, leftUnsupported || rightUnsupported
		case "=":
			rightNode := actual.Y
			var remainder node.Node
			if tail, ok := actual.Y.(*expr.Binary); ok && strings.EqualFold(strings.TrimSpace(tail.Op), "AND") {
				rightNode, remainder = tail.X, tail.Y
			}
			var pairs []relationPair
			toOne := false
			left, right := selectorText(actual.X), selectorText(rightNode)
			if left != "" && right != "" {
				pairs = []relationPair{{left: left, right: right}}
			} else {
				l, lok := actual.X.(*expr.Literal)
				r, rok := rightNode.(*expr.Literal)
				if !lok || !rok || (l.Kind != "int" && l.Kind != "numeric") || (r.Kind != "int" && r.Kind != "numeric") || l.Value != "1" || r.Value != "1" {
					return nil, false, true
				}
				toOne = true
			}
			if remainder != nil {
				more, one, unsupported := relationPairs(remainder)
				return append(pairs, more...), toOne || one, unsupported
			}
			return pairs, toOne, false
		}
	}
	return nil, false, true
}

func orientPair(pair relationPair, childNamespace string) (*spec.RelationLink, error) {
	leftNamespace, leftColumn := splitSelector(pair.left)
	rightNamespace, rightColumn := splitSelector(pair.right)
	if leftNamespace == "" || rightNamespace == "" || leftColumn == "" || rightColumn == "" {
		return nil, fmt.Errorf("relation link %s = %s requires qualified columns", pair.left, pair.right)
	}
	switch {
	case strings.EqualFold(leftNamespace, childNamespace) && !strings.EqualFold(rightNamespace, childNamespace):
		return &spec.RelationLink{
			ParentNamespace: rightNamespace, ParentColumn: rightColumn,
			ChildNamespace: leftNamespace, ChildColumn: leftColumn,
		}, nil
	case strings.EqualFold(rightNamespace, childNamespace) && !strings.EqualFold(leftNamespace, childNamespace):
		return &spec.RelationLink{
			ParentNamespace: leftNamespace, ParentColumn: leftColumn,
			ChildNamespace: rightNamespace, ChildColumn: rightColumn,
		}, nil
	default:
		return nil, fmt.Errorf("relation link %s = %s does not identify child %s", pair.left, pair.right, childNamespace)
	}
}

func selectorText(value node.Node) string {
	selector, ok := value.(*expr.Selector)
	if !ok {
		return ""
	}
	return strings.TrimSpace(sqlparser.Stringify(selector))
}

func splitSelector(value string) (string, string) {
	value = strings.TrimSpace(strings.Trim(value, "`\""))
	index := strings.LastIndex(value, ".")
	if index <= 0 || index+1 >= len(value) {
		return "", ""
	}
	return strings.Trim(value[:index], "`\""), strings.Trim(value[index+1:], "`\"")
}

func queryNamespace(parsed *query.Select) string {
	if parsed == nil {
		return ""
	}
	if alias := strings.TrimSpace(parsed.From.Alias); alias != "" {
		return alias
	}
	return terminalName(parsed.From.X)
}

func relationSource(value node.Node) (*spec.ViewSource, error) {
	result := &spec.ViewSource{}
	if table, _, err := sqlparser.SourceTable(value); err != nil {
		return nil, err
	} else if table != "" {
		result.Table = table
		return result, nil
	}
	switch actual := value.(type) {
	case *expr.Raw:
		raw := strings.TrimSpace(actual.Raw)
		if len(raw) >= 2 && raw[0] == '(' && raw[len(raw)-1] == ')' {
			raw = strings.TrimSpace(raw[1 : len(raw)-1])
		}
		result.SQL = raw
		result.Embeds = dql.EmbeddedSQLRefs(raw)
	case *expr.Parenthesis:
		raw := strings.TrimSpace(actual.Raw)
		if len(raw) >= 2 && raw[0] == '(' && raw[len(raw)-1] == ')' {
			raw = strings.TrimSpace(raw[1 : len(raw)-1])
		}
		result.SQL = raw
		result.Embeds = dql.EmbeddedSQLRefs(raw)
	case *expr.Ident, *expr.Selector:
		result.Table = strings.TrimSpace(sqlparser.Stringify(value))
	}
	return result, nil
}

func terminalName(value node.Node) string {
	switch actual := value.(type) {
	case *expr.Ident:
		return strings.TrimSpace(actual.Name)
	case *expr.Selector:
		if actual.X != nil {
			return terminalName(actual.X)
		}
		return strings.TrimSpace(actual.Name)
	default:
		return ""
	}
}
