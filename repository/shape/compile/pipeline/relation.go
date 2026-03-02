package pipeline

import (
	"fmt"
	"strings"

	dqldiag "github.com/viant/datly/repository/shape/dql/diag"
	dqlshape "github.com/viant/datly/repository/shape/dql/shape"
	"github.com/viant/datly/repository/shape/plan"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/node"
	"github.com/viant/sqlparser/query"
)

func ExtractJoinRelations(raw string, queryNode *query.Select) ([]*plan.Relation, []*dqlshape.Diagnostic) {
	if queryNode == nil || len(queryNode.Joins) == 0 {
		return nil, nil
	}
	rootAlias := rootNamespace(queryNode)
	var relations []*plan.Relation
	var diagnostics []*dqlshape.Diagnostic

	for idx, join := range queryNode.Joins {
		if join == nil {
			continue
		}
		offset := relationOffset(raw, join)
		span := pointSpan(raw, offset)
		ref, table := relationRef(join, idx+1)
		relation := &plan.Relation{
			Name:   ref,
			Holder: ExportedName(ref),
			Ref:    ref,
			Table:  table,
			Kind:   strings.TrimSpace(join.Kind),
			Raw:    strings.TrimSpace(join.Raw),
		}
		if relation.Holder == "" {
			relation.Holder = fmt.Sprintf("Rel%d", idx+1)
		}
		if join.On == nil || join.On.X == nil {
			diagnostics = append(diagnostics, &dqlshape.Diagnostic{
				Code:     dqldiag.CodeRelMissingON,
				Severity: dqlshape.SeverityWarning,
				Message:  "join is missing ON condition",
				Hint:     "use explicit ON condition to derive relation links",
				Span:     span,
			})
			relation.Warnings = append(relation.Warnings, "missing ON condition")
			relations = append(relations, relation)
			continue
		}
		pairs := collectJoinPairs(join.On.X)
		if len(pairs) == 0 {
			onExpr := strings.TrimSpace(sqlparser.Stringify(join.On.X))
			if shouldFallbackToRawJoinPairs(onExpr) {
				pairs = collectJoinPairsFromRaw(onExpr)
			}
		}
		if len(pairs) == 0 {
			diagnostics = append(diagnostics, &dqlshape.Diagnostic{
				Code:     dqldiag.CodeRelUnsupported,
				Severity: dqlshape.SeverityWarning,
				Message:  "join ON condition could not be translated into relation links",
				Hint:     "use equality predicates between concrete columns, e.g. a.id = b.ref_id",
				Span:     span,
			})
			relation.Warnings = append(relation.Warnings, "unsupported ON predicate")
			relations = append(relations, relation)
			continue
		}
		for _, pair := range pairs {
			link, warning := orientJoinPair(pair, rootAlias, ref)
			if warning != "" {
				diagnostics = append(diagnostics, &dqlshape.Diagnostic{
					Code:     dqldiag.CodeRelAmbiguous,
					Severity: dqlshape.SeverityWarning,
					Message:  warning,
					Hint:     "use explicit aliases so one side belongs to root and the other to joined table",
					Span:     span,
				})
				relation.Warnings = append(relation.Warnings, warning)
			}
			if link == nil {
				continue
			}
			relation.On = append(relation.On, link)
		}
		if len(relation.On) == 0 {
			diagnostics = append(diagnostics, &dqlshape.Diagnostic{
				Code:     dqldiag.CodeRelNoLinks,
				Severity: dqlshape.SeverityWarning,
				Message:  "join ON condition does not expose extractable column links",
				Hint:     "ensure both sides of '=' are concrete column references",
				Span:     span,
			})
			relation.Warnings = append(relation.Warnings, "no extractable links")
		}
		relations = append(relations, relation)
	}
	return relations, diagnostics
}

func collectJoinPairsFromRaw(input string) []joinPair {
	input = strings.TrimSpace(input)
	if input == "" {
		return nil
	}
	var (
		result []joinPair
		i      int
	)
	for i < len(input) {
		left, next, ok := parseRelationSelector(input, i)
		if !ok {
			i++
			continue
		}
		j := skipRelationSpaces(input, next)
		if j >= len(input) || input[j] != '=' {
			i = next
			continue
		}
		right, end, ok := parseRelationSelector(input, j+1)
		if !ok {
			i = j + 1
			continue
		}
		if strings.TrimSpace(left) == "" || strings.TrimSpace(right) == "" {
			i = end
			continue
		}
		result = append(result, joinPair{left: left, right: right})
		i = end
	}
	return result
}

func parseRelationSelector(input string, start int) (string, int, bool) {
	i := skipRelationSpaces(input, start)
	nsStart := i
	if nsStart >= len(input) || !isRelationIdentifierStart(input[nsStart]) {
		return "", start, false
	}
	i++
	for i < len(input) && isRelationIdentifierPart(input[i]) {
		i++
	}
	ns := input[nsStart:i]
	i = skipRelationSpaces(input, i)
	if i >= len(input) || input[i] != '.' {
		return "", start, false
	}
	i++
	i = skipRelationSpaces(input, i)
	colStart := i
	if colStart >= len(input) || !isRelationIdentifierStart(input[colStart]) {
		return "", start, false
	}
	i++
	for i < len(input) && isRelationIdentifierPart(input[i]) {
		i++
	}
	col := input[colStart:i]
	return strings.TrimSpace(ns) + "." + strings.TrimSpace(col), i, true
}

func skipRelationSpaces(input string, index int) int {
	for index < len(input) {
		switch input[index] {
		case ' ', '\t', '\n', '\r':
			index++
		default:
			return index
		}
	}
	return index
}

func isRelationIdentifierStart(ch byte) bool {
	return ch == '_' || (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z')
}

func isRelationIdentifierPart(ch byte) bool {
	return isRelationIdentifierStart(ch) || (ch >= '0' && ch <= '9')
}

func shouldFallbackToRawJoinPairs(input string) bool {
	input = strings.TrimSpace(strings.ToLower(input))
	if input == "" {
		return false
	}
	// Restrict raw fallback to simple selector equality text to avoid brittle extraction
	// for quoted identifiers, function calls, casts, and richer predicates.
	bannedFragments := []string{
		"`", "\"", "'", "(", ")", "::", " collate ", " case ", " when ", " then ", " else ", " end ",
		" coalesce", " cast", " concat", " substr", " lower", " upper", " trim",
	}
	for _, fragment := range bannedFragments {
		if strings.Contains(input, fragment) {
			return false
		}
	}
	return true
}

type joinPair struct {
	left  string
	right string
}

func collectJoinPairs(n node.Node) []joinPair {
	switch actual := n.(type) {
	case *expr.Binary:
		op := strings.ToUpper(strings.TrimSpace(actual.Op))
		if op == "AND" || op == "OR" {
			left := collectJoinPairs(actual.X)
			right := collectJoinPairs(actual.Y)
			return append(left, right...)
		}
		if op != "=" {
			return nil
		}
		left := selectorName(actual.X)
		right := selectorName(actual.Y)
		if left == "" || right == "" {
			return nil
		}
		return []joinPair{{left: left, right: right}}
	case *expr.Parenthesis:
		return collectJoinPairs(actual.X)
	default:
		return nil
	}
}

func selectorName(n node.Node) string {
	switch actual := n.(type) {
	case *expr.Selector:
		return strings.TrimSpace(sqlparser.Stringify(actual))
	case *expr.Parenthesis:
		return selectorName(actual.X)
	default:
		return ""
	}
}

func orientJoinPair(pair joinPair, rootAlias, refAlias string) (*plan.RelationLink, string) {
	leftNS, leftCol := splitSelector(pair.left)
	rightNS, rightCol := splitSelector(pair.right)
	if leftCol == "" || rightCol == "" {
		return nil, ""
	}
	switch {
	case leftNS == rootAlias && (rightNS == refAlias || rightNS == ""):
		return &plan.RelationLink{
			ParentNamespace: leftNS,
			ParentColumn:    leftCol,
			RefNamespace:    firstNonEmpty(rightNS, refAlias),
			RefColumn:       rightCol,
			Expression:      pair.left + "=" + pair.right,
		}, ""
	case rightNS == rootAlias && (leftNS == refAlias || leftNS == ""):
		return &plan.RelationLink{
			ParentNamespace: rightNS,
			ParentColumn:    rightCol,
			RefNamespace:    firstNonEmpty(leftNS, refAlias),
			RefColumn:       leftCol,
			Expression:      pair.left + "=" + pair.right,
		}, ""
	case leftNS == "" && rightNS == "":
		return &plan.RelationLink{
			ParentNamespace: rootAlias,
			ParentColumn:    leftCol,
			RefNamespace:    refAlias,
			RefColumn:       rightCol,
			Expression:      pair.left + "=" + pair.right,
		}, "join columns lack namespaces, relation orientation was inferred"
	case leftNS == refAlias:
		parentNS := rightNS
		if parentNS == "" {
			parentNS = rootAlias
		}
		return &plan.RelationLink{
			ParentNamespace: parentNS,
			ParentColumn:    rightCol,
			RefNamespace:    leftNS,
			RefColumn:       leftCol,
			Expression:      pair.left + "=" + pair.right,
		}, ""
	case rightNS == refAlias:
		parentNS := leftNS
		if parentNS == "" {
			parentNS = rootAlias
		}
		return &plan.RelationLink{
			ParentNamespace: parentNS,
			ParentColumn:    leftCol,
			RefNamespace:    rightNS,
			RefColumn:       rightCol,
			Expression:      pair.left + "=" + pair.right,
		}, ""
	default:
		return nil, fmt.Sprintf("ambiguous join link %q cannot be oriented between root=%q and ref=%q", pair.left+"="+pair.right, rootAlias, refAlias)
	}
}

func relationOffset(raw string, join *query.Join) int {
	if strings.TrimSpace(raw) == "" {
		return 0
	}
	if join != nil && join.On != nil && join.On.X != nil {
		if onExpr := strings.TrimSpace(sqlparser.Stringify(join.On.X)); onExpr != "" {
			if idx := strings.Index(strings.ToLower(raw), strings.ToLower(onExpr)); idx >= 0 {
				return idx
			}
		}
	}
	if join != nil && strings.TrimSpace(join.Raw) != "" {
		if idx := strings.Index(strings.ToLower(raw), strings.ToLower(strings.TrimSpace(join.Raw))); idx >= 0 {
			return idx
		}
	}
	return 0
}

func rootNamespace(queryNode *query.Select) string {
	if queryNode == nil {
		return ""
	}
	if alias := strings.TrimSpace(queryNode.From.Alias); alias != "" {
		return alias
	}
	if queryNode.From.X == nil {
		return ""
	}
	root := strings.TrimSpace(sqlparser.Stringify(queryNode.From.X))
	root = strings.Trim(root, "`\"")
	if root == "" {
		return ""
	}
	if idx := strings.LastIndex(root, "."); idx != -1 {
		root = root[idx+1:]
	}
	return root
}

func relationRef(join *query.Join, ordinal int) (string, string) {
	if join == nil {
		return fmt.Sprintf("join_%d", ordinal), ""
	}
	ref := strings.TrimSpace(join.Alias)
	table := ""
	if join.With != nil {
		table = strings.TrimSpace(sqlparser.Stringify(join.With))
	}
	if ref == "" {
		ref = table
		if idx := strings.LastIndex(ref, "."); idx != -1 {
			ref = ref[idx+1:]
		}
	}
	ref = SanitizeName(strings.Trim(ref, "`\""))
	if ref == "" {
		ref = fmt.Sprintf("join_%d", ordinal)
	}
	return ref, table
}

func splitSelector(selector string) (string, string) {
	selector = strings.TrimSpace(selector)
	if selector == "" {
		return "", ""
	}
	selector = strings.Trim(selector, "`\"")
	if idx := strings.Index(selector, "."); idx != -1 {
		return strings.Trim(selector[:idx], "`\""), strings.Trim(selector[idx+1:], "`\"")
	}
	return "", selector
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
