package compile

import (
	"fmt"
	"strings"

	"github.com/viant/datly/transcribe/dql"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/node"
	"github.com/viant/sqlparser/query"
)

// extractNestedViewDirectives removes controls from parser-owned subqueries
// and returns them for one canonical application after the relation tree exists.
func extractNestedViewDirectives(parsed *query.Select) ([]viewDirective, error) {
	if parsed == nil {
		return nil, nil
	}
	result := make([]viewDirective, 0)
	uses := directCTEUses(parsed)
	for _, with := range parsed.WithSelects {
		if with == nil {
			continue
		}
		publicTargets := uses[strings.ToLower(strings.TrimSpace(with.Alias))]
		publicTarget := strings.TrimSpace(with.Alias)
		if len(publicTargets) == 1 {
			publicTarget = publicTargets[0]
		}
		var directives []viewDirective
		var err error
		switch {
		case with.X != nil:
			directives, err = extractQueryViewDirectives(with.X, publicTarget)
			if len(directives) > 0 {
				with.Raw = ""
			}
		case strings.TrimSpace(with.Raw) != "":
			var rewritten string
			rewritten, directives, err = rewriteSubqueryViewDirectives(with.Raw, publicTarget)
			if len(directives) > 0 {
				with.Raw = rewritten
			}
		}
		if err != nil {
			return nil, err
		}
		if len(directives) > 0 && len(publicTargets) == 0 {
			return nil, &Error{Code: CodeViewDirective, Cause: fmt.Errorf("CTE %q contains view controls but is not a direct canonical view source", with.Alias)}
		}
		if len(directives) > 0 && len(publicTargets) > 1 {
			return nil, &Error{Code: CodeViewDirective, Cause: fmt.Errorf("CTE %q with view controls is used by more than one canonical view", with.Alias)}
		}
		result = append(result, directives...)
	}

	rootTarget := queryNamespace(parsed)
	directives, err := rewriteSourceViewDirectives(parsed.From.X, rootTarget)
	if err != nil {
		return nil, err
	}
	result = append(result, directives...)
	for _, join := range parsed.Joins {
		if join == nil {
			continue
		}
		target := strings.TrimSpace(join.Alias)
		if target == "" {
			target = terminalName(join.With)
		}
		directives, err = rewriteSourceViewDirectives(join.With, target)
		if err != nil {
			return nil, err
		}
		result = append(result, directives...)
	}
	if parsed.Union != nil && parsed.Union.X != nil {
		directives, err = extractQueryViewDirectives(parsed.Union.X, rootTarget)
		if err != nil {
			return nil, err
		}
		result = append(result, directives...)
	}
	return result, nil
}

func directCTEUses(parsed *query.Select) map[string][]string {
	result := map[string][]string{}
	aliases := map[string]bool{}
	for _, with := range parsed.WithSelects {
		if with != nil && strings.TrimSpace(with.Alias) != "" {
			aliases[strings.ToLower(strings.TrimSpace(with.Alias))] = true
		}
	}
	add := func(source node.Node, publicTarget string, shadowed map[string]bool) {
		identifier, ok := source.(*expr.Ident)
		if !ok {
			return
		}
		key := strings.ToLower(strings.TrimSpace(identifier.Name))
		if !aliases[key] || shadowed[key] {
			return
		}
		result[key] = append(result[key], publicTarget)
	}
	rootTarget := queryNamespace(parsed)
	add(parsed.From.X, rootTarget, nil)
	for _, join := range parsed.Joins {
		if join == nil {
			continue
		}
		target := strings.TrimSpace(join.Alias)
		if target == "" {
			target = terminalName(join.With)
		}
		add(join.With, target, nil)
	}
	for branch := parsed.Union; branch != nil && branch.X != nil; branch = branch.X.Union {
		shadowed := map[string]bool{}
		for _, with := range branch.X.WithSelects {
			if with != nil && strings.TrimSpace(with.Alias) != "" {
				shadowed[strings.ToLower(strings.TrimSpace(with.Alias))] = true
			}
		}
		// A UNION branch contributes to the same canonical root view. Its joins
		// remain internal to that branch and never become canonical relations.
		add(branch.X.From.X, rootTarget, shadowed)
	}
	return result
}

func rewriteSourceViewDirectives(source node.Node, publicTarget string) ([]viewDirective, error) {
	switch actual := source.(type) {
	case *expr.Raw:
		rewritten, directives, err := rewriteSubqueryViewDirectives(actual.Raw, publicTarget)
		if err != nil {
			return nil, err
		}
		if len(directives) > 0 {
			actual.Raw = rewritten
			actual.X = nil
		}
		return directives, nil
	case *expr.Parenthesis:
		rewritten, directives, err := rewriteSubqueryViewDirectives(actual.Raw, publicTarget)
		if err != nil {
			return nil, err
		}
		if len(directives) > 0 {
			actual.Raw = rewritten
			actual.X = nil
		}
		return directives, nil
	default:
		return nil, nil
	}
}

func rewriteSubqueryViewDirectives(source, publicTarget string) (string, []viewDirective, error) {
	if strings.TrimSpace(source) == "" {
		return source, nil, nil
	}
	prepared, err := prepareSubquery(source)
	if err != nil {
		return "", nil, &Error{Code: CodeSQLParse, Cause: fmt.Errorf("parse nested view source: %w", err)}
	}
	if prepared == nil {
		return source, nil, nil
	}
	directives, err := extractQueryViewDirectives(prepared.query, publicTarget)
	if err != nil {
		return "", nil, err
	}
	if len(directives) == 0 {
		return source, nil, nil
	}
	rewritten := strings.TrimSpace((sqlparser.Stringifier{PreserveWindow: true}).String(prepared.query))
	for _, embed := range prepared.embeds {
		rewritten = strings.ReplaceAll(rewritten, embed.placeholder, embed.raw)
	}
	if prepared.wrapped {
		rewritten = "(" + rewritten + ")"
	}
	return rewritten, directives, nil
}

type preparedSubquery struct {
	query   *query.Select
	wrapped bool
	embeds  []embedPlaceholder
}

type embedPlaceholder struct {
	placeholder string
	raw         string
}

func prepareSubquery(source string) (*preparedSubquery, error) {
	trimmed := strings.TrimSpace(source)
	wrapped := len(trimmed) >= 2 && trimmed[0] == '(' && trimmed[len(trimmed)-1] == ')'
	if wrapped {
		trimmed = strings.TrimSpace(trimmed[1 : len(trimmed)-1])
	}
	refs := dql.EmbeddedSQLRefs(trimmed)
	if len(refs) == 1 && strings.TrimSpace(trimmed) == refs[0].Raw {
		return nil, nil
	}
	prepared := &preparedSubquery{wrapped: wrapped}
	for index, ref := range refs {
		if ref == nil || ref.Raw == "" {
			continue
		}
		placeholder := embedPlaceholderName(trimmed, index)
		trimmed = strings.ReplaceAll(trimmed, ref.Raw, placeholder)
		prepared.embeds = append(prepared.embeds, embedPlaceholder{placeholder: placeholder, raw: ref.Raw})
	}
	parsed, err := parseReadSQL(trimmed)
	if err != nil {
		return nil, err
	}
	if parsed == nil {
		return nil, fmt.Errorf("nested source is not a SELECT query")
	}
	prepared.query = parsed
	return prepared, nil
}

func embedPlaceholderName(source string, index int) string {
	for {
		candidate := fmt.Sprintf("__datly_embed_ref_%d__", index)
		if !strings.Contains(source, candidate) {
			return candidate
		}
		index++
	}
}

func extractQueryViewDirectives(parsed *query.Select, publicTarget string) ([]viewDirective, error) {
	nested, err := extractNestedViewDirectives(parsed)
	if err != nil {
		return nil, err
	}
	directives, err := extractViewDirectives(parsed)
	if err != nil {
		return nil, err
	}
	directives = append(nested, directives...)
	localTarget := queryNamespace(parsed)
	for _, directive := range directives {
		if localTarget == "" || !strings.EqualFold(directive.target, localTarget) {
			return nil, &Error{Code: CodeViewDirective, Cause: fmt.Errorf("%s target %q is internal to nested source %q; nested source controls may target only its root view", directive.name, directive.target, localTarget)}
		}
	}
	if publicTarget != "" && localTarget != "" && !strings.EqualFold(publicTarget, localTarget) {
		for index := range directives {
			if strings.EqualFold(directives[index].target, localTarget) {
				directives[index].target = publicTarget
			}
		}
	}
	return directives, nil
}
