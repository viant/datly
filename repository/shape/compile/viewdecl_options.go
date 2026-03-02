package compile

import (
	"fmt"
	"strconv"
	"strings"

	dqldiag "github.com/viant/datly/repository/shape/dql/diag"
	dqlshape "github.com/viant/datly/repository/shape/dql/shape"
	"github.com/viant/parsly"
)

func extractDeclarationSQL(fragment string) string {
	cursor := parsly.NewCursor("", []byte(fragment), 0)
	for cursor.Pos < cursor.InputSize {
		match := cursor.MatchAfterOptional(vdWhitespaceMatcher, vdCommentMatcher)
		if match.Code == vdCommentToken {
			text := match.Text(cursor)
			if len(text) < 4 {
				return ""
			}
			return normalizeHintSQL(text[2 : len(text)-2])
		}
		cursor.Pos++
	}
	return ""
}

func normalizeHintSQL(body string) string {
	body = strings.TrimSpace(body)
	if body == "" {
		return ""
	}
	if strings.HasPrefix(body, "{") {
		if closeIdx := strings.Index(body, "}"); closeIdx != -1 {
			body = strings.TrimSpace(body[closeIdx+1:])
		}
	}
	if body == "" {
		return ""
	}
	switch body[0] {
	case '?':
		body = strings.TrimSpace(body[1:])
	case '!':
		body = strings.TrimSpace(body[1:])
		if strings.HasPrefix(body, "!") {
			body = strings.TrimSpace(body[1:])
		}
		if len(body) >= 3 {
			var status int
			if _, err := fmt.Sscanf(body[:3], "%d", &status); err == nil {
				body = strings.TrimSpace(body[3:])
			}
		}
	}
	return strings.TrimSpace(body)
}

func applyDeclaredViewOptions(view *declaredView, tail, dql string, offset int, diags *[]*dqlshape.Diagnostic) {
	if view == nil || strings.TrimSpace(tail) == "" {
		return
	}
	cursor := parsly.NewCursor("", []byte(tail), 0)
	for cursor.Pos < cursor.InputSize {
		_ = cursor.MatchOne(vdWhitespaceMatcher)
		if cursor.MatchOne(vdDotMatcher).Code != vdDotToken {
			cursor.Pos++
			continue
		}
		_ = cursor.MatchOne(vdWhitespaceMatcher)
		name, ok := readIdentifier(cursor)
		if !ok {
			continue
		}
		_ = cursor.MatchOne(vdWhitespaceMatcher)
		group := cursor.MatchOne(vdExprGroupMatcher)
		if group.Code != vdExprGroupToken {
			continue
		}
		content := group.Text(cursor)
		if len(content) < 2 {
			continue
		}
		args := splitArgs(content[1 : len(content)-1])
		switch {
		case strings.EqualFold(name, "WithURI"):
			if !expectArgs(view, name, args, 1, -1, dql, offset, diags) {
				continue
			}
			view.URI = trimQuote(args[0])
		case strings.EqualFold(name, "WithConnector"), strings.EqualFold(name, "Connector"):
			if !expectArgs(view, name, args, 1, -1, dql, offset, diags) {
				continue
			}
			view.Connector = trimQuote(args[0])
		case strings.EqualFold(name, "Cardinality"):
			if !expectArgs(view, name, args, 1, 1, dql, offset, diags) {
				continue
			}
			card := strings.ToLower(strings.TrimSpace(trimQuote(args[0])))
			switch card {
			case "one", "many":
				view.Cardinality = card
			default:
				*diags = append(*diags, &dqlshape.Diagnostic{
					Code:     dqldiag.CodeViewCardinality,
					Severity: dqlshape.SeverityWarning,
					Message:  fmt.Sprintf("unsupported cardinality %q for declared view %q", args[0], view.Name),
					Hint:     "use Cardinality('one') or Cardinality('many')",
					Span:     relationSpan(dql, offset),
				})
			}
		case strings.EqualFold(name, "WithTag"), strings.EqualFold(name, "Tag"):
			if !expectArgs(view, name, args, 1, 1, dql, offset, diags) {
				continue
			}
			view.Tag = trimQuote(args[0])
		case strings.EqualFold(name, "WithCodec"), strings.EqualFold(name, "Codec"):
			if !expectArgs(view, name, args, 1, -1, dql, offset, diags) {
				continue
			}
			view.Codec = trimQuote(args[0])
			view.CodecArgs = nil
			for _, arg := range args[1:] {
				view.CodecArgs = append(view.CodecArgs, strings.TrimSpace(arg))
			}
		case strings.EqualFold(name, "WithHandler"), strings.EqualFold(name, "Handler"):
			if !expectArgs(view, name, args, 1, -1, dql, offset, diags) {
				continue
			}
			view.HandlerName = trimQuote(args[0])
			view.HandlerArgs = nil
			for _, arg := range args[1:] {
				view.HandlerArgs = append(view.HandlerArgs, strings.TrimSpace(arg))
			}
		case strings.EqualFold(name, "WithStatusCode"), strings.EqualFold(name, "StatusCode"):
			if !expectArgs(view, name, args, 1, 1, dql, offset, diags) {
				continue
			}
			statusCode, err := strconv.Atoi(strings.TrimSpace(trimQuote(args[0])))
			if err != nil {
				*diags = append(*diags, &dqlshape.Diagnostic{
					Code:     dqldiag.CodeDeclOptionArgs,
					Severity: dqlshape.SeverityWarning,
					Message:  fmt.Sprintf("invalid status code %q for declared view %q", args[0], view.Name),
					Hint:     "use numeric status code, e.g. StatusCode(400)",
					Span:     relationSpan(dql, offset),
				})
				continue
			}
			view.StatusCode = &statusCode
		case strings.EqualFold(name, "WithErrorMessage"):
			if !expectArgs(view, name, args, 1, 1, dql, offset, diags) {
				continue
			}
			view.ErrorMessage = trimQuote(args[0])
		case strings.EqualFold(name, "WithPredicate"), strings.EqualFold(name, "Predicate"):
			if !expectArgs(view, name, args, 2, -1, dql, offset, diags) {
				continue
			}
			view.Predicates = append(view.Predicates, declaredPredicate{
				Name:      trimQuote(args[0]),
				Source:    trimQuote(args[1]),
				Arguments: append([]string{}, args[2:]...),
			})
		case strings.EqualFold(name, "EnsurePredicate"):
			if !expectArgs(view, name, args, 2, -1, dql, offset, diags) {
				continue
			}
			view.Predicates = append(view.Predicates, declaredPredicate{
				Name:      trimQuote(args[0]),
				Source:    trimQuote(args[1]),
				Ensure:    true,
				Arguments: append([]string{}, args[2:]...),
			})
		case strings.EqualFold(name, "QuerySelector"):
			if !expectArgs(view, name, args, 1, 1, dql, offset, diags) {
				continue
			}
			view.QuerySelector = trimQuote(args[0])
			if !isAllowedQuerySelector(strings.ToLower(view.Name)) {
				*diags = append(*diags, &dqlshape.Diagnostic{
					Code:     dqldiag.CodeDeclQuerySelector,
					Severity: dqlshape.SeverityWarning,
					Message:  fmt.Sprintf("query selector %q can only be used with limit, offset, page, fields, orderby", view.QuerySelector),
					Hint:     "use QuerySelector on declarations named limit/offset/page/fields/orderby",
					Span:     relationSpan(dql, offset),
				})
			}
		case strings.EqualFold(name, "WithCache"):
			if !expectArgs(view, name, args, 1, 1, dql, offset, diags) {
				continue
			}
			view.CacheRef = trimQuote(args[0])
		case strings.EqualFold(name, "WithLimit"):
			if !expectArgs(view, name, args, 1, 1, dql, offset, diags) {
				continue
			}
			limit, err := strconv.Atoi(strings.TrimSpace(trimQuote(args[0])))
			if err != nil {
				appendOptionArgDiagnostic(view, name, fmt.Sprintf("invalid integer limit %q", args[0]), dql, offset, diags)
				continue
			}
			view.Limit = &limit
		case strings.EqualFold(name, "Cacheable"):
			if !expectArgs(view, name, args, 1, 1, dql, offset, diags) {
				continue
			}
			value, err := strconv.ParseBool(strings.TrimSpace(trimQuote(args[0])))
			if err != nil {
				appendOptionArgDiagnostic(view, name, fmt.Sprintf("invalid bool cacheable %q", args[0]), dql, offset, diags)
				continue
			}
			view.Cacheable = &value
		case strings.EqualFold(name, "When"):
			if !expectArgs(view, name, args, 1, 1, dql, offset, diags) {
				continue
			}
			view.When = trimQuote(args[0])
		case strings.EqualFold(name, "Scope"):
			if !expectArgs(view, name, args, 1, 1, dql, offset, diags) {
				continue
			}
			view.Scope = trimQuote(args[0])
		case strings.EqualFold(name, "WithType"):
			if !expectArgs(view, name, args, 1, 1, dql, offset, diags) {
				continue
			}
			view.DataType = trimQuote(args[0])
		case strings.EqualFold(name, "Of"):
			if !expectArgs(view, name, args, 1, 1, dql, offset, diags) {
				continue
			}
			view.Of = trimQuote(args[0])
		case strings.EqualFold(name, "Value"):
			if !expectArgs(view, name, args, 1, 1, dql, offset, diags) {
				continue
			}
			view.Value = trimQuote(args[0])
		case strings.EqualFold(name, "Async"):
			if !expectArgs(view, name, args, 0, 0, dql, offset, diags) {
				continue
			}
			view.Async = true
		case strings.EqualFold(name, "Output"):
			if !expectArgs(view, name, args, 0, 0, dql, offset, diags) {
				continue
			}
			view.Output = true
		}
	}
}

func splitArgs(raw string) []string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	var result []string
	var current strings.Builder
	inSingle := false
	inDouble := false
	escape := false
	parens := 0
	brackets := 0
	braces := 0
	for i := 0; i < len(raw); i++ {
		ch := raw[i]
		if escape {
			current.WriteByte(ch)
			escape = false
			continue
		}
		switch ch {
		case '\\':
			current.WriteByte(ch)
			escape = true
		case '\'':
			if !inDouble {
				inSingle = !inSingle
			}
			current.WriteByte(ch)
		case '"':
			if !inSingle {
				inDouble = !inDouble
			}
			current.WriteByte(ch)
		case '(':
			if !inSingle && !inDouble {
				parens++
			}
			current.WriteByte(ch)
		case ')':
			if !inSingle && !inDouble && parens > 0 {
				parens--
			}
			current.WriteByte(ch)
		case '[':
			if !inSingle && !inDouble {
				brackets++
			}
			current.WriteByte(ch)
		case ']':
			if !inSingle && !inDouble && brackets > 0 {
				brackets--
			}
			current.WriteByte(ch)
		case '{':
			if !inSingle && !inDouble {
				braces++
			}
			current.WriteByte(ch)
		case '}':
			if !inSingle && !inDouble && braces > 0 {
				braces--
			}
			current.WriteByte(ch)
		case ',':
			if inSingle || inDouble || parens > 0 || brackets > 0 || braces > 0 {
				current.WriteByte(ch)
				continue
			}
			part := strings.TrimSpace(current.String())
			if part != "" {
				result = append(result, part)
			}
			current.Reset()
		default:
			current.WriteByte(ch)
		}
	}
	if tail := strings.TrimSpace(current.String()); tail != "" {
		result = append(result, tail)
	}
	return result
}

func trimQuote(v string) string {
	v = strings.TrimSpace(v)
	if len(v) >= 2 {
		if (v[0] == '\'' && v[len(v)-1] == '\'') || (v[0] == '"' && v[len(v)-1] == '"') {
			return v[1 : len(v)-1]
		}
	}
	return v
}

func expectArgs(view *declaredView, option string, args []string, min, max int, dql string, offset int, diags *[]*dqlshape.Diagnostic) bool {
	if len(args) < min {
		appendOptionArgDiagnostic(view, option, fmt.Sprintf("expected at least %d args, got %d", min, len(args)), dql, offset, diags)
		return false
	}
	if max >= 0 && len(args) > max {
		appendOptionArgDiagnostic(view, option, fmt.Sprintf("expected at most %d args, got %d", max, len(args)), dql, offset, diags)
		return false
	}
	return true
}

func appendOptionArgDiagnostic(view *declaredView, option, detail, dql string, offset int, diags *[]*dqlshape.Diagnostic) {
	viewName := ""
	if view != nil {
		viewName = view.Name
	}
	*diags = append(*diags, &dqlshape.Diagnostic{
		Code:     dqldiag.CodeDeclOptionArgs,
		Severity: dqlshape.SeverityWarning,
		Message:  fmt.Sprintf("invalid %s declaration for view %q: %s", option, viewName, detail),
		Hint:     "check option arity and argument formatting",
		Span:     relationSpan(dql, offset),
	})
}

func isAllowedQuerySelector(name string) bool {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "limit", "offset", "page", "fields", "orderby":
		return true
	default:
		return false
	}
}
