package parser

import (
	"strings"

	"github.com/viant/datly/internal/inference"
)

// NormalizeSQLForParse builds a parser-safe SQL copy for translation-time shape discovery.
// It expands constants while preserving template builtins, then rewrites template fragments
// such as predicate builders into concrete SQL tokens so the SQL parser can recover clause boundaries.
func NormalizeSQLForParse(SQL string, state *inference.State) string {
	if state != nil {
		SQL = state.ExpandPreserveBuiltins(SQL)
	}
	return rewritePrivateShorthand(replaceTemplateTokens(SQL))
}

func rewritePrivateShorthand(input string) string {
	var b strings.Builder
	b.Grow(len(input))
	for i := 0; i < len(input); {
		if !hasPrefixFold(input[i:], "private") {
			b.WriteByte(input[i])
			i++
			continue
		}
		if i > 0 && isParseIdentifierPart(input[i-1]) {
			b.WriteByte(input[i])
			i++
			continue
		}
		pos := i + len("private")
		pos = skipParseSpaces(input, pos)
		if pos >= len(input) || input[pos] != '(' {
			b.WriteByte(input[i])
			i++
			continue
		}
		body, closeIdx, ok := readParseCallBody(input, pos)
		if !ok {
			b.WriteByte(input[i])
			i++
			continue
		}
		firstArg, ok := firstCallArg(body)
		if !ok {
			b.WriteByte(input[i])
			i++
			continue
		}
		b.WriteString(strings.TrimSpace(firstArg))
		i = closeIdx + 1
	}
	return b.String()
}

func hasPrefixFold(s, prefix string) bool {
	if len(s) < len(prefix) {
		return false
	}
	return strings.EqualFold(s[:len(prefix)], prefix)
}

func firstCallArg(body string) (string, bool) {
	depth := 0
	quote := byte(0)
	for i := 0; i < len(body); i++ {
		ch := body[i]
		if quote != 0 {
			if ch == '\\' && i+1 < len(body) {
				i++
				continue
			}
			if ch == quote {
				quote = 0
			}
			continue
		}
		if ch == '\'' || ch == '"' {
			quote = ch
			continue
		}
		switch ch {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		case ',':
			if depth == 0 {
				arg := strings.TrimSpace(body[:i])
				return arg, arg != ""
			}
		}
	}
	arg := strings.TrimSpace(body)
	return arg, arg != ""
}

func replaceTemplateTokens(input string) string {
	var b strings.Builder
	b.Grow(len(input))
	for i := 0; i < len(input); {
		if input[i] != '$' {
			b.WriteByte(input[i])
			i++
			continue
		}
		if i+1 < len(input) && input[i+1] == '{' {
			body, end, ok := readParseTemplateExpr(input, i+1)
			if !ok {
				b.WriteByte(input[i])
				i++
				continue
			}
			replacement, keep := normalizeTemplateExprBody(body)
			if keep {
				b.WriteString(input[i : end+1])
			} else {
				b.WriteString(replacement)
			}
			i = end + 1
			continue
		}
		token, end, ok := readParseSelector(input, i)
		if !ok {
			b.WriteByte(input[i])
			i++
			continue
		}
		if strings.EqualFold(token, "$criteria.AppendBinding") {
			pos := skipParseSpaces(input, end)
			if pos < len(input) && input[pos] == '(' {
				_, close, ok := readParseCallBody(input, pos)
				if ok {
					b.WriteByte('1')
					i = close + 1
					continue
				}
			}
		}
		if shouldPreserveParseSelector(token) {
			b.WriteString(token)
			i = end
			continue
		}
		if isParseReservedToken(token) {
			b.WriteString(token)
		} else {
			b.WriteByte('1')
		}
		i = end
	}
	return b.String()
}

func normalizeTemplateExprBody(body string) (string, bool) {
	trimmed := strings.TrimSpace(body)
	if isParseReservedName(trimmed) {
		return "", true
	}
	if selector := normalizeTemplateSelector(trimmed); selector != "" {
		return selector, false
	}
	lower := strings.ToLower(trimmed)
	if strings.Contains(lower, `build("where")`) || strings.Contains(lower, "build('where')") {
		return " WHERE 1 ", false
	}
	if strings.Contains(lower, `build("and")`) || strings.Contains(lower, "build('and')") {
		return " AND 1 ", false
	}
	if strings.Contains(lower, `build("or")`) || strings.Contains(lower, "build('or')") {
		return " OR 1 ", false
	}
	if strings.Contains(lower, `build("having")`) || strings.Contains(lower, "build('having')") {
		return " HAVING 1 ", false
	}
	return "1", false
}

func normalizeTemplateSelector(input string) string {
	if input == "" {
		return ""
	}
	for i := 0; i < len(input); i++ {
		ch := input[i]
		if !(isParseIdentifierPart(ch) || ch == '.') {
			return ""
		}
	}
	parts := strings.Split(input, ".")
	builder := strings.Builder{}
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if builder.Len() > 0 {
			builder.WriteByte('_')
		}
		builder.WriteString(part)
	}
	result := builder.String()
	if result == "" {
		return ""
	}
	if !isParseIdentifierStart(result[0]) {
		return ""
	}
	return result
}

func readParseTemplateExpr(input string, openBrace int) (string, int, bool) {
	if openBrace <= 0 || openBrace >= len(input) || input[openBrace] != '{' || input[openBrace-1] != '$' {
		return "", -1, false
	}
	for i := openBrace + 1; i < len(input); i++ {
		if input[i] == '}' {
			return input[openBrace+1 : i], i, true
		}
	}
	return "", -1, false
}

func readParseSelector(input string, start int) (string, int, bool) {
	if start < 0 || start >= len(input) || input[start] != '$' {
		return "", start, false
	}
	i := start + 1
	if i >= len(input) || !isParseIdentifierStart(input[i]) {
		return "", start, false
	}
	i++
	for i < len(input) && isParseIdentifierPart(input[i]) {
		i++
	}
	for i < len(input) && input[i] == '.' {
		i++
		if i >= len(input) || !isParseIdentifierStart(input[i]) {
			return "", start, false
		}
		i++
		for i < len(input) && isParseIdentifierPart(input[i]) {
			i++
		}
	}
	return input[start:i], i, true
}

func readParseCallBody(input string, openParen int) (string, int, bool) {
	depth := 0
	quote := byte(0)
	for i := openParen; i < len(input); i++ {
		ch := input[i]
		if quote != 0 {
			if ch == '\\' && i+1 < len(input) {
				i++
				continue
			}
			if ch == quote {
				quote = 0
			}
			continue
		}
		if ch == '\'' || ch == '"' {
			quote = ch
			continue
		}
		if ch == '(' {
			depth++
			continue
		}
		if ch == ')' {
			depth--
			if depth == 0 {
				return input[openParen+1 : i], i, true
			}
		}
	}
	return "", -1, false
}

func shouldPreserveParseSelector(token string) bool {
	return strings.HasPrefix(token, "$View.")
}

func isParseReservedToken(token string) bool {
	if len(token) > 0 && token[0] == '$' {
		token = token[1:]
	}
	return isParseReservedName(token)
}

func isParseReservedName(name string) bool {
	return name == "sql.Insert" || name == "sql.Update" || name == "Nop"
}

func skipParseSpaces(input string, index int) int {
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

func isParseIdentifierStart(ch byte) bool {
	return ch == '_' || (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z')
}

func isParseIdentifierPart(ch byte) bool {
	return isParseIdentifierStart(ch) || (ch >= '0' && ch <= '9')
}
