package pipeline

// read_normalize.go — SQL normalization and template-token replacement used
// by BuildRead to produce parser-friendly SQL from raw DQL.

import "strings"

// normalizeParserSQL rewrites private(…) shorthands and template tokens into
// plain SQL that the parser can handle.
func normalizeParserSQL(sqlText string) string {
	if sqlText == "" {
		return sqlText
	}
	return rewritePrivateShorthand(replaceTemplateTokens(sqlText))
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
		if i > 0 && isReadIdentifierPart(input[i-1]) {
			b.WriteByte(input[i])
			i++
			continue
		}
		pos := i + len("private")
		pos = skipReadSpaces(input, pos)
		if pos >= len(input) || input[pos] != '(' {
			b.WriteByte(input[i])
			i++
			continue
		}
		body, closeIdx, ok := readReadCallBody(input, pos)
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
			body, end, ok := readReadTemplateExpr(input, i+1)
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
		token, end, ok := readReadSelector(input, i)
		if !ok {
			b.WriteByte(input[i])
			i++
			continue
		}
		if strings.EqualFold(token, "$criteria.AppendBinding") {
			pos := skipReadSpaces(input, end)
			if pos < len(input) && input[pos] == '(' {
				_, close, ok := readReadCallBody(input, pos)
				if ok {
					b.WriteByte('1')
					i = close + 1
					continue
				}
			}
		}
		if isReadReservedToken(token) {
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
	if isReadReservedName(trimmed) {
		return "", true
	}
	lower := strings.ToLower(trimmed)
	if strings.Contains(lower, `build("where")`) || strings.Contains(lower, "build('where')") {
		return " WHERE 1 ", false
	}
	if strings.Contains(lower, `build("and")`) || strings.Contains(lower, "build('and')") {
		return " AND 1 ", false
	}
	return "1", false
}

func readReadTemplateExpr(input string, openBrace int) (string, int, bool) {
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

func readReadSelector(input string, start int) (string, int, bool) {
	if start < 0 || start >= len(input) || input[start] != '$' {
		return "", start, false
	}
	i := start + 1
	if i >= len(input) || !isReadIdentifierStart(input[i]) {
		return "", start, false
	}
	i++
	for i < len(input) && isReadIdentifierPart(input[i]) {
		i++
	}
	for i < len(input) && input[i] == '.' {
		i++
		if i >= len(input) || !isReadIdentifierStart(input[i]) {
			return "", start, false
		}
		i++
		for i < len(input) && isReadIdentifierPart(input[i]) {
			i++
		}
	}
	return input[start:i], i, true
}

func readReadCallBody(input string, openParen int) (string, int, bool) {
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

func isReadReservedToken(token string) bool {
	if len(token) > 0 && token[0] == '$' {
		token = token[1:]
	}
	return isReadReservedName(token)
}

func isReadReservedName(name string) bool {
	return name == "sql.Insert" || name == "sql.Update" || name == "Nop"
}

func skipReadSpaces(input string, index int) int {
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

func isReadIdentifierStart(ch byte) bool {
	return ch == '_' || (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z')
}

func isReadIdentifierPart(ch byte) bool {
	return isReadIdentifierStart(ch) || (ch >= '0' && ch <= '9')
}
