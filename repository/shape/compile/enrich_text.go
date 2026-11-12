package compile

// enrich_text.go — low-level text/SQL scanning primitives used by the
// enrichment phase (enrich.go and enrich_table.go).

import "strings"

// findSummaryJoinBody locates the body of a JOIN(...) SUMMARY ON 1=1 clause.
func findSummaryJoinBody(input string) (string, bool) {
	lower := strings.ToLower(input)
	for i := 0; i < len(input); i++ {
		if !hasCompileWordAt(lower, i, "join") {
			continue
		}
		pos := skipCompileSpaces(input, i+len("join"))
		if pos >= len(input) || input[pos] != '(' {
			continue
		}
		body, end, ok := readCompileParenBody(input, pos)
		if !ok {
			continue
		}
		rest := strings.ToLower(input[end+1:])
		rest = strings.Join(strings.Fields(rest), " ")
		if strings.HasPrefix(rest, "summary on 1=1") || strings.HasPrefix(rest, "summary on 1 = 1") {
			return body, true
		}
	}
	return "", false
}

// extractLeadingRuleHeaderJSON returns the JSON body of a leading /* {...} */ comment.
func extractLeadingRuleHeaderJSON(input string) (string, bool) {
	index := skipCompileSpaces(input, 0)
	if index+2 > len(input) || input[index:index+2] != "/*" {
		return "", false
	}
	end := strings.Index(input[index+2:], "*/")
	if end < 0 {
		return "", false
	}
	body := strings.TrimSpace(input[index+2 : index+2+end])
	if body == "" || body[0] != '{' || body[len(body)-1] != '}' {
		return "", false
	}
	return body, true
}

// findFirstEmbedRef returns the path after "embed:" in the first ${embed:…}
// template expression found in input.
func findFirstEmbedRef(input string) (string, bool) {
	for i := 0; i < len(input); i++ {
		if input[i] != '$' || i+1 >= len(input) || input[i+1] != '{' {
			continue
		}
		body, end, ok := readCompileTemplateExpr(input, i+1)
		if !ok {
			continue
		}
		_ = end
		trimmed := strings.TrimSpace(body)
		if len(trimmed) < len("embed:") || !strings.HasPrefix(strings.ToLower(trimmed), "embed:") {
			continue
		}
		ref := strings.TrimSpace(trimmed[len("embed:"):])
		if ref == "" {
			continue
		}
		return ref, true
	}
	return "", false
}

// joinSubquery holds the body and alias of a JOIN(...) AS alias clause.
type joinSubquery struct {
	body  string
	alias string
}

// scanJoinSubqueries collects all JOIN(body) alias pairs from input.
func scanJoinSubqueries(input string) []joinSubquery {
	result := make([]joinSubquery, 0)
	lower := strings.ToLower(input)
	for i := 0; i < len(input); i++ {
		if !hasCompileWordAt(lower, i, "join") {
			continue
		}
		pos := skipCompileSpaces(input, i+len("join"))
		if pos >= len(input) || input[pos] != '(' {
			continue
		}
		body, end, ok := readCompileParenBody(input, pos)
		if !ok {
			continue
		}
		pos = skipCompileSpaces(input, end+1)
		if hasCompileWordAt(lower, pos, "as") {
			pos = skipCompileSpaces(input, pos+len("as"))
		}
		aliasStart := pos
		if aliasStart >= len(input) || !isCompileWordStart(input[aliasStart]) {
			i = end
			continue
		}
		pos++
		for pos < len(input) && isCompileWordPart(input[pos]) {
			pos++
		}
		alias := strings.TrimSpace(input[aliasStart:pos])
		if alias != "" {
			result = append(result, joinSubquery{body: body, alias: alias})
		}
		i = end
	}
	return result
}

// parseJoinEmbedRef returns the embed path from a body of the form ${embed:path}.
func parseJoinEmbedRef(body string) (string, bool) {
	trimmed := strings.TrimSpace(body)
	if !strings.HasPrefix(trimmed, "${") || !strings.HasSuffix(trimmed, "}") {
		return "", false
	}
	inner := strings.TrimSpace(trimmed[2 : len(trimmed)-1])
	if len(inner) < len("embed:") || !strings.HasPrefix(strings.ToLower(inner), "embed:") {
		return "", false
	}
	ref := strings.TrimSpace(inner[len("embed:"):])
	return ref, ref != ""
}

func readCompileTemplateExpr(input string, openBrace int) (string, int, bool) {
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

func readCompileParenBody(input string, openParen int) (string, int, bool) {
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

func hasCompileWordAt(lower string, pos int, word string) bool {
	if pos < 0 || pos+len(word) > len(lower) {
		return false
	}
	if lower[pos:pos+len(word)] != word {
		return false
	}
	if pos > 0 && isCompileWordPart(lower[pos-1]) {
		return false
	}
	next := pos + len(word)
	if next < len(lower) && isCompileWordPart(lower[next]) {
		return false
	}
	return true
}

func skipCompileSpaces(input string, index int) int {
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

func isCompileWordStart(ch byte) bool {
	return ch == '_' || (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z')
}

func isCompileWordPart(ch byte) bool {
	return isCompileWordStart(ch) || (ch >= '0' && ch <= '9')
}
