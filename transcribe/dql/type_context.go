package dql

import (
	"strings"

	sqltext "github.com/viant/sqlparser/source"
)

func parsePackageLineDirective(line string) (string, bool) {
	args, ok := parseExactHashDirectiveCall(line, "package")
	if !ok || len(args) != 1 {
		return "", false
	}
	value, ok := parseQuotedLiteral(args[0])
	if !ok {
		return "", false
	}
	value = strings.TrimSpace(value)
	if value == "" {
		return "", false
	}
	return value, true
}

func parseImportLineDirective(line string) (string, string, bool) {
	args, ok := parseExactHashDirectiveCall(line, "import")
	if !ok || len(args) != 2 {
		return "", "", false
	}
	alias, ok := parseQuotedLiteral(args[0])
	if !ok {
		return "", "", false
	}
	pkg, ok := parseQuotedLiteral(args[1])
	if !ok {
		return "", "", false
	}
	alias = strings.TrimSpace(alias)
	pkg = strings.TrimSpace(pkg)
	if alias == "" || pkg == "" {
		return "", "", false
	}
	return alias, pkg, true
}

func parseExactHashDirectiveCall(line, directive string) ([]string, bool) {
	input := strings.TrimSpace(line)
	if input == "" || input[0] != '#' {
		return nil, false
	}
	index := skipSpaces(input, 1)
	start := index
	for index < len(input) && isIdentifierPart(input[index]) {
		index++
	}
	if start == index || !strings.EqualFold(input[start:index], directive) {
		return nil, false
	}
	index = skipSpaces(input, index)
	if index >= len(input) || input[index] != '(' {
		return nil, false
	}
	body, end, ok := readCallBody(input, index)
	if !ok {
		return nil, false
	}
	index = skipSpaces(input, end+1)
	if index != len(input) {
		return nil, false
	}
	return splitCallArgs(body), true
}

func parseQuotedLiteral(input string) (string, bool) {
	input = strings.TrimSpace(input)
	if len(input) < 2 {
		return "", false
	}
	if (input[0] == '\'' && input[len(input)-1] == '\'') || (input[0] == '"' && input[len(input)-1] == '"') {
		return input[1 : len(input)-1], true
	}
	return "", false
}

func skipSpaces(input string, index int) int {
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

func readCallBody(input string, pos int) (string, int, bool) {
	if pos >= len(input) || input[pos] != '(' {
		return "", 0, false
	}
	body, end, ok := sqltext.ReadGroupString(input, pos, '(', ')')
	if !ok || len(body) < 2 {
		return "", 0, false
	}
	return body[1 : len(body)-1], end - 1, true
}

func splitCallArgs(input string) []string {
	return sqltext.SplitArgs(input)
}
