package dql

import (
	"strings"

	sqltext "github.com/viant/sqlparser/source"
)

type directiveKind string

const (
	directiveKindUnknown directiveKind = ""
	directiveKindSet     directiveKind = "set"
	directiveKindDefine  directiveKind = "define"
	directiveKindSetting directiveKind = "setting"
)

type directiveBlock struct {
	start     int
	end       int
	bodyStart int
	body      string
	kind      directiveKind
}

func extractDirectiveBlocks(dql string) []directiveBlock {
	var result []directiveBlock
	for pos := 0; pos < len(dql); pos++ {
		start := pos
		kind, keywordLen, ok := matchDirectiveAt(dql, start)
		if !ok {
			continue
		}
		pos = start + keywordLen
		for pos < len(dql) && sqltext.IsWhitespace(dql[pos]) {
			pos++
		}
		groupText, end, ok := sqltext.ReadGroupString(dql, pos, '(', ')')
		if !ok {
			continue
		}
		if len(groupText) < 2 {
			continue
		}
		result = append(result, directiveBlock{
			start:     start,
			end:       end,
			bodyStart: pos + 1,
			body:      groupText[1 : len(groupText)-1],
			kind:      kind,
		})
		pos = end - 1
	}
	return result
}

func matchDirectiveAt(input string, pos int) (directiveKind, int, bool) {
	if pos < 0 || pos >= len(input) || input[pos] != '#' {
		return directiveKindUnknown, 0, false
	}
	remaining := input[pos:]
	switch {
	case hasDirectivePrefix(remaining, "#settings"):
		return directiveKindSetting, len("#settings"), true
	case hasDirectivePrefix(remaining, "#setting"):
		return directiveKindSetting, len("#setting"), true
	case hasDirectivePrefix(remaining, "#define"):
		return directiveKindDefine, len("#define"), true
	case hasDirectivePrefix(remaining, "#set"):
		return directiveKindSet, len("#set"), true
	default:
		return directiveKindUnknown, 0, false
	}
}

func hasDirectivePrefix(input string, directive string) bool {
	if len(input) < len(directive) {
		return false
	}
	if !strings.EqualFold(input[:len(directive)], directive) {
		return false
	}
	if len(input) == len(directive) {
		return true
	}
	switch input[len(directive)] {
	case '(', ' ', '\t', '\r', '\n':
		return true
	default:
		return false
	}
}

func parseDirectiveCall(body string) (string, []string, string, bool) {
	cursor := newByteCursor(body)
	if !consumeAssignmentPrefix(cursor) {
		return "", nil, "", false
	}
	skipWhitespace(cursor)
	if cursor.pos >= len(cursor.input) || cursor.input[cursor.pos] != '$' {
		return "", nil, "", false
	}
	cursor.pos++
	name, ok := readIdentifier(cursor)
	if !ok {
		return "", nil, "", false
	}
	skipWhitespace(cursor)
	groupText, ok := readBracketGroup(cursor, '(', ')')
	if !ok {
		return "", nil, "", false
	}
	return name, sqltext.SplitArgs(groupText), strings.TrimSpace(string(cursor.input[cursor.pos:])), true
}
