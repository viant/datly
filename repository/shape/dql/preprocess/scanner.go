package preprocess

import (
	"strings"

	"github.com/viant/parsly"
	"github.com/viant/parsly/matcher"
)

var (
	ppWhitespaceToken = 1
	ppExprGroupToken  = 2

	ppWhitespaceMatcher = parsly.NewToken(ppWhitespaceToken, "Whitespace", matcher.NewWhiteSpace())
	ppExprGroupMatcher  = parsly.NewToken(ppExprGroupToken, "( ... )", matcher.NewBlock('(', ')', '\\'))
)

type setDirectiveBlock struct {
	start int
	end   int
	body  string
	kind  directiveKind
}

type directiveKind int

const (
	directiveUnknown directiveKind = iota
	directiveSet
	directiveDefine
	directiveSettings
)

func isDirectiveLine(line string) bool {
	if line == "" {
		return false
	}
	if isTypeContextDirectiveLine(line) {
		return true
	}
	if isSetLine(line) {
		return true
	}
	if strings.HasPrefix(line, "#if(") || strings.HasPrefix(line, "#elseif(") || strings.HasPrefix(line, "#else") || strings.HasPrefix(line, "#end") {
		return true
	}
	return false
}

func isSetLine(line string) bool {
	if line == "" {
		return false
	}
	return lineDirectiveKind(line) != directiveUnknown
}

func extractSetDirectiveBlocks(dql string) []setDirectiveBlock {
	cursor := parsly.NewCursor("", []byte(dql), 0)
	var result []setDirectiveBlock
	for cursor.Pos < cursor.InputSize {
		start := cursor.Pos
		kind, keywordLen, ok := matchDirectiveAt(dql, start)
		if !ok {
			cursor.Pos++
			continue
		}
		cursor.Pos += keywordLen
		group := cursor.MatchAfterOptional(ppWhitespaceMatcher, ppExprGroupMatcher)
		if group.Code != ppExprGroupToken {
			cursor.Pos = start + 1
			continue
		}
		groupText := group.Text(cursor)
		if len(groupText) < 2 {
			continue
		}
		end := cursor.Pos
		result = append(result, setDirectiveBlock{
			start: start,
			end:   end,
			body:  groupText[1 : len(groupText)-1],
			kind:  kind,
		})
	}
	return result
}

func lineDirectiveKind(line string) directiveKind {
	if line == "" {
		return directiveUnknown
	}
	switch {
	case strings.HasPrefix(line, "#settings("), strings.HasPrefix(line, "#settings ("):
		return directiveSettings
	case strings.HasPrefix(line, "#setting("), strings.HasPrefix(line, "#setting ("):
		return directiveSettings
	case strings.HasPrefix(line, "#define("), strings.HasPrefix(line, "#define ("):
		return directiveDefine
	case strings.HasPrefix(line, "#set("), strings.HasPrefix(line, "#set ("):
		return directiveSet
	default:
		return directiveUnknown
	}
}

func matchDirectiveAt(dql string, pos int) (directiveKind, int, bool) {
	if pos < 0 || pos >= len(dql) || dql[pos] != '#' {
		return directiveUnknown, 0, false
	}
	remaining := dql[pos:]
	switch {
	case hasDirectivePrefix(remaining, "#settings"):
		return directiveSettings, len("#settings"), true
	case hasDirectivePrefix(remaining, "#setting"):
		return directiveSettings, len("#setting"), true
	case hasDirectivePrefix(remaining, "#define"):
		return directiveDefine, len("#define"), true
	case hasDirectivePrefix(remaining, "#set"):
		return directiveSet, len("#set"), true
	default:
		return directiveUnknown, 0, false
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
	next := input[len(directive)]
	return next == '(' || next == ' ' || next == '\t' || next == '\r' || next == '\n'
}
