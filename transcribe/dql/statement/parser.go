package statement

import (
	"strings"

	"github.com/viant/parsly"
	"github.com/viant/parsly/matcher"
	aexpr "github.com/viant/velty/ast/expr"
	veltyparser "github.com/viant/velty/parser"
)

const (
	whitespaceToken = iota + 1
	groupToken
	execToken
	readToken
	unsupportedToken
	veltyToken
	veltyEndToken
	anyToken
)

var (
	whitespaceMatcher  = parsly.NewToken(whitespaceToken, "whitespace", matcher.NewWhiteSpace())
	groupMatcher       = parsly.NewToken(groupToken, "group", matcher.NewBlock('(', ')', '\\'))
	execMatcher        = parsly.NewToken(execToken, "SQL exec", matcher.NewFragmentsFold([]byte("insert"), []byte("update"), []byte("delete"), []byte("call"), []byte("begin")))
	readMatcher        = parsly.NewToken(readToken, "SQL read", matcher.NewFragmentsFold([]byte("select")))
	unsupportedMatcher = parsly.NewToken(unsupportedToken, "unsupported SQL operation", matcher.NewFragmentsFold(
		[]byte("alter"), []byte("commit"), []byte("create"), []byte("drop"), []byte("grant"),
		[]byte("load"), []byte("merge"), []byte("register"), []byte("revoke"), []byte("rollback"), []byte("truncate"),
	))
	veltyMatcher    = parsly.NewToken(veltyToken, "Velty directive", matcher.NewFragments([]byte("#set"), []byte("#foreach"), []byte("#if")))
	veltyEndMatcher = parsly.NewToken(veltyEndToken, "Velty end", matcher.NewFragmentsFold([]byte("#end")))
	anyTokenMatcher = parsly.NewToken(anyToken, "character", anyMatcher{})
)

type anyMatcher struct{}

func (anyMatcher) Match(cursor *parsly.Cursor) int {
	if cursor.Pos < cursor.InputSize {
		return 1
	}
	return 0
}

// Parse classifies explicitly delimited top-level statements in preprocessed
// DQL. Newlines remain SQL whitespace; only top-level semicolons delimit SQL.
// Offsets are relative to sqlText and map through transcribe.SourceMap.
func Parse(sqlText string) Statements {
	if strings.TrimSpace(sqlText) == "" {
		return nil
	}
	spans := splitStatementSpans(sqlText)
	result := make(Statements, 0, len(spans))
	for _, span := range spans {
		classified := classifyStatement(sqlText[span.start:span.end])
		result = append(result, &Statement{
			Start: span.start, End: span.end,
			SQLStart: span.start + classified.sqlStart, SQLEnd: span.start + classified.sqlEnd,
			TemplateBalanced: classified.templateBalanced,
			OperationStart:   span.start + classified.operationStart,
			Kind:             classified.kind, Operation: classified.operation,
		})
	}
	return result
}

type statementSpan struct {
	start int
	end   int
}

func splitStatementSpans(sqlText string) []statementSpan {
	cursor := parsly.NewCursor("", []byte(sqlText), 0)
	start := 0
	var result []statementSpan
	for cursor.Pos < cursor.InputSize {
		if consumeCommentOrQuoted(sqlText, cursor) {
			continue
		}
		if cursor.Input[cursor.Pos] == '(' {
			if block := cursor.MatchOne(groupMatcher); block.Code == groupToken {
				continue
			}
		}
		if cursor.Input[cursor.Pos] == ';' {
			if span, ok := trimmedSpan(sqlText, start, cursor.Pos); ok {
				result = append(result, span)
			}
			cursor.Pos++
			start = cursor.Pos
			continue
		}
		cursor.Pos++
	}
	if span, ok := trimmedSpan(sqlText, start, len(sqlText)); ok {
		result = append(result, span)
	}
	return result
}

func trimmedSpan(source string, start, end int) (statementSpan, bool) {
	for start < end && matcher.IsWhiteSpace(source[start]) {
		start++
	}
	for end > start && matcher.IsWhiteSpace(source[end-1]) {
		end--
	}
	return statementSpan{start: start, end: end}, start < end && hasStatementContent(source[start:end])
}

func hasStatementContent(source string) bool {
	cursor := parsly.NewCursor("", []byte(source), 0)
	for cursor.Pos < cursor.InputSize {
		_ = cursor.MatchOne(whitespaceMatcher)
		if cursor.Pos >= cursor.InputSize {
			return false
		}
		if startsWithAt(source, cursor.Pos, "--") || startsWithAt(source, cursor.Pos, "/*") {
			_ = consumeCommentOrQuoted(source, cursor)
			continue
		}
		return true
	}
	return false
}

type classification struct {
	kind             Kind
	operation        string
	sqlStart         int
	sqlEnd           int
	templateBalanced bool
	operationStart   int
}

func classifyStatement(source string) classification {
	cursor := parsly.NewCursor("", []byte(source), 0)
	templatePrefix := false
	prefixBlocks := 0
	sqlCandidate := -1
	for cursor.Pos < cursor.InputSize {
		if consumeCommentOrQuoted(source, cursor) {
			continue
		}
		if cursor.Input[cursor.Pos] == '(' {
			if templatePrefix && sqlCandidate < 0 {
				sqlCandidate = cursor.Pos
			}
			if block := cursor.MatchOne(groupMatcher); block.Code == groupToken {
				continue
			}
		}
		matched := cursor.MatchAfterOptional(whitespaceMatcher, veltyMatcher, veltyEndMatcher, execMatcher, readMatcher, unsupportedMatcher, anyTokenMatcher)
		matchedCode := matched.Code
		matchedOffset := matched.Offset
		matchedText := matched.Text(cursor)
		switch matchedCode {
		case veltyToken:
			templatePrefix = true
			if isVeltyBlockStart(matchedText) {
				prefixBlocks++
			}
			_ = cursor.MatchAfterOptional(whitespaceMatcher, groupMatcher)
		case veltyEndToken:
			if prefixBlocks > 0 {
				prefixBlocks--
			}
		case execToken, readToken, unsupportedToken:
			if !isOperationStart(source, matchedOffset) || !consumeOperationBoundary(cursor) {
				continue
			}
			kind := KindUnknown
			switch matchedCode {
			case readToken:
				kind = KindRead
			case execToken:
				kind = KindExec
			}
			sqlStart := 0
			if templatePrefix {
				sqlStart = sqlCandidate
				if sqlStart < 0 {
					sqlStart = matchedOffset
				}
			}
			sqlEnd := len(source)
			templateBalanced := true
			if prefixBlocks > 0 {
				sqlEnd, templateBalanced = templateSuffixStart(source, cursor.Pos, prefixBlocks)
			}
			return classification{
				kind: kind, operation: strings.ToUpper(strings.TrimSpace(matchedText)),
				sqlStart: sqlStart, sqlEnd: sqlEnd, templateBalanced: templateBalanced, operationStart: matchedOffset,
			}
		case anyToken:
			if templatePrefix && sqlCandidate < 0 {
				sqlCandidate = matchedOffset
			}
			if matchedText == "$" {
				templatePrefix = true
			}
			kind, operation, ok := matchVeltyOperation(matchedText, cursor)
			if ok {
				return classification{kind: kind, operation: operation, sqlStart: matchedOffset, sqlEnd: len(source), templateBalanced: true, operationStart: matchedOffset}
			}
			_ = nextWhitespace(cursor)
		}
	}
	return classification{kind: KindUnknown, sqlEnd: len(source), templateBalanced: true}
}

func isVeltyBlockStart(directive string) bool {
	directive = strings.ToLower(strings.TrimSpace(directive))
	return directive == "#if" || directive == "#foreach"
}

// templateSuffixStart finds the first directive that closes a block opened
// before SQL. Nested directives inside SQL remain part of the SQL program.
func templateSuffixStart(source string, start, prefixBlocks int) (int, bool) {
	cursor := parsly.NewCursor("", []byte(source), 0)
	cursor.Pos = start
	depth := prefixBlocks
	suffixStart := -1
	for cursor.Pos < cursor.InputSize {
		if consumeCommentOrQuoted(source, cursor) {
			continue
		}
		matched := cursor.MatchAny(veltyMatcher, veltyEndMatcher, anyTokenMatcher)
		switch matched.Code {
		case veltyToken:
			if isVeltyBlockStart(matched.Text(cursor)) {
				depth++
			}
			_ = cursor.MatchAfterOptional(whitespaceMatcher, groupMatcher)
		case veltyEndToken:
			if depth == 0 {
				return len(source), false
			}
			if suffixStart < 0 && depth == prefixBlocks {
				suffixStart = matched.Offset
			}
			depth--
		}
	}
	if depth != 0 || suffixStart < 0 {
		return len(source), false
	}
	return suffixStart, true
}

func isOperationStart(source string, offset int) bool {
	if offset <= 0 {
		return offset == 0
	}
	previous := source[offset-1]
	return !((previous >= 'a' && previous <= 'z') ||
		(previous >= 'A' && previous <= 'Z') ||
		(previous >= '0' && previous <= '9') ||
		previous == '_' || previous == '$' || previous == '.')
}

func consumeCommentOrQuoted(sqlText string, cursor *parsly.Cursor) bool {
	if cursor.Pos >= cursor.InputSize {
		return false
	}
	if startsWithAt(sqlText, cursor.Pos, "--") {
		cursor.Pos += 2
		for cursor.Pos < cursor.InputSize && sqlText[cursor.Pos] != '\n' {
			cursor.Pos++
		}
		return true
	}
	if startsWithAt(sqlText, cursor.Pos, "/*") {
		cursor.Pos += 2
		for cursor.Pos+1 < cursor.InputSize {
			if sqlText[cursor.Pos] == '*' && sqlText[cursor.Pos+1] == '/' {
				cursor.Pos += 2
				return true
			}
			cursor.Pos++
		}
		cursor.Pos = cursor.InputSize
		return true
	}
	switch sqlText[cursor.Pos] {
	case '\'', '"', '`':
		quote := sqlText[cursor.Pos]
		cursor.Pos++
		for cursor.Pos < cursor.InputSize {
			ch := sqlText[cursor.Pos]
			cursor.Pos++
			if ch == quote && (cursor.Pos < 2 || sqlText[cursor.Pos-2] != '\\') {
				break
			}
		}
		return true
	}
	return false
}

func startsWithAt(text string, offset int, candidate string) bool {
	return offset >= 0 && offset+len(candidate) <= len(text) && text[offset:offset+len(candidate)] == candidate
}

func matchVeltyOperation(matchedText string, cursor *parsly.Cursor) (Kind, string, bool) {
	if matchedText != "$" {
		return KindUnknown, "", false
	}
	selector, err := veltyparser.MatchSelector(cursor)
	if err != nil || selector == nil {
		return KindUnknown, "", false
	}
	if strings.EqualFold(selector.ID, "Nop") {
		return KindExec, "Nop", true
	}
	if !strings.EqualFold(selector.ID, "dml") || selector.X == nil {
		return KindUnknown, "", false
	}
	nested, ok := selector.X.(*aexpr.Select)
	if !ok || !IsDMLServiceMethod(nested.ID) {
		return KindUnknown, "", false
	}
	return KindService, nested.ID, true
}

// IsDMLServiceMethod reports whether name belongs to the Velty DML capability.
func IsDMLServiceMethod(name string) bool {
	switch {
	case strings.EqualFold(name, "Insert"), strings.EqualFold(name, "Update"),
		strings.EqualFold(name, "Delete"), strings.EqualFold(name, "Execute"):
		return true
	default:
		return false
	}
}

func nextWhitespace(cursor *parsly.Cursor) bool {
	before := cursor.Pos
	_ = cursor.MatchOne(whitespaceMatcher)
	return before != cursor.Pos
}

func consumeOperationBoundary(cursor *parsly.Cursor) bool {
	if cursor.Pos >= cursor.InputSize {
		return true
	}
	if !matcher.IsWhiteSpace(cursor.Input[cursor.Pos]) {
		return false
	}
	_ = cursor.MatchOne(whitespaceMatcher)
	return true
}
