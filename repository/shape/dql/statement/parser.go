package statement

import (
	"strings"

	"github.com/viant/datly/view/keywords"
	"github.com/viant/parsly"
	"github.com/viant/parsly/matcher"
	aexpr "github.com/viant/velty/ast/expr"
	veltyparser "github.com/viant/velty/parser"
)

const (
	stmtWhitespaceToken = iota
	stmtExprGroupToken
	stmtExecToken
	stmtReadToken
	stmtExprToken
	stmtExprEndToken
	stmtAnyToken
)

var (
	stmtWhitespaceMatcher = parsly.NewToken(stmtWhitespaceToken, "Whitespace", matcher.NewWhiteSpace())
	stmtExprGroupMatcher  = parsly.NewToken(stmtExprGroupToken, "( ... )", matcher.NewBlock('(', ')', '\\'))
	stmtExecMatcher       = parsly.NewToken(stmtExecToken, "Exec", matcher.NewFragmentsFold([]byte("insert"), []byte("update"), []byte("delete"), []byte("call"), []byte("begin")))
	stmtReadMatcher       = parsly.NewToken(stmtReadToken, "Read", matcher.NewFragmentsFold([]byte("select")))
	stmtExprMatcher       = parsly.NewToken(stmtExprToken, "Expression", matcher.NewFragments([]byte("#set"), []byte("#foreach"), []byte("#if")))
	stmtExprEndMatcher    = parsly.NewToken(stmtExprEndToken, "#end", matcher.NewFragmentsFold([]byte("#end")))
	stmtAnyMatcher        = parsly.NewToken(stmtAnyToken, "Any", &anyMatcher{})
)

type anyMatcher struct{}

func (a *anyMatcher) Match(cursor *parsly.Cursor) int {
	if cursor.Pos < cursor.InputSize {
		return 1
	}
	return 0
}

func parseStatements(sqlText string) Statements {
	cursor := parsly.NewCursor("", []byte(sqlText), 0)
	var (
		result  Statements
		current *Statement
	)
	for cursor.Pos < cursor.InputSize {
		if consumeCommentOrQuoted(sqlText, cursor) {
			continue
		}
		if cursor.Input[cursor.Pos] == '(' {
			if block := cursor.MatchOne(stmtExprGroupMatcher); block.Code == stmtExprGroupToken {
				continue
			}
		}
		_ = cursor.MatchOne(stmtWhitespaceMatcher)
		beforeMatch := cursor.Pos
		matched := cursor.MatchAfterOptional(stmtWhitespaceMatcher, stmtExprMatcher, stmtExprEndMatcher, stmtExecMatcher, stmtReadMatcher, stmtAnyMatcher)
		switch matched.Code {
		case stmtExprToken:
			_ = cursor.MatchAfterOptional(stmtWhitespaceMatcher, stmtExprGroupMatcher)
		case stmtExecToken, stmtReadToken:
			isExec := matched.Code == stmtExecToken
			kind := KindRead
			if isExec {
				kind = KindExec
			}
			if nextWhitespace(cursor) {
				if current != nil {
					current.End = beforeMatch
				}
				current = &Statement{
					Start:  beforeMatch,
					End:    -1,
					Kind:   kind,
					IsExec: isExec,
				}
				result = append(result, current)
			}
		case stmtAnyToken:
			kind, method, ok := getStmtSelector(matched, cursor)
			if ok {
				if current != nil {
					current.End = beforeMatch
				}
				current = &Statement{
					Start:          beforeMatch,
					End:            -1,
					IsExec:         true,
					Kind:           kind,
					SelectorMethod: method,
				}
				result = append(result, current)
			}
			if !ok {
				advanceToWhitespace(cursor)
			}
			_ = nextWhitespace(cursor)
		}
	}
	if current != nil {
		current.End = len(sqlText)
	}
	if len(result) == 0 {
		kind, isExec, selector := inferDefaultKind(sqlText)
		result = append(result, &Statement{
			Start:          0,
			End:            len(sqlText),
			Kind:           kind,
			IsExec:         isExec,
			SelectorMethod: selector,
		})
	}
	return result
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
	if offset < 0 || offset+len(candidate) > len(text) {
		return false
	}
	return text[offset:offset+len(candidate)] == candidate
}

func getStmtSelector(matched *parsly.TokenMatch, cursor *parsly.Cursor) (string, string, bool) {
	if matched.Text(cursor) != "$" {
		return "", "", false
	}
	selector, err := veltyparser.MatchSelector(cursor)
	if err != nil || selector == nil {
		return "", "", false
	}
	if strings.EqualFold(selector.ID, "Nop") {
		return KindExec, "Nop", true
	}
	if !strings.EqualFold(selector.ID, keywords.KeySQL) || selector.X == nil {
		return "", "", false
	}
	aSelector, ok := selector.X.(*aexpr.Select)
	if !ok {
		return "", "", false
	}
	if aSelector.ID != "Insert" && aSelector.ID != "Update" {
		return "", "", false
	}
	return KindService, aSelector.ID, true
}

func nextWhitespace(cursor *parsly.Cursor) bool {
	before := cursor.Pos
	_ = cursor.MatchOne(stmtWhitespaceMatcher)
	return before != cursor.Pos
}

func advanceToWhitespace(cursor *parsly.Cursor) {
	for cursor.Pos < cursor.InputSize {
		if matcher.IsWhiteSpace(cursor.Input[cursor.Pos]) {
			return
		}
		cursor.Pos++
	}
}
