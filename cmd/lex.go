package cmd

import (
	"bytes"
	"github.com/viant/datly/cmd/matchers"
	"github.com/viant/parsly"
	"github.com/viant/parsly/matcher"
)

const (
	whitespaceToken int = iota
	condBlockToken
	exprGroupToken
	importKeywordToken
	quotedToken
	setTerminatedToken
	setToken
	artificialToken
	commentToken
	typeToken
	dotToken
	selectToken
)

var whitespaceMatcher = parsly.NewToken(whitespaceToken, "Whitespace", matcher.NewWhiteSpace())
var condBlockMatcher = parsly.NewToken(condBlockToken, "#if .... #end", matcher.NewSeqBlock("#if", "#end"))
var exprGroupMatcher = parsly.NewToken(exprGroupToken, "( .... )", matcher.NewBlock('(', ')', '\\'))
var importKeywordMatcher = parsly.NewToken(importKeywordToken, "import", matcher.NewFragmentsFold([]byte("import")))
var quotedMatcher = parsly.NewToken(quotedToken, "quoted block", matcher.NewQuote('"', '\\'))
var setTerminatedMatcher = parsly.NewToken(setTerminatedToken, "#set", newStringTerminator("#set"))
var setMatcher = parsly.NewToken(setToken, "#set", matcher.NewFragments([]byte("#set")))
var artificialMatcher = parsly.NewToken(artificialToken, "$_", matcher.NewSpacedSet([]string{"$_ = $"}))
var commentMatcher = parsly.NewToken(commentToken, "/**/", matcher.NewSeqBlock("/*", "*/"))
var typeMatcher = parsly.NewToken(typeToken, "<T>", matcher.NewSeqBlock("<", ">"))
var dotMatcher = parsly.NewToken(dotToken, "call", matcher.NewByte('.'))
var selectMatcher = parsly.NewToken(selectToken, "Function call", matchers.NewIdentity())

type stringTerminatorMatcher struct {
	value []byte
}

func (t *stringTerminatorMatcher) Match(cursor *parsly.Cursor) (matched int) {
	if len(t.value) >= cursor.InputSize-cursor.Pos {
		return 0
	}

	for i := cursor.Pos; i < cursor.InputSize-len(t.value); i++ {
		if bytes.Equal(cursor.Input[i:i+len(t.value)], t.value) {
			return matched
		}

		matched++
	}

	return 0
}

func newStringTerminator(by string) *stringTerminatorMatcher {
	return &stringTerminatorMatcher{value: []byte(by)}
}
