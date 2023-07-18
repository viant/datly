package cmd

import (
	"github.com/viant/datly/cmd/matchers"
	"github.com/viant/parsly"
	"github.com/viant/parsly/matcher"
)

const (
	whitespaceToken int = iota
	condBlockToken
	exprGroupToken
	importKeywordToken
	aliasKeywordToken
	packageKeywordToken
	quotedToken
	setTerminatedToken
	setToken
	parameterDeclarationToken
	commentToken
	typeToken
	dotToken
	selectToken

	execStmtToken
	readStmtToken
	exprToken
	exprEndToken
	packageNameToken
	semicolonToken
	anyToken
)

var whitespaceMatcher = parsly.NewToken(whitespaceToken, "Whitespace", matcher.NewWhiteSpace())
var condBlockMatcher = parsly.NewToken(condBlockToken, "#if .... #end", matcher.NewSeqBlock("#if", "#end"))
var exprGroupMatcher = parsly.NewToken(exprGroupToken, "( .... )", matcher.NewBlock('(', ')', '\\'))
var importKeywordMatcher = parsly.NewToken(importKeywordToken, "import", matcher.NewFragmentsFold([]byte("import")))
var aliasKeywordMatcher = parsly.NewToken(aliasKeywordToken, "as", matcher.NewFragmentsFold([]byte("as")))
var quotedMatcher = parsly.NewToken(quotedToken, "quoted block", matcher.NewQuote('"', '\\'))
var setTerminatedMatcher = parsly.NewToken(setTerminatedToken, "#set", matchers.NewStringTerminator("#set"))
var setMatcher = parsly.NewToken(setToken, "#set", matcher.NewFragments([]byte("#set")))
var parameterDeclarationMatcher = parsly.NewToken(parameterDeclarationToken, "$_", matcher.NewSpacedSet([]string{"$_ = $"}))
var commentMatcher = parsly.NewToken(commentToken, "/**/", matcher.NewSeqBlock("/*", "*/"))
var typeMatcher = parsly.NewToken(typeToken, "<T>", matcher.NewSeqBlock("<", ">"))
var dotMatcher = parsly.NewToken(dotToken, "call", matcher.NewByte('.'))
var selectMatcher = parsly.NewToken(selectToken, "Function call", matchers.NewIdentity())

var execStmtMatcher = parsly.NewToken(execStmtToken, "Exec statement", matcher.NewFragmentsFold([]byte("insert"), []byte("update"), []byte("delete"), []byte("call"), []byte("begin")))
var readStmtMatcher = parsly.NewToken(readStmtToken, "Select statement", matcher.NewFragmentsFold([]byte("select")))
var exprMatcher = parsly.NewToken(exprToken, "Expression", matcher.NewFragments([]byte("#set"), []byte("#foreach"), []byte("#if")))
var anyMatcher = parsly.NewToken(anyToken, "Any", matchers.NewAny())
var exprEndMatcher = parsly.NewToken(exprEndToken, "#end", matcher.NewFragmentsFold([]byte("#end")))
