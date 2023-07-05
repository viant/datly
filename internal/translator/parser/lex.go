package parser

import (
	"github.com/viant/datly/cmd/matchers"
	"github.com/viant/datly/view/keywords"
	"github.com/viant/parsly"
	"github.com/viant/parsly/matcher"
	"github.com/viant/velty/ast/expr"
	"github.com/viant/velty/parser"
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
var packageMatcher = parsly.NewToken(packageKeywordToken, "package", matcher.NewFragmentsFold([]byte("package")))
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

func nextWhitespace(cursor *parsly.Cursor) bool {
	beforeMatch := cursor.Pos
	cursor.MatchOne(whitespaceMatcher)
	return beforeMatch != cursor.Pos
}

func getStmtSelector(matched *parsly.TokenMatch, cursor *parsly.Cursor) (*expr.Select, string, bool) {
	text := matched.Text(cursor)
	if text != "$" {
		return nil, "", false
	}

	selector, err := parser.MatchSelector(cursor)
	if err != nil || selector.ID != keywords.KeySQL || selector.X == nil {
		return nil, "", false
	}

	aSelector, ok := selector.X.(*expr.Select)
	if !ok || (aSelector.ID != "Insert" && aSelector.ID != "Update") {
		return nil, "", false
	}

	return aSelector, selector.ID, ok
}
