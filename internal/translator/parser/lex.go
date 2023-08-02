package parser

import (
	matchers2 "github.com/viant/datly/internal/translator/parser/matchers"
	"github.com/viant/datly/view/keywords"
	"github.com/viant/parsly"
	"github.com/viant/parsly/matcher"
	"github.com/viant/velty/ast/expr"
	"github.com/viant/velty/parser"
)

const (
	whitespaceToken int = iota
	exprGroupToken
	importKeywordToken
	aliasKeywordToken
	singleQuotedToken
	doubleQuotedToken
	setTerminatedToken
	setToken
	parameterDeclarationToken
	commentToken
	comaTerminatedToken

	typeToken
	dotToken
	selectToken

	execStmtToken
	readStmtToken
	exprToken
	exprEndToken
	anyToken
	wordToken
	scopeBlockToken
	commentBlockToken
	selectorStartToken
	comaTerminatorToken
	parenthesesBlockToken
	endToken
	elseToken
	assignToken
	forEachToken
	ifToken
	numberToken
	boolToken
	stringToken

	selectorToken

	insertToken

	intoToken

	valuesToken
)

var whitespaceMatcher = parsly.NewToken(whitespaceToken, "Whitespace", matcher.NewWhiteSpace())
var exprGroupMatcher = parsly.NewToken(exprGroupToken, "( .... )", matcher.NewBlock('(', ')', '\\'))
var setTerminatedMatcher = parsly.NewToken(setTerminatedToken, "#set", matchers2.NewStringTerminator("#set"))
var setMatcher = parsly.NewToken(setToken, "#set", matcher.NewFragments([]byte("#set")))
var parameterDeclarationMatcher = parsly.NewToken(parameterDeclarationToken, "$_", matcher.NewSpacedSet([]string{"$_ = $"}))
var commentMatcher = parsly.NewToken(commentToken, "/**/", matcher.NewSeqBlock("/*", "*/"))
var typeMatcher = parsly.NewToken(typeToken, "<T>", matcher.NewSeqBlock("<", ">"))
var dotMatcher = parsly.NewToken(dotToken, "call", matcher.NewByte('.'))
var selectMatcher = parsly.NewToken(selectToken, "Function call", matchers2.NewIdentity())

var execStmtMatcher = parsly.NewToken(execStmtToken, "Exec statement", matcher.NewFragmentsFold([]byte("insert"), []byte("update"), []byte("delete"), []byte("call"), []byte("begin")))
var readStmtMatcher = parsly.NewToken(readStmtToken, "Select statement", matcher.NewFragmentsFold([]byte("select")))
var exprMatcher = parsly.NewToken(exprToken, "Expression", matcher.NewFragments([]byte("#set"), []byte("#foreach"), []byte("#if")))
var anyMatcher = parsly.NewToken(anyToken, "Any", matchers2.NewAny())
var exprEndMatcher = parsly.NewToken(exprEndToken, "#end", matcher.NewFragmentsFold([]byte("#end")))

var selectorStartMatcher = parsly.NewToken(selectorStartToken, "Selector start", matcher.NewByte('$'))

var fullWordMatcher = parsly.NewToken(wordToken, "Word", matchers2.NewWordMatcher(true))

var commentBlockMatcher = parsly.NewToken(commentBlockToken, "Comment", matcher.NewSeqBlock("/*", "*/"))

var scopeBlockMatcher = parsly.NewToken(scopeBlockToken, "{ .... }", matcher.NewBlock('{', '}', '\\'))

var comaTerminatorMatcher = parsly.NewToken(comaTerminatorToken, "coma", matcher.NewTerminator(',', true))

var parenthesesBlockMatcher = parsly.NewToken(parenthesesBlockToken, "Parentheses", matcher.NewBlock('(', ')', '\\'))

var endMatcher = parsly.NewToken(endToken, "End", matcher.NewFragment("#end"))
var elseMatcher = parsly.NewToken(elseToken, "Else", matcher.NewFragment("#else"))
var elseIfMatcher = parsly.NewToken(elseToken, "ElseIf", matcher.NewFragment("#elseif"))
var assignMatcher = parsly.NewToken(assignToken, "Set", matcher.NewFragment("#set"))
var forEachMatcher = parsly.NewToken(forEachToken, "ForEach", matcher.NewFragment("#foreach"))
var ifMatcher = parsly.NewToken(ifToken, "If", matcher.NewFragment("#if"))

var numberMatcher = parsly.NewToken(numberToken, "Number", matcher.NewNumber())
var boolMatcher = parsly.NewToken(boolToken, "Boolean", matcher.NewFragmentsFold([]byte("true"), []byte("false")))
var boolTokenMatcher = parsly.NewToken(boolToken, "Boolean", matcher.NewFragments(
	[]byte("&&"), []byte("||"),
))

var singleQuoteStringMatcher = parsly.NewToken(stringToken, "String", matcher.NewBlock('\'', '\'', '\\'))
var doubleQuoteStringMatcher = parsly.NewToken(stringToken, "String", matcher.NewBlock('"', '"', '\\'))
var backtickQuoteStringMatcher = parsly.NewToken(stringToken, "String", matcher.NewBlock('`', '`', '\\'))

var selectorMatcher = parsly.NewToken(selectorToken, "selector", matchers2.NewSelector())

var insertMatcher = parsly.NewToken(insertToken, "insert", matcher.NewFragmentsFold([]byte("insert")))
var intoMatcher = parsly.NewToken(intoToken, "into", matcher.NewFragmentsFold([]byte("into")))
var valuesMatcher = parsly.NewToken(valuesToken, "values", matcher.NewFragmentsFold([]byte("values")))
var importKeywordMatcher = parsly.NewToken(importKeywordToken, "import", matcher.NewFragmentsFold([]byte("import")))

var quotedMatcher = parsly.NewToken(doubleQuotedToken, "quoted block", matcher.NewQuote('"', '\\'))

var aliasKeywordMatcher = parsly.NewToken(aliasKeywordToken, "as", matcher.NewFragmentsFold([]byte("as")))

var singleQuotedMatcher = parsly.NewToken(singleQuotedToken, "single quoted block", matcher.NewQuote('\'', '\''))

var comaTerminatedMatcher = parsly.NewToken(comaTerminatedToken, "arg", matcher.NewTerminator(',', true))

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
