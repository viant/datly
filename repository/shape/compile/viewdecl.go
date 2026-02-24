package compile

import (
	"fmt"
	"strings"

	"github.com/viant/datly/repository/shape/compile/pipeline"
	dqldiag "github.com/viant/datly/repository/shape/dql/diag"
	dqlshape "github.com/viant/datly/repository/shape/dql/shape"
	"github.com/viant/parsly"
	"github.com/viant/parsly/matcher"
)

type declaredView struct {
	Name          string
	SQL           string
	URI           string
	Connector     string
	Cardinality   string
	Tag           string
	Codec         string
	CodecArgs     []string
	HandlerName   string
	HandlerArgs   []string
	StatusCode    *int
	ErrorMessage  string
	QuerySelector string
	CacheRef      string
	Limit         *int
	Cacheable     *bool
	When          string
	Scope         string
	DataType      string
	Of            string
	Value         string
	Async         bool
	Output        bool
	Predicates    []declaredPredicate
}

type declaredPredicate struct {
	Name      string
	Source    string
	Ensure    bool
	Arguments []string
}

const (
	vdWhitespaceToken = iota
	vdSetToken
	vdDefineToken
	vdExprGroupToken
	vdCommentToken
	vdParamDeclToken
	vdTypeToken
	vdDotToken
)

var (
	vdWhitespaceMatcher = parsly.NewToken(vdWhitespaceToken, "Whitespace", matcher.NewWhiteSpace())
	vdSetMatcher        = parsly.NewToken(vdSetToken, "#set", matcher.NewFragment("#set"))
	vdDefineMatcher     = parsly.NewToken(vdDefineToken, "#define", matcher.NewFragment("#define"))
	vdExprGroupMatcher  = parsly.NewToken(vdExprGroupToken, "( ... )", matcher.NewBlock('(', ')', '\\'))
	vdCommentMatcher    = parsly.NewToken(vdCommentToken, "Comment", matcher.NewSeqBlock("/*", "*/"))
	vdParamDeclMatcher  = parsly.NewToken(vdParamDeclToken, "$_ = $", matcher.NewSpacedSet([]string{"$_ = $"}))
	vdTypeMatcher       = parsly.NewToken(vdTypeToken, "< ... >", matcher.NewSeqBlock("<", ">"))
	vdDotMatcher        = parsly.NewToken(vdDotToken, ".", matcher.NewByte('.'))
)

func extractDeclaredViews(dql string) ([]*declaredView, []*dqlshape.Diagnostic) {
	if strings.TrimSpace(dql) == "" {
		return nil, nil
	}
	var views []*declaredView
	var diags []*dqlshape.Diagnostic
	for _, block := range extractSetBlocks(dql) {
		holder, kind, location, tail, ok := parseSetDeclarationBody(block.Body)
		if !ok {
			continue
		}
		if kind != "view" && kind != "data_view" {
			continue
		}
		sqlText := extractDeclarationSQL(tail)
		if sqlText == "" {
			diags = append(diags, &dqlshape.Diagnostic{
				Code:     dqldiag.CodeViewMissingSQL,
				Severity: dqlshape.SeverityWarning,
				Message:  fmt.Sprintf("view declaration %q has no inline SQL hint", location),
				Hint:     "use /* SELECT ... */ in declaration comment to derive an additional view",
				Span:     relationSpan(dql, block.Offset),
			})
			continue
		}
		name := pipeline.SanitizeName(location)
		if name == "" {
			name = pipeline.SanitizeName(holder)
		}
		if name == "" {
			continue
		}
		view := &declaredView{Name: name, SQL: strings.TrimSpace(sqlText)}
		applyDeclaredViewOptions(view, tail, dql, block.Offset, &diags)
		views = append(views, view)
	}
	return views, diags
}
