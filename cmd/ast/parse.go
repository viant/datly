package ast

import (
	"bytes"
	"fmt"
	"github.com/viant/datly/cmd/option"
	"github.com/viant/datly/sanitizer"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/keywords"
	"github.com/viant/parsly"
	"github.com/viant/velty/ast"
	"github.com/viant/velty/ast/expr"
	"github.com/viant/velty/ast/stmt"
	"github.com/viant/velty/parser"
	"reflect"
	"strconv"
	"strings"
)

type paramTypeDetector struct {
	uriParams  map[string]bool
	paramTypes map[string]string
	variables  map[string]bool
	viewMeta   *option.ViewMeta
}

func newParamTypeDetector(route *option.Route, meta *option.ViewMeta) *paramTypeDetector {
	uriParams := map[string]bool{}
	paramTypes := map[string]string{}
	if route != nil {
		if route.URIParams != nil {
			uriParams = route.URIParams
		}

		if route.Declare != nil {
			paramTypes = route.Declare
		}
	}

	return &paramTypeDetector{
		uriParams:  uriParams,
		paramTypes: paramTypes,
		variables:  map[string]bool{},
		viewMeta:   meta,
	}
}

func Parse(SQL string, route *option.Route, hints option.ParameterHints) (*option.ViewMeta, error) {
	viewMeta := option.NewViewMeta()
	iterator := sanitizer.NewIterator(SQL, hints)
	SQL = iterator.SQL

	block, err := parser.Parse([]byte(SQL))
	if err != nil {
		return nil, err
	}

	from := []byte(SQL)
	cursor := parsly.NewCursor("", from, 0)
	if err := addTemplateIfNeeded(cursor, viewMeta); err != nil {
		return nil, err
	}

	cursor.MatchOne(whitespaceMatcher)

	for i := 0; i < cursor.Pos; i++ {
		cursor.Input[i] = ' '
	}

	actualSourceStart := cursor.Pos

	detector := newParamTypeDetector(route, viewMeta)
	detector.implyDefaultParams(block.Statements(), true, nil, false)
	viewMeta.SetVariables(detector.variables)

	if err = detector.correctUntyped(iterator, viewMeta, route); err != nil {
		return nil, err
	}

	if IsSQLExecMode(SQL) {
		viewMeta.Mode = view.SQLExecMode
		var err error
		err = buildViewMetaInExecSQLMode(SQL, viewMeta, detector.variables)
		if err != nil {
			fmt.Printf("error while build ExecSQL: %v", err)
		}
	}

	viewMeta.Source = sanitizer.Sanitize(SQL[actualSourceStart:])
	return viewMeta, nil
}

func IsSQLExecMode(SQL string) bool {
	lcSQL := strings.ToLower(SQL)
	return strings.Contains(lcSQL, "call") ||
		(strings.Contains(lcSQL, "begin") && strings.Contains(lcSQL, "end")) ||
		isUpdate(lcSQL) ||
		isDelete(lcSQL) ||
		isInsert(lcSQL)
}

func isDelete(lcSQL string) bool {
	return strings.Contains(lcSQL, "delete ") && strings.Contains(lcSQL, "from ")
}

func isUpdate(lcSQL string) bool {
	return strings.Contains(lcSQL, "update ") && strings.Contains(lcSQL, "set ")
}

func isInsert(lcSQL string) bool {
	return strings.Contains(lcSQL, "insert ") && strings.Contains(lcSQL, "into ") && strings.Contains(lcSQL, "values")
}

func (d *paramTypeDetector) implyDefaultParams(statements []ast.Statement, required bool, rType reflect.Type, multi bool) {
	for _, statement := range statements {
		switch actual := statement.(type) {
		case stmt.ForEach:
			d.variables[actual.Item.ID] = true
		case stmt.Statement:
			d.indexStmt(&actual, required, rType, multi)
		case *expr.Select:
			d.indexParameter(actual, required, rType, multi)
		case *stmt.Statement:
			d.indexStmt(actual, required, rType, multi)
		case *stmt.ForEach:
			d.variables[actual.Item.ID] = true
			set, ok := actual.Set.(*expr.Select)
			if ok && !d.variables[set.ID] {
				d.implyDefaultParams([]ast.Statement{set}, false, rType, true)
			}

		case *expr.Unary:
			d.implyDefaultParams([]ast.Statement{actual}, false, actual.Type(), false)
		case *expr.Binary:
			xType := actual.X.Type()
			if xType == nil {
				xType = actual.Y.Type()
			}

			d.implyDefaultParams([]ast.Statement{actual.X, actual.Y}, false, xType, false)
		case *expr.Parentheses:
			d.implyDefaultParams([]ast.Statement{actual.P}, false, actual.Type(), false)
		case *stmt.If:
			d.implyDefaultParams([]ast.Statement{actual.Condition}, false, actual.Type(), false)
		}

		switch actual := statement.(type) {
		case ast.StatementContainer:
			d.implyDefaultParams(actual.Statements(), false, nil, false)
		}
	}
}

func (d *paramTypeDetector) indexStmt(actual *stmt.Statement, required bool, rType reflect.Type, multi bool) {
	x, ok := actual.X.(*expr.Select)
	if ok {
		d.variables[x.ID] = true
	}

	y, ok := actual.Y.(*expr.Select)
	if ok && !d.variables[y.ID] {
		d.indexParameter(y, required, rType, multi)
	}
}

func (d *paramTypeDetector) indexParameter(actual *expr.Select, required bool, rType reflect.Type, multi bool) {
	prefix, paramName := getHolderName(actual.FullName)

	if !isParameter(d.variables, paramName) {
		return
	}

	pType := "string"
	assumed := true

	if declared, ok := d.paramTypes[paramName]; ok {
		pType = declared
		assumed = false
	}

	if rType != nil && prefix != keywords.ParamsMetadataKey {
		pType = rType.String()
		assumed = false
	}

	kind := "query"
	if d.uriParams[paramName] {
		kind = string(view.PathKind)
	}

	d.viewMeta.AddParameter(&option.Parameter{
		Assumed:  assumed,
		Id:       paramName,
		Name:     paramName,
		Kind:     kind,
		DataType: pType,
		FullName: actual.FullName,
		Multi:    multi,
		Required: option.BoolPtr(required && prefix != keywords.ParamsMetadataKey),
	})
}

func getHolderName(identifier string) (string, string) {
	paramName := paramId(identifier)
	prefix, paramName := removePrefixIfNeeded(paramName)
	paramName = withoutPath(paramName)
	return prefix, paramName
}

func isParameter(variables map[string]bool, paramName string) bool {
	if isVariable := variables[paramName]; isVariable {
		return false
	}

	switch paramName {
	case keywords.Criteria[1:], keywords.SelectorCriteria[1:], keywords.Pagination[1:], keywords.ColumnsIn[1:]:
		return false
	}

	return true
}

func paramId(identifier string) string {
	paramName := identifier[1:]
	if paramName[0] == '{' {
		paramName = paramName[1 : len(paramName)-1]
	}
	return paramName
}

func withoutPath(name string) string {
	if index := strings.Index(name, "."); index != -1 {
		return name[:index]
	}

	return name
}

func removePrefixIfNeeded(name string) (prefix string, actual string) {
	prefixes := []string{
		keywords.AndPrefix, keywords.WherePrefix, keywords.OrPrefix,
		keywords.ParamsKey + ".", keywords.ParamsMetadataKey + ".",
	}
	for _, prefix := range prefixes {
		if strings.HasPrefix(name, prefix) {
			return prefix[:len(prefix)-1], name[len(prefix):]
		}
	}

	return "", name
}

func addTemplateIfNeeded(cursor *parsly.Cursor, meta *option.ViewMeta) error {
	matched := cursor.MatchAfterOptional(whitespaceMatcher, templateHeaderMatcher)
	switch matched.Code {
	case parsly.Invalid, parsly.EOF:
		return nil
	}

	for {
		var parameter *option.Parameter
		matched = cursor.MatchAfterOptional(whitespaceMatcher, paramMatcher, templateEndMatcher)
		switch matched.Code {
		case paramToken:
			parameter = &option.Parameter{}
		case templateEndToken:
			return nil
		default:
			return cursor.NewError(paramMatcher)
		}

		matched = cursor.MatchAfterOptional(whitespaceMatcher, identityMatcher)
		switch matched.Code {
		case identityToken:
			parameter.Id = matched.Text(cursor)
		default:
			return cursor.NewError(paramMatcher)
		}

		matched = cursor.MatchAfterOptional(whitespaceMatcher, squareBracketsMatcher)
		switch matched.Code {
		case squareBracketsToken:
			blockContent := matched.Bytes(cursor)
			bracketsCursor := parsly.NewCursor("", blockContent[1:len(blockContent)-1], 0)
			if err := addParamLocation(bracketsCursor, parameter); err != nil {
				return err
			}
		default:
			return cursor.NewError(paramMatcher)
		}
		meta.AddParameter(parameter)
	}
}

func addParamLocation(cursor *parsly.Cursor, parameter *option.Parameter) error {
	i := 0
	var matched *parsly.TokenMatch
	for {
		if i != 0 {
			matched = cursor.MatchAfterOptional(whitespaceMatcher, colonMatcher)
			switch matched.Code {
			case parsly.EOF:
				return nil
			case parsly.Invalid:
				return cursor.NewError(colonMatcher)
			}
		}

		matched = cursor.MatchAfterOptional(whitespaceMatcher, wordMatcher)
		switch matched.Code {
		case parsly.EOF:
			return nil
		case parsly.Invalid:
			return cursor.NewError(wordMatcher)
		}

		value := matched.Text(cursor)
		switch i {
		case 0:
			parameter.Kind = value
		case 1:
			parameter.Name = value
		case 2:
			parameter.DataType = value
		case 3:
			asBool, err := strconv.ParseBool(value)
			if err != nil {
				return err
			}
			parameter.Required = option.BoolPtr(asBool)
		}
		i++
	}
}

func ExtractCondBlock(SQL string) (string, []string) {
	builder := new(bytes.Buffer)
	var expressions []string
	cursor := parsly.NewCursor("", []byte(SQL), 0)
outer:
	for i := 0; i < len(cursor.Input); i++ {
		match := cursor.MatchOne(condBlockMatcher)
		switch match.Code {
		case parsly.EOF:
			break outer
		case condBlockToken:

			block := match.Text(cursor)[3:]
			cur := parsly.NewCursor("", []byte(block), 0)
			match = cur.MatchAfterOptional(whitespaceMatcher, exprGroupMatcher)
			if match.Code == exprGroupToken {
				matched := string(cur.Input[cur.Pos:])
				if index := strings.Index(matched, "#"); index != -1 {
					expression := strings.TrimSpace(matched[:index])
					expressions = append(expressions, expression)
					if strings.Contains(expression, "=") {
						builder.WriteString(expression)
					}
				}
			}

		default:
			builder.WriteByte(cursor.Input[cursor.Pos])
			cursor.Pos++
		}
	}
	return builder.String(), expressions
}

// ParseURIParams extract URI params from URI
func ParseURIParams(URI string) []string {
	var params []string
	cursor := parsly.NewCursor("", []byte(URI), 0)
outer:
	for i := 0; i < len(cursor.Input); i++ {

		match := cursor.MatchOne(scopeBlockMatcher)
		switch match.Code {
		case parsly.EOF:
			break outer
		case scopeBlockToken:
			block := match.Text(cursor)
			params = append(params, block[1:len(block)-1])
		default:
			cursor.Pos++
		}
	}
	return params
}
