package ast

import (
	"bytes"
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

func Parse(SQL string, uriParams map[string]bool) (*ViewMeta, error) {
	if uriParams == nil {
		uriParams = map[string]bool{}
	}

	block, err := parser.Parse([]byte(SQL))
	if err != nil {
		return nil, err
	}

	viewMeta := &ViewMeta{
		index: map[string]int{},
	}

	from := []byte(SQL)
	cursor := parsly.NewCursor("", from, 0)
	if err := addTemplateIfNeeded(cursor, viewMeta); err != nil {
		return nil, err
	}

	cursor.MatchOne(whitespaceMatcher)
	actualSource := string(cursor.Input[cursor.Pos:])

	for i := 0; i < cursor.Pos; i++ {
		cursor.Input[i] = ' '
	}

	variables := map[string]bool{}
	implyDefaultParams(variables, block.Statements(), viewMeta, true, nil, uriParams)

	SQL = removeVeltySyntax(string(from))
	correctUntyped(SQL, variables, viewMeta)

	viewMeta.Source = actualSource
	return viewMeta, nil
}

func removeVeltySyntax(SQL string) string {
	cursor := parsly.NewCursor("", []byte(SQL), 0)
	sb := strings.Builder{}

outer:
	for cursor.Pos < cursor.InputSize {
		matched := cursor.MatchAny(whitespaceMatcher, ifMatcher, assignMatcher, elseIfMatcher, elseMatcher, forEachMatcher, endMatcher, anyMatcher)
		switch matched.Code {
		case endToken, elseToken:
			continue outer
		case elseIfToken, assignToken, forEachToken, ifToken:
			cursor.MatchOne(exprGroupMatcher)
			continue outer
		}

		sb.WriteString(matched.Text(cursor))
	}

	return sb.String()
}

func implyDefaultParams(variables map[string]bool, statements []ast.Statement, meta *ViewMeta, required bool, rType reflect.Type, uriParams map[string]bool) {
	for _, statement := range statements {
		switch actual := statement.(type) {
		case stmt.ForEach:
			variables[actual.Item.ID] = true
		case stmt.Statement:
			x, ok := actual.X.(*expr.Select)
			if ok {
				variables[x.ID] = true
			}

			y, ok := actual.Y.(*expr.Select)
			if ok && !variables[y.ID] {
				indexParameter(variables, y, meta, required, rType, uriParams)
			}

		case *expr.Select:
			indexParameter(variables, actual, meta, required, rType, uriParams)

		case *stmt.Statement:
			x, ok := actual.X.(*expr.Select)
			if !ok {
				continue
			}
			variables[x.ID] = true
		case *stmt.ForEach:
			variables[actual.Item.ID] = true
		case *expr.Unary:
			implyDefaultParams(variables, []ast.Statement{actual}, meta, false, actual.Type(), uriParams)
		case *expr.Binary:
			xType := actual.X.Type()
			if xType == nil {
				xType = actual.Y.Type()
			}

			implyDefaultParams(variables, []ast.Statement{actual.X, actual.Y}, meta, false, xType, uriParams)
		case *expr.Parentheses:
			implyDefaultParams(variables, []ast.Statement{actual.P}, meta, false, actual.Type(), uriParams)
		case *stmt.If:
			implyDefaultParams(variables, []ast.Statement{actual.Condition}, meta, false, actual.Type(), uriParams)
		}

		switch actual := statement.(type) {
		case ast.StatementContainer:
			implyDefaultParams(variables, actual.Statements(), meta, false, nil, uriParams)
		}
	}
}

func indexParameter(variables map[string]bool, actual *expr.Select, meta *ViewMeta, required bool, rType reflect.Type, uriParams map[string]bool) {
	prefix, paramName := getParamName(actual.FullName)

	if !isParameter(variables, paramName) {
		return
	}

	pType := "string"
	assumed := true
	if rType != nil && prefix != keywords.ParamsMetadataKey {
		pType = rType.String()
		assumed = false
	}

	kind := "query"
	if uriParams[paramName] {
		kind = string(view.PathKind)
	}

	meta.addParameter(&Parameter{
		Assumed:  assumed,
		Id:       paramName,
		Name:     paramName,
		Kind:     kind,
		DataType: pType,
		fullName: actual.FullName,
		Required: boolPtr(required && prefix != keywords.ParamsMetadataKey),
	})
}

func getParamName(identifier string) (string, string) {
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

func addTemplateIfNeeded(cursor *parsly.Cursor, meta *ViewMeta) error {
	matched := cursor.MatchAfterOptional(whitespaceMatcher, templateHeaderMatcher)
	switch matched.Code {
	case parsly.Invalid, parsly.EOF:
		return nil
	}

	for {
		var parameter *Parameter
		matched = cursor.MatchAfterOptional(whitespaceMatcher, paramMatcher, templateEndMatcher)
		switch matched.Code {
		case paramToken:
			parameter = &Parameter{}
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
		meta.addParameter(parameter)
	}
}

func addParamLocation(cursor *parsly.Cursor, parameter *Parameter) error {
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
			parameter.Required = boolPtr(asBool)
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
					expressions = append(expressions, strings.TrimSpace(matched[:index]))
				}
			}

		default:
			builder.WriteByte(cursor.Input[cursor.Pos])
			cursor.Pos++
		}
	}
	return builder.String(), expressions
}

//ParseURIParams extract URI params from URI
func ParseURIParams(URI string) []string {
	var params []string
	cursor := parsly.NewCursor("", []byte(URI), 0)
outer:
	for i := 0; i < len(cursor.Input); i++ {

		match := cursor.MatchOne(scopeBlockMatcher)
		switch match.Code {
		case parsly.EOF:
			break outer
		case scopeBlock:
			block := match.Text(cursor)
			params = append(params, block[1:len(block)-1])
		default:
			cursor.Pos++
		}
	}
	return params
}
