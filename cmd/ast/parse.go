package ast

import (
	"bytes"
	"github.com/viant/datly/view/keywords"
	"github.com/viant/parsly"
	"github.com/viant/velty/ast"
	"github.com/viant/velty/ast/expr"
	"github.com/viant/velty/ast/stmt"
	"github.com/viant/velty/parser"
	"strconv"
	"strings"
)

func Parse(SQL string) (*ViewMeta, error) {
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

outer:
	for _, statement := range block.Statements() {
		switch actual := statement.(type) {
		case *stmt.Append:
		//Do nothing
		case *expr.Select:
			id := paramId(actual)
			if strings.HasPrefix(id, keywords.ParamsKey) {
				viewMeta.HasVeltySyntax = true
				break outer
			}
		default:
			viewMeta.HasVeltySyntax = true
			break outer
		}
	}

	cursor.MatchOne(whitespaceMatcher)
	actualSource := string(cursor.Input[cursor.Pos:])

	for i := 0; i < cursor.Pos; i++ {
		cursor.Input[i] = ' '
	}

	variables := map[string]bool{}
	implyDefaultParams(variables, block.Statements(), viewMeta, true)

	if viewMeta.HasVeltySyntax {
		viewMeta.Source = actualSource
	} else {
		for _, parameter := range viewMeta.Parameters {
			actualSource = strings.ReplaceAll(actualSource, parameter.fullName, "?")
		}
		viewMeta.From = actualSource
	}
	return viewMeta, nil
}

func implyDefaultParams(variables map[string]bool, statements []ast.Statement, meta *ViewMeta, required bool) {
	for _, statement := range statements {
		switch actual := statement.(type) {
		case *expr.Select:
			paramName := paramId(actual)

			paramName = removePrefixIfNeeded(paramName)
			paramName = withoutPath(paramName)

			if isVariable := variables[paramName]; isVariable {
				continue
			}

			switch paramName {
			case keywords.Criteria[1:], keywords.SelectorCriteria[1:], keywords.Pagination[1:], keywords.ColumnsIn[1:]:
				continue
			}

			meta.addParameter(&Parameter{
				Id:       paramName,
				Name:     paramName,
				Kind:     "query",
				Type:     "string",
				fullName: actual.FullName,
				Required: required,
			}, true)

		case *stmt.Statement:
			x, ok := actual.X.(*expr.Select)
			if !ok {
				continue
			}
			variables[x.ID] = true
		case *stmt.ForEach:
			variables[actual.Item.ID] = true
		}

		switch actual := statement.(type) {
		case ast.StatementContainer:
			implyDefaultParams(variables, actual.Statements(), meta, false)
		}
	}
}

func paramId(actual *expr.Select) string {
	paramName := actual.FullName[1:]
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

func removePrefixIfNeeded(name string) string {
	prefixes := []string{
		keywords.AndPrefix, keywords.WherePrefix, keywords.OrPrefix,
		keywords.ParamsKey + ".", keywords.ParamsMetadataKey + ".",
	}
	for _, prefix := range prefixes {
		if strings.HasPrefix(name, prefix) {
			return name[len(prefix):]
		}
	}

	return name
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
		meta.addParameter(parameter, false)
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
			parameter.Type = value
		case 3:
			asBool, err := strconv.ParseBool(value)
			if err != nil {
				return err
			}
			parameter.Required = asBool
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
