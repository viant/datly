package ast

import (
	"bytes"
	"fmt"
	"github.com/viant/datly/cmd/option"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/keywords"
	"github.com/viant/parsly"
	"github.com/viant/sqlx/io/read/cache/ast"
	"strconv"
	"strings"
)

var resetWords = []string{"AND", "OR", "WITH", "HAVING", "LIMIT", "OFFSET", "WHERE", "SELECT", "UNION", "ALL", "AS", "BETWEEN", "IN"}

func (d *paramTypeDetector) correctUntyped(SQL string, variables map[string]bool, meta *option.ViewMeta) []*option.Parameter {
	var typer option.Typer
	var untyped []*option.Parameter
	previouslyMatched := -1

	cursor := parsly.NewCursor("", []byte(SQL), 0)

	for cursor.Pos < cursor.InputSize {
		matched := cursor.MatchAfterOptional(whitespaceMatcher, forEachMatcher, ifMatcher, assignMatcher, elseIfMatcher, elseMatcher, endMatcher,
			commentBlockMatcher, doubleQuoteStringMatcher, singleQuoteStringMatcher, boolTokenMatcher, boolMatcher, numberMatcher, parenthesesBlockMatcher, fullWordMatcher, anyMatcher)
		switch matched.Code {
		case numberToken:
			text := matched.Text(cursor)

			_, err := strconv.Atoi(text)
			if err == nil {
				typer = option.NewLiteralType(ast.IntType)
			} else {
				typer = option.NewLiteralType(ast.Float64Type)
			}

		case boolToken:
			typer = option.NewLiteralType(ast.BoolType)
		case stringToken:
			typer = option.NewLiteralType(ast.StringType)
		case parenthesesBlockToken:
			sqlFragment := matched.Text(cursor)
			untypedInBlock := d.correctUntyped(sqlFragment[1:len(sqlFragment)-1], variables, meta)
			if !isVeltyMatchToken(previouslyMatched) {
				untyped = append(untyped, untypedInBlock...)
			}

		case parsly.EOF, anyToken:
			//Do nothing
		default:
			text := matched.Text(cursor)
			for _, word := range resetWords {
				if strings.EqualFold(text, word) {
					typer = nil
					untyped = nil
					goto quitSwitch
				}
			}

			firstLetter := bytes.ToUpper([]byte{text[0]})[0]
			if (firstLetter < 'A' || firstLetter > 'Z') && firstLetter != '$' {
				goto quitSwitch
			}

			if text[0] == '$' {
				prefix, paramName := getHolderName(text)
				if prefix == keywords.ParamsMetadataKey {
					continue
				}

				if isParameter(variables, paramName) {
					aParam, ok := meta.ParamByName(paramName)
					if !ok {
						fmt.Printf("ParamName: %v, params: %+v\n", paramName, meta.Parameters)
						continue
					}

					hint, ok := d.hints[paramName]
					if ok {
						parameter := &option.Parameter{}
						_, _ = UnmarshalHint(hint.Hint, parameter)

						if IsDataViewKind(hint.Hint) {
							parameter.Kind = string(view.DataViewKind)
						}

						inherit(aParam, parameter)
					}

					if aParam.Assumed {
						untyped = append(untyped, aParam)
					}
				}
			} else {
				typer = &option.ColumnType{ColumnName: strings.ToLower(text)}
			}
		}

		if typer != nil {
			for _, param := range untyped {
				param.Typer = typer
			}

			untyped = nil
		}

	quitSwitch:
		if matched.Token.Code != anyToken {
			previouslyMatched = matched.Token.Code
		}
	}

	return untyped
}

func isVeltyMatchToken(matched int) bool {
	switch matched {
	case endToken, elseToken, assignToken, forEachToken, ifToken:
		return true
	}

	return false
}

func IsDataViewKind(hint string) bool {
	_, sqlPart := SplitHint(hint)
	if strings.HasSuffix(sqlPart, "*/") {
		sqlPart = sqlPart[:len(sqlPart)-len("*/")]
	}

	return strings.TrimSpace(sqlPart) != ""
}

func inherit(generated *option.Parameter, inlined *option.Parameter) {
	if inlined.DataType != "" {
		generated.DataType = inlined.DataType
		generated.Assumed = false
	}

	if inlined.Required != nil {
		generated.Required = inlined.Required
	}

	if inlined.Name != "" {
		generated.Name = inlined.Name
	}

	if inlined.Kind != "" {
		generated.Kind = inlined.Kind
	}

	if inlined.Id != "" {
		generated.Id = inlined.Id
	}

	if inlined.Codec != "" {
		generated.Codec = inlined.Codec
	}
}
