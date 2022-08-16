package ast

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/viant/datly/cmd/option"
	"github.com/viant/parsly"
	"github.com/viant/sqlx/io/read/cache/ast"
	"strings"
)

var resetWords = []string{"AND", "OR", "WITH", "HAVING", "LIMIT", "OFFSET", "WHERE", "SELECT", "UNION", "ALL", "AS", "BETWEEN", "IN"}

func correctUntyped(SQL string, variables map[string]bool, meta *option.ViewMeta) {
	var typer option.Typer
	var untyped []*option.Parameter

	cursor := parsly.NewCursor("", []byte(SQL), 0)
outer:
	for cursor.Pos < cursor.InputSize {
		matched := cursor.MatchAfterOptional(whitespaceMatcher, commentBlockMatcher, doubleQuoteStringMatcher, singleQuoteStringMatcher, boolTokenMatcher, boolMatcher, numberMatcher, fullWordMatcher, anyMatcher)
		switch matched.Code {
		case numberToken:
			typer = &option.LiteralType{RType: ast.Float64Type}
		case boolToken:
			typer = &option.LiteralType{RType: ast.BoolType}
		case stringToken:
			typer = &option.LiteralType{RType: ast.StringType}
		case parsly.EOF, anyToken:
			//Do nothing
		default:
			text := matched.Text(cursor)
			for _, word := range resetWords {
				if strings.EqualFold(text, word) {
					typer = nil
					untyped = nil
					continue outer
				}
			}

			firstLetter := bytes.ToUpper([]byte{text[0]})[0]
			if (firstLetter < 'A' || firstLetter > 'Z') && firstLetter != '$' {
				continue outer
			}

			if text[0] == '$' {
				_, paramName := getHolderName(text)
				if isParameter(variables, paramName) {
					aParam, ok := meta.ParamByName(paramName)
					if !ok {
						fmt.Printf("ParamName: %v, params: %+v\n", paramName, meta.Parameters)
						continue
					}

					matched = cursor.MatchAfterOptional(whitespaceMatcher, commentBlockMatcher)
					if matched.Code == commentBlockToken {
						parameter := &option.Parameter{}
						commentContent := bytes.TrimSpace(bytes.Trim(matched.Bytes(cursor), "/**/"))
						_ = json.Unmarshal(commentContent, parameter)
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
	}
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
}
