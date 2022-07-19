package ast

import (
	"bytes"
	"github.com/viant/parsly"
	"github.com/viant/sqlx/io/read/cache/ast"
	"reflect"
	"strings"
)

var resetWords = []string{"AND", "OR", "WITH", "HAVING", "LIMIT", "OFFSET", "WHERE", "SELECT", "UNION", "ALL", "AS", "BETWEEN", "IN"}

type (
	Typer interface{}

	LiteralType struct {
		RType reflect.Type
	}

	ColumnType struct {
		ColumnName string
	}
)

func correctUntyped(SQL string, variables map[string]bool, meta *ViewMeta) {
	var typer Typer
	var untyped []string

	cursor := parsly.NewCursor("", []byte(SQL), 0)
outer:
	for cursor.Pos < cursor.InputSize {
		matched := cursor.MatchAfterOptional(whitespaceMatcher, doubleQuoteStringMatcher, singleQuoteStringMatcher, boolTokenMatcher, boolMatcher, numberMatcher, fullWordMatcher)
		switch matched.Code {
		case numberToken:
			typer = &LiteralType{RType: ast.Float64Type}
		case boolToken:
			typer = &LiteralType{RType: ast.BoolType}
		case stringToken:
			typer = &LiteralType{RType: ast.StringType}
		case parsly.EOF:
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
				_, paramName := getParamName(text)
				if isParameter(variables, paramName) {
					untyped = append(untyped, paramName)
				}
			} else {
				typer = &ColumnType{ColumnName: text}
			}
		}

		if typer != nil {
			for _, paramName := range untyped {
				param, ok := meta.ParamByName(paramName)
				if !ok {
					continue
				}

				param.Typer = typer
			}
		}
	}
}
