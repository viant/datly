package criteria

import (
	"bytes"
	"fmt"
	"github.com/viant/datly/converter"
	"github.com/viant/datly/view"
	"github.com/viant/parsly"
	"reflect"
	"strings"
)

var numericTokens = []*parsly.Token{notEqualMatcher, equalMatcher, greaterEqualMatcher, greaterMatcher, lowerEqualMatcher, lowerMatcher, inMatcher}

func Parse(criteria string, columns view.Columns, methods map[string]*view.Method) (*Criteria, error) {
	buffer := bytes.Buffer{}
	placeholders := make([]interface{}, 0)

	criteria = strings.TrimSpace(criteria)
	if len(criteria) == 0 {
		return &Criteria{
			Expression:   "",
			Placeholders: []interface{}{},
		}, nil
	}

	cursor := parsly.NewCursor("", []byte(criteria), 0)
	if err := parse(cursor, &buffer, &placeholders, columns, methods); err != nil {
		return nil, err
	}

	return &Criteria{
		Expression:   buffer.String(),
		Placeholders: placeholders,
	}, nil
}

func parse(cursor *parsly.Cursor, buffer *bytes.Buffer, placeholders *[]interface{}, columns view.Columns, methods map[string]*view.Method) error {
	isFirstTime := true
	for cursor.Pos < cursor.InputSize {
		if !isFirstTime {
			if err := matchOperator(cursor, buffer); err != nil {
				return err
			}
		}
		isFirstTime = false

		matched := cursor.MatchAfterOptional(whitespaceMatcher, parenthesesMatcher)
		if matched.Code == parenthesesToken {
			aBlock := matched.Text(cursor)
			buffer.WriteString(" (")
			aBlockCursor := parsly.NewCursor("", []byte(aBlock[1:len(aBlock)-1]), 0)
			if err := parse(aBlockCursor, buffer, placeholders, columns, methods); err != nil {
				return err
			}
			buffer.WriteByte(')')
			continue
		}

		if err := matchExpression(cursor, columns, buffer, placeholders, methods); err != nil {
			return err
		}
	}

	return nil
}

func matchOperator(cursor *parsly.Cursor, buffer *bytes.Buffer) error {
	matched := cursor.MatchAfterOptional(whitespaceMatcher, andMatcher, orMatcher)
	switch matched.Code {
	case orToken, andToken:
		buffer.WriteByte(' ')
		operator := matched.Text(cursor)
		buffer.WriteString(operator)
		return nil
	default:
		return cursor.NewError(andMatcher, orMatcher)
	}
}

func matchExpression(cursor *parsly.Cursor, columns view.Columns, buffer *bytes.Buffer, placeholders *[]interface{}, methods map[string]*view.Method) error {
	column, err := matchColumn(cursor, columns)
	if err != nil {
		return err
	}

	buffer.WriteByte(' ')
	buffer.WriteString(column.Name)

	matchedToken, tokenValue, err := matchExpressionToken(cursor, column.ColumnType())
	if err != nil {
		return err
	}

	buffer.WriteByte(' ')
	buffer.WriteString(tokenValue)

	switch matchedToken {
	case inToken:
		return matchDataSet(cursor, columns, column, buffer, placeholders, methods)
	default:
		return matchFieldValue(cursor, columns, column.ColumnType(), column.Format, buffer, placeholders, methods)
	}
}

func matchDataSet(cursor *parsly.Cursor, columns view.Columns, column *view.Column, buffer *bytes.Buffer, placeholders *[]interface{}, methods map[string]*view.Method) error {
	matched := cursor.MatchAfterOptional(whitespaceMatcher, parenthesesMatcher)
	switch matched.Code {
	case parenthesesToken:
		buffer.WriteString(" (")
		dataSet := matched.Text(cursor)
		dataSetCursor := parsly.NewCursor("", []byte(dataSet[1:len(dataSet)-1]), 0)

		for dataSetCursor.Pos < dataSetCursor.InputSize {
			matched = dataSetCursor.MatchAfterOptional(whitespaceMatcher, comaMatcher)

			var valueCursor *parsly.Cursor
			switch matched.Code {
			case comaToken:
				value := matched.Text(dataSetCursor)
				valueCursor = parsly.NewCursor("", []byte(value[:len(value)-1]), 0)
			case parsly.Invalid:
				valueCursor = parsly.NewCursor("", dataSetCursor.Input[dataSetCursor.Pos:], 0)
				dataSetCursor.Pos = dataSetCursor.InputSize
			case parsly.EOF:
				return dataSetCursor.NewError(comaMatcher)
			}

			if err := matchFieldValue(valueCursor, columns, column.ColumnType(), column.Format, buffer, placeholders, methods); err != nil {
				return err
			}

			if matched.Code == comaToken {
				buffer.WriteString(", ")
			}
		}

		buffer.WriteByte(')')

		return nil
	default:
		return cursor.NewError(parenthesesMatcher)
	}
}

func matchFieldValue(cursor *parsly.Cursor, columns view.Columns, columnType reflect.Type, format string, buffer *bytes.Buffer, placeholders *[]interface{}, methods map[string]*view.Method) error {
	valueCandidates, err := expressionValueCandidates(columnType)
	if err != nil {
		return err
	}

	matched := cursor.MatchAfterOptional(whitespaceMatcher, valueCandidates...)
	value := matched.Text(cursor)

	switch matched.Code {
	case keywordToken:
		if _, err = columns.Lookup(value); err == nil {
			return appendField(value, columns, columnType, buffer)
		}

		if method, ok := methods[value]; ok {
			return appendMethod(cursor, methods, method, columns, buffer, placeholders)
		}

		return fmt.Errorf("not found column or method with name %v", value)

	case parsly.EOF, parsly.Invalid:
		return cursor.NewError(valueCandidates...)

	case stringToken, timeToken:
		cursorText := matched.Text(cursor)
		buffer.WriteByte(' ')
		buffer.WriteByte('?')

		converted, err := converter.Convert(cursorText[1:len(cursorText)-1], columnType, format)
		if err != nil {
			return err
		}
		*placeholders = append(*placeholders, converted)
		return nil
	default:
		rawValue := matched.Text(cursor)
		converted, err := converter.Convert(rawValue, columnType, format)
		if err != nil {
			return err
		}

		*placeholders = append(*placeholders, converted)
		buffer.WriteByte(' ')
		buffer.WriteByte('?')
		return nil
	}
}

func appendMethod(cursor *parsly.Cursor, methods map[string]*view.Method, method *view.Method, columns view.Columns, buffer *bytes.Buffer, placeholders *[]interface{}) error {
	buffer.WriteByte(' ')
	buffer.WriteString(method.Name)
	matched := cursor.MatchOne(parenthesesMatcher)
	switch matched.Code {
	case parenthesesToken:
		block := matched.Text(cursor)
		blockCursor := parsly.NewCursor("", []byte(block[1:len(block)-1]), 0)
		buffer.WriteByte('(')
		if err := matchMethod(blockCursor, methods, method.Args, columns, buffer, placeholders); err != nil {
			return err
		}

		buffer.WriteByte(')')
		return nil
	}

	return cursor.NewError(parenthesesMatcher)
}

func matchMethod(cursor *parsly.Cursor, methods map[string]*view.Method, args []*view.Schema, columns view.Columns, buffer *bytes.Buffer, placeholders *[]interface{}) error {
	for i, arg := range args {
		if i != 0 {
			buffer.WriteString(", ")
		}
		matched := cursor.MatchAfterOptional(whitespaceMatcher, comaMatcher)

		var valueCursor *parsly.Cursor
		switch matched.Code {
		case comaToken:
			argumentValue := matched.Text(cursor)
			valueCursor = parsly.NewCursor("", []byte(argumentValue[:len(argumentValue)-1]), 0)
		case parsly.Invalid:
			valueCursor = cursor
		case parsly.EOF:
			return cursor.NewError(comaMatcher)
		}

		if err := matchFieldValue(valueCursor, columns, arg.Type(), "", buffer, placeholders, methods); err != nil {
			return err
		}
	}

	matched := cursor.MatchOne(whitespaceMatcher)
	switch matched.Code {
	case parsly.EOF:
		return nil
	default:
		return fmt.Errorf("unable to match %v", string(cursor.Input[cursor.Pos:]))
	}
}

func appendField(value string, columns view.Columns, columnType reflect.Type, buffer *bytes.Buffer) error {
	valueColumn, err := findColumn(value, columns)
	if err != nil {
		return err
	}

	if valueColumn.ColumnType() != columnType {
		return fmt.Errorf("columns type missmatch, wanted %v, got %v", columnType.String(), valueColumn.ColumnType().String())
	}

	buffer.WriteByte(' ')
	buffer.WriteString(valueColumn.Name)
	return nil
}

func matchColumn(cursor *parsly.Cursor, columns view.Columns) (*view.Column, error) {
	candidates := []*parsly.Token{fieldMatcher}
	matched := cursor.MatchAfterOptional(whitespaceMatcher, candidates...)

	switch matched.Code {
	case keywordToken:
		fieldValue := matched.Text(cursor)
		return findColumn(fieldValue, columns)

	default:
		return nil, cursor.NewError(candidates...)
	}
}

func findColumn(fieldName string, columns view.Columns) (*view.Column, error) {
	lookup, err := columns.Lookup(fieldName)

	if err != nil {
		return nil, err
	}

	if !lookup.Filterable {
		return nil, fmt.Errorf("can't use %v field in expression", fieldName)
	}

	return lookup, err
}

func matchExpressionToken(cursor *parsly.Cursor, fieldType reflect.Type) (int, string, error) {
	expressionTokens, err := expressionTokenCandidates(fieldType)
	if err != nil {
		return 0, "", err
	}

	matched := cursor.MatchAfterOptional(whitespaceMatcher, expressionTokens...)

	switch matched.Code {
	case parsly.EOF, parsly.Invalid:
		return 0, "", cursor.NewError(expressionTokens...)
	case notEqualToken:
		return matched.Code, "<>", nil
	default:
		tokenValue := matched.Text(cursor)
		return matched.Code, tokenValue, nil
	}
}

func expressionTokenCandidates(fieldType reflect.Type) ([]*parsly.Token, error) {
	switch fieldType.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64:

		return numericTokens, nil

	case reflect.Bool:
		return []*parsly.Token{notEqualMatcher, equalMatcher, inMatcher}, nil

	case reflect.String:
		return []*parsly.Token{notEqualMatcher, equalMatcher, likeMatcher, inMatcher}, nil

	case reflect.Struct:
		if fieldType == converter.TimeType {
			return numericTokens, nil
		}
	}

	return nil, fmt.Errorf("unsupported field criteria type %v", fieldType.String())
}

func expressionValueCandidates(columnType reflect.Type) ([]*parsly.Token, error) {
	switch columnType.Kind() {
	case reflect.Bool:
		return []*parsly.Token{booleanMatcher, fieldMatcher}, nil

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return []*parsly.Token{intMatcher, fieldMatcher}, nil

	case reflect.Float64, reflect.Float32:
		return []*parsly.Token{numericMatcher, fieldMatcher}, nil

	case reflect.String:
		return []*parsly.Token{stringMatcher, fieldMatcher}, nil

	case reflect.Struct:
		if columnType == converter.TimeType {
			return []*parsly.Token{timeMatcher, fieldMatcher}, nil
		}
	}

	return nil, fmt.Errorf("unsupported field criteria type %v", columnType.String())
}
