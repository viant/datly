package sql

import (
	"fmt"
	"github.com/viant/parsly"
	"reflect"
	"strings"
)

func Parse(input []byte) (Node, error) {
	var err error

	cursor := parsly.NewCursor("", input, 0)
	var parent = &Binary{}

	matched := cursor.MatchAfterOptional(Whitespace, Group)
	if matched.Code == groupToken {
		body := matched.Text(cursor)
		return addParentheses([]byte(body[1:len(body)-1]), cursor, parent)
	}

	parent.X, err = expectLiteralOrSelector(cursor, true)
	if err != nil {
		return nil, err
	}

	expectLogicalOperator := false
	operator, err := expectIsExpression(cursor)
	if err != nil {

		return nil, err
	}

	if operator != "" {
		unary := &Unary{X: parent.X, Operator: operator}
		parent.X = unary
		expectLogicalOperator = true
	}

	operator, err = expectOperator(cursor, expectLogicalOperator)
	if err != nil || operator == "" {
		return parent.X, err
	}

	parent.Operator = operator
	if strings.ToLower(operator) == "in" {
		parent.Y, err = expectDataset(cursor)
		if err != nil {
			return nil, err
		}
		operator, err = expectOperator(cursor, true)
		if err != nil || operator == "" {
			return parent, err
		}
		parent.X = &Binary{X: parent.X, Operator: parent.Operator, Y: parent.Y}
		parent.Operator = operator
	}

	parent.Y, err = Parse(input[cursor.Pos:])
	return parent, err
}

func addParentheses(input []byte, cursor *parsly.Cursor, parent *Binary) (Node, error) {
	var err error
	x := &Parentheses{}
	if x.P, err = Parse(input); err != nil {
		return nil, err
	}

	operator, err := expectOperator(cursor, true)
	if err != nil || operator == "" {
		return x, err
	}
	parent.Operator = operator
	parent.X = x
	parent.Y, err = Parse(input[cursor.Pos:])
	return parent, err
}

func expectDataset(cursor *parsly.Cursor) (Node, error) {
	matched := cursor.MatchAfterOptional(Whitespace, Group)
	if matched.Code != groupToken {
		return nil, cursor.NewError(Group)
	}

	text := matched.Text(cursor)
	text = text[1 : len(text)-1]

	datasetCursor := parsly.NewCursor("", []byte(text), 0)

	node, err := expectLiteralOrSelector(datasetCursor, true)
	if err != nil {
		return nil, err
	}

	values := make([]string, 0)
	literal := node.(*Literal)
	values = append(values, literal.Value)

	kind := literal.Kind

outer:
	for {

		matched = datasetCursor.MatchAfterOptional(Whitespace, Next)
		switch matched.Code {
		case parsly.EOF:
			break outer
		case nextToken:
		default:
			return nil, datasetCursor.NewError(Next)
		}

		node, err = expectLiteralOrSelector(datasetCursor, true)
		if err != nil {
			return nil, err
		}

		nextLiteral := node.(*Literal)
		values = append(values, nextLiteral.Value)

		if kind == -1 {
			kind = nextLiteral.Kind
		}

		if nextLiteral.Kind != -1 && kind != nextLiteral.Kind {
			return nil, fmt.Errorf("inconsistent value type")
		}

	}
	return &Literal{Value: strings.Join(values, ","), Kind: kind}, nil
}

func expectIsExpression(cursor *parsly.Cursor) (string, error) {
	matched := cursor.MatchAfterOptional(Whitespace, IsKeyword)
	switch matched.Code {
	case parsly.EOF:
		return "", nil
	case isToken:
		operator := matched.Text(cursor)
		matched := cursor.MatchAfterOptional(Whitespace, NotKeyword, NullKeyword)
		switch matched.Code {
		case parsly.EOF:
			return "", fmt.Errorf("unpexpcted eof")
		case notToken:
			operator += " " + matched.Text(cursor)
			matched := cursor.MatchAfterOptional(Whitespace, NullKeyword)
			if matched.Code == nullToken {
				operator += " " + matched.Text(cursor)
				return operator, nil
			}
			return "", cursor.NewError(NullKeyword)
		case nullToken:
			operator += " " + matched.Text(cursor)
			return operator, nil
		default:
			return "", cursor.NewError(NotKeyword, NullKeyword)
		}
	}
	return "", nil
}

func expectOperator(cursor *parsly.Cursor, expectLogicalOperator bool) (string, error) {
	var expectedTokens = []*parsly.Token{LogicalOperator, BinaryOperator, InKeyword}
	if expectLogicalOperator {
		expectedTokens = []*parsly.Token{LogicalOperator, InKeyword}
	}
	matched := cursor.MatchAfterOptional(Whitespace, expectedTokens...)
	if matched.Code == parsly.EOF {
		return "", nil
	}
	switch matched.Code {
	case operatorLogicalToken, binaryOperator, inToken:
		return matched.Text(cursor), nil
	}
	return "", cursor.NewError(expectedTokens...)
}

func expectLiteralOrSelector(cursor *parsly.Cursor, isRequired bool) (Node, error) {
	var expectedTokens = []*parsly.Token{NullKeyword, BooleanLiteral, NumberLiteral, StringLiteral, SelectorMatch}
	matched := cursor.MatchAfterOptional(Whitespace, expectedTokens...)
	if matched.Code == parsly.EOF {
		if isRequired {
			return nil, fmt.Errorf("encounter EOF")
		}
		return nil, nil
	}

	switch matched.Code {
	case booleanLiteralToken:
		return &Literal{Value: matched.Text(cursor), Kind: int(reflect.Bool)}, nil
	case numberToken:
		return &Literal{Value: matched.Text(cursor), Kind: int(reflect.Int)}, nil
	case selectorToken:
		return &Selector{Name: matched.Text(cursor)}, nil
	case stringLiteralToken:
		return &Literal{Value: matched.Text(cursor), Kind: int(reflect.String)}, nil
	case nullToken:
		return &Literal{Value: matched.Text(cursor), Kind: -1}, nil
	}
	return nil, cursor.NewError(expectedTokens...)
}
