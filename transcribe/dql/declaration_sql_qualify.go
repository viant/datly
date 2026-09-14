package dql

import (
	"strings"

	"github.com/viant/sqlparser"
	qexpr "github.com/viant/sqlparser/expr"
)

func parseCriteriaIn(qualify *qexpr.Qualify) *DeclarationCriteriaIn {
	if qualify == nil || qualify.X == nil {
		return nil
	}
	binary, ok := qualify.X.(*qexpr.Binary)
	if !ok {
		return nil
	}
	call, ok := binary.X.(*qexpr.Call)
	if !ok || !strings.EqualFold(sqlparser.Stringify(call.X), "$criteria.In") {
		return nil
	}
	column, helperField, ok := parseCriteriaInArgs(call.Raw)
	if !ok || column == "" || helperField == "" {
		return nil
	}
	return &DeclarationCriteriaIn{
		Column:      column,
		HelperField: helperField,
	}
}

func parseScalarProperty(qualify *qexpr.Qualify) *DeclarationScalarProperty {
	if qualify == nil || qualify.X == nil {
		return nil
	}
	binary, ok := qualify.X.(*qexpr.Binary)
	if !ok || strings.TrimSpace(binary.Op) != "=" {
		return nil
	}
	column := trimSelectorSuffix(sqlparser.Stringify(binary.X))
	placeholder, ok := binary.Y.(*qexpr.Placeholder)
	if !ok {
		return nil
	}
	ref, ok := parsePlaceholderRef(placeholder.Name)
	if !ok || len(ref) != 2 || column == "" {
		return nil
	}
	return &DeclarationScalarProperty{
		Column:      column,
		SourceField: ref[0],
		Property:    ref[1],
	}
}

func parsePlaceholderRef(raw string) ([]string, bool) {
	raw = strings.TrimSpace(raw)
	raw = strings.TrimPrefix(raw, "$")
	raw = strings.TrimPrefix(raw, "{")
	raw = strings.TrimSuffix(raw, "}")
	if raw == "" {
		return nil, false
	}
	parts := strings.Split(raw, ".")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
		if parts[i] == "" {
			return nil, false
		}
	}
	return parts, true
}

func parseCriteriaInArgs(raw string) (string, string, bool) {
	text := strings.TrimSpace(raw)
	if !strings.HasPrefix(text, "(") || !strings.HasSuffix(text, ")") {
		return "", "", false
	}
	cursor := &argCursor{input: text[1 : len(text)-1]}
	column, ok := cursor.readStringLiteral()
	if !ok {
		return "", "", false
	}
	if !cursor.consumeComma() {
		return "", "", false
	}
	ref, ok := cursor.readPlaceholderRef()
	if !ok || len(ref) != 2 || ref[1] != "Values" {
		return "", "", false
	}
	cursor.skipSpace()
	if !cursor.done() {
		return "", "", false
	}
	return column, ref[0], true
}

type argCursor struct {
	input string
	pos   int
}

func (c *argCursor) done() bool {
	return c.pos >= len(c.input)
}

func (c *argCursor) skipSpace() {
	for c.pos < len(c.input) {
		switch c.input[c.pos] {
		case ' ', '\n', '\r', '\t':
			c.pos++
		default:
			return
		}
	}
}

func (c *argCursor) readStringLiteral() (string, bool) {
	c.skipSpace()
	if c.done() {
		return "", false
	}
	quote := c.input[c.pos]
	if quote != '"' && quote != '\'' {
		return "", false
	}
	c.pos++
	start := c.pos
	escape := false
	for c.pos < len(c.input) {
		ch := c.input[c.pos]
		if escape {
			escape = false
			c.pos++
			continue
		}
		if ch == '\\' {
			escape = true
			c.pos++
			continue
		}
		if ch == quote {
			value := c.input[start:c.pos]
			c.pos++
			return value, true
		}
		c.pos++
	}
	return "", false
}

func (c *argCursor) consumeComma() bool {
	c.skipSpace()
	if c.done() || c.input[c.pos] != ',' {
		return false
	}
	c.pos++
	return true
}

func (c *argCursor) readPlaceholderRef() ([]string, bool) {
	c.skipSpace()
	if c.done() || c.input[c.pos] != '$' {
		return nil, false
	}
	start := c.pos
	c.pos++
	for c.pos < len(c.input) {
		ch := c.input[c.pos]
		if (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') || ch == '_' || ch == '.' {
			c.pos++
			continue
		}
		break
	}
	return parsePlaceholderRef(c.input[start:c.pos])
}
