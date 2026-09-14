package dql

import (
	"fmt"
	"strings"

	sqltext "github.com/viant/sqlparser/source"
)

type byteCursor struct {
	input []byte
	pos   int
}

func newByteCursor(input string) *byteCursor {
	return &byteCursor{input: []byte(input)}
}

func consumeAssignmentPrefix(cursor *byteCursor) bool {
	if cursor.pos+2 > len(cursor.input) || string(cursor.input[cursor.pos:cursor.pos+2]) != "$_" {
		return false
	}
	cursor.pos += 2
	skipWhitespace(cursor)
	if cursor.pos >= len(cursor.input) || cursor.input[cursor.pos] != '=' {
		return false
	}
	cursor.pos++
	return true
}

func skipWhitespace(cursor *byteCursor) {
	for cursor.pos < len(cursor.input) {
		switch cursor.input[cursor.pos] {
		case ' ', '\t', '\n', '\r':
			cursor.pos++
		default:
			return
		}
	}
}

func readIdentifier(cursor *byteCursor) (string, bool) {
	if cursor.pos >= len(cursor.input) || !isIdentifierStart(cursor.input[cursor.pos]) {
		return "", false
	}
	start := cursor.pos
	cursor.pos++
	for cursor.pos < len(cursor.input) && isIdentifierPart(cursor.input[cursor.pos]) {
		cursor.pos++
	}
	return string(cursor.input[start:cursor.pos]), true
}

func isIdentifierStart(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_'
}

func isIdentifierPart(ch byte) bool {
	return isIdentifierStart(ch) || (ch >= '0' && ch <= '9')
}

func readBracketGroup(cursor *byteCursor, open, close byte) (string, bool) {
	if cursor.pos >= len(cursor.input) || cursor.input[cursor.pos] != open {
		return "", false
	}
	group, end, ok := sqltext.ReadGroupString(string(cursor.input), cursor.pos, open, close)
	if !ok || len(group) < 2 {
		return "", false
	}
	cursor.pos = end
	return group[1 : len(group)-1], true
}

func trimQuote(input string) string {
	return sqltext.TrimQuote(input)
}

func normalizeTypeExpr(input string) string {
	input = strings.TrimSpace(trimQuote(input))
	if input == "?" {
		return ""
	}
	return input
}

func parseIntArg(input string) (int, bool) {
	input = strings.TrimSpace(trimQuote(input))
	if input == "" {
		return 0, false
	}
	sign := 1
	if input[0] == '-' {
		sign = -1
		input = input[1:]
	}
	if input == "" {
		return 0, false
	}
	value := 0
	for i := 0; i < len(input); i++ {
		if input[i] < '0' || input[i] > '9' {
			return 0, false
		}
		value = value*10 + int(input[i]-'0')
	}
	return sign * value, true
}

type optionCursor struct {
	raw    string
	cursor int
	start  int
	name   string
	args   []string
	err    error
}

func newOptionCursor(raw string) *optionCursor {
	return &optionCursor{raw: raw}
}

func (o *optionCursor) next() bool {
	o.name = ""
	o.args = nil
	o.start = 0
	for o.cursor < len(o.raw) && sqltext.IsWhitespace(o.raw[o.cursor]) {
		o.cursor++
	}
	if o.cursor >= len(o.raw) || o.raw[o.cursor] != '.' {
		return false
	}
	o.start = o.cursor
	o.cursor++
	for o.cursor < len(o.raw) && sqltext.IsWhitespace(o.raw[o.cursor]) {
		o.cursor++
	}
	start := o.cursor
	for o.cursor < len(o.raw) {
		ch := o.raw[o.cursor]
		if (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '_' {
			o.cursor++
			continue
		}
		break
	}
	if o.cursor == start {
		o.err = fmt.Errorf("malformed declaration option at offset %d: expected option name", o.start)
		return false
	}
	o.name = strings.TrimSpace(o.raw[start:o.cursor])
	for o.cursor < len(o.raw) && sqltext.IsWhitespace(o.raw[o.cursor]) {
		o.cursor++
	}
	if o.cursor >= len(o.raw) || o.raw[o.cursor] != '(' {
		o.err = fmt.Errorf("malformed declaration option %s: expected '('", o.name)
		return false
	}
	groupText, end, ok := sqltext.ReadGroupString(o.raw, o.cursor, '(', ')')
	if !ok || len(groupText) < 2 {
		o.err = fmt.Errorf("malformed declaration option %s: unterminated argument group", o.name)
		return false
	}
	o.cursor = end
	o.args = sqltext.SplitArgs(groupText[1 : len(groupText)-1])
	return true
}

func (o *optionCursor) Err() error {
	return o.err
}

func (o *optionCursor) option() (string, []string) {
	return o.name, o.args
}
