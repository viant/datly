package dql

import (
	"strings"

	sqltext "github.com/viant/sqlparser/source"
)

// This file holds the cursor-based decoders for a declaration's head — the
// `$_ = $Name<Type>(kind/location) tail` structure. parseDeclarations (in
// declarations.go) drives these; the tail option parsers live in
// declarations_options.go.

func parseDeclarationHead(body string) (holder, kind, location, tail string, tailOffset int, ok bool) {
	cursor := newByteCursor(body)
	skipWhitespace(cursor)
	if !consumeAssignmentPrefix(cursor) {
		return "", "", "", "", 0, false
	}
	skipWhitespace(cursor)
	if cursor.pos >= len(cursor.input) || cursor.input[cursor.pos] != '$' {
		return "", "", "", "", 0, false
	}
	cursor.pos++
	id, matched := readIdentifier(cursor)
	if !matched || id[0] == '_' {
		return "", "", "", "", 0, false
	}
	holder = id
	skipWhitespace(cursor)
	if cursor.pos < len(cursor.input) && cursor.input[cursor.pos] == '<' {
		typeExpr, ok := readBracketGroup(cursor, '<', '>')
		if !ok {
			return "", "", "", "", 0, false
		}
		_ = typeExpr
	}
	skipWhitespace(cursor)
	groupExpr, ok := readBracketGroup(cursor, '(', ')')
	if !ok {
		return "", "", "", "", 0, false
	}
	raw := strings.TrimSpace(groupExpr)
	slash := strings.Index(raw, "/")
	if slash == -1 {
		return "", "", "", "", 0, false
	}
	kind = strings.ToLower(strings.TrimSpace(raw[:slash]))
	location = strings.TrimSpace(raw[slash+1:])
	tail, tailOffset = declarationTail(cursor)
	return holder, kind, location, tail, tailOffset, true
}

func parseImplicitDeclarationHead(body string) (holder, tail string, tailOffset int, ok bool) {
	cursor := newByteCursor(body)
	skipWhitespace(cursor)
	if !consumeAssignmentPrefix(cursor) {
		return "", "", 0, false
	}
	skipWhitespace(cursor)
	if cursor.pos >= len(cursor.input) || cursor.input[cursor.pos] != '$' {
		return "", "", 0, false
	}
	cursor.pos++
	id, matched := readIdentifier(cursor)
	if !matched || id[0] == '_' {
		return "", "", 0, false
	}
	holder = id
	skipWhitespace(cursor)
	if cursor.pos < len(cursor.input) && cursor.input[cursor.pos] == '<' {
		typeExpr, ok := readBracketGroup(cursor, '<', '>')
		if !ok {
			return "", "", 0, false
		}
		_ = typeExpr
	}
	skipWhitespace(cursor)
	tail, tailOffset = declarationTail(cursor)
	return holder, tail, tailOffset, true
}

func declarationTail(cursor *byteCursor) (string, int) {
	raw := string(cursor.input[cursor.pos:])
	tail := strings.TrimSpace(raw)
	if tail == "" {
		return "", cursor.pos
	}
	return tail, cursor.pos + strings.Index(raw, tail)
}

func parseDeclarationTypes(body string) (string, string) {
	cursor := newByteCursor(body)
	skipWhitespace(cursor)
	if !consumeAssignmentPrefix(cursor) {
		return "", ""
	}
	skipWhitespace(cursor)
	if cursor.pos >= len(cursor.input) || cursor.input[cursor.pos] != '$' {
		return "", ""
	}
	cursor.pos++
	if _, ok := readIdentifier(cursor); !ok {
		return "", ""
	}
	skipWhitespace(cursor)
	if cursor.pos >= len(cursor.input) || cursor.input[cursor.pos] != '<' {
		return "", ""
	}
	group, ok := readBracketGroup(cursor, '<', '>')
	if !ok {
		return "", ""
	}
	args := sqltext.SplitArgs(group)
	if len(args) == 0 {
		return "", ""
	}
	inputType := normalizeTypeExpr(args[0])
	outputType := ""
	if len(args) > 1 {
		outputType = normalizeTypeExpr(args[1])
	}
	return inputType, outputType
}
