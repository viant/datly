package dql

import (
	"fmt"
	"go/token"
	"unicode"
	"unicode/utf8"
)

func parseClientInputType(argument string) (string, error) {
	name, quoted := parseQuotedLiteral(argument)
	first, _ := utf8.DecodeRuneInString(name)
	if !quoted || !token.IsIdentifier(name) || !unicode.IsUpper(first) {
		return "", fmt.Errorf("client_input_type requires a quoted exported Go identifier")
	}
	return name, nil
}
