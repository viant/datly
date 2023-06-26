package codegen

import "strings"

func trimParenthesis(text string) string {
	text = strings.TrimSpace(text)
	if text == "" {
		return text
	}
	if text[0] == '(' {
		text = text[1:]
	}
	if text[len(text)-1] == ')' {
		text = text[:len(text)-1]
	}
	return text
}
