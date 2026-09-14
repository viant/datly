package template

import (
	"strings"

	sqltext "github.com/viant/sqlparser/source"
)

func protectContextVariables(source string, variables ...string) string {
	var offsets []int
	for offset := 0; offset < len(source); {
		relative := strings.IndexByte(source[offset:], '$')
		if relative == -1 {
			break
		}
		position := offset + relative
		next := position + 1
		for _, variable := range variables {
			if !isTemplateVariable(source, position, variable) {
				continue
			}
			if _, end, kind := sqltext.ProtectedRangeAt(source, position); kind != "" {
				// A protected context call is SQL data in its entirety, including
				// nested $arguments. Escaping only its root would leave those
				// arguments executable after the context root becomes literal.
				for cursor := position; cursor < end; {
					relative := strings.IndexByte(source[cursor:end], '$')
					if relative < 0 {
						break
					}
					cursor += relative
					offsets = append(offsets, cursor)
					cursor++
				}
				next = end
				break
			}
		}
		offset = next
	}
	if len(offsets) == 0 {
		return source
	}
	for i := len(offsets) - 1; i >= 0; i-- {
		offset := offsets[i]
		source = source[:offset] + "#[[$]]#" + source[offset+1:]
	}
	return source
}

// hasTemplateCode lexes SQL text before invoking Velty. Template-looking text
// inside SQL literals, quoted identifiers, or comments is data, not code.
func hasTemplateCode(source string, variables ...string) bool {
	scanner := sqltext.NewCodeScanner(source, 0)
	for position, ok := scanner.Next(); ok; position, ok = scanner.Next() {
		switch source[position] {
		case '#':
			if isTemplateDirective(source, position) {
				return true
			}
		case '$':
			for _, variable := range variables {
				if isTemplateVariable(source, position, variable) {
					return true
				}
			}
		}
	}
	return false
}

func isTemplateVariable(source string, offset int, expected string) bool {
	i := offset + 1
	if i < len(source) && source[i] == '!' {
		i++
	}
	braced := i < len(source) && source[i] == '{'
	if braced {
		i++
	}
	start := i
	for i < len(source) && isIdentifierPart(source[i]) {
		i++
	}
	if source[start:i] != expected {
		return false
	}
	if braced {
		return i < len(source) && (source[i] == '}' || source[i] == '.')
	}
	return i == len(source) || !isIdentifierPart(source[i])
}

func isTemplateDirective(source string, offset int) bool {
	i := offset + 1
	for i < len(source) && isSpace(source[i]) {
		i++
	}
	parenthesized := false
	if i < len(source) && source[i] == '(' {
		parenthesized = true
		i++
		for i < len(source) && isSpace(source[i]) {
			i++
		}
	}
	start := i
	for i < len(source) && isIdentifierPart(source[i]) {
		i++
	}
	if start == i {
		return false
	}
	name := strings.ToLower(source[start:i])
	if parenthesized {
		return isDirectiveName(name)
	}
	switch name {
	case "if", "elseif", "set", "foreach", "for", "evaluate":
		for i < len(source) && isSpace(source[i]) {
			i++
		}
		return i < len(source) && source[i] == '('
	case "else", "end", "break":
		return i == len(source) || !isIdentifierPart(source[i])
	default:
		return false
	}
}

func isDirectiveName(name string) bool {
	switch name {
	case "if", "elseif", "else", "set", "foreach", "for", "evaluate", "end", "break":
		return true
	default:
		return false
	}
}

func isSpace(ch byte) bool {
	switch ch {
	case ' ', '\t', '\n', '\r':
		return true
	default:
		return false
	}
}

func isIdentifierPart(ch byte) bool {
	return ch == '_' || ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9'
}
