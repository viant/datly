package preprocess

import (
	"strings"

	"github.com/viant/datly/repository/shape/dql/decl"
)

type directiveCall struct {
	name  string
	args  []string
	start int
	end   int
}

type directiveParseError struct {
	name    string
	start   int
	message string
}

func scanDollarCalls(input string, names map[string]bool) []directiveCall {
	calls, _ := scanDollarCallsStrict(input, names)
	return calls
}

func scanDollarCallsStrict(input string, names map[string]bool) ([]directiveCall, []directiveParseError) {
	parsed, parseErrors := decl.ScanCalls(input, decl.CallScanOptions{
		AllowedNames:  names,
		RequireDollar: true,
		AllowDollar:   true,
		Strict:        true,
	})
	result := make([]directiveCall, 0)
	for _, call := range parsed {
		result = append(result, directiveCall{
			name:  call.Name,
			args:  call.Args,
			start: call.Offset,
			end:   call.EndOffset,
		})
	}
	errs := make([]directiveParseError, 0, len(parseErrors))
	for _, parseErr := range parseErrors {
		errs = append(errs, directiveParseError{
			name:    parseErr.Name,
			start:   parseErr.Offset,
			message: parseErr.Message,
		})
	}
	return result, errs
}

func readCallBody(input string, openParen int) (string, int, bool) {
	depth := 0
	quote := byte(0)
	for i := openParen; i < len(input); i++ {
		ch := input[i]
		if quote != 0 {
			if ch == '\\' && i+1 < len(input) {
				i++
				continue
			}
			if ch == quote {
				quote = 0
			}
			continue
		}
		if ch == '\'' || ch == '"' {
			quote = ch
			continue
		}
		if ch == '(' {
			depth++
			continue
		}
		if ch == ')' {
			depth--
			if depth == 0 {
				return input[openParen+1 : i], i, true
			}
		}
	}
	return "", -1, false
}

func splitCallArgs(input string) []string {
	args := make([]string, 0)
	current := strings.Builder{}
	depth := 0
	quote := byte(0)
	for i := 0; i < len(input); i++ {
		ch := input[i]
		if quote != 0 {
			current.WriteByte(ch)
			if ch == '\\' && i+1 < len(input) {
				i++
				current.WriteByte(input[i])
				continue
			}
			if ch == quote {
				quote = 0
			}
			continue
		}
		if ch == '\'' || ch == '"' {
			quote = ch
			current.WriteByte(ch)
			continue
		}
		if ch == '(' {
			depth++
			current.WriteByte(ch)
			continue
		}
		if ch == ')' {
			if depth > 0 {
				depth--
			}
			current.WriteByte(ch)
			continue
		}
		if ch == ',' && depth == 0 {
			args = append(args, strings.TrimSpace(current.String()))
			current.Reset()
			continue
		}
		current.WriteByte(ch)
	}
	if value := strings.TrimSpace(current.String()); value != "" {
		args = append(args, value)
	}
	return args
}

func skipSpaces(input string, index int) int {
	for index < len(input) {
		switch input[index] {
		case ' ', '\t', '\n', '\r':
			index++
		default:
			return index
		}
	}
	return index
}

func skipInlineSpaces(input string, index int) int {
	for index < len(input) {
		switch input[index] {
		case ' ', '\t':
			index++
		default:
			return index
		}
	}
	return index
}

func isIdentifierStart(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_'
}

func isIdentifierPart(ch byte) bool {
	return isIdentifierStart(ch) || (ch >= '0' && ch <= '9')
}

func parseQuotedLiteral(input string) (string, bool) {
	value := strings.TrimSpace(input)
	if len(value) < 2 {
		return "", false
	}
	quote := value[0]
	if quote != '\'' && quote != '"' {
		return "", false
	}
	if value[len(value)-1] != quote {
		return "", false
	}
	return value[1 : len(value)-1], true
}

func hasWordFoldAt(input string, pos int, word string) bool {
	if pos < 0 || pos+len(word) > len(input) {
		return false
	}
	if !strings.EqualFold(input[pos:pos+len(word)], word) {
		return false
	}
	next := pos + len(word)
	if next >= len(input) {
		return true
	}
	return !isIdentifierPart(input[next])
}
