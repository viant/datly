package decl

import (
	"strings"

	"github.com/viant/parsly"
)

// Call represents a parsed function call with offsets in the scanned input.
type Call struct {
	Name      string
	Args      []string
	Offset    int
	EndOffset int
	Dollar    bool
}

// CallParseError represents a malformed call span.
type CallParseError struct {
	Name    string
	Offset  int
	Message string
}

// CallScanOptions controls call scanning behavior.
type CallScanOptions struct {
	AllowedNames  map[string]bool
	RequireDollar bool
	AllowDollar   bool
	Strict        bool
}

// ScanCalls parses function calls and returns parsed calls plus malformed-call errors.
func ScanCalls(input string, options CallScanOptions) ([]Call, []CallParseError) {
	calls := make([]Call, 0)
	parseErrors := make([]CallParseError, 0)
	cursor := parsly.NewCursor("", []byte(input), 0)
	for cursor.Pos < cursor.InputSize {
		matched := cursor.MatchAfterOptional(
			whitespaceMatcher,
			commentBlockMatcher,
			singleQuotedMatcher,
			doubleQuotedMatcher,
			dollarIdentifierMatcher,
			identifierMatcher,
			anyMatcher,
		)
		switch matched.Code {
		case dollarIdentifierToken, identifierToken:
			rawName := matched.Text(cursor)
			hasDollar := matched.Code == dollarIdentifierToken
			name := strings.ToLower(strings.TrimPrefix(rawName, "$"))
			if options.AllowedNames != nil && !options.AllowedNames[name] {
				continue
			}
			if options.RequireDollar && !hasDollar {
				continue
			}
			if !options.AllowDollar && hasDollar {
				continue
			}
			nameOffset := matched.Offset
			block := cursor.MatchAfterOptional(whitespaceMatcher, parenthesesBlockMatcher)
			if block.Code != parenthesesBlockToken {
				if options.Strict {
					parseErrors = append(parseErrors, CallParseError{
						Name:    name,
						Offset:  nameOffset,
						Message: "invalid call syntax, expected (...)",
					})
				}
				continue
			}
			blockText := block.Text(cursor)
			argsText := ""
			if len(blockText) >= 2 {
				argsText = blockText[1 : len(blockText)-1]
			}
			calls = append(calls, Call{
				Name:      name,
				Args:      splitArgs(argsText),
				Offset:    nameOffset,
				EndOffset: block.Offset + len(blockText),
				Dollar:    hasDollar,
			})
		case parsly.Invalid:
			if options.Strict {
				parseErrors = append(parseErrors, CallParseError{
					Offset:  cursor.Pos,
					Message: "invalid token while scanning calls",
				})
			}
			cursor.Pos++
		}
	}
	return calls, parseErrors
}
