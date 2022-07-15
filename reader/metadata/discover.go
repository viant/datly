package metadata

import (
	"bytes"
	"github.com/viant/datly/view/keywords"
	"github.com/viant/parsly"
	"strings"
)

var where = []byte("where")
var afterWhere = [][]byte{
	[]byte("group"),
	[]byte("order"),
	[]byte("limit"),
	[]byte("having"),
}

func EnrichWithDiscover(template string, withParentheses bool) string {
	buffer := bytes.Buffer{}
	tempAsBytes := []byte(strings.TrimSpace(template))

	for {
		if tempAsBytes[0] != '(' || tempAsBytes[len(tempAsBytes)-1] != ')' {
			break
		}

		tempAsBytes = bytes.TrimSpace(tempAsBytes[1 : len(tempAsBytes)-1])
	}

	if withParentheses {
		buffer.WriteByte('(')
	}

	appendAutoDiscover(tempAsBytes, &buffer)

	if withParentheses {
		buffer.WriteByte(')')
	}

	return buffer.String()
}

func appendAutoDiscover(tempAsBytes []byte, buffer *bytes.Buffer) {
	if containsAnyCriteria(tempAsBytes) {
		buffer.Write(tempAsBytes)
		return
	}

	cursor := parsly.NewCursor("", tempAsBytes, 0)
	candidates := []*parsly.Token{parenthesesMatcher}

	matched := cursor.MatchAfterOptional(whitespaceMatcher, candidates...)
	candidates = []*parsly.Token{parenthesesMatcher, WhitespaceTerminator}

	var prevPos int
	var hasCriteria bool
	var breakOuter bool
	var wroteCriteria bool

outer:
	for !breakOuter {
		matched = cursor.MatchAfterOptional(whitespaceMatcher, candidates...)
		switch matched.Code {
		case parenthesesToken:
			matched = cursor.MatchAfterOptional(whitespaceMatcher, parenthesesMatcher)
			continue outer
		case whitespaceTerminateToken:
			text := []byte(matched.Text(cursor))
			if bytes.EqualFold(text, where) {
				hasCriteria = true
				continue
			}

			for _, clause := range afterWhere {
				if bytes.EqualFold(clause, text) {
					buffer.WriteByte(' ')
					if hasCriteria {
						buffer.WriteString(keywords.AndCriteria)
					} else {
						buffer.WriteString(keywords.WhereCriteria)
					}
					breakOuter = true
					wroteCriteria = true
					break
				}
			}

		case parsly.EOF, parsly.Invalid:
			breakOuter = true
		}

		buffer.Write(tempAsBytes[prevPos:cursor.Pos])
		prevPos = cursor.Pos
	}

	buffer.Write(tempAsBytes[cursor.Pos:])

	if !wroteCriteria {
		buffer.WriteByte(' ')
		if !hasCriteria {
			buffer.WriteString(keywords.WhereCriteria)
		} else {
			buffer.WriteString(keywords.AndCriteria)
		}
	}
}

func containsAnyCriteria(asBytes []byte) bool {
	return bytes.Contains(asBytes, []byte(keywords.WhereCriteria)) || bytes.Contains(asBytes, []byte(keywords.AndCriteria)) || bytes.Contains(asBytes, []byte(keywords.OrCriteria))
}
