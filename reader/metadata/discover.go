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
		if len(tempAsBytes) == 0 || tempAsBytes[0] != '(' || tempAsBytes[len(tempAsBytes)-1] != ')' {
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

	var hasCriteria bool
	var criteriaKeyword string
	var hasPagination bool
	var prevPos int

outer:
	for criteriaKeyword == "" {
		prevPos = cursor.Pos
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

			if bytes.Equal(text, []byte(keywords.Pagination)) {
				hasPagination = true
			}

			for _, clause := range afterWhere {
				if bytes.EqualFold(clause, text) {
					if hasCriteria {
						criteriaKeyword = keywords.AndCriteria
					} else {
						criteriaKeyword = keywords.WhereCriteria
					}
					break
				}
			}

		case parsly.EOF:
			prevPos = cursor.Pos
			if !hasCriteria {
				criteriaKeyword = keywords.WhereCriteria
			} else {
				criteriaKeyword = keywords.AndCriteria
			}
		}
	}

	buffer.Write(tempAsBytes[:prevPos])
	buffer.WriteByte(' ')
	buffer.Write([]byte(criteriaKeyword))
	buffer.Write(tempAsBytes[prevPos:])
	if !hasPagination {
		buffer.WriteByte(' ')
		buffer.WriteString(keywords.Pagination)
	}
}

func containsAnyCriteria(asBytes []byte) bool {
	return bytes.Contains(asBytes, []byte(keywords.WhereCriteria)) || bytes.Contains(asBytes, []byte(keywords.AndCriteria)) || bytes.Contains(asBytes, []byte(keywords.OrCriteria))
}
