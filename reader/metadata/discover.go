package metadata

import (
	"bytes"
	"github.com/viant/datly/view/keywords"
	"strings"
)

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

	buffer.Write(tempAsBytes)
	if ContainsWhereClause(tempAsBytes) {
		buffer.WriteByte(' ')
		buffer.WriteString(keywords.AndCriteria)
	} else {
		buffer.WriteByte(' ')
		buffer.WriteString(keywords.WhereCriteria)
	}

	if withParentheses {
		buffer.WriteByte(')')
	}

	return buffer.String()
}
