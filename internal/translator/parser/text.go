package parser

import (
	"bytes"
	"github.com/viant/parsly/matcher"
)

func SplitByWhitespace(fragment string) []string {
	var result []string
	buffer := new(bytes.Buffer)
	quoted := byte(0)
	for i := 0; i < len(fragment)-1; i++ {
		c := fragment[i]
		switch c {
		case '\'', '"', '`':
			if quoted == 0 {
				quoted = fragment[i]
			} else if quoted == fragment[i] {
				quoted = 0
			}
		}
		if quoted != 0 {
			continue
		}
		if matcher.IsWhiteSpace(c) {
			if buffer.Len() > 0 {
				result = append(result, buffer.String())
			}
			buffer.Reset()
			continue
		}
		buffer.WriteByte(fragment[i])
	}
	if buffer.Len() > 0 {
		result = append(result, buffer.String())
	}
	return result
}
