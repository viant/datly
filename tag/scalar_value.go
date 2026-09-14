package tag

import (
	"fmt"
	"strings"
)

// encodeScalarValue quotes scalar tag values containing the option delimiter.
// Tagly preserves the quoted token while matching key/value pairs.
func encodeScalarValue(value string) string {
	if !strings.ContainsAny(value, ",=") && !strings.HasPrefix(value, `'`) {
		return value
	}
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, `'`, `\'`)
	return `'` + value + `'`
}

// decodeScalarValue removes the scalar quoting consumed around Tagly's token.
// Unquoted values pass through unchanged.
func decodeScalarValue(value string) (string, error) {
	if value == "" || value[0] != '\'' {
		return value, nil
	}
	if len(value) < 2 || value[len(value)-1] != '\'' {
		return "", fmt.Errorf("unterminated quoted tag value")
	}
	value = value[1 : len(value)-1]
	var result strings.Builder
	for index := 0; index < len(value); index++ {
		if value[index] != '\\' {
			result.WriteByte(value[index])
			continue
		}
		index++
		if index == len(value) {
			return "", fmt.Errorf("unterminated tag value escape")
		}
		result.WriteByte(value[index])
	}
	return result.String(), nil
}
