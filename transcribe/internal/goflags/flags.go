// Package goflags parses Go's quoted whole-argument flag syntax.
package goflags

import (
	"fmt"
	"strings"
)

func Split(value string) ([]string, error) {
	var result []string
	for {
		value = strings.TrimLeft(value, " \t\r\n")
		if value == "" {
			return result, nil
		}
		if quote := value[0]; quote == '\'' || quote == '"' {
			end := strings.IndexByte(value[1:], quote)
			if end < 0 {
				return nil, fmt.Errorf("unterminated GOFLAGS quote %c", quote)
			}
			result = append(result, value[1:end+1])
			value = value[end+2:]
		} else {
			end := strings.IndexAny(value, " \t\r\n")
			if end < 0 {
				return append(result, value), nil
			}
			result = append(result, value[:end])
			value = value[end:]
		}
	}
}
