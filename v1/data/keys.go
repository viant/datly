package data

import "strings"

func KeysOf(value string, removeUnderscores bool) []string {
	result := make([]string, 4, 5)
	result[0] = value
	result[1] = strings.Title(value)
	result[2] = strings.ToUpper(value)
	result[3] = strings.ToLower(value)
	if removeUnderscores {
		result = append(result, strings.ReplaceAll(value, "_", ""))
	}
	return result
}
