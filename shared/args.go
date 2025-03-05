package shared

import "strings"

func EnsureArgs(query string, args *[]interface{}) {
	parameterCount := strings.Count(query, "?")
	for i := len(*args); i < parameterCount; i++ { //ensure parameters
		*args = append(*args, "")
	}
}
