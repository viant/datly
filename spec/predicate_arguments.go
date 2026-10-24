package spec

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// ParseHandlerArgument parses an authored scalar literal. Strings use JSON/Go
// double quotes or DQL single quotes. Bare names, expressions, maps and lists
// are not configuration literals. Integers retain the native int type on reload.
func ParseHandlerArgument(text string) (any, error) {
	text = strings.TrimSpace(text)
	if len(text) >= 2 && text[0] == '\'' && text[len(text)-1] == '\'' {
		return strings.ReplaceAll(text[1:len(text)-1], "''", "'"), nil
	}
	if len(text) >= 2 && text[0] == '"' {
		var value string
		err := json.Unmarshal([]byte(text), &value)
		return value, err
	}
	switch text {
	case "true":
		return true, nil
	case "false":
		return false, nil
	case "null":
		return nil, nil
	}
	if value, err := strconv.Atoi(text); err == nil {
		return value, nil
	}
	return nil, fmt.Errorf("invalid handler argument %q: expected quoted string, bool, int or null", text)
}

// ValidateHandlerArgument limits persisted configuration to immutable scalar
// values. The public variadic SDK interface itself remains ...any.
func ValidateHandlerArgument(value any) error {
	switch value.(type) {
	case nil, string, bool, int:
		return nil
	}
	return fmt.Errorf("unsupported handler argument %T: expected string, bool, int or nil", value)
}

func RenderHandlerArgument(value any) (string, error) {
	if err := ValidateHandlerArgument(value); err != nil {
		return "", err
	}
	data, err := json.Marshal(value)
	return string(data), err
}
