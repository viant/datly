package tag

import (
	"fmt"
	"strings"
	"unicode"
)

const InvariantName = "invariant"

// ParseInvariant accepts one authored group name. Group-to-method naming is a
// semantic compilation decision; this boundary does not split lists silently.
func ParseInvariant(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", fmt.Errorf("invariant requires one nonempty group name")
	}
	for _, char := range value {
		if !unicode.IsLetter(char) && !unicode.IsDigit(char) && char != '_' && char != '-' {
			return "", fmt.Errorf("invariant %q requires one group name; multiple groups are not supported", value)
		}
	}
	return value, nil
}
